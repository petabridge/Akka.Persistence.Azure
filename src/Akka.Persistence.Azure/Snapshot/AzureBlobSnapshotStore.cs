// -----------------------------------------------------------------------
// <copyright file="AzureBlobSnapshotStore.cs" company="Petabridge, LLC">
//      Copyright (C) 2015 - 2023 Petabridge, LLC <https://petabridge.com>
// </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Akka.Configuration;
using Akka.Event;
using Akka.Persistence.Azure.Util;
using Akka.Persistence.Snapshot;
using Akka.Util;
using Akka.Util.Internal;
using Azure;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Blobs.Specialized;

#nullable enable
namespace Akka.Persistence.Azure.Snapshot
{
    /// <summary>
    ///     Azure Blob Storage-backed snapshot store for Akka.Persistence.
    /// </summary>
    public class AzureBlobSnapshotStore : SnapshotStore
    {
        private static readonly Dictionary<int, TimeSpan> RetryInterval =
            new Dictionary<int, TimeSpan>()
            {
                { 5, TimeSpan.FromMilliseconds(100) },
                { 4, TimeSpan.FromMilliseconds(500) },
                { 3, TimeSpan.FromMilliseconds(1000) },
                { 2, TimeSpan.FromMilliseconds(2000) },
                { 1, TimeSpan.FromMilliseconds(4000) },
                { 0, TimeSpan.FromMilliseconds(8000) },
            };

        private const string TimeStampMetaDataKey = "Timestamp";
        private const string SeqNoMetaDataKey = "SeqNo";

        private readonly ILoggingAdapter _log = Context.GetLogger();
        private readonly SerializationHelper _serialization;
        private readonly AzureBlobSnapshotStoreSettings _settings;
        private readonly BlobServiceClient _serviceClient;

        private readonly CancellationTokenSource _shutdownCts;

        public AzureBlobSnapshotStore(Config? config = null)
        {
            _serialization = new SerializationHelper(Context.System);
            _settings = config is null
                ? AzurePersistence.Get(Context.System).BlobSettings
                : AzureBlobSnapshotStoreSettings.Create(config);

            var setup = Context.System.Settings.Setup.Get<AzureBlobSnapshotSetup>();
            if (setup.HasValue)
                _settings = setup.Value.Apply(_settings);
            
            var multiSetup = Context.System.Settings.Setup.Get<AzureBlobMultiSnapshotSetup>();
            if (multiSetup.HasValue)
            {
                var snapshotId = Self.Path.Name.SplitDottedPathHonouringQuotes().Last();
                setup = Option<AzureBlobSnapshotSetup>.Create(multiSetup.Value!.Get(snapshotId)!);
                if(setup.HasValue)
                    _settings = setup.Value.Apply(_settings);
            }
            
            if (_settings.BlobServiceClientFactory != null)
            {
                _serviceClient = _settings.BlobServiceClientFactory.Invoke();
            }
            else
            {
                _serviceClient = _settings.ServiceUri != null && _settings.AzureCredential != null
                    ? _serviceClient = new BlobServiceClient(
                        serviceUri: _settings.ServiceUri, 
                        credential: _settings.AzureCredential,
                        options: _settings.BlobClientOptions)
                    : !string.IsNullOrWhiteSpace(_settings.ConnectionString) 
                        ? _serviceClient = new BlobServiceClient(connectionString: _settings.ConnectionString)
                        : throw new ConfigurationException(
                            "No connection method configured. ConnectionString, AzureCredential, or BlobServiceClient " +
                            "must be specified.");
            }

            _shutdownCts = new CancellationTokenSource();
        }

        public BlobContainerClient Container => _serviceClient.GetBlobContainerClient(_settings.ContainerName);

        private async Task<BlobContainerClient> InitCloudStorage(int remainingTries, CancellationToken cancellationToken)
        {
            try
            {
                var blobClient = _serviceClient.GetBlobContainerClient(_settings.ContainerName);

                using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
                cts.CancelAfter(_settings.ConnectTimeout);
                using (cts)
                {
                    if (!_settings.AutoInitialize)
                    {
                        var exists = await blobClient.ExistsAsync(cts.Token);

                        if (!exists)
                        {
                            remainingTries = 0;

                            throw new Exception(
                                $"Container {_settings.ContainerName} doesn't exist. Either create it or turn auto-initialize on");
                        }
                        
                        _log.Info("Successfully connected to existing container {0}", _settings.ContainerName);
                        
                        return blobClient;
                    }
                
                    if (await blobClient.ExistsAsync(cts.Token))
                    {
                        _log.Info("Successfully connected to existing container {0}", _settings.ContainerName);
                    }
                    else
                    {
                        try
                        {
                            await blobClient.CreateAsync(_settings.ContainerPublicAccessType,
                                cancellationToken: cts.Token);
                            _log.Info("Created Azure Blob Container {0}", _settings.ContainerName);
                        }
                        catch (Exception e)
                        {
                            throw new Exception($"Failed to create Azure Blob Container {_settings.ContainerName}", e);
                        }
                    }

                    return blobClient;
                }
            }
            catch (Exception ex)
            {
                _log.Error(ex, "[{0}] more tries to initialize table storage remaining...", remainingTries);
                if (remainingTries == 0)
                    throw;
                await Task.Delay(RetryInterval[remainingTries], cancellationToken);
                if (cancellationToken.IsCancellationRequested)
                    throw;
                
                return await InitCloudStorage(remainingTries - 1, cancellationToken);
            }
        }

        protected override void PreStart()
        {
            _log.Debug("Initializing Azure Container Storage...");

            InitCloudStorage(5, _shutdownCts.Token).GetAwaiter().GetResult();

            _log.Debug("Successfully started Azure Container Storage!");

            // need to call the base in order to ensure Akka.Persistence starts up correctly
            base.PreStart();
        }

        protected override void PostStop()
        {
            _shutdownCts.Cancel();
            _shutdownCts.Dispose();
            base.PostStop();
        }

        protected override async Task<SelectedSnapshot?> LoadAsync(
            string persistenceId,
            SnapshotSelectionCriteria criteria,
            CancellationToken cancellationToken)
        {
            using var cts = CancellationTokenSource.CreateLinkedTokenSource(_shutdownCts.Token, cancellationToken);
            cts.CancelAfter(_settings.RequestTimeout);
            using(cts)
            {
                var prefix = SeqNoHelper.ToSnapshotSearchQuery(persistenceId, _settings.Folders);
                var results = Container.GetBlobsAsync(
                    prefix: prefix,
                    traits: BlobTraits.Metadata,
                    cancellationToken: cts.Token);

                // Every page must be enumerated, not just the first one:
                // - List Blobs may return partial or even empty pages together with a continuation
                //   token (e.g. when many blob versions or soft-deleted blobs sit under the prefix).
                // - Blob names sort ascending by sequence number, so once a persistence id has more
                //   snapshots than fit in one page, the newest snapshot is on the LAST page.
                BlobItem? filtered = null;
                long filteredSeqNo = -1;
                long filteredTimestamp = -1;
                await foreach (var blob in results.WithCancellation(cts.Token))
                {
                    if (!IsSnapshotOf(blob.Name, prefix)
                        || !FilterBlobSeqNo(criteria, blob)
                        || !FilterBlobTimestamp(criteria, blob))
                        continue;

                    // highest seqNo wins; if there are multiple snapshots taken at same SeqNo, latest timestamp wins
                    var seqNo = FetchBlobSeqNo(blob);
                    var timestamp = FetchBlobTimestamp(blob);
                    if (seqNo > filteredSeqNo || (seqNo == filteredSeqNo && timestamp > filteredTimestamp))
                    {
                        filtered = blob;
                        filteredSeqNo = seqNo;
                        filteredTimestamp = timestamp;
                    }
                }

                // couldn't find what we were looking for, return null to sender
                if (filtered == null)
                    return null;

                using var memoryStream = new MemoryStream();
                var blobClient = Container.GetBlockBlobClient(filtered.Name);
                var downloadInfo = await blobClient.DownloadAsync(cts.Token);
                await downloadInfo.Value.Content.CopyToAsync(memoryStream);

                var snapshot = _serialization.SnapshotFromBytes(memoryStream.ToArray());

                var result =
                    new SelectedSnapshot(
                        new SnapshotMetadata(
                            persistenceId,
                            FetchBlobSeqNo(filtered),
                            new DateTime(FetchBlobTimestamp(filtered))),
                        snapshot.Data);

                return result;
            }
        }

        protected override async Task SaveAsync(SnapshotMetadata metadata, object snapshot, CancellationToken cancellationToken)
        {
            var blobClient = Container.GetBlockBlobClient(metadata.ToSnapshotBlobId(_settings.Folders));
            var snapshotData = _serialization.SnapshotToBytes(new Serialization.Snapshot(snapshot));

            using var cts = CancellationTokenSource.CreateLinkedTokenSource(_shutdownCts.Token, cancellationToken);
            cts.CancelAfter(_settings.RequestTimeout);
            using (cts)
            {
                var blobMetadata = new Dictionary<string, string>
                {
                    [TimeStampMetaDataKey] = metadata.Timestamp.Ticks.ToString(),
                    /*
                     * N.B. No need to convert the key into the Journal format we use here.
                     * The blobs themselves don't have their sort order affected by
                     * the presence of this metadata, so we should just save the SeqNo
                     * in a format that can be easily deserialized later.
                     */
                    [SeqNoMetaDataKey] = metadata.SequenceNr.ToString()
                };

                using var stream = new MemoryStream(snapshotData);
                await blobClient.UploadAsync(
                    stream, 
                    metadata: blobMetadata,
                    cancellationToken: cts.Token);
            }
        }

        protected override async Task DeleteAsync(SnapshotMetadata metadata, CancellationToken cancellationToken)
        {
            var blobClient = Container.GetBlobClient(metadata.ToSnapshotBlobId(_settings.Folders));

            using var cts = CancellationTokenSource.CreateLinkedTokenSource(_shutdownCts.Token, cancellationToken);
            cts.CancelAfter(_settings.RequestTimeout);
            using (cts)
            {
                if (metadata.Timestamp == DateTime.MinValue)
                {
                    // Short-circuit the timestamp query if the metadata does not require us to check for timestamp
                    await blobClient.DeleteIfExistsAsync(cancellationToken: cts.Token);
                }
                else
                {
                    var response = await blobClient.GetPropertiesAsync(cancellationToken: cts.Token);
                    if (response.HasValue)
                    {
                        var timestamp = new DateTime(long.Parse(response.Value.Metadata[TimeStampMetaDataKey])); 
                        if(timestamp <= metadata.Timestamp)
                            await blobClient.DeleteAsync(cancellationToken: cts.Token);
                    }
                }
            }
        }

        protected override async Task DeleteAsync(string persistenceId, SnapshotSelectionCriteria criteria, CancellationToken cancellationToken)
        {
            using var cts = CancellationTokenSource.CreateLinkedTokenSource(_shutdownCts.Token, cancellationToken);
            cts.CancelAfter(_settings.RequestTimeout);
            using (cts)
            {
                var prefix = SeqNoHelper.ToSnapshotSearchQuery(persistenceId, _settings.Folders);
                var items = Container.GetBlobsAsync(
                    prefix: prefix,
                    traits: BlobTraits.Metadata,
                    cancellationToken: cts.Token);

                var filtered = items
                    .Where(x => IsSnapshotOf(x.Name, prefix))
                    .Where(x => FilterBlobSeqNo(criteria, x))
                    .Where(x => FilterBlobTimestamp(criteria, x));

                var deleteTasks = new List<Task>();
                await foreach (var blob in filtered.WithCancellation(cts.Token))
                {
                    var blobClient = Container.GetBlobClient(blob.Name);
                    deleteTasks.Add(blobClient.DeleteIfExistsAsync(cancellationToken: cts.Token));
                }

                await Task.WhenAll(deleteTasks);
            }
        }

        /// <summary>
        /// Snapshot blob ids are exactly "{prefix}-{seqNr:d19}" (see <see cref="SeqNoHelper.ToSnapshotBlobId"/>).
        /// A plain prefix match also returns other persistence ids that share the prefix
        /// (listing "snapshot-orders" also returns "snapshot-orders-2-..."), so the name must be verified.
        /// </summary>
        private static bool IsSnapshotOf(string blobName, string prefix)
        {
            if (blobName.Length != prefix.Length + 20 || blobName[prefix.Length] != '-')
                return false;

            for (var i = prefix.Length + 1; i < blobName.Length; i++)
            {
                if (blobName[i] < '0' || blobName[i] > '9')
                    return false;
            }

            return true;
        }

        private static bool FilterBlobSeqNo(SnapshotSelectionCriteria criteria, BlobItem x)
        {
            var seqNo = FetchBlobSeqNo(x);
            return seqNo <= criteria.MaxSequenceNr && seqNo >= criteria.MinSequenceNr;
        }

        private static long FetchBlobSeqNo(BlobItem x)
        {
            return long.Parse(x.Metadata[SeqNoMetaDataKey]);
        }

        private static bool FilterBlobTimestamp(SnapshotSelectionCriteria criteria, BlobItem x)
        {
            var ticks = FetchBlobTimestamp(x);
            return ticks <= criteria.MaxTimeStamp.Ticks &&
                   (!criteria.MinTimestamp.HasValue || ticks >= criteria.MinTimestamp.Value.Ticks);
        }

        private static long FetchBlobTimestamp(BlobItem x)
        {
            return long.Parse(x.Metadata[TimeStampMetaDataKey]);
        }
    }
}