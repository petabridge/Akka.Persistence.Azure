// -----------------------------------------------------------------------
// <copyright file="AzureBlobSnapshotStore.cs" company="Petabridge, LLC">
//      Copyright (C) 2015 - 2018 Petabridge, LLC <https://petabridge.com>
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
using Azure;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Blobs.Specialized;

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

        private readonly Lazy<BlobContainerClient> _containerClient;
        private readonly ILoggingAdapter _log = Context.GetLogger();
        private readonly SerializationHelper _serialization;
        private readonly AzureBlobSnapshotStoreSettings _settings;
        private readonly BlobServiceClient _serviceClient;

        public AzureBlobSnapshotStore(Config config = null)
        {
            _serialization = new SerializationHelper(Context.System);
            _settings = config is null
                ? AzurePersistence.Get(Context.System).BlobSettings
                : AzureBlobSnapshotStoreSettings.Create(config);

            var setup = Context.System.Settings.Setup.Get<AzureBlobSnapshotSetup>();
            if (setup.HasValue)
                _settings = setup.Value.Apply(_settings);
            
            if (_settings.Development)
            {
                _serviceClient = new BlobServiceClient(connectionString: "UseDevelopmentStorage=true");
            }
            else
            {
                _serviceClient = _settings.ServiceUri != null && _settings.DefaultAzureCredential != null
                    ? _serviceClient = new BlobServiceClient(
                        serviceUri: _settings.ServiceUri, 
                        credential: _settings.DefaultAzureCredential,
                        options: _settings.BlobClientOptions)
                    : _serviceClient = new BlobServiceClient(connectionString: _settings.ConnectionString);
            }

            _containerClient = new Lazy<BlobContainerClient>(() => InitCloudStorage(5).Result);
        }

        public BlobContainerClient Container => _containerClient.Value;

        private async Task<BlobContainerClient> InitCloudStorage(int remainingTries)
        {
            try
            {
                var blobClient = _serviceClient.GetBlobContainerClient(_settings.ContainerName);

                using var cts = new CancellationTokenSource(_settings.ConnectTimeout);
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
            catch (Exception ex)
            {
                _log.Error(ex, "[{0}] more tries to initialize table storage remaining...", remainingTries);
                if (remainingTries == 0)
                    throw;
                await Task.Delay(RetryInterval[remainingTries]);
                return await InitCloudStorage(remainingTries - 1);
            }
        }

        protected override void PreStart()
        {
            _log.Debug("Initializing Azure Container Storage...");

            // forces loading of the value
            var name = Container.Name;

            _log.Debug("Successfully started Azure Container Storage!");

            // need to call the base in order to ensure Akka.Persistence starts up correctly
            base.PreStart();
        }


        protected override async Task<SelectedSnapshot> LoadAsync(string persistenceId,
            SnapshotSelectionCriteria criteria)
        {
            using var cts = new CancellationTokenSource(_settings.RequestTimeout);
            {
                var results = Container.GetBlobsAsync(
                    prefix: SeqNoHelper.ToSnapshotSearchQuery(persistenceId), 
                    traits: BlobTraits.Metadata,
                    cancellationToken: cts.Token);

                var pageEnumerator = results.AsPages().GetAsyncEnumerator(cts.Token);

                if (!await pageEnumerator.MoveNextAsync())
                    return null;

                // TODO: see if there's ever a scenario where the most recent snapshots aren't in the first page of the pagination list.
                // apply filter criteria
                var filtered = pageEnumerator.Current.Values
                    .Where(x => FilterBlobSeqNo(criteria, x))
                    .Where(x => FilterBlobTimestamp(criteria, x))
                    .OrderByDescending(FetchBlobSeqNo) // ordering matters - get highest seqNo item
                    .ThenByDescending(FetchBlobTimestamp) // if there are multiple snapshots taken at same SeqNo, need latest timestamp
                    .FirstOrDefault();

                // couldn't find what we were looking for. Onto the next part of the query
                // or return null to sender possibly.
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

        protected override async Task SaveAsync(SnapshotMetadata metadata, object snapshot)
        {
            var blobClient = Container.GetBlockBlobClient(metadata.ToSnapshotBlobId());
            var snapshotData = _serialization.SnapshotToBytes(new Serialization.Snapshot(snapshot));

            using var cts = new CancellationTokenSource(_settings.RequestTimeout);
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

        protected override async Task DeleteAsync(SnapshotMetadata metadata)
        {
            var blobClient = Container.GetBlobClient(metadata.ToSnapshotBlobId());

            using var cts = new CancellationTokenSource(_settings.RequestTimeout);
            await blobClient.DeleteIfExistsAsync(cancellationToken: cts.Token);
        }

        protected override async Task DeleteAsync(string persistenceId, SnapshotSelectionCriteria criteria)
        {
            using var cts = new CancellationTokenSource(_settings.RequestTimeout);
            var items = Container.GetBlobsAsync(
                prefix: SeqNoHelper.ToSnapshotSearchQuery(persistenceId), 
                traits: BlobTraits.Metadata,
                cancellationToken: cts.Token);

            var filtered = items
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