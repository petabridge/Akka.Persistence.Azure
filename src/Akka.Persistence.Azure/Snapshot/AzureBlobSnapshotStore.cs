// -----------------------------------------------------------------------
// <copyright file="AzureBlobSnapshotStore.cs" company="Petabridge, LLC">
//      Copyright (C) 2015 - 2018 Petabridge, LLC <https://petabridge.com>
// </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Akka.Configuration;
using Akka.Event;
using Akka.Persistence.Azure.Util;
using Akka.Persistence.Snapshot;
using Azure;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Microsoft.Azure.Cosmos.Table;

namespace Akka.Persistence.Azure.Snapshot
{
    /// <summary>
    ///     Azure Blob Storage-backed snapshot store for Akka.Persistence.
    /// </summary>
    public class AzureBlobSnapshotStore : SnapshotStore
    {
        private const string TimeStampMetaDataKey = "Timestamp";
        private const string SeqNoMetaDataKey = "SeqNo";

        private readonly Lazy<BlobContainerClient> _container;
        private readonly ILoggingAdapter _log = Context.GetLogger();
        private readonly SerializationHelper _serialization;
        private readonly AzureBlobSnapshotStoreSettings _settings;
        private readonly CloudStorageAccount _storageAccount;
        private readonly BlobServiceClient _blobServiceClient;

        public AzureBlobSnapshotStore(Config config = null)
        {
            _serialization = new SerializationHelper(Context.System);
            _settings = config is null
                ? AzurePersistence.Get(Context.System).BlobSettings
                : AzureBlobSnapshotStoreSettings.Create(config);

            _blobServiceClient = new BlobServiceClient(_settings.ConnectionString);

            _storageAccount = _settings.Development ? 
                CloudStorageAccount.DevelopmentStorageAccount : 
                CloudStorageAccount.Parse(_settings.ConnectionString);
            _container = new Lazy<BlobContainerClient>(() => InitCloudStorage().Result);
        }

        public BlobContainerClient Container => _container.Value;

        private async Task<BlobContainerClient> InitCloudStorage()
        {
            var containerRef = _blobServiceClient.GetBlobContainerClient(_settings.ContainerName);
            var op = new OperationContext();

            using (var cts = new CancellationTokenSource(_settings.ConnectTimeout))
            {
                var create = await containerRef.CreateIfNotExistsAsync(PublicAccessType.BlobContainer, cancellationToken: cts.Token);
                //first try
                if (create?.Value != null)
                    _log.Info("Created Azure Blob Container", _settings.ContainerName);
                else
                    _log.Info("Successfully connected to existing container", _settings.ContainerName);

                return containerRef;
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
            //var requestOptions = GenerateOptions();
            Pageable<BlobItem> results = null;
            using (var cts = new CancellationTokenSource(_settings.RequestTimeout))
            {
                results = Container.GetBlobs(BlobTraits.None, BlobStates.None, SeqNoHelper.ToSnapshotSearchQuery(persistenceId));
            }

            // if we made it down here, the initial request succeeded.

            async Task<SelectedSnapshot> FilterAndFetch(Pageable<BlobItem> segment)
            {
                // apply filter criteria
                var filtered = segment
                    .Where(x => FilterBlobSeqNo(criteria, x))
                    .Where(x => FilterBlobTimestamp(criteria, x))
                    .OrderByDescending(x => FetchBlobSeqNo(x)) // ordering matters - get highest seqNo item
                    .ThenByDescending(x =>
                        FetchBlobTimestamp(
                            x)) // if there are multiple snapshots taken at same SeqNo, need latest timestamp
                    .FirstOrDefault();

                // couldn't find what we were looking for. Onto the next part of the query
                // or return null to sender possibly.
                if (filtered == null)
                    return null;
                var client = Container.GetBlobClient(filtered.Name);
                using (var cts = new CancellationTokenSource(_settings.RequestTimeout))
                using (var memoryStream = new MemoryStream())
                {
                    await client.DownloadToAsync(memoryStream, cts.Token);

                    var snapshot = _serialization.SnapshotFromBytes(memoryStream.ToArray());

                    var returnValue =
                        new SelectedSnapshot(
                            new SnapshotMetadata(
                                persistenceId,
                                FetchBlobSeqNo(filtered),
                                new DateTime(FetchBlobTimestamp(filtered))),
                            snapshot.Data);

                    return returnValue;
                }
            }

            // TODO: see if there's ever a scenario where the most recent snapshots aren't in the beginning of the pagination list.
            var result = await FilterAndFetch(results);
            return result;
        }

        protected override async Task SaveAsync(SnapshotMetadata metadata, object snapshot)
        {
            var blob = Container.GetBlobClient(metadata.ToSnapshotBlobId());
            var snapshotData = _serialization.SnapshotToBytes(new Serialization.Snapshot(snapshot));

            using (var cts = new CancellationTokenSource(_settings.RequestTimeout))
            {
                var meta = new Dictionary<string, string> 
                { 
                    {
                        TimeStampMetaDataKey, metadata.Timestamp.Ticks.ToString()
                    },
                    {
                        /*
                         * N.B. No need to convert the key into the Journal format we use here.
                         * The blobs themselves don't have their sort order affected by
                         * the presence of this metadata, so we should just save the SeqNo
                         * in a format that can be easily deserialized later.
                         */
                        SeqNoMetaDataKey, metadata.SequenceNr.ToString()
                    }
                };
                
                //Should we set metadata here?
                await blob.SetMetadataAsync(meta, cancellationToken: cts.Token);

                //or here
                await blob.UploadAsync(new MemoryStream(snapshotData)/*, metadata: meta*/);

                /*await blob.UploadFromByteArrayAsync(
                    snapshotData, 
                    0, 
                    snapshotData.Length,
                    AccessCondition.GenerateEmptyCondition(),
                    GenerateOptions(), 
                    new OperationContext(),
                    cts.Token);*/
            }
        }

        protected override async Task DeleteAsync(SnapshotMetadata metadata)
        {
            var blob = Container.GetBlobClient(metadata.ToSnapshotBlobId());
            using (var cts = new CancellationTokenSource(_settings.RequestTimeout))
            {
                await blob.DeleteIfExistsAsync(DeleteSnapshotsOption.None, cancellationToken:  cts.Token);
            }
        }

        protected override async Task DeleteAsync(string persistenceId, SnapshotSelectionCriteria criteria)
        {
            //var requestOptions = GenerateOptions();
            List<BlobItem> results = new List<BlobItem>();
            CancellationToken cancel;
            using (var cts = new CancellationTokenSource(_settings.RequestTimeout))
            {
                /*
                 * Query only the metadata - don't need to stream the entire blob back to us
                 * in order to delete it from storage in the next request.
                 */
                var items = Container.GetBlobsAsync(prefix: SeqNoHelper.ToSnapshotSearchQuery(persistenceId), cancellationToken: cts.Token);
                var pages = items.AsPages(cts.Token.ToString());
                
                results = await GetBlobs(pages, cancellationToken: cts.Token);

                async ValueTask<List<BlobItem>> GetBlobs(IAsyncEnumerable<Page<BlobItem>> source, CancellationToken cancellationToken)
                {                    
                    var list = new List<BlobItem>();

                    await foreach (var item in source.WithCancellation(cancellationToken).ConfigureAwait(false))
                    {
                        list.AddRange(item.Values.ToList());
                    }

                    return list;
                }
                cancel = cts.Token;
            }

            // if we made it down here, the initial request succeeded.

            async Task FilterAndDelete(List<BlobItem> segment)
            {
                // apply filter criteria
                var filtered = segment
                    .Where(x => FilterBlobSeqNo(criteria, x))
                    .Where(x => FilterBlobTimestamp(criteria, x));

                var deleteTasks = new List<Task>();
                using (var cts = new CancellationTokenSource(_settings.RequestTimeout))
                {
                    foreach (var blob in filtered)
                    {
                        var client = Container.GetBlobClient(blob.Name);
                        deleteTasks.Add(client.DeleteIfExistsAsync(DeleteSnapshotsOption.None, cancellationToken: cts.Token));
                    }

                    await Task.WhenAll(deleteTasks);
                }
            }
            //Todo: fix
            var continuationToken = cancel;
            var deleteTask = FilterAndDelete(results);
            await deleteTask;

            /*while (continuationToken != null)
            {
                // get the next round of results in parallel with the deletion of the previous
                var nextResults = await Container.GetBlobsAsync(cancellationToken: continuationToken);

                // finish our previous delete tasks
                await deleteTask;

                // start next round of deletes
                deleteTask = FilterAndDelete(nextResults);

                // move the loop forward if there are more results to be processed still
                continuationToken = nextResults.ContinuationToken;
            }*/

            // wait for the final delete operation to complete
            await deleteTask;
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

        /*private BlobRequestOptions GenerateOptions()
        {
            return GenerateOptions(_settings);
        }

        private static BlobRequestOptions GenerateOptions(AzureBlobSnapshotStoreSettings settings)
        {
            var option = new BlobClientOptions();
            return new BlobClientOptions { MaximumExecutionTime = settings.RequestTimeout};
        }*/
    }
}