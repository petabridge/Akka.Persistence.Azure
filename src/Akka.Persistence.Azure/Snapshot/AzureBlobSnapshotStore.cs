using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Akka.Event;
using Akka.Persistence.Azure.Util;
using Akka.Persistence.Snapshot;
using Microsoft.IO;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage.Table;

namespace Akka.Persistence.Azure.Snapshot
{
    public class AzureBlobSnapshotStore : SnapshotStore
    {
        private readonly SerializationHelper _serialization;
        private readonly AzureBlobSnapshotStoreSettings _settings;
        private readonly CloudStorageAccount _storageAccount;
        private readonly ILoggingAdapter _log = Context.GetLogger();

        private readonly Lazy<CloudBlobContainer> _container;

        private const string TimeStampMetaDataKey = "Timestamp";
        private const string SeqNoMetaDataKey = "SeqNo";

        public CloudBlobContainer Container => _container.Value;

        public AzureBlobSnapshotStore(AzureBlobSnapshotStoreSettings settings)
        {
            _settings = settings;
            _serialization = new SerializationHelper(Context.System);
            _storageAccount = CloudStorageAccount.Parse(settings.ConnectionString);

            _container = new Lazy<CloudBlobContainer>(() => InitCloudStorage().Result);
        }

        private async Task<CloudBlobContainer> InitCloudStorage()
        {
            var blobClient = _storageAccount.CreateCloudBlobClient();
            var containerRef = blobClient.GetContainerReference(_settings.ContainerName);
            var op = new OperationContext();

            using (var cts = new CancellationTokenSource(_settings.ConnectTimeout))
            {
                if (await containerRef.CreateIfNotExistsAsync(BlobContainerPublicAccessType.Container, new BlobRequestOptions(), op, cts.Token))
                {
                    _log.Info("Created Azure Blob Container", _settings.ContainerName);
                }
                else
                {
                    _log.Info("Successfully connected to existing container", _settings.ContainerName);
                }

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


        protected override async Task<SelectedSnapshot> LoadAsync(string persistenceId, SnapshotSelectionCriteria criteria)
        {
            var requestOptions = GenerateOptions();
            BlobResultSegment results = null;
            using (var cts = new CancellationTokenSource(_settings.RequestTimeout))
            {
                results = await Container.ListBlobsSegmentedAsync(SeqNoHelper.ToSnapshotSearchQuery(persistenceId), true,
                    BlobListingDetails.Metadata, null, null, requestOptions, new OperationContext(), cts.Token);
            }

            // if we made it down here, the initial request succeeded.

            async Task<SelectedSnapshot[]> FilterAndFetch(BlobResultSegment segment)
            {
                // apply filter criteria
                var filtered = segment.Results.Where(x => x is CloudBlockBlob)
                    .Cast<CloudBlockBlob>()
                    .Where(x => FilterBlobSeqNo(criteria, x))
                    .Where(x => FilterBlobTimestamp(criteria, x));

                var deleteTasks = new List<Task>();
                using (var cts = new CancellationTokenSource(_settings.RequestTimeout))
                {
                    foreach (var blob in filtered)
                    {
                        deleteTasks.Add(blob.DeleteIfExistsAsync(DeleteSnapshotsOption.None, AccessCondition.GenerateIfExistsCondition(),
                            GenerateOptions(), new OperationContext(), cts.Token));
                    }

                    await Task.WhenAll(deleteTasks);
                }
            }
        }

        protected override async Task SaveAsync(SnapshotMetadata metadata, object snapshot)
        {
            var blob = Container.GetBlockBlobReference(metadata.ToSnapshotBlobId());
            var snapshotData = _serialization.SnapshotToBytes(new Serialization.Snapshot(snapshot));

            using (var cts = new CancellationTokenSource(_settings.RequestTimeout))
            {
                blob.Metadata.Add(TimeStampMetaDataKey, metadata.Timestamp.Ticks.ToString());

                /*
                 * N.B. No need to convert the key into the Journal format we use here.
                 * The blobs themselves don't have their sort order affected by
                 * the presence of this metadata, so we should just save the SeqNo
                 * in a format that can be easily deserialized later.
                 */
                blob.Metadata.Add(SeqNoMetaDataKey, metadata.SequenceNr.ToString());

                await blob.UploadFromByteArrayAsync(snapshotData, 0, snapshotData.Length,
                    AccessCondition.GenerateIfNotExistsCondition(),
                    GenerateOptions(), new OperationContext(),
                    cts.Token);
            }
        }

        protected override async Task DeleteAsync(SnapshotMetadata metadata)
        {
            var blob = Container.GetBlockBlobReference(metadata.ToSnapshotBlobId());
            using (var cts = new CancellationTokenSource(_settings.RequestTimeout))
            {

                await blob.DeleteIfExistsAsync(DeleteSnapshotsOption.None, AccessCondition.GenerateIfExistsCondition(),
                    GenerateOptions(), new OperationContext(),
                    cts.Token);
            }
        }

        protected override async Task DeleteAsync(string persistenceId, SnapshotSelectionCriteria criteria)
        {
            var requestOptions = GenerateOptions();
            BlobResultSegment results = null;
            using (var cts = new CancellationTokenSource(_settings.RequestTimeout))
            {
                /*
                 * Query only the metadata - don't need to stream the entire blob back to us
                 * in order to delete it from storage in the next request.
                 */
                results = await Container.ListBlobsSegmentedAsync(SeqNoHelper.ToSnapshotSearchQuery(persistenceId), true,
                    BlobListingDetails.Metadata, null, null, requestOptions, new OperationContext(), cts.Token);
            }

            // if we made it down here, the initial request succeeded.

            async Task FilterAndDelete(BlobResultSegment segment)
            {
                // apply filter criteria
                var filtered = segment.Results.Where(x => x is CloudBlockBlob)
                    .Cast<CloudBlockBlob>()
                    .Where(x => FilterBlobSeqNo(criteria, x))
                    .Where(x => FilterBlobTimestamp(criteria, x));

                var deleteTasks = new List<Task>();
                using (var cts = new CancellationTokenSource(_settings.RequestTimeout))
                {
                    foreach (var blob in filtered)
                    {
                        deleteTasks.Add(blob.DeleteIfExistsAsync(DeleteSnapshotsOption.None, AccessCondition.GenerateIfExistsCondition(),
                            GenerateOptions(), new OperationContext(), cts.Token));
                    }

                    await Task.WhenAll(deleteTasks);
                }
            }

            var continuationToken = results.ContinuationToken;
            var deleteTask = FilterAndDelete(results);

            while (continuationToken != null)
            {
                // get the next round of results in parallel with the deletion of the previous
                var nextResults = await Container.ListBlobsSegmentedAsync(continuationToken);

                // finish our previous delete tasks
                await deleteTask;

                // start next round of deletes
                deleteTask = FilterAndDelete(nextResults);

                // move the loop forward if there are more results to be processed still
                continuationToken = nextResults.ContinuationToken;
            }

            // wait for the final delete operation to complete
            await deleteTask;
        }

        private static bool FilterBlobSeqNo(SnapshotSelectionCriteria criteria, CloudBlob x)
        {
            var seqNo = long.Parse(x.Metadata[SeqNoMetaDataKey]);
            return seqNo <= criteria.MaxSequenceNr && seqNo >= criteria.MinSequenceNr;
        }

        private static bool FilterBlobTimestamp(SnapshotSelectionCriteria criteria, CloudBlob x)
        {
            var ticks = long.Parse(x.Metadata[TimeStampMetaDataKey]);
            return ticks <= criteria.MaxTimeStamp.Ticks &&
                   (!criteria.MinTimestamp.HasValue || criteria.MinTimestamp.Value.Ticks >= ticks);
        }

        private BlobRequestOptions GenerateOptions()
        {
            return GenerateOptions(_settings);
        }

        private static BlobRequestOptions GenerateOptions(AzureBlobSnapshotStoreSettings settings)
        {
            return new BlobRequestOptions() { MaximumExecutionTime = settings.RequestTimeout };
        }
    }
}
