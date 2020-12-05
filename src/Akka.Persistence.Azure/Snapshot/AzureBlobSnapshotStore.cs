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
using Akka.Persistence.Azure.TableEntities;
using Akka.Persistence.Azure.Util;
using Akka.Persistence.Snapshot;
using Microsoft.Azure.Cosmos;

namespace Akka.Persistence.Azure.Snapshot
{
    /// <summary>
    ///     Azure Cosmos SQL backed snapshot store for Akka.Persistence.
    /// </summary>
    public class AzureBlobSnapshotStore : SnapshotStore
    {
        private readonly string CosmosDatabaseId = "akka";

        private Database _cosmosDatabase = null;

        private readonly Lazy<Container> _container;
        private readonly ILoggingAdapter _log = Context.GetLogger();
        private readonly SerializationHelper _serialization;
        private readonly AzureBlobSnapshotStoreSettings _settings;
        private readonly CosmosClient _cosmosClient;

        public AzureBlobSnapshotStore(Config config = null)
        {
            _serialization = new SerializationHelper(Context.System);
            _settings = config is null
                ? AzurePersistence.Get(Context.System).BlobSettings
                : AzureBlobSnapshotStoreSettings.Create(config);

            _cosmosClient = new CosmosClient(_settings.ConnectionString);

            _container = new Lazy<Container>(() => InitContainer().Result);
        }

        public Container Container => _container.Value;

        private async Task<Container> InitContainer()
        {
            _cosmosDatabase = await _cosmosClient.CreateDatabaseIfNotExistsAsync(CosmosDatabaseId);
            ContainerProperties containerProperties = new ContainerProperties(id: _settings.ContainerName, partitionKeyPath: "/PersistenceId");
            containerProperties.IndexingPolicy.CompositeIndexes.Add(new System.Collections.ObjectModel.Collection<CompositePath>
            {   //Throws if not defined ==> 
                //"Errors":["The order by query does not have a corresponding composite index that it can be served from."
                new CompositePath 
                { 
                 Path = "/SeqNo", 
                 Order = CompositePathSortOrder.Descending            
                },
                new CompositePath
                {
                 Path = "/Timestamp",
                 Order = CompositePathSortOrder.Descending
                }
            });
            using (var cts = new CancellationTokenSource(_settings.ConnectTimeout))
            {
                var containerRef = await _cosmosDatabase.CreateContainerIfNotExistsAsync(
                containerProperties: containerProperties,
                throughput: 400, cancellationToken: cts.Token);

                //think about a proper log messages
                if (containerRef.StatusCode == System.Net.HttpStatusCode.OK)
                    _log.Info("Created Cosmos Container / Successfully connected to existing container", _settings.ContainerName);
                else
                    _log.Info("Successfully connected to existing container", _settings.ContainerName);

                return containerRef;
            }
        }

        protected override void PreStart()
        {
            _log.Debug("Initializing Azure Container Storage...");

            // forces loading of the value
            var name = Container.Id;

            _log.Debug("Successfully started Azure Container Storage!");

            // need to call the base in order to ensure Akka.Persistence starts up correctly
            base.PreStart();
        }


        protected override async Task<SelectedSnapshot> LoadAsync(string persistenceId,
            SnapshotSelectionCriteria criteria)
        {
            //need to improve these blocks
            var min = criteria.MinTimestamp.HasValue ? "AND s.Timestamp <= @toStamp" : "";
            QueryDefinition query = new QueryDefinition($"SELECT * FROM SnapshotItem s WHERE s.PersistenceId = @id " +
                $"AND (s.SeqNo > @fromSeqNo AND s.SeqNo <= @toSeqNo) AND (s.Timestamp >= @fromStamp {min})" +
                $"ORDER BY s.SeqNo DESC, s.Timestamp DESC")
               .WithParameter("@id", persistenceId)
               .WithParameter("@fromSeqNo", criteria.MinSequenceNr)
               .WithParameter("@toSeqNo", criteria.MaxSequenceNr)
               .WithParameter("@fromStamp", criteria.MinTimestamp.Value.Ticks);

            if (criteria.MinTimestamp.HasValue)
                query.WithParameter("@toStamp", criteria.MaxTimeStamp.Ticks);

            SelectedSnapshot selectedSnapshot = null;

            using (FeedIterator<SnapshotItem> setIterator = Container.GetItemQueryIterator<SnapshotItem>(
                query,
                requestOptions: new QueryRequestOptions()
                {
                    PartitionKey = new PartitionKey(persistenceId),
                    MaxConcurrency = 1,
                    MaxItemCount = 1
                }))
            {

                while (setIterator.HasMoreResults)
                {
                    FeedResponse<SnapshotItem> response = await setIterator.ReadNextAsync();
                    var resultItem = response.FirstOrDefault();//we only need the latest
                    if(resultItem != null)
                    {
                        var snapshot = _serialization.SnapshotFromBytes(resultItem.Snapshot);

                        selectedSnapshot =
                            new SelectedSnapshot(
                                new SnapshotMetadata(
                                    persistenceId,
                                    resultItem.SeqNo,
                                    new DateTime(resultItem.Timestamp)),
                                snapshot.Data);

                        break; //we are only interested in the first snapshot
                    }
                }

            }
            return selectedSnapshot;
        }

        protected override async Task SaveAsync(SnapshotMetadata metadata, object snapshot)
        {
            var snapshotData = _serialization.SnapshotToBytes(new Serialization.Snapshot(snapshot));

            var item = new SnapshotItem(metadata.PersistenceId, metadata.SequenceNr, snapshotData, metadata.Timestamp.Ticks);

            using (var cts = new CancellationTokenSource(_settings.RequestTimeout))
            {
                var upsert = await Container.UpsertItemAsync(item, new PartitionKey(item.PartitionKey), cancellationToken: cts.Token);
                _log.Info($"Upserted with statuscode: {upsert.StatusCode} persisttenceId: {upsert.Resource.PersistenceId}");
            }
        }

        
        protected override async Task DeleteAsync(SnapshotMetadata metadata)
        {
            using (var cts = new CancellationTokenSource(_settings.RequestTimeout))
            {
                var item = await Container.DeleteItemAsync<SnapshotItem>($"{metadata.PersistenceId}{metadata.SequenceNr}", 
                    new PartitionKey(metadata.PersistenceId), cancellationToken: cts.Token);
                if(item.StatusCode == System.Net.HttpStatusCode.NoContent)
                    _log.Info($"Deleted Snapshot: {metadata.PersistenceId}{metadata.SequenceNr}");
                else
                    _log.Info($"Failed to delete Snapshot: {metadata.PersistenceId}{metadata.SequenceNr}");
            }
        }

        protected override async Task DeleteAsync(string persistenceId, SnapshotSelectionCriteria criteria)
        {
            var min = criteria.MinTimestamp.HasValue ? "AND s.Timestamp <= @toStamp" : "";
            QueryDefinition query = new QueryDefinition($"SELECT * FROM SnapshotItem s WHERE s.PersistenceId = @id " +
                $"AND (s.SeqNo > @fromSeqNo AND s.SeqNo <= @toSeqNo) AND (s.Timestamp >= @fromStamp {min})" +
                $"ORDER BY s.SeqNo DESC, s.Timestamp DESC")
               .WithParameter("@id", persistenceId)
               .WithParameter("@fromSeqNo", criteria.MinSequenceNr)
               .WithParameter("@toSeqNo", criteria.MaxSequenceNr)
               .WithParameter("@fromStamp", criteria.MinTimestamp.Value.Ticks);

            if (criteria.MinTimestamp.HasValue)
                query.WithParameter("@toStamp", criteria.MaxTimeStamp.Ticks);

            using (FeedIterator<SnapshotItem> setIterator = Container.GetItemQueryIterator<SnapshotItem>(
                query,
                requestOptions: new QueryRequestOptions()
                {
                    PartitionKey = new PartitionKey(persistenceId),
                    MaxConcurrency = 1,
                    MaxItemCount = 1
                }))
            {
                //make this better?
                using (var cts = new CancellationTokenSource(_settings.RequestTimeout))
                {
                    while (setIterator.HasMoreResults)
                    {
                        FeedResponse<SnapshotItem> response = await setIterator.ReadNextAsync();
                        foreach (var i in response.Resource)
                        {
                            var item = await Container.DeleteItemAsync<SnapshotItem>($"{i.PersistenceId}{i.SeqNo}", 
                                new PartitionKey(i.PersistenceId), cancellationToken: cts.Token);
                            if (item.StatusCode == System.Net.HttpStatusCode.NoContent)
                                _log.Info($"Deleted Snapshot: {i.PersistenceId}{i.SeqNo}");
                            else
                                _log.Info($"Failed to delete Snapshot: {i.PersistenceId}{i.SeqNo}");
                        }                            
                        
                    }
                }
            }
        }
        
    }
}