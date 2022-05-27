using System;
using Azure;
using Azure.Data.Tables;

namespace Akka.Persistence.Azure.TableEntities
{
    internal sealed class AllPersistenceIdsEntry 
    {
        private const string ManifestKeyName = "manifest";
        public const string PartitionKeyValue = "allPersistenceIdsIdx";

        // In order to use this in a TableQuery a parameterless constructor is required
        public AllPersistenceIdsEntry(TableEntity entity)
        {
            PartitionKey = entity.PartitionKey;
            ETag = entity.ETag;
            RowKey = entity.RowKey;
            Timestamp = entity.Timestamp;
            
            Manifest = entity.ContainsKey(ManifestKeyName)
                ? entity.GetString(ManifestKeyName)
                : string.Empty;
        }

        public AllPersistenceIdsEntry(
            string persistenceId,
            string manifest = "")
        {
            PartitionKey = PartitionKeyValue;
            RowKey = persistenceId;
            Manifest = manifest;
        }

        public string PartitionKey { get; }
        public ETag ETag { get; }
        public string RowKey { get; }
        public DateTimeOffset? Timestamp { get; }

        public string Manifest { get; }


        public TableEntity WriteEntity()
        {
            var entity = new TableEntity
            {
                PartitionKey = PartitionKey,
                ETag = ETag,
                RowKey = RowKey,
                Timestamp = Timestamp,
            };
            
            if (!string.IsNullOrWhiteSpace(Manifest))
                entity[ManifestKeyName] = Manifest;

            return entity;
        }
    }
}