using Microsoft.Azure.Cosmos.Table;
using System;
using System.Collections.Generic;

namespace Akka.Persistence.Azure.TableEntities
{
    internal sealed class AllPersistenceIdsEntry
        : ITableEntity
    {
        private const string ManifestKeyName = "manifest";
        public const string PartitionKeyValue = "allPersistenceIdsIdx";

        // In order to use this in a TableQuery a parameterless constructor is required
        public AllPersistenceIdsEntry()
        {
        }

        public AllPersistenceIdsEntry(
            string persistenceId,
            string manifest = "")
        {
            PartitionKey = PartitionKeyValue;

            RowKey = persistenceId;

            Manifest = manifest;
        }

        public string ETag { get; set; }

        public string Manifest { get; set; }

        public string PartitionKey { get; set; }

        public string RowKey { get; set; }

        public DateTimeOffset Timestamp { get; set; }

        public void ReadEntity(
            IDictionary<string, EntityProperty> properties,
            OperationContext operationContext)
        {
            Manifest =
                properties.ContainsKey(ManifestKeyName)
                    ? properties[ManifestKeyName].StringValue
                    : string.Empty;
        }

        public IDictionary<string, EntityProperty> WriteEntity(
            OperationContext operationContext)
        {
            var dict =
                new Dictionary<string, EntityProperty>
                {
                    [ManifestKeyName] = EntityProperty.GeneratePropertyForString(Manifest),
                };

            return dict;
        }
    }
}