using System;
using System.Collections.Generic;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;

namespace Akka.Persistence.Azure.TableEntities
{
    internal sealed class AllPersistenceIdsEntry
        : ITableEntity
    {
        private const string ManifestKeyName = "manifest";
        public const string PartitionKeyValue = "allPersistenceIdsIdx";
        public const string UtcTicksKeyName = "utcTicks";

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

            UtcTicks = DateTime.UtcNow.Ticks;
        }

        public string ETag { get; set; }

        public string Manifest { get; set; }

        public string PartitionKey { get; set; }

        public string RowKey { get; set; }

        public DateTimeOffset Timestamp { get; set; }

        public long UtcTicks { get; set; }

        public void ReadEntity(
            IDictionary<string, EntityProperty> properties,
            OperationContext operationContext)
        {
            Manifest =
                properties.ContainsKey(ManifestKeyName)
                    ? properties[ManifestKeyName].StringValue
                    : string.Empty;

            UtcTicks = properties[UtcTicksKeyName].Int64Value.Value;
        }

        public IDictionary<string, EntityProperty> WriteEntity(
            OperationContext operationContext)
        {
            var dict =
                new Dictionary<string, EntityProperty>
                {
                    [ManifestKeyName] = EntityProperty.GeneratePropertyForString(Manifest),
                    [UtcTicksKeyName] = EntityProperty.GeneratePropertyForLong(UtcTicks)
                };

            return dict;
        }
    }
}