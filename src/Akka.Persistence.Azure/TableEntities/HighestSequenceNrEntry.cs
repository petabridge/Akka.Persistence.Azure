using System;
using System.Collections.Generic;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;

namespace Akka.Persistence.Azure.TableEntities
{
    internal sealed class HighestSequenceNrEntry
        : ITableEntity
    {
        public const string HighestSequenceNrKey = "highestSequenceNr";
        private const string ManifestKeyName = "manifest";
        public const string RowKeyValue = "highestSequenceNr";
        public const string UtcTicksKeyName = "utcTicks";

        // In order to use this in a TableQuery a parameterless constructor is required
        public HighestSequenceNrEntry()
        {
        }

        public HighestSequenceNrEntry(
            string persistenceId,
            long highestSequenceNr,
            string manifest = "")
        {
            PartitionKey = persistenceId;

            RowKey = RowKeyValue;

            HighestSequenceNr = highestSequenceNr;

            Manifest = manifest;

            UtcTicks = DateTime.UtcNow.Ticks;
        }

        public string ETag { get; set; }

        public long HighestSequenceNr { get; set; }

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

            HighestSequenceNr = properties[HighestSequenceNrKey].Int64Value.Value;
            UtcTicks = properties[UtcTicksKeyName].Int64Value.Value;
        }

        public IDictionary<string, EntityProperty> WriteEntity(
            OperationContext operationContext)
        {
            var dict =
                new Dictionary<string, EntityProperty>
                {
                    [HighestSequenceNrKey] = EntityProperty.GeneratePropertyForLong(HighestSequenceNr),
                    [ManifestKeyName] = EntityProperty.GeneratePropertyForString(Manifest),
                    [UtcTicksKeyName] = EntityProperty.GeneratePropertyForLong(UtcTicks)
                };

            return dict;
        }
    }
}