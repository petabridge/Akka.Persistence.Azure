using Microsoft.Azure.Cosmos.Table;
using System;
using System.Collections.Generic;

namespace Akka.Persistence.Azure.Journal
{
    internal sealed class HighestSequenceNrEntry
        : ITableEntity
    {
        public const string HighestSequenceNrKey = "highestSequenceNr";
        private const string ManifestKeyName = "manifest";
        public const string RowKeyValue = "highestSequenceNr";

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
        }

        public string ETag { get; set; }

        public long HighestSequenceNr { get; set; }

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

            HighestSequenceNr = properties[HighestSequenceNrKey].Int64Value.Value;
        }

        public IDictionary<string, EntityProperty> WriteEntity(
            OperationContext operationContext)
        {
            var dict =
                new Dictionary<string, EntityProperty>
                {
                    [HighestSequenceNrKey] = EntityProperty.GeneratePropertyForLong(HighestSequenceNr),
                    [ManifestKeyName] = EntityProperty.GeneratePropertyForString(Manifest),
                };

            return dict;
        }
    }
}
