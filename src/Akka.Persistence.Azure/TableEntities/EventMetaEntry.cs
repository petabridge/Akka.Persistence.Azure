using Microsoft.Azure.Cosmos.Table;
using System;
using System.Collections.Generic;

namespace Akka.Persistence.Azure.TableEntities
{
    internal sealed class EventMetaEntry : ITableEntity
    {
        public const string PartitionKeyValue = "eventMetaEntry";
        private const string ManifestKeyName = "manifest";
        public const string SeqNoKeyName = "seqNo";
        private const string PersistenceIdKeyName = "persistenceId";

        // In order to use this in a TableQuery a parameterless constructor is required
        public EventMetaEntry()
        {
        }
        public EventMetaEntry(string persistentId, long seqNo, string rowKey, string manifest = "")
        {
            PartitionKey = PartitionKeyValue;
            RowKey = rowKey;
            PersistenceId = persistentId;
            SeqNo = seqNo;
            Manifest = manifest;
        }
        /// <summary>
         ///     The serialization manifest.
         /// </summary>
        public string Manifest { get; set; }
        public string PartitionKey { get; set; }
        public string RowKey { get; set; }
        public DateTimeOffset Timestamp { get; set; }
        public string ETag { get; set; }

        public long SeqNo { get; set; }
        public string PersistenceId { get; set; }

        public void ReadEntity(IDictionary<string, EntityProperty> properties, OperationContext operationContext)
        {
            Manifest =
                properties.ContainsKey(ManifestKeyName)
                    ? properties[ManifestKeyName].StringValue
                    : string.Empty;

            // an exception is fine here - means the data is corrupted anyway
            SeqNo = properties[SeqNoKeyName].Int64Value.Value;
            PersistenceId = properties[PersistenceIdKeyName].StringValue;
        }

        public IDictionary<string, EntityProperty> WriteEntity(OperationContext operationContext)
        {
            var dict =
                new Dictionary<string, EntityProperty>
                {
                    [ManifestKeyName] = EntityProperty.GeneratePropertyForString(Manifest),
                    [SeqNoKeyName] = EntityProperty.GeneratePropertyForLong(SeqNo),
                    [PersistenceIdKeyName] = EntityProperty.GeneratePropertyForString(PersistenceId)
                };

            return dict;
        }
    }
}
