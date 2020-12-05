using Akka.Persistence.Azure.Util;
using Microsoft.Azure.Cosmos.Table;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;

namespace Akka.Persistence.Azure.TableEntities
{
    internal sealed class SnapshotEntryTable: TableEntity
    {
        private const string SnapshotKeyName = "snapshot";
        public const string SeqNoKeyName = "seqno";

        public SnapshotEntryTable()
        {

        }
        public SnapshotEntryTable(string persistentId, long seqNo, byte[] snapshot, DateTimeOffset timestamp)
        {
            PartitionKey = persistentId;
            RowKey = seqNo.ToJournalRowKey();
            Snapshot = snapshot;
            Timestamp = timestamp;
        }
        /// <summary>
        ///     The persistent snapshot
        /// </summary>
        public byte[] Snapshot { get; set; }
        /// <summary>
        ///     The sequence number.
        /// </summary>
        /// <remarks>
        ///     We store this as a separate field since we may pad the <see cref="RowKey" />
        ///     in order to get the rows for each partition to sort in descending order.
        /// </remarks>
        public long SeqNo { get; set; }

        public override void ReadEntity(IDictionary<string, EntityProperty> properties, OperationContext operationContext)
        {
            // an exception is fine here - means the data is corrupted anyway
            SeqNo = properties[SeqNoKeyName].Int64Value.Value;
            Snapshot = properties[SnapshotKeyName].BinaryValue;
        }

        public override IDictionary<string, EntityProperty> WriteEntity(OperationContext operationContext)
        {
            var dict =
                new Dictionary<string, EntityProperty>
                {
                    [SnapshotKeyName] = EntityProperty.GeneratePropertyForByteArray(Snapshot),
                    [SeqNoKeyName] = EntityProperty.GeneratePropertyForLong(SeqNo)
                };

            return dict;
        }
    }

    internal sealed class SnapshotItem
    {
        public SnapshotItem(string persistentId, long seqNo, byte[] snapshot, long timestamp)
        {
            //
            Id = $"{persistentId}{seqNo}";
            PersistenceId = persistentId;
            Snapshot = snapshot;
            Timestamp = timestamp;
            SeqNo = seqNo;
        }
        [JsonProperty(PropertyName = "id")]
        public string Id { get; set; }
        /// <summary>
        ///     The persistent snapshot
        /// </summary>
        public byte[] Snapshot { get; set; }
        /// <summary>
        ///     The sequence number.
        /// </summary>
        /// <remarks>
        ///     We store this as a separate field since we may pad the <see cref="RowKey" />
        ///     in order to get the rows for each partition to sort in descending order.
        /// </remarks>
        public long SeqNo { get; set; }
        public long Timestamp { get; set; }
        public string PersistenceId { get; set; }
        public string PartitionKey => PersistenceId;
        public static string PartitionKeyPath => "/PersistenceId";

    }
}
