// -----------------------------------------------------------------------
// <copyright file="PersistentJournalEntry.cs" company="Petabridge, LLC">
//      Copyright (C) 2015 - 2018 Petabridge, LLC <https://petabridge.com>
// </copyright>
// -----------------------------------------------------------------------

using Akka.Persistence.Azure.Journal;
using Akka.Persistence.Azure.Util;
using Microsoft.Azure.Cosmos.Table;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Akka.Persistence.Azure.TableEntities
{
    /// <summary>
    ///     INTERNAL API.
    ///     Lightweight representation of the <see cref="IPersistentRepresentation" /> used
    ///     inside the <see cref="AzureTableStorageJournal" />.
    /// </summary>
    /// <remarks>
    ///     Implementation note - as of 9.3.0, the built-in <see cref="TableEntity" /> type
    ///     doesn't perform any sort of reflection caching for .NET Core:
    ///     https://github.com/Azure/azure-storage-net/blob/master/Lib/Common/Table/TableEntity.cs#L301
    ///     Hence why we use a manual <see cref="ITableEntity" /> altogether - to just skip
    ///     those expensive parts at run-time.
    ///     Also, due to the way this API is designed... We have to be ok with mutability.
    /// </remarks>
    internal sealed class PersistentJournalEntry : ITableEntity
    {
        public const string TagsKeyName = "tags";
        public const string UtcTicksKeyName = "utcTicks";
        private const string ManifestKeyName = "manifest";
        private const string PayloadKeyName = "payload";
        public const string SeqNoKeyName = "seqno";

        public PersistentJournalEntry()
        {
        }

        public PersistentJournalEntry(
            string persistentId,
            long seqNo,
            byte[] payload,
            string manifest = "",
            params string[] tags)
        {
            Payload = payload;
            Manifest = manifest;
            Tags = tags ?? new string[]{};
            PartitionKey = persistentId;
            SeqNo = seqNo;
            RowKey = seqNo.ToJournalRowKey();
            UtcTicks = DateTime.UtcNow.Ticks;
        }

        public string ETag { get; set; }

        /// <summary>
        ///     The serialization manifest.
        /// </summary>
        public string Manifest { get; set; }

        public string PartitionKey { get; set; }

        /// <summary>
        ///     The persistent payload
        /// </summary>
        public byte[] Payload { get; set; }

        public string RowKey { get; set; }

        /// <summary>
        ///     The sequence number.
        /// </summary>
        /// <remarks>
        ///     We store this as a separate field since we may pad the <see cref="RowKey" />
        ///     in order to get the rows for each partition to sort in descending order.
        /// </remarks>
        public long SeqNo { get; set; }

        /// <summary>
        ///     Tags associated with this entry, if any
        /// </summary>
        public string[] Tags { get; set; }

        public DateTimeOffset Timestamp { get; set; }

        /// <summary>
        ///     Ticks of current UTC at the time the entry was created
        /// </summary>
        /// <remarks>
        ///     Azure Table Storage does not index the Timestamp value so performing
        ///     any query against it will be extremely slow
        /// </remarks>
        public long UtcTicks { get; set; }

        public void ReadEntity(IDictionary<string, EntityProperty> properties, OperationContext operationContext)
        {
            Manifest =
                properties.ContainsKey(ManifestKeyName)
                    ? properties[ManifestKeyName].StringValue
                    : string.Empty;

            // an exception is fine here - means the data is corrupted anyway
            SeqNo = properties[SeqNoKeyName].Int64Value.Value;
            Payload = properties[PayloadKeyName].BinaryValue;
            Tags = properties[TagsKeyName].StringValue.Split(new []{';'}, StringSplitOptions.RemoveEmptyEntries);
            UtcTicks = properties[UtcTicksKeyName].Int64Value.Value;
        }

        public IDictionary<string, EntityProperty> WriteEntity(OperationContext operationContext)
        {
            var dict =
                new Dictionary<string, EntityProperty>
                {
                    [PayloadKeyName] = EntityProperty.GeneratePropertyForByteArray(Payload),
                    [ManifestKeyName] = EntityProperty.GeneratePropertyForString(Manifest),
                    [SeqNoKeyName] = EntityProperty.GeneratePropertyForLong(SeqNo),
                    [TagsKeyName] = EntityProperty.GeneratePropertyForString(string.Join(";", Tags)),
                    [UtcTicksKeyName] = EntityProperty.GeneratePropertyForLong(UtcTicks)
                };

            return dict;
        }
    }
}