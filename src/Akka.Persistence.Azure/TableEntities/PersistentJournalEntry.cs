// -----------------------------------------------------------------------
// <copyright file="PersistentJournalEntry.cs" company="Petabridge, LLC">
//      Copyright (C) 2015 - 2018 Petabridge, LLC <https://petabridge.com>
// </copyright>
// -----------------------------------------------------------------------

using Akka.Persistence.Azure.Journal;
using Akka.Persistence.Azure.Util;
using System;
using Azure;
using Azure.Data.Tables;

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
    internal sealed class PersistentJournalEntry
    {
        public const string TagsKeyName = "tags";
        public const string UtcTicksKeyName = "utcTicks";
        private const string ManifestKeyName = "manifest";
        private const string PayloadKeyName = "payload";
        private const string SeqNoKeyName = "seqno";

        public PersistentJournalEntry(TableEntity entity)
        {
            PartitionKey = entity.PartitionKey;
            ETag = entity.ETag;
            RowKey = entity.RowKey;
            Timestamp = entity.Timestamp;
            
            Manifest = entity.ContainsKey(ManifestKeyName)
                ? entity.GetString(ManifestKeyName)
                : string.Empty;

            // an exception is fine here - means the data is corrupted anyway
            SeqNo = entity.GetInt64(SeqNoKeyName).Value;
            Payload = entity.GetBinary(PayloadKeyName);
            Tags = entity.GetString(TagsKeyName).Split(new []{';'}, StringSplitOptions.RemoveEmptyEntries);
            UtcTicks = entity.GetInt64(UtcTicksKeyName).Value;
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

        public string PartitionKey { get; }

        public ETag ETag { get; }

        public string RowKey { get; }

        public DateTimeOffset? Timestamp { get; }

        /// <summary>
        ///     The serialization manifest.
        /// </summary>
        public string Manifest { get; }

        /// <summary>
        ///     The persistent payload
        /// </summary>
        public byte[] Payload { get; }

        /// <summary>
        ///     The sequence number.
        /// </summary>
        /// <remarks>
        ///     We store this as a separate field since we may pad the <see cref="RowKey" />
        ///     in order to get the rows for each partition to sort in descending order.
        /// </remarks>
        public long SeqNo { get; }

        /// <summary>
        ///     Tags associated with this entry, if any
        /// </summary>
        public string[] Tags { get; }

        /// <summary>
        ///     Ticks of current UTC at the time the entry was created
        /// </summary>
        /// <remarks>
        ///     Azure Table Storage does not index the Timestamp value so performing
        ///     any query against it will be extremely slow
        /// </remarks>
        public long UtcTicks { get; }

        public TableEntity WriteEntity()
        {
            var entity = new TableEntity
            {
                PartitionKey = PartitionKey,
                ETag = ETag,
                RowKey = RowKey,
                Timestamp = Timestamp,
                [PayloadKeyName] = Payload,
                [SeqNoKeyName] = SeqNo,
                [TagsKeyName] = string.Join(";", Tags),
                [UtcTicksKeyName] = UtcTicks
            };

            if (!string.IsNullOrWhiteSpace(Manifest))
                entity[ManifestKeyName] = Manifest;

            return entity;
        }
    }
}