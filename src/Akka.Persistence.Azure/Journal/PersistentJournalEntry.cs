// -----------------------------------------------------------------------
// <copyright file="PersistentJournalEntry.cs" company="Petabridge, LLC">
//      Copyright (C) 2015 - 2018 Petabridge, LLC <https://petabridge.com>
// </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using Akka.Persistence.Azure.Util;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;

namespace Akka.Persistence.Azure.Journal
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
        private const string PayloadKeyName = "payload";
        private const string ManifestKeyName = "manifest";
        private const string SeqNoKeyName = "seqno";

        public PersistentJournalEntry()
        {
        }

        public PersistentJournalEntry(string persistentId, long seqNo, byte[] payload, string manifest)
        {
            Payload = payload;
            Manifest = manifest;

            PartitionKey = persistentId;
            SeqNo = seqNo;
            RowKey = seqNo.ToJournalRowKey();
        }

        /// <summary>
        ///     The serialization manifest.
        /// </summary>
        public string Manifest { get; set; }

        /// <summary>
        ///     The persistent payload
        /// </summary>
        public byte[] Payload { get; set; }

        /// <summary>
        ///     The sequence number.
        /// </summary>
        /// <remarks>
        ///     We store this as a separate field since we may pad the <see cref="RowKey" />
        ///     in order to get the rows for each partition to sort in descending order.
        /// </remarks>
        public long SeqNo { get; set; }

        public void ReadEntity(IDictionary<string, EntityProperty> properties, OperationContext operationContext)
        {
            if (properties.ContainsKey(ManifestKeyName))
                Manifest = properties[ManifestKeyName].StringValue;
            else
                Manifest = string.Empty;

            // an exception is fine here - means the data is corrupted anyway
            SeqNo = properties[SeqNoKeyName].Int64Value.Value;
            Payload = properties[PayloadKeyName].BinaryValue;
        }

        public IDictionary<string, EntityProperty> WriteEntity(OperationContext operationContext)
        {
            var dict = new Dictionary<string, EntityProperty>
            {
                [PayloadKeyName] = EntityProperty.GeneratePropertyForByteArray(Payload),
                [ManifestKeyName] = EntityProperty.GeneratePropertyForString(Manifest),
                [SeqNoKeyName] = EntityProperty.GeneratePropertyForLong(SeqNo)
            };
            return dict;
        }

        public string PartitionKey { get; set; }
        public string RowKey { get; set; }
        public DateTimeOffset Timestamp { get; set; }
        public string ETag { get; set; }
    }
}