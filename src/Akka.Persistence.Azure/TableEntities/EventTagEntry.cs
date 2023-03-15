// -----------------------------------------------------------------------
// <copyright file="EventTagEntry.cs" company="Petabridge, LLC">
//      Copyright (C) 2015 - 2023 Petabridge, LLC <https://petabridge.com>
// </copyright>
// -----------------------------------------------------------------------

using System;
using System.Linq;
using Akka.Persistence.Azure.Util;
using Azure;
using Azure.Data.Tables;

namespace Akka.Persistence.Azure.TableEntities
{
    internal sealed class EventTagEntry
    {
        public const char Delimiter = ':';
        public const char AsciiIncrementedDelimiter = ';'; //(char)((byte)Delimiter + 1)
        public const string PartitionKeyValue = "eventTagIdx";
        public const string UtcTicksKeyName = "utcTicks";
        private const string ManifestKeyName = "manifest";
        private const string PayloadKeyName = "payload";
        public const string IdxPartitionKeyKeyName = "idxPartitionKey";
        public const string IdxRowKeyKeyName = "idxRowKey";
        public const string IdxTagKeyName = "idxTag";

        // In order to use this in a TableQuery a parameterless constructor is required
        public EventTagEntry(TableEntity entity)
        {
            PartitionKey = entity.PartitionKey;
            ETag = entity.ETag;
            RowKey = entity.RowKey;
            Timestamp = entity.Timestamp;
            
            IdxPartitionKey = entity.GetString(IdxPartitionKeyKeyName);
            IdxRowKey = entity.GetString(IdxRowKeyKeyName);
            IdxTag = entity.GetString(IdxTagKeyName);
            UtcTicks = entity.GetInt64(UtcTicksKeyName).Value;
            Payload = entity.GetBinary(PayloadKeyName);
            
            Manifest = entity.ContainsKey(ManifestKeyName)
                ? entity.GetString(ManifestKeyName)
                : string.Empty;
        }

        public EventTagEntry(
            string persistenceId,
            string tag,
            long seqNo,
            byte[] payload,
            string manifest,
            long? utcTicks = null)
        {
            if (persistenceId.Any(x => x == Delimiter))
                throw new ArgumentException($"Must not contain {Delimiter}.", nameof(persistenceId));

            if (tag.Any(x => x == Delimiter))
                throw new ArgumentException($"Must not contain {Delimiter}.", nameof(tag));

            PartitionKey = GetPartitionKey(tag);
            Manifest = manifest;
            IdxPartitionKey = persistenceId;
            IdxRowKey = seqNo.ToJournalRowKey();
            IdxTag = tag;
            UtcTicks = utcTicks ?? DateTime.UtcNow.Ticks;
            RowKey = GetRowKey(UtcTicks, IdxPartitionKey, IdxRowKey);
            Payload = payload;
        }

        public string PartitionKey { get; }
        public ETag ETag { get; }
        public string RowKey { get; }
        public DateTimeOffset? Timestamp { get; }

        public string IdxPartitionKey { get; }

        public string IdxRowKey { get; }

        public string IdxTag { get; }

        public string Manifest { get; }

        public byte[] Payload { get; }

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
                [UtcTicksKeyName] = UtcTicks,
                [IdxPartitionKeyKeyName] = IdxPartitionKey,
                [IdxRowKeyKeyName] = IdxRowKey,
                [IdxTagKeyName] = IdxTag
            };

            if (!string.IsNullOrWhiteSpace(Manifest))
                entity[ManifestKeyName] = Manifest;

            return entity;
        }

        public static string GetPartitionKey(string tag)
        {
            return PartitionKeyEscapeHelper.Escape($"{PartitionKeyValue}-{tag}");
        }

        public static string GetRowKey(
            long utcTicks,
            string idxPartitionKey,
            string idxRowKey)
        {
            return $"{utcTicks.ToJournalRowKey()}{Delimiter}{idxPartitionKey}{Delimiter}{idxRowKey}";
        }
    }
}