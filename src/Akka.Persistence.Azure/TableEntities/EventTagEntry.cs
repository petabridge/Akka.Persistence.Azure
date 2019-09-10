using System;
using System.Collections.Generic;
using System.Linq;
using Akka.Persistence.Azure.Util;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;

namespace Akka.Persistence.Azure.TableEntities
{
    internal sealed class EventTagEntry
        : ITableEntity
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
        public EventTagEntry()
        {
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
            {
                throw new ArgumentException($"Must not contain {Delimiter}.", nameof(persistenceId));
            }

            if (tag.Any(x => x == Delimiter))
            {
                throw new ArgumentException($"Must not contain {Delimiter}.", nameof(tag));
            }

            PartitionKey = GetPartitionKey(tag);

            Manifest = manifest;

            IdxPartitionKey = persistenceId;

            IdxRowKey = seqNo.ToJournalRowKey();

            IdxTag = tag;

            UtcTicks = utcTicks ?? DateTime.UtcNow.Ticks;

            RowKey = GetRowKey(UtcTicks, IdxPartitionKey, IdxRowKey);

            Payload = payload;
        }

        public string ETag { get; set; }

        public string IdxPartitionKey { get; set; }

        public string IdxRowKey { get; set; }

        public string IdxTag { get; set; }

        public string Manifest { get; set; }

        public string PartitionKey { get; set; }

        public byte[] Payload { get; set; }

        public string RowKey { get; set; }

        public DateTimeOffset Timestamp { get; set; }

        public long UtcTicks { get; set; }

        public void ReadEntity(IDictionary<string, EntityProperty> properties, OperationContext operationContext)
        {
            //var parts = RowKey.Split(new[] { Delimiter }, StringSplitOptions.RemoveEmptyEntries);

            Manifest =
                properties.ContainsKey(ManifestKeyName)
                    ? properties[ManifestKeyName].StringValue
                    : string.Empty;

            //IdxPartitionKey = parts[1];

            //IdxRowKey = parts[2];

            //IdxTag = parts[0];

            IdxPartitionKey = properties[IdxPartitionKeyKeyName].StringValue;

            IdxRowKey = properties[IdxRowKeyKeyName].StringValue;

            IdxTag = properties[IdxTagKeyName].StringValue;

            UtcTicks = properties[UtcTicksKeyName].Int64Value.Value;

            Payload = properties[PayloadKeyName].BinaryValue;
        }

        public IDictionary<string, EntityProperty> WriteEntity(OperationContext operationContext)
        {
            var dict =
                new Dictionary<string, EntityProperty>
                {
                    [ManifestKeyName] = EntityProperty.GeneratePropertyForString(Manifest),
                    [PayloadKeyName] = EntityProperty.GeneratePropertyForByteArray(Payload),
                    [UtcTicksKeyName] = EntityProperty.GeneratePropertyForLong(UtcTicks),
                    [IdxPartitionKeyKeyName] = EntityProperty.GeneratePropertyForString(IdxPartitionKey),
                    [IdxRowKeyKeyName] = EntityProperty.GeneratePropertyForString(IdxRowKey),
                    [IdxTagKeyName] = EntityProperty.GeneratePropertyForString(IdxTag)
                };

            return dict;
        }

        public static string GetPartitionKey(string tag)
        {
            return $"{PartitionKeyValue}-{tag}";
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