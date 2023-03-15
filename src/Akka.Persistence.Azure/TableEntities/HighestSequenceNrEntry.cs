// -----------------------------------------------------------------------
// <copyright file="HighestSequenceNrEntry.cs" company="Petabridge, LLC">
//      Copyright (C) 2015 - 2023 Petabridge, LLC <https://petabridge.com>
// </copyright>
// -----------------------------------------------------------------------

using System;
using Azure;
using Azure.Data.Tables;

namespace Akka.Persistence.Azure.TableEntities
{
    internal sealed class HighestSequenceNrEntry 
    {
        public const string HighestSequenceNrKey = "highestSequenceNr";
        private const string ManifestKeyName = "manifest";
        public const string RowKeyValue = "highestSequenceNr";

        public HighestSequenceNrEntry(TableEntity entity)
        {
            PartitionKey = entity.PartitionKey;
            ETag = entity.ETag;
            RowKey = entity.RowKey;
            Timestamp = entity.Timestamp;
            
            HighestSequenceNr = entity.GetInt64(HighestSequenceNrKey).Value;
            
            Manifest =
                entity.ContainsKey(ManifestKeyName)
                    ? entity.GetString(ManifestKeyName)
                    : string.Empty;
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

        public string PartitionKey { get; }
        
        public ETag ETag { get; }
        public string RowKey { get; }
        
        public DateTimeOffset? Timestamp { get; }

        public long HighestSequenceNr { get; }

        public string Manifest { get; }

        public TableEntity WriteEntity()
        {
            var entity = new TableEntity
            {
                PartitionKey = PartitionKey,
                ETag = ETag,
                RowKey = RowKey,
                Timestamp = Timestamp,
                [HighestSequenceNrKey] = HighestSequenceNr,
            };

            if (!string.IsNullOrWhiteSpace(Manifest))
                entity[ManifestKeyName] = Manifest;
            
            return entity;
        }
    }
}