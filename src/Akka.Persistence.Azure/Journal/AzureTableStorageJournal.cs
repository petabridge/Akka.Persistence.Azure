// -----------------------------------------------------------------------
// <copyright file="AzureTableStorageJournal.cs" company="Petabridge, LLC">
//      Copyright (C) 2015 - 2018 Petabridge, LLC <https://petabridge.com>
// </copyright>
// -----------------------------------------------------------------------

using Akka.Actor;
using Akka.Event;
using Akka.Persistence.Azure.Query;
using Akka.Persistence.Azure.TableEntities;
using Akka.Persistence.Azure.Util;
using Akka.Persistence.Journal;
using Akka.Util.Internal;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Akka.Configuration;
using Debug = System.Diagnostics.Debug;
using Microsoft.Azure.Cosmos.Table;

namespace Akka.Persistence.Azure.Journal
{
    /// <inheritdoc />
    /// <summary>
    ///     Akka.Persistence Journal implementation that uses Azure Table Storage
    ///     as the backing store.
    ///     Designed to work against a single table and process operations in a manner
    ///     that is least taxing on the CPU and bandwidth by taking advantage of opportunities
    ///     for parallelism without doing it on an unbounded scale.
    /// </summary>
    public class AzureTableStorageJournal : AsyncWriteJournal
    {
        private static readonly Dictionary<int, TimeSpan> RetryInterval =
            new Dictionary<int, TimeSpan>()
        {
            { 5, TimeSpan.FromMilliseconds(100) },
            { 4, TimeSpan.FromMilliseconds(500) },
            { 3, TimeSpan.FromMilliseconds(1000) },
            { 2, TimeSpan.FromMilliseconds(2000) },
            { 1, TimeSpan.FromMilliseconds(4000) },
            { 0, TimeSpan.FromMilliseconds(8000) },
        };

        private readonly HashSet<string> _allPersistenceIds = new HashSet<string>();
        private readonly ILoggingAdapter _log = Context.GetLogger();

        private long _lastEventMetaRowKey = 0L;

        private ImmutableDictionary<string, IImmutableSet<IActorRef>> _persistenceIdSubscribers = ImmutableDictionary.Create<string, IImmutableSet<IActorRef>>();
        private ImmutableDictionary<string, IImmutableSet<IActorRef>> _tagSubscribers = ImmutableDictionary.Create<string, IImmutableSet<IActorRef>>();

        private readonly SerializationHelper _serialization;
        private readonly AzureTableStorageJournalSettings _settings;
        private readonly CloudStorageAccount _storageAccount;
        private readonly Lazy<CloudTable> _tableStorage;

        private readonly HashSet<IActorRef> _newEventsSubscriber = new HashSet<IActorRef>();

        public AzureTableStorageJournal(Config config = null)
        {
            _settings = config is null ? 
                AzurePersistence.Get(Context.System).TableSettings :
                AzureTableStorageJournalSettings.Create(config);

            _serialization = new SerializationHelper(Context.System);
            _storageAccount = _settings.Development ?
                CloudStorageAccount.DevelopmentStorageAccount : 
                CloudStorageAccount.Parse(_settings.ConnectionString);

            _tableStorage = new Lazy<CloudTable>(() => InitCloudStorage(5).Result);
            var rowKey = ReadMaxOrderingId();
            _lastEventMetaRowKey = rowKey + 1;
        }

        public CloudTable Table => _tableStorage.Value;

        protected bool HasPersistenceIdSubscribers =>
            _persistenceIdSubscribers.Count != 0;

        protected bool HasTagSubscribers =>
            _tagSubscribers.Count != 0;

        public override async Task<long> ReadHighestSequenceNrAsync(
            string persistenceId,
            long fromSequenceNr)
        {

            _log.Debug("Entering method ReadHighestSequenceNrAsync");
            
            var sequenceNumberQuery = GenerateHighestSequenceNumberQuery(persistenceId);
            TableQuerySegment<HighestSequenceNrEntry> result = null;
            long seqNo = 0L;

            do
            {
                result = await Table.ExecuteQuerySegmentedAsync(sequenceNumberQuery, result?.ContinuationToken);

                if (result.Results.Count > 0)
                {
                    seqNo = Math.Max(seqNo, result.Results.Max(x => x.HighestSequenceNr));
                }
            } while (result.ContinuationToken != null);

            _log.Debug("Leaving method ReadHighestSequenceNrAsync with SeqNo [{0}] for PersistentId [{1}]", seqNo, persistenceId);

            return seqNo;
        }

        private void GetLatestEventMetaEntryRow()
        {

            _log.Debug("Entering method GetLatestEventMetaEntryRowAsync");

            //long seqNo = 0L;
            var result = Table.ExecuteQuery(GenerateLatestEventMetaEntryQuery())?.FirstOrDefault();

            _log.Debug("Leaving method GetLatestEventMetaEntryRowAsync with SeqNo [{0}] for PersistentId [{1}] with Rowkey [{0}]", result.SeqNo, result.PersistenceId, result.RowKey);

            //return seqNo;
        }


        public override async Task ReplayMessagesAsync(
            IActorContext context,
            string persistenceId,
            long fromSequenceNr,
            long toSequenceNr,
            long max,
            Action<IPersistentRepresentation> recoveryCallback)
        {
            
            _log.Debug("Entering method ReplayMessagesAsync for persistentId [{0}] from seqNo range [{1}, {2}] and taking up to max [{3}]", persistenceId, fromSequenceNr, toSequenceNr, max);

            if (max == 0)
                return;
            await foreach(var entry in ReplayAsync(persistenceId, fromSequenceNr, toSequenceNr, max))
            {
                var deserialized = _serialization.PersistentFromBytes(entry.Payload);

                // Write the new persistent because it sets the sender as deadLetters which is not correct
                var persistent =
                    new Persistent(
                        deserialized.Payload,
                        deserialized.SequenceNr,
                        PartitionKeyEscapeHelper.Unescape(deserialized.PersistenceId),
                        deserialized.Manifest,
                        deserialized.IsDeleted,
                        ActorRefs.NoSender,
                        deserialized.WriterGuid);

                if (_log.IsDebugEnabled && _settings.VerboseLogging)
                {
                    _log.Debug("Recovering [{0}] for entity [{1}].", persistent, entry.PartitionKey);
                }

                recoveryCallback(persistent);
            }            

            _log.Debug("Leaving method ReplayMessagesAsync");
        }

        protected override async Task DeleteMessagesToAsync(
            string persistenceId,
            long toSequenceNr)
        {

            _log.Debug("Entering method DeleteMessagesToAsync for persistentId [{0}] and up to seqNo [{1}]", persistenceId, toSequenceNr);

            var query = GeneratePersistentJournalEntryDeleteQuery(persistenceId, toSequenceNr);

            var nextSegment = Table.ExecuteQuerySegmentedAsync(query, null);

            while (nextSegment != null)
            {
                var queryResults = await nextSegment;

                if (_log.IsDebugEnabled && _settings.VerboseLogging)
                {
                    _log.Debug("Have [{0}] messages to delete for entity [{1}]", queryResults.Results.Count, persistenceId);
                }

                nextSegment =
                    queryResults.ContinuationToken != null
                        ? Table.ExecuteQuerySegmentedAsync(query, queryResults.ContinuationToken)
                        : null;

                if (queryResults.Results.Count > 0)
                {
                    var tableBatchOperation = new TableBatchOperation();

                    foreach (var toBeDeleted in queryResults.Results)
                    {
                        tableBatchOperation.Delete(toBeDeleted);
                    }

                    var deleteTask = Table.ExecuteBatchAsync(tableBatchOperation);

                    await deleteTask;
                }
            }

            _log.Debug("Leaving method DeleteMessagesToAsync for persistentId [{0}] and up to seqNo [{1}]", persistenceId, toSequenceNr);
        }

        protected override void PreStart()
        {
            //GetLatestEventMetaEntryRow();
            _log.Debug("Initializing Azure Table Storage...");

            // forces loading of the value
            var name = Table.Name;

            _log.Debug("Successfully started Azure Table Storage!");

            // need to call the base in order to ensure Akka.Persistence starts up correctly
            base.PreStart();
        }

        protected override bool ReceivePluginInternal(object message)
        {
            switch (message)
            {
                case ReplayTaggedMessages replay:
                    ReplayTaggedMessagesAsync(replay)
                        .PipeTo(replay.ReplyTo, success: h => new RecoverySuccess(h), failure: e => new ReplayMessagesFailure(e));
                    return true;
                case ReplayAllEvents replay:
                    ReplayAllEventsAsync(replay)
                        .PipeTo(replay.ReplyTo, success: h => new EventReplaySuccess(h),
                            failure: e => new EventReplayFailure(e));
                    return true;
                case SubscribePersistenceId subscribe:
                    AddPersistenceIdSubscriber(Sender, subscribe.PersistenceId);
                    Context.Watch(Sender);
                    return true;
                case SelectCurrentPersistenceIds request:
                    SelectAllPersistenceIdsAsync(request.Offset)
                        .PipeTo(request.ReplyTo, success: result => new CurrentPersistenceIds(result.Ids, request.Offset));
                    return true;
                case SubscribeTag subscribe:
                    AddTagSubscriber(Sender, subscribe.Tag);
                    Context.Watch(Sender);
                    return true;
                case SubscribeNewEvents _:
                    AddNewEventsSubscriber(Sender);
                    Context.Watch(Sender);
                    return true;
                case Terminated terminated:
                    RemoveSubscriber(terminated.ActorRef);
                    return true;
                default:
                    return false;
            }
        }

        protected override async Task<IImmutableList<Exception>> WriteMessagesAsync(
            IEnumerable<AtomicWrite> atomicWrites)
        {
            try
            {
                var taggedEntries = ImmutableDictionary<string, List<EventTagEntry>>.Empty;

                var exceptions = ImmutableList<Exception>.Empty;

                var highSequenceNumbers = ImmutableDictionary<string, long>.Empty;

                using (var currentWrites = atomicWrites.GetEnumerator())
                {
                    while (currentWrites.MoveNext())
                    {
                        Debug.Assert(currentWrites.Current != null, "atomicWrites.Current != null");

                        var list = currentWrites.Current.Payload.AsInstanceOf<IImmutableList<IPersistentRepresentation>>();

                        var batchItems = ImmutableList<ITableEntity>.Empty;

                        foreach (var t in list)
                        {
                            var item = t;

                            Debug.Assert(item != null, nameof(item) + " != null");

                            byte[] payloadBytes = null;

                            string[] tags = {};

                            // If the payload is a tagged payload, reset to a non-tagged payload
                            if (item.Payload is Tagged tagged)
                            {
                                item = item.WithPayload(tagged.Payload);

                                payloadBytes = _serialization.PersistentToBytes(item);

                                if (tagged.Tags.Count > 0)
                                {
                                    tags = tagged.Tags.ToArray();

                                }
                            }

                            if (payloadBytes == null)
                            {
                                payloadBytes = _serialization.PersistentToBytes(item);
                            }

                            var newItem =
                                new PersistentJournalEntry(
                                    item.PersistenceId,
                                    item.SequenceNr,
                                    payloadBytes,
                                    item.Manifest,
                                    tags);

                            batchItems = batchItems.Add(newItem);

                            foreach (var tag in tags)
                            {
                                if (!taggedEntries.ContainsKey(tag))
                                {
                                    taggedEntries = taggedEntries.SetItem(tag, new List<EventTagEntry>());
                                }

                                taggedEntries[tag].Add(
                                    new EventTagEntry(
                                        newItem.PartitionKey,
                                        tag,
                                        newItem.SeqNo,
                                        newItem.Payload,
                                        newItem.Manifest,
                                        newItem.UtcTicks));
                            }

                            highSequenceNumbers =
                                highSequenceNumbers.SetItem(
                                    item.PersistenceId,
                                    item.SequenceNr);
                        }

                        try
                        {
                            var persistenceBatch = new TableBatchOperation();

                            highSequenceNumbers.ForEach(
                                x => batchItems = batchItems.Add(
                                    new HighestSequenceNrEntry(x.Key, x.Value)));

                            // Encode partition keys for writing
                            foreach (var tableEntity in batchItems)
                            {
                                tableEntity.PartitionKey = PartitionKeyEscapeHelper.Escape(tableEntity.PartitionKey);
                            }
                            
                            batchItems.ForEach(x => persistenceBatch.InsertOrReplace(x));

                            if (_log.IsDebugEnabled && _settings.VerboseLogging)
                                _log.Debug("Attempting to write batch of {0} messages to Azure storage", persistenceBatch.Count);

                            var persistenceResults = await Table.ExecuteBatchAsync(persistenceBatch);

                            if (_log.IsDebugEnabled && _settings.VerboseLogging)
                                foreach (var r in persistenceResults)
                                    _log.Debug("Azure table storage wrote entity [{0}] with status code [{1}]", r.Etag, r.HttpStatusCode);
                        }
                        catch (Exception ex)
                        {
                            exceptions = exceptions.Add(ex);
                        }
                    }
                }

                if (exceptions.IsEmpty)
                {
                    var allPersistenceIdsBatch = new TableBatchOperation();
                    var eventMetaBatch = new TableBatchOperation();

                    highSequenceNumbers.ForEach(x =>
                    {
                        var encodedKey = PartitionKeyEscapeHelper.Escape(x.Key);
                        allPersistenceIdsBatch.InsertOrReplace(new AllPersistenceIdsEntry(encodedKey));

                        //A decreasing rowkey makes the latest entity to be on top. Meaning O(1)
                        //https://stackoverflow.com/questions/36889485/querying-azure-table-to-get-last-inserted-data-for-a-partition
                        
                        var rowkey = string.Format("{0:D19}", DateTime.MaxValue.Ticks - DateTime.UtcNow.Ticks);
                        eventMetaBatch.Insert(new EventMetaEntry(x.Key, x.Value, _lastEventMetaRowKey.ToJournalRowKey()));
                        _lastEventMetaRowKey++;
                    });

                    var allPersistenceResults = await Table.ExecuteBatchAsync(allPersistenceIdsBatch);
                    var eventMetaResults = await Table.ExecuteBatchAsync(eventMetaBatch);

                    if (_log.IsDebugEnabled && _settings.VerboseLogging)
                    {
                        foreach (var r in allPersistenceResults)
                            _log.Debug("Azure table storage wrote entity [{0}] with status code [{1}]", r.Etag, r.HttpStatusCode);

                        foreach (var e in eventMetaResults)
                            _log.Debug("Azure table storage wrote entity [{0}] with status code [{1}]", e.Etag, e.HttpStatusCode);

                    }

                    if (HasPersistenceIdSubscribers)
                    {                        
                        highSequenceNumbers.ForEach(x => NotifyPersistenceIdChange(x.Key));
                    }

                    if (taggedEntries.Count > 0)
                    {
                        var eventTagsBatch = new TableBatchOperation();

                        foreach (var kvp in taggedEntries)
                        {
                            eventTagsBatch.Clear();

                            foreach (var item in kvp.Value)
                            {
                                item.PartitionKey = PartitionKeyEscapeHelper.Escape(item.PartitionKey);
                                eventTagsBatch.InsertOrReplace(item);
                            }

                            var eventTagsResults = await Table.ExecuteBatchAsync(eventTagsBatch);

                            if (_log.IsDebugEnabled && _settings.VerboseLogging)
                                foreach (var r in eventTagsResults)
                                    _log.Debug("Azure table storage wrote entity [{0}] with status code [{1}]", r.Etag, r.HttpStatusCode);

                            if (HasTagSubscribers && taggedEntries.Count != 0)
                            {
                                foreach (var tag in taggedEntries.Keys)
                                {
                                    NotifyTagChange(tag);
                                }
                            }

                        }
                    }
                }

                /*
                 * Part of the Akka.Persistence design.
                 *
                 * Either return null or return an exception for each failed AtomicWrite.
                 *
                 * Either everything fails or everything succeeds is the idea I guess.
                 */
                return exceptions.IsEmpty ? null : exceptions;
            }
            catch (Exception ex)
            {
                _log.Error(ex, "Error during WriteMessagesAsync");
                throw;
            }
        }

        private static TableQuery GenerateAllPersistenceIdsQuery()
        {
            var filter =
                TableQuery.GenerateFilterCondition(
                    "PartitionKey",
                    QueryComparisons.Equal,
                    AllPersistenceIdsEntry.PartitionKeyValue);

            var returnValue = new TableQuery().Where(filter).Select(new[] { "RowKey" });

            return returnValue;
        }
        private void AddNewEventsSubscriber(IActorRef subscriber)
        {
            _newEventsSubscriber.Add(subscriber);
        }
        private static TableQuery<HighestSequenceNrEntry> GenerateHighestSequenceNumberQuery(
            string persistenceId)
        {
            var filter =
                TableQuery.CombineFilters(
                    TableQuery.GenerateFilterCondition(
                        "PartitionKey",
                        QueryComparisons.Equal,
                        PartitionKeyEscapeHelper.Escape(persistenceId)),
                    TableOperators.And,
                    TableQuery.GenerateFilterCondition(
                        "RowKey",
                        QueryComparisons.Equal,
                        HighestSequenceNrEntry.RowKeyValue));

            var returnValue = new TableQuery<HighestSequenceNrEntry>().Where(filter);

            return returnValue;
        }
        private TableQuery<EventMetaEntry> GenerateLatestEventMetaEntryQuery()
        {
            var filter = TableQuery.GenerateFilterCondition(
                        "PartitionKey",
                        QueryComparisons.Equal,
                        EventMetaEntry.PartitionKeyValue);

            var returnValue = new TableQuery<EventMetaEntry>().Where(filter).Take(1);

            return returnValue;
        }

        private  TableQuery<EventMetaEntry> GenerateEventMetaEntryQuery(long fromSequence, long toSequence)
        {
            var partitionFilter = TableQuery.GenerateFilterCondition(
                        "PartitionKey",
                        QueryComparisons.Equal,
                        EventMetaEntry.PartitionKeyValue);

            if(fromSequence > 0)
            {
                partitionFilter = TableQuery.CombineFilters(
                    partitionFilter,
                    TableOperators.And,
                    TableQuery.GenerateFilterCondition(
                            "RowKey",
                            QueryComparisons.GreaterThan,
                            fromSequence.ToJournalRowKey()));
            }
            

            if (toSequence != long.MaxValue)
            {
                partitionFilter = TableQuery.CombineFilters(
                    partitionFilter,
                    TableOperators.And,
                    TableQuery.GenerateFilterCondition(
                            "RowKey",
                            QueryComparisons.LessThanOrEqual,
                            toSequence.ToJournalRowKey()));
            }

            var returnValue = new TableQuery<EventMetaEntry>().Where(partitionFilter);

            return returnValue;
        }


        private static TableQuery<EventMetaEntry> GenerateMaxOrderingIdQuery()
        {
            var filter = TableQuery.GenerateFilterCondition(
                        "PartitionKey",
                        QueryComparisons.Equal,
                        EventMetaEntry.PartitionKeyValue);

            var returnValue = new TableQuery<EventMetaEntry>().Where(filter);
            return returnValue;
        }
        private long ReadMaxOrderingId()
        {
            var max = Table.ExecuteQuery(GenerateMaxOrderingIdQuery()).Max(x => x.RowKey);
            max = max is null ? "0" : max;
            return long.Parse(max);
        }
        //private static TableQuery GeneratePersistentJournalEntryDeleteQuery(
        private static TableQuery<PersistentJournalEntry> GeneratePersistentJournalEntryDeleteQuery(
            string persistenceId,
            long toSequenceNr)
        {
            var persistenceIdFilter =
                TableQuery.GenerateFilterCondition(
                    "PartitionKey",
                    QueryComparisons.Equal,
                    PartitionKeyEscapeHelper.Escape(persistenceId));

            var highestSequenceNrFilter =
                TableQuery.GenerateFilterCondition(
                    "RowKey",
                    QueryComparisons.NotEqual,
                    HighestSequenceNrEntry.RowKeyValue);

            var rowKeyLessThanFilter =
                TableQuery.GenerateFilterCondition(
                    "RowKey",
                    QueryComparisons.LessThanOrEqual,
                    toSequenceNr.ToJournalRowKey());

            var rowKeyFilter =
                TableQuery.CombineFilters(
                    highestSequenceNrFilter,
                    TableOperators.And,
                    rowKeyLessThanFilter);

            var filter =
                TableQuery.CombineFilters(
                    persistenceIdFilter,
                    TableOperators.And,
                    rowKeyFilter);

            var returnValue = new TableQuery<PersistentJournalEntry>().Where(filter);

            return returnValue;
        }
        //changed to netstandard 2.1
        private async IAsyncEnumerable<EventMetaEntry> GetEventMeta(long fromSequence, long toSequence)
        {
            var query = GenerateEventMetaEntryQuery(fromSequence, toSequence);
            TableQuerySegment<EventMetaEntry> result = null;

            do
            {
                result = await Table.ExecuteQuerySegmentedAsync(query, result?.ContinuationToken);
                var ordered = result.Results.OrderBy(x => x.RowKey);
                foreach (var r in ordered)
                    yield return r;

            } while (result.ContinuationToken != null);
        }
        private static TableQuery<EventTagEntry> GenerateEventTagEntryDeleteQuery(
            string persistenceId,
            long fromSequenceNr,
            long toSequenceNr)
        {
            var persistenceIdFilter =
                TableQuery.GenerateFilterCondition(
                    "PartitionKey",
                    QueryComparisons.Equal,
                    EventTagEntry.PartitionKeyValue);

            var idxPartitionKeyFilter =
                TableQuery.GenerateFilterCondition(
                    EventTagEntry.IdxPartitionKeyKeyName,
                    QueryComparisons.Equal,
                    persistenceId);

            var idxRowKeyGreaterThanFilter =
                TableQuery.GenerateFilterCondition(
                    EventTagEntry.IdxRowKeyKeyName,
                    QueryComparisons.GreaterThanOrEqual,
                    fromSequenceNr.ToJournalRowKey());

            var idxRowKeyLessThanFilter =
                TableQuery.GenerateFilterCondition(
                    EventTagEntry.IdxRowKeyKeyName,
                    QueryComparisons.LessThanOrEqual,
                    toSequenceNr.ToJournalRowKey());

            var partitionKeyFilter =
                TableQuery.CombineFilters(
                    persistenceIdFilter,
                    TableOperators.And,
                    idxPartitionKeyFilter);

            var idxRowKeyFilter = 
                TableQuery.CombineFilters(
                    idxRowKeyGreaterThanFilter,
                    TableOperators.And,
                    idxRowKeyLessThanFilter);

            var filter =
                TableQuery.CombineFilters(
                    partitionKeyFilter,
                    TableOperators.And,
                    idxRowKeyFilter);

            var returnValue = new TableQuery<EventTagEntry>().Where(filter);

            return returnValue;
        }

        private static TableQuery<PersistentJournalEntry> GeneratePersistentJournalEntryReplayQuery(
            string persistentId,
            long fromSequenceNumber,
            long toSequenceNumber)
        {
            var persistenceIdFilter =
                TableQuery.GenerateFilterCondition(
                    "PartitionKey",
                    QueryComparisons.Equal,
                    PartitionKeyEscapeHelper.Escape(persistentId));

            var highestSequenceNrFilter =
                TableQuery.GenerateFilterCondition(
                    "RowKey",
                    QueryComparisons.NotEqual,
                    HighestSequenceNrEntry.RowKeyValue);

            var filter =
                TableQuery.CombineFilters(
                    persistenceIdFilter,
                    TableOperators.And,
                    highestSequenceNrFilter);

            if (fromSequenceNumber > 0)
            {
                filter =
                    TableQuery.CombineFilters(
                        filter,
                        TableOperators.And,
                        TableQuery.GenerateFilterCondition(
                            "RowKey",
                            QueryComparisons.GreaterThanOrEqual,
                            fromSequenceNumber.ToJournalRowKey()));
            }

            if (toSequenceNumber != long.MaxValue)
            {
                filter =
                    TableQuery.CombineFilters(
                        filter,
                        TableOperators.And,
                        TableQuery.GenerateFilterCondition(
                            "RowKey",
                            QueryComparisons.LessThanOrEqual,
                            toSequenceNumber.ToJournalRowKey()));
            }

            var returnValue = new TableQuery<PersistentJournalEntry>().Where(filter);

            return returnValue;
        }

        private static TableQuery<EventTagEntry> GenerateTaggedMessageQuery(
            ReplayTaggedMessages replay)
        {
            var partitionKeyFilter =
                TableQuery.GenerateFilterCondition(
                    "PartitionKey",
                    QueryComparisons.Equal,
                    PartitionKeyEscapeHelper.Escape(EventTagEntry.GetPartitionKey(replay.Tag)));

            var utcTicksTRowKeyFilter =
                TableQuery.CombineFilters(
                    TableQuery.GenerateFilterCondition(
                        "RowKey",
                        QueryComparisons.GreaterThan,
                        $"{replay.FromOffset.ToJournalRowKey()}{EventTagEntry.AsciiIncrementedDelimiter}"),
                    TableOperators.And,
                    TableQuery.GenerateFilterCondition(
                        "RowKey",
                        QueryComparisons.LessThanOrEqual,
                        $"{replay.ToOffset.ToJournalRowKey()}{EventTagEntry.Delimiter}"));

            var filter =
                TableQuery.CombineFilters(
                    partitionKeyFilter,
                    TableOperators.And,
                    utcTicksTRowKeyFilter);

            var returnValue = new TableQuery<EventTagEntry>().Where(filter);

            return returnValue;
        }

        private static TableQuery<PersistentJournalEntry> GenerateAllEventsMessageQuery(ReplayAllEvents replay)
        {
            var filter =
                TableQuery.CombineFilters(
                    TableQuery.GenerateFilterCondition(
                        PersistentJournalEntry.SeqNoKeyName,
                        QueryComparisons.GreaterThanOrEqual,
                        replay.FromOffset.ToJournalRowKey()),
                    TableOperators.And,
                    TableQuery.GenerateFilterCondition(
                        PersistentJournalEntry.SeqNoKeyName,
                        QueryComparisons.LessThanOrEqual,
                        replay.ToOffset.ToJournalRowKey()));

            var returnValue = new TableQuery<PersistentJournalEntry>().Where(filter);

            return returnValue;
        }

        private void AddPersistenceIdSubscriber(IActorRef subscriber,  string persistenceId)
        {
            if (!_persistenceIdSubscribers.TryGetValue(persistenceId, out var subscriptions))
            {
                _persistenceIdSubscribers = _persistenceIdSubscribers.Add(persistenceId, ImmutableHashSet.Create(subscriber));
            }
            else
            {
                _persistenceIdSubscribers = _persistenceIdSubscribers.SetItem(persistenceId, subscriptions.Add(subscriber));
            }
        }

        private void AddTagSubscriber(IActorRef subscriber, string tag)
        {
            if (!_tagSubscribers.TryGetValue(tag, out var subscriptions))
            {
                _tagSubscribers = _tagSubscribers.Add(tag, ImmutableHashSet.Create(subscriber));
            }
            else
            {
                _tagSubscribers = _tagSubscribers.SetItem(tag, subscriptions.Add(subscriber));
            }
        }

        private async Task<IEnumerable<string>> GetAllPersistenceIds()
        {
            //We are now reading from a single partition
            var query = GenerateMaxOrderingIdQuery();
            TableContinuationToken token = null;
            var returnValue = ImmutableList<string>.Empty;

            do
            {
                var segment = await Table.ExecuteQuerySegmentedAsync(query, token);
                token = segment.ContinuationToken;
                if (segment.Results.Count > 0)
                {
                    returnValue = returnValue.AddRange(segment.Results.Select(x => x.PersistenceId));
                }
            } while (token != null);

            return returnValue.Distinct();
        }
        protected virtual async Task<(IEnumerable<string> Ids, long LastOrdering)> SelectAllPersistenceIdsAsync(long offset)
        {
            var maxOderingId = ReadMaxOrderingId();
            var ids = await GetAllPersistenceIds();
            return (ids, maxOderingId);
        }
        
        protected virtual async Task<long> ReplayAllEventsAsync(ReplayAllEvents replay)
        {
            var maxOrderingId = ReadMaxOrderingId();

            await foreach (var meta in GetEventMeta(replay.FromOffset, replay.ToOffset))
            {
                await foreach(var entry in ReplayAsync(meta.PersistenceId, meta.SeqNo, meta.SeqNo, replay.Max))
                {
                    var rowKey = long.Parse(meta.RowKey);
                    var deserialized = _serialization.PersistentFromBytes(entry.Payload);
                    var persistent =
                        new Persistent(
                            deserialized.Payload,
                            deserialized.SequenceNr,
                            deserialized.PersistenceId,
                            deserialized.Manifest,
                            deserialized.IsDeleted,
                            ActorRefs.NoSender,
                            deserialized.WriterGuid);

                    foreach (var adapted in AdaptFromJournal(persistent))
                    {
                        _log.Debug("Sending replayed message: persistenceId:{0} - sequenceNr:{1} - event:{2}",
                            deserialized.PersistenceId, deserialized.SequenceNr, deserialized.Payload);
                        replay.ReplyTo.Tell(new ReplayedEvent(adapted, rowKey), ActorRefs.NoSender);

                    }

                    maxOrderingId = Math.Max(maxOrderingId, rowKey);
                }
            }
            return maxOrderingId;
        }
        private async IAsyncEnumerable<PersistentJournalEntry> ReplayAsync(
            string persistenceId,
            long fromSequenceNr,
            long toSequenceNr,
            long max)
        {
            var replayQuery = GeneratePersistentJournalEntryReplayQuery(persistenceId, fromSequenceNr, toSequenceNr);
            // While we can specify the TakeCount, the CloudTable client does
            //    not really respect this fact and will keep pulling results.
            replayQuery.TakeCount =
                max > int.MaxValue
                    ? int.MaxValue
                    : (int)max;

            var nextTask = Table.ExecuteQuerySegmentedAsync(replayQuery, null);
            var count = 0L;
            while (nextTask != null)
            {
                var tableQueryResult = await nextTask;

                if (_log.IsDebugEnabled && _settings.VerboseLogging)
                {
                    _log.Debug("Recovered [{0}] messages for entity [{1}]", tableQueryResult.Results.Count, persistenceId);
                }

                if (tableQueryResult.ContinuationToken != null)
                {
                    if (_log.IsDebugEnabled && _settings.VerboseLogging)
                    {
                        _log.Debug("Have additional messages to download for entity [{0}]", persistenceId);
                    }
                    // start the next query while we process the results of this one
                    nextTask = Table.ExecuteQuerySegmentedAsync(replayQuery, tableQueryResult.ContinuationToken);
                }
                else
                {
                    if (_log.IsDebugEnabled && _settings.VerboseLogging)
                    {
                        _log.Debug("Completed download of messages for entity [{0}]", persistenceId);
                    }

                    // terminates the loop
                    nextTask = null;
                }

                foreach (var savedEvent in tableQueryResult.Results)
                {
                    // check if we've hit max recovery
                    if (count >= max)
                        break;
                    ++count;
                    yield return savedEvent;
                }
            }
        } 
        private async Task<CloudTable> InitCloudStorage(
            int remainingTries)
        {
            try
            {
                var tableClient = _storageAccount.CreateCloudTableClient();
                var tableRef = tableClient.GetTableReference(_settings.TableName);
                var op = new OperationContext();
                using (var cts = new CancellationTokenSource(_settings.ConnectTimeout))
                {
                    if (await tableRef.CreateIfNotExistsAsync(new TableRequestOptions(), op, cts.Token))
                        _log.Info("Created Azure Cloud Table", _settings.TableName);
                    else
                        _log.Info("Successfully connected to existing table", _settings.TableName);
                }

                return tableRef;
            }
            catch (Exception ex)
            {
                _log.Error(ex, "[{0}] more tries to initialize table storage remaining...", remainingTries);
                if (remainingTries == 0)
                    throw;
                await Task.Delay(RetryInterval[remainingTries]);
                return await InitCloudStorage(remainingTries - 1);
            }
        }

        private void NotifyTagChange(string tag)
        {
            if (_tagSubscribers.TryGetValue(tag, out var subscribers))
            {
                var changed = new TaggedEventAppended(tag);
                foreach (var subscriber in subscribers)
                    subscriber.Tell(changed);
            }
        }

        private void RemoveSubscriber(
            IActorRef subscriber)
        {
            _persistenceIdSubscribers = _persistenceIdSubscribers.SetItems(_persistenceIdSubscribers
                .Where(kv => kv.Value.Contains(subscriber))
                .Select(kv => new KeyValuePair<string, IImmutableSet<IActorRef>>(kv.Key, kv.Value.Remove(subscriber))));

            _tagSubscribers = _tagSubscribers.SetItems(_tagSubscribers
                .Where(kv => kv.Value.Contains(subscriber))
                .Select(kv => new KeyValuePair<string, IImmutableSet<IActorRef>>(kv.Key, kv.Value.Remove(subscriber))));

            _newEventsSubscriber.Remove(subscriber);
        }

        /// <summary>
        /// Replays all events with given tag within provided boundaries from current database.
        /// </summary>
        /// <param name="replay">TBD</param>
        /// <returns>TBD</returns>
        private async Task<long> ReplayTaggedMessagesAsync(
            ReplayTaggedMessages replay)
        {
            var query = GenerateTaggedMessageQuery(replay);

            // While we can specify the TakeCount, the CloudTable client does
            //    not really respect this fact and will keep pulling results.
            query.TakeCount =
                replay.Max > int.MaxValue
                    ? int.MaxValue
                    : (int)replay.Max;

            // In order to actually break at the limit we ask for we have to
            //    keep a separate counter and track it ourselves.
            var counter = 0;

            TableQuerySegment<EventTagEntry> result = null;

            var maxOrderingId = 0L;

            do
            {
                result = await Table.ExecuteQuerySegmentedAsync(query, result?.ContinuationToken);

                foreach (var entry in result.Results.OrderBy(x => x.UtcTicks))
                {
                    var deserialized = _serialization.PersistentFromBytes(entry.Payload);

                    var persistent =
                        new Persistent(
                            deserialized.Payload,
                            deserialized.SequenceNr,
                            deserialized.PersistenceId,
                            deserialized.Manifest,
                            deserialized.IsDeleted,
                            ActorRefs.NoSender,
                            deserialized.WriterGuid);

                    foreach (var adapted in AdaptFromJournal(persistent))
                    {
                        _log.Debug("Sending replayed message: persistenceId:{0} - sequenceNr:{1} - event:{2}",
                            deserialized.PersistenceId, deserialized.SequenceNr, deserialized.Payload);

                        replay.ReplyTo.Tell(new ReplayedTaggedMessage(adapted, replay.Tag, entry.UtcTicks), ActorRefs.NoSender);

                        counter++;
                    }

                    maxOrderingId = Math.Max(maxOrderingId, entry.UtcTicks);
                }

                if (counter >= replay.Max)
                {
                    break;
                }
            } while (result.ContinuationToken != null);

            return maxOrderingId;
        }

        private bool TryAddPersistenceId(
            string persistenceId)
        {
            lock (_allPersistenceIds)
            {
                return _allPersistenceIds.Add(persistenceId);
            }
        }
        private void NotifyPersistenceIdChange(string persistenceId)
        {
            if (_persistenceIdSubscribers.TryGetValue(persistenceId, out var subscribers))
            {
                var changed = new EventAppended(persistenceId);
                foreach (var subscriber in subscribers)
                    subscriber.Tell(changed);
            }
        }

    }
}