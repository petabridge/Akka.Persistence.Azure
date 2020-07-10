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
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Debug = System.Diagnostics.Debug;

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
        private readonly HashSet<IActorRef> _allPersistenceIdSubscribers = new HashSet<IActorRef>();
        private readonly ILoggingAdapter _log = Context.GetLogger();
        private readonly Dictionary<string, ISet<IActorRef>> _persistenceIdSubscribers = new Dictionary<string, ISet<IActorRef>>();
        private readonly SerializationHelper _serialization;
        private readonly AzureTableStorageJournalSettings _settings;
        private readonly CloudStorageAccount _storageAccount;
        private readonly Lazy<CloudTable> _tableStorage;
        private readonly Dictionary<string, ISet<IActorRef>> _tagSubscribers = new Dictionary<string, ISet<IActorRef>>();

        public AzureTableStorageJournal()
        {
            _settings = AzurePersistence.Get(Context.System).TableSettings;
            _serialization = new SerializationHelper(Context.System);
            _storageAccount = CloudStorageAccount.Parse(_settings.ConnectionString);
            _tableStorage = new Lazy<CloudTable>(() => InitCloudStorage(5).Result);
        }

        public CloudTable Table => _tableStorage.Value;

        protected bool HasAllPersistenceIdSubscribers =>
            _allPersistenceIdSubscribers.Count != 0;

        protected bool HasPersistenceIdSubscribers =>
            _persistenceIdSubscribers.Count != 0;

        protected bool HasTagSubscribers =>
            _tagSubscribers.Count != 0;

        public override async Task<long> ReadHighestSequenceNrAsync(
            string persistenceId,
            long fromSequenceNr)
        {
            NotifyNewPersistenceIdAdded(persistenceId);

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

        public override async Task ReplayMessagesAsync(
            IActorContext context,
            string persistenceId,
            long fromSequenceNr,
            long toSequenceNr,
            long max,
            Action<IPersistentRepresentation> recoveryCallback)
        {
            NotifyNewPersistenceIdAdded(persistenceId);

            _log.Debug("Entering method ReplayMessagesAsync for persistentId [{0}] from seqNo range [{1}, {2}] and taking up to max [{3}]", persistenceId, fromSequenceNr, toSequenceNr, max);

            if (max == 0)
                return;

            var replayQuery = GeneratePersistentJournalEntryReplayQuery(persistenceId, fromSequenceNr, toSequenceNr);

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
                        return;
                    ++count;

                    var deserialized = _serialization.PersistentFromBytes(savedEvent.Payload);

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
                        _log.Debug("Recovering [{0}] for entity [{1}].", persistent, savedEvent.PartitionKey);
                    }

                    recoveryCallback(persistent);
                }
            }

            _log.Debug("Leaving method ReplayMessagesAsync");
        }

        protected override async Task DeleteMessagesToAsync(
            string persistenceId,
            long toSequenceNr)
        {
            NotifyNewPersistenceIdAdded(persistenceId);

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
            _log.Debug("Initializing Azure Table Storage...");

            // forces loading of the value
            var name = Table.Name;

            _log.Debug("Successfully started Azure Table Storage!");

            // need to call the base in order to ensure Akka.Persistence starts up correctly
            base.PreStart();
        }

        protected override bool ReceivePluginInternal(
            object message)
        {
            switch (message)
            {
                case ReplayTaggedMessages replay:
                    ReplayTaggedMessagesAsync(replay)
                        .PipeTo(replay.ReplyTo, success: h => new RecoverySuccess(h), failure: e => new ReplayMessagesFailure(e));
                    break;
                case SubscribePersistenceId subscribe:
                    AddPersistenceIdSubscriber(Sender, subscribe.PersistenceId);
                    Context.Watch(Sender);
                    break;
                case SubscribeAllPersistenceIds subscribe:
                    AddAllPersistenceIdSubscriber(Sender);
                    Context.Watch(Sender);
                    break;
                case SubscribeTag subscribe:
                    AddTagSubscriber(Sender, subscribe.Tag);
                    Context.Watch(Sender);
                    break;
                case Terminated terminated:
                    RemoveSubscriber(terminated.ActorRef);
                    break;
                default:
                    return false;
            }

            return true;
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

                    highSequenceNumbers.ForEach(x =>
                    {
                        var encodedKey = PartitionKeyEscapeHelper.Escape(x.Key);
                        allPersistenceIdsBatch.InsertOrReplace(new AllPersistenceIdsEntry(encodedKey));
                    });

                    var allPersistenceResults = await Table.ExecuteBatchAsync(allPersistenceIdsBatch);

                    if (_log.IsDebugEnabled && _settings.VerboseLogging)
                        foreach (var r in allPersistenceResults)
                            _log.Debug("Azure table storage wrote entity [{0}] with status code [{1}]", r.Etag, r.HttpStatusCode);

                    if (HasPersistenceIdSubscribers || HasAllPersistenceIdSubscribers)
                    {
                        highSequenceNumbers.ForEach(x => NotifyNewPersistenceIdAdded(x.Key));
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

        private async Task AddAllPersistenceIdSubscriber(
            IActorRef subscriber)
        {
            lock (_allPersistenceIdSubscribers)
            {
                _allPersistenceIdSubscribers.Add(subscriber);
            }
            subscriber.Tell(new CurrentPersistenceIds(await GetAllPersistenceIds()));
        }

        private void AddPersistenceIdSubscriber(
            IActorRef subscriber,
            string persistenceId)
        {
            if (!_persistenceIdSubscribers.TryGetValue(persistenceId, out var subscriptions))
            {
                subscriptions = new HashSet<IActorRef>();
                _persistenceIdSubscribers.Add(persistenceId, subscriptions);
            }

            subscriptions.Add(subscriber);
        }

        private void AddTagSubscriber(
            IActorRef subscriber,
            string tag)
        {
            if (!_tagSubscribers.TryGetValue(tag, out var subscriptions))
            {
                subscriptions = new HashSet<IActorRef>();
                _tagSubscribers.Add(tag, subscriptions);
            }

            subscriptions.Add(subscriber);
        }

        private async Task<IEnumerable<string>> GetAllPersistenceIds()
        {
            var query = GenerateAllPersistenceIdsQuery();

            TableQuerySegment result = null;

            var returnValue = ImmutableList<string>.Empty;

            do
            {
                result = await Table.ExecuteQuerySegmentedAsync(query, result?.ContinuationToken);

                if (result.Results.Count > 0)
                {
                    returnValue = returnValue.AddRange(result.Results.Select(x => x.RowKey));
                }
            } while (result.ContinuationToken != null);

            return returnValue;
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

        private void NotifyNewPersistenceIdAdded(
            string persistenceId)
        {
            var isNew = TryAddPersistenceId(persistenceId);
            if (isNew && HasAllPersistenceIdSubscribers)
            {
                var added = new PersistenceIdAdded(persistenceId);
                foreach (var subscriber in _allPersistenceIdSubscribers)
                    subscriber.Tell(added);
            }
        }

        private void NotifyTagChange(
            string tag)
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
            var pidSubscriptions = _persistenceIdSubscribers.Values.Where(x => x.Contains(subscriber));
            foreach (var subscription in pidSubscriptions)
                subscription.Remove(subscriber);

            var tagSubscriptions = _tagSubscribers.Values.Where(x => x.Contains(subscriber));
            foreach (var subscription in tagSubscriptions)
                subscription.Remove(subscriber);

            _allPersistenceIdSubscribers.Remove(subscriber);
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
    }
}