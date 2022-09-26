// -----------------------------------------------------------------------
// <copyright file="AzureTableStorageJournal.cs" company="Petabridge, LLC">
//      Copyright (C) 2015 - 2022 Petabridge, LLC <https://petabridge.com>
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
using Azure;
using Azure.Data.Tables;
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
        private readonly TableServiceClient _tableServiceClient;
        private TableClient _tableStorage_DoNotUseDirectly;
        private readonly Dictionary<string, ISet<IActorRef>> _tagSubscribers = new Dictionary<string, ISet<IActorRef>>();
        private readonly CancellationTokenSource _shutdownCts;

        public AzureTableStorageJournal(Config config = null)
        {
            _settings = config is null ? 
                AzurePersistence.Get(Context.System).TableSettings :
                AzureTableStorageJournalSettings.Create(config);

            var setup = Context.System.Settings.Setup.Get<AzureTableStorageJournalSetup>();
            if (setup.HasValue)
                _settings = setup.Value.Apply(_settings);
            
            _serialization = new SerializationHelper(Context.System);

            if (_settings.Development)
            {
                _tableServiceClient = new TableServiceClient(connectionString: "UseDevelopmentStorage=true");
            }
            else
            {
                // Use TokenCredential if both ServiceUri and TokenCredential are populated in the settings 
                _tableServiceClient = _settings.ServiceUri != null && _settings.AzureCredential != null
                    ? new TableServiceClient(
                        endpoint: _settings.ServiceUri,
                        tokenCredential: _settings.AzureCredential,
                        options: _settings.TableClientOptions)
                    : new TableServiceClient(connectionString: _settings.ConnectionString);
            }

            _shutdownCts = new CancellationTokenSource();
        }

        public TableClient Table
        {
            get
            {
                if (_tableStorage_DoNotUseDirectly == null)
                    throw new Exception("Table storage has not been initialized yet. PreStart() has not been invoked");
                return _tableStorage_DoNotUseDirectly;
            }
        }

        protected bool HasAllPersistenceIdSubscribers => _allPersistenceIdSubscribers.Count != 0;

        protected bool HasPersistenceIdSubscribers => _persistenceIdSubscribers.Count != 0;

        protected bool HasTagSubscribers => _tagSubscribers.Count != 0;

        public override async Task<long> ReadHighestSequenceNrAsync(
            string persistenceId,
            long fromSequenceNr)
        {
            NotifyNewPersistenceIdAdded(persistenceId);

            _log.Debug("Entering method ReadHighestSequenceNrAsync");

            var seqNo = await HighestSequenceNumberQuery(persistenceId, null, _shutdownCts.Token)
                .Select(entity => entity.GetInt64(HighestSequenceNrEntry.HighestSequenceNrKey).Value)
                .AggregateAsync(0L, Math.Max, cancellationToken: _shutdownCts.Token);
            
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

            _log.Debug("Entering method ReplayMessagesAsync for persistentId [{0}] from seqNo range [{1}, {2}] and taking up to max [{3}]", 
                persistenceId, fromSequenceNr, toSequenceNr, max);

            if (max == 0)
                return;

            var pages = PersistentJournalEntryReplayQuery(persistenceId, fromSequenceNr, toSequenceNr, null, _shutdownCts.Token)
                .AsPages().GetAsyncEnumerator(_shutdownCts.Token);

            ValueTask<bool>? nextTask = pages.MoveNextAsync();
            var count = 0L;
            while(nextTask.HasValue)
            {
                await nextTask.Value;
                var currentPage = pages.Current;
                    
                if (_log.IsDebugEnabled && _settings.VerboseLogging)
                {
                    _log.Debug("Recovered [{0}] messages for entity [{1}]", currentPage.Values.Count, persistenceId);
                }

                if (currentPage.ContinuationToken != null)
                {
                    if (_log.IsDebugEnabled && _settings.VerboseLogging)
                    {
                        _log.Debug("Have additional messages to download for entity [{0}]", persistenceId);
                    }
                    // start the next query while we process the results of this one
                    nextTask = pages.MoveNextAsync();
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

                foreach (var entity in currentPage.Values)
                {
                    // check if we've hit max recovery
                    if (count >= max)
                        return;
                    ++count;

                    var savedEvent = new PersistentJournalEntry(entity);
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
                            deserialized.WriterGuid,
                            timestamp: savedEvent.UtcTicks);

                    if (_log.IsDebugEnabled && _settings.VerboseLogging)
                    {
                        _log.Debug("Recovering [{0}] for entity [{1}].", persistent, savedEvent.PartitionKey);
                    }

                    recoveryCallback(persistent);
                }
            }

            _log.Debug("Leaving method ReplayMessagesAsync");
        }

        protected override async Task DeleteMessagesToAsync(string persistenceId, long toSequenceNr)
        {
            NotifyNewPersistenceIdAdded(persistenceId);

            _log.Debug("Entering method DeleteMessagesToAsync for persistentId [{0}] and up to seqNo [{1}]", persistenceId, toSequenceNr);

            var pages = PersistentJournalEntryDeleteQuery(persistenceId, toSequenceNr, null, _shutdownCts.Token)
                .AsPages().GetAsyncEnumerator(_shutdownCts.Token);

            ValueTask<bool>? nextTask = pages.MoveNextAsync();
            while (nextTask.HasValue)
            {
                await nextTask.Value;
                var currentPage = pages.Current;

                if (_log.IsDebugEnabled && _settings.VerboseLogging)
                {
                    _log.Debug("Have [{0}] messages to delete for entity [{1}]", currentPage.Values.Count, persistenceId);
                }

                if (currentPage.ContinuationToken != null)
                    nextTask = pages.MoveNextAsync();
                else
                    nextTask = null;

                var response = await Table.ExecuteBatchAsLimitedBatches(currentPage.Values
                    .Select(entity => new TableTransactionAction(TableTransactionActionType.Delete, entity)).ToList(), _shutdownCts.Token);
                
                if (_log.IsDebugEnabled && _settings.VerboseLogging)
                {
                    foreach (var r in response)
                    {
                        _log.Debug("Azure table storage wrote entities with status code [{0}]", r.Status);
                    }
                }
            }

            _log.Debug("Leaving method DeleteMessagesToAsync for persistentId [{0}] and up to seqNo [{1}]", persistenceId, toSequenceNr);
        }

        protected override void PreStart()
        {
            _log.Debug("Initializing Azure Table Storage...");

            InitCloudStorage(5, _shutdownCts.Token)
                .ConfigureAwait(false).GetAwaiter().GetResult();

            _log.Debug("Successfully started Azure Table Storage!");

            // need to call the base in order to ensure Akka.Persistence starts up correctly
            base.PreStart();
        }

        protected override void PostStop()
        {
            _shutdownCts.Cancel();
            _shutdownCts.Dispose();
            base.PostStop();
        }

        protected override bool ReceivePluginInternal(object message)
        {
            switch (message)
            {
                case ReplayTaggedMessages replay:
                    ReplayTaggedMessagesAsync(replay, _shutdownCts.Token)
                        .PipeTo(replay.ReplyTo, success: h => new RecoverySuccess(h), failure: e => new ReplayMessagesFailure(e));
                    break;
                case SubscribePersistenceId subscribe:
                    AddPersistenceIdSubscriber(Sender, subscribe.PersistenceId);
                    Context.Watch(Sender);
                    break;
                case SubscribeAllPersistenceIds _:
                    AddAllPersistenceIdSubscriber(Sender, _shutdownCts.Token);
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

        protected override async Task<IImmutableList<Exception>> WriteMessagesAsync(IEnumerable<AtomicWrite> atomicWrites)
        {
            try
            {
                var taggedEntries = new Dictionary<string, List<EventTagEntry>>();
                var exceptions = new List<Exception>();
                var highSequenceNumbers = new Dictionary<string, long>();

                using (var currentWrites = atomicWrites.GetEnumerator())
                {
                    while (currentWrites.MoveNext())
                    {
                        Debug.Assert(currentWrites.Current != null, "atomicWrites.Current != null");

                        var list = (IImmutableList<IPersistentRepresentation>) currentWrites.Current.Payload;
                        var batchItems = new List<TableTransactionAction>();
                        foreach (var t in list)
                        {
                            var item = t;
                            Debug.Assert(item != null, nameof(item) + " != null");

                            string[] tags = {};
                            // If the payload is a tagged payload, reset to a non-tagged payload
                            if (item.Payload is Tagged tagged)
                            {
                                item = item.WithPayload(tagged.Payload);
                                if (tagged.Tags.Count > 0)
                                    tags = tagged.Tags.ToArray();
                            }

                            var payloadBytes = _serialization.PersistentToBytes(item);
                            
                            var newItem = new PersistentJournalEntry(
                                persistentId: item.PersistenceId, 
                                seqNo: item.SequenceNr,
                                payload: payloadBytes,
                                manifest: item.Manifest,
                                tags: tags);

                            batchItems.Add(new TableTransactionAction(TableTransactionActionType.Add, newItem.WriteEntity()));

                            foreach (var tag in tags)
                            {
                                if (!taggedEntries.ContainsKey(tag))
                                    taggedEntries[tag] = new List<EventTagEntry>();

                                taggedEntries[tag].Add(new EventTagEntry(
                                    persistenceId: newItem.PartitionKey,
                                    tag: tag,
                                    seqNo: newItem.SeqNo,
                                    payload: newItem.Payload,
                                    manifest: newItem.Manifest,
                                    utcTicks: newItem.UtcTicks));
                            }

                            highSequenceNumbers[item.PersistenceId] = item.SequenceNr;
                        }

                        try
                        {
                            foreach (var entry in highSequenceNumbers)
                            {
                                batchItems.Add(new TableTransactionAction(
                                    TableTransactionActionType.UpsertReplace,
                                    new HighestSequenceNrEntry(entry.Key, entry.Value).WriteEntity()));
                            }

                            // Encode partition keys for writing
                            foreach (var action in batchItems)
                            {
                                action.Entity.PartitionKey = PartitionKeyEscapeHelper.Escape(action.Entity.PartitionKey);
                            }

                            if (_log.IsDebugEnabled && _settings.VerboseLogging)
                                _log.Debug("Attempting to write batch of {0} messages to Azure storage", batchItems.Count);

                            var response = await Table.ExecuteBatchAsLimitedBatches(batchItems, _shutdownCts.Token);
                            if (_log.IsDebugEnabled && _settings.VerboseLogging)
                            {
                                foreach (var r in response)
                                {
                                    _log.Debug("Azure table storage wrote entities with status code [{0}]", r.Status);
                                }
                            }

                            exceptions.Add(null);
                        }
                        catch (Exception ex)
                        {
                            _log.Warning(ex, "Failure while writing messages to Azure table storage");
                            exceptions.Add(ex);
                        }
                    }
                }

                if (exceptions.All(ex => ex == null))
                {
                    var allPersistenceIdsBatch = new List<TableTransactionAction>();

                    foreach (var item in highSequenceNumbers)
                    {
                        allPersistenceIdsBatch.Add(new TableTransactionAction(
                            TableTransactionActionType.UpdateReplace,
                            new AllPersistenceIdsEntry(PartitionKeyEscapeHelper.Escape(item.Key)).WriteEntity()));
                    }

                    var allPersistenceResponse = await Table.ExecuteBatchAsLimitedBatches(allPersistenceIdsBatch, _shutdownCts.Token);

                    if (_log.IsDebugEnabled && _settings.VerboseLogging)
                        foreach (var r in allPersistenceResponse)
                            _log.Debug("Azure table storage wrote entity with status code [{0}]", r.Status);

                    if (HasPersistenceIdSubscribers || HasAllPersistenceIdSubscribers)
                    {
                        highSequenceNumbers.ForEach(x => NotifyNewPersistenceIdAdded(x.Key));
                    }

                    if (taggedEntries.Count > 0)
                    {
                        foreach (var kvp in taggedEntries)
                        {
                            var eventTagsBatch = new List<TableTransactionAction>();

                            foreach (var item in kvp.Value)
                            {
                                eventTagsBatch.Add(new TableTransactionAction(TableTransactionActionType.UpsertReplace, item.WriteEntity()));
                            }

                            var eventTagsResponse = await Table.ExecuteBatchAsLimitedBatches(eventTagsBatch, _shutdownCts.Token);

                            if (_log.IsDebugEnabled && _settings.VerboseLogging)
                                foreach (var r in eventTagsResponse)
                                    _log.Debug("Azure table storage wrote entity with status code [{0}]", r.Status);

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
                return exceptions.Any(ex => ex != null) ? exceptions.ToImmutableList() : null;
            }
            catch (Exception ex)
            {
                _log.Error(ex, "Error during WriteMessagesAsync");
                throw;
            }
        }

        private AsyncPageable<TableEntity> GenerateAllPersistenceIdsQuery(
            int? maxPerPage,
            CancellationToken cancellationToken)
        {
            return Table.QueryAsync<TableEntity>(
                filter: $"PartitionKey eq '{AllPersistenceIdsEntry.PartitionKeyValue}'",
                maxPerPage: maxPerPage,
                @select: new[] { "RowKey" }, 
                cancellationToken: cancellationToken
            );
        }

        private AsyncPageable<TableEntity> HighestSequenceNumberQuery(
            string persistenceId,
            int? maxPerPage,
            CancellationToken cancellationToken)
        {
            return Table.QueryAsync<TableEntity>(
                filter: $"PartitionKey eq '{PartitionKeyEscapeHelper.Escape(persistenceId)}' and " +
                        $"RowKey eq '{HighestSequenceNrEntry.RowKeyValue}'",
                maxPerPage: maxPerPage,
                @select: null,
                cancellationToken: cancellationToken
            );
        }

        private AsyncPageable<TableEntity> PersistentJournalEntryDeleteQuery(
            string persistenceId,
            long toSequenceNr,
            int? maxPerPage,
            CancellationToken cancellationToken)
        {
            return Table.QueryAsync<TableEntity>(
                filter: $"PartitionKey eq '{PartitionKeyEscapeHelper.Escape(persistenceId)}' and " +
                        $"RowKey ne '{HighestSequenceNrEntry.RowKeyValue}' and " +
                        $"RowKey le '{toSequenceNr.ToJournalRowKey()}'",
                maxPerPage: maxPerPage,
                @select: null,
                cancellationToken: cancellationToken);
        }

        private AsyncPageable<TableEntity> EventTagEntryDeleteQuery(
            string persistenceId,
            long fromSequenceNr,
            long toSequenceNr,
            int? maxPerPage,
            CancellationToken cancellationToken)
        {
            return Table.QueryAsync<TableEntity>(
                filter: $"PartitionKey eq '{EventTagEntry.PartitionKeyValue}' and " +
                        $"{EventTagEntry.IdxPartitionKeyKeyName} eq '{persistenceId}' and " +
                        $"{EventTagEntry.IdxRowKeyKeyName} ge '{fromSequenceNr.ToJournalRowKey()}' and " +
                        $"{EventTagEntry.IdxRowKeyKeyName} le '{toSequenceNr.ToJournalRowKey()}'",
                maxPerPage: maxPerPage,
                @select: null,
                cancellationToken: cancellationToken);
        }

        private AsyncPageable<TableEntity> PersistentJournalEntryReplayQuery(
            string persistentId,
            long fromSequenceNumber,
            long toSequenceNumber,
            int? maxPerPage,
            CancellationToken cancellationToken)
        {
            var filter = $"PartitionKey eq '{PartitionKeyEscapeHelper.Escape(persistentId)}' and " +
                         $"RowKey ne '{HighestSequenceNrEntry.RowKeyValue}'";
            
            if (fromSequenceNumber > 0)
                filter += $" and RowKey ge '{fromSequenceNumber.ToJournalRowKey()}'"; 

            if (toSequenceNumber < long.MaxValue)
                filter += $" and RowKey le '{toSequenceNumber.ToJournalRowKey()}'";

            return Table.QueryAsync<TableEntity>(filter, maxPerPage, null, cancellationToken);
        }

        private AsyncPageable<TableEntity> TaggedMessageQuery(
            ReplayTaggedMessages replay,
            int? maxPerPage,
            CancellationToken cancellationToken)
        {
            return Table.QueryAsync<TableEntity>(
                filter: $"PartitionKey eq '{PartitionKeyEscapeHelper.Escape(EventTagEntry.GetPartitionKey(replay.Tag))}' and " +
                        $"RowKey gt '{replay.FromOffset.ToJournalRowKey()}{EventTagEntry.AsciiIncrementedDelimiter}' and " +
                        $"RowKey le '{replay.ToOffset.ToJournalRowKey()}{EventTagEntry.Delimiter}'",
                maxPerPage: maxPerPage,
                @select: null,
                cancellationToken: cancellationToken);
        }

        private async Task AddAllPersistenceIdSubscriber(IActorRef subscriber, CancellationToken cancellationToken)
        {
            lock (_allPersistenceIdSubscribers)
            {
                _allPersistenceIdSubscribers.Add(subscriber);
            }
            subscriber.Tell(new CurrentPersistenceIds(await GetAllPersistenceIds(cancellationToken)));
        }

        private void AddPersistenceIdSubscriber(IActorRef subscriber, string persistenceId)
        {
            if (!_persistenceIdSubscribers.TryGetValue(persistenceId, out var subscriptions))
            {
                subscriptions = new HashSet<IActorRef>();
                _persistenceIdSubscribers.Add(persistenceId, subscriptions);
            }

            subscriptions.Add(subscriber);
        }

        private void AddTagSubscriber(IActorRef subscriber, string tag)
        {
            if (!_tagSubscribers.TryGetValue(tag, out var subscriptions))
            {
                subscriptions = new HashSet<IActorRef>();
                _tagSubscribers.Add(tag, subscriptions);
            }

            subscriptions.Add(subscriber);
        }

        private async Task<IEnumerable<string>> GetAllPersistenceIds(CancellationToken cancellationToken)
        {
            return await GenerateAllPersistenceIdsQuery(null, cancellationToken)
                .Select(item => item.RowKey).ToListAsync(cancellationToken);
        }

        private async Task InitCloudStorage(int remainingTries, CancellationToken cancellationToken)
        {
            try
            {
                var tableClient = _tableServiceClient.GetTableClient(_settings.TableName);
                
                var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
                cts.CancelAfter(_settings.ConnectTimeout);
                using (cts)
                {
                    if (!_settings.AutoInitialize)
                    {
                        var exists = await IsTableExist(_settings.TableName, cts.Token);

                        if (!exists)
                        {
                            remainingTries = 0;
                            
                            throw new Exception(
                                $"Table {_settings.TableName} doesn't exist. Either create it or turn auto-initialize on");
                        }
                        
                        _log.Info("Successfully connected to existing table", _settings.TableName);

                        _tableStorage_DoNotUseDirectly = tableClient;
                        return;
                    }
                    
                    if (await tableClient.CreateIfNotExistsAsync(cts.Token) != null)
                        _log.Info("Created Azure Cloud Table", _settings.TableName);
                    else
                        _log.Info("Successfully connected to existing table", _settings.TableName);
                    _tableStorage_DoNotUseDirectly = tableClient;
                }
            }
            catch (Exception ex)
            {
                _log.Error(ex, "[{0}] more tries to initialize table storage remaining...", remainingTries);
                if (remainingTries == 0)
                    throw;
                
                await Task.Delay(RetryInterval[remainingTries], cancellationToken);
                if (cancellationToken.IsCancellationRequested)
                    throw;
                
                await InitCloudStorage(remainingTries - 1, cancellationToken);
            }
        }

        private async Task<bool> IsTableExist(string name, CancellationToken cancellationToken)
        {
            var tables = await _tableServiceClient.QueryAsync(t => t.Name == name, cancellationToken: cancellationToken)
                .ToListAsync(cancellationToken)
                .ConfigureAwait(false);
            return tables.Count > 0;
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
        /// <param name="cancellationToken"></param>
        /// <returns>TBD</returns>
        private async Task<long> ReplayTaggedMessagesAsync(ReplayTaggedMessages replay, CancellationToken cancellationToken)
        {
            // In order to actually break at the limit we ask for we have to
            //    keep a separate counter and track it ourselves.
            var counter = 0;
            var maxOrderingId = 0L;

            var pages = TaggedMessageQuery(replay, null, cancellationToken).AsPages().GetAsyncEnumerator(cancellationToken);
            ValueTask<bool>? nextTask = pages.MoveNextAsync();
            
            while (nextTask != null)
            {
                await nextTask.Value;
                var currentPage = pages.Current;

                if (currentPage.ContinuationToken != null)
                    nextTask = pages.MoveNextAsync();
                else
                    nextTask = null;
                
                foreach (var entry in currentPage.Values.Select(entity => new EventTagEntry(entity)).OrderBy(x => x.UtcTicks))
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
                            deserialized.WriterGuid,
                            timestamp: entry.UtcTicks);

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
            }

            return maxOrderingId;
        }

        private bool TryAddPersistenceId(string persistenceId)
        {
            lock (_allPersistenceIds)
            {
                return _allPersistenceIds.Add(persistenceId);
            }
        }
    }
}