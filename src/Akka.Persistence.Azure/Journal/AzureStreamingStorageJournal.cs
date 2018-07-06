using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Akka.Actor;
using Akka.Event;
using Akka.Persistence.Azure.Util;
using Akka.Persistence.Journal;
using Akka.Streams;
using Akka.Streams.Dsl;
using Akka.Streams.Implementation.Fusing;
using Akka.Util.Internal;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;
using Debug = System.Diagnostics.Debug;

namespace Akka.Persistence.Azure.Journal
{
    public class AzureStreamingStorageJournal : WriteJournalBase
    {
        private readonly ILoggingAdapter _log = Context.GetLogger();
        private readonly SerializationHelper _serialization;
        private readonly AzureTableStorageJournalSettings _settings;
        private readonly CloudStorageAccount _storageAccount;
        private readonly Lazy<CloudTable> _tableStorage;

        private IActorRef _writeStream;

        public CloudTable Table => _tableStorage.Value;

        public AzureStreamingStorageJournal()
        {
            _settings = AzurePersistence.Get(Context.System).TableSettings;
            _serialization = new SerializationHelper(Context.System);
            _storageAccount = CloudStorageAccount.Parse(_settings.ConnectionString);

            _tableStorage = new Lazy<CloudTable>(() => InitCloudStorage().Result);
        }

        private async Task<CloudTable> InitCloudStorage()
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

        protected override bool Receive(object message)
        {
            switch (message)
            {
                case WriteMessages write:
                    HandleWrite(write);
                    return true;
                case ReplayMessages replay:
                    HandleReplay(replay);
                    return true;
                case DeleteMessagesTo delete:
                    HandleDelete(delete);
                    return true;
                default:
                    return false;
            }
        }

        private void HandleDelete(DeleteMessagesTo delete)
        {
            DeleteMessagesToAsync(delete);
        }

        private void HandleReplay(ReplayMessages replay)
        {
            ReplayMessagesAsync(replay, Context);
        }

        private void HandleWrite(WriteMessages write)
        {
            _writeStream.Tell(write);
        }

        protected override void PreStart()
        {
            _log.Debug("Initializing Azure Table Storage...");

            // forces loading of the value
            var name = Table.Name;

            _log.Debug("Successfully started Azure Table Storage!");

            _writeStream = CreateWriteStream();

            // need to call the base in order to ensure Akka.Persistence starts up correctly
            base.PreStart();
        }

        private IActorRef CreateWriteStream()
        {
            var source = Source.ActorRef<WriteMessages>(10000, OverflowStrategy.Fail);
            return source
                .GroupedWithin(20, TimeSpan.FromMilliseconds(50))
                .SelectAsync(10, WriteMessagesAsync)
                .WithAttributes(ActorAttributes.CreateSupervisionStrategy(cause => Akka.Streams.Supervision.Directive.Resume))
                .To(Sink.Ignore<bool>()).Run(Context.Materializer(namePrefix: "writeStream"));
        }

        protected async Task<bool> WriteMessagesAsync(IEnumerable<WriteMessages> messages)
        {
#if DEBUG
            _log.Debug("Entering method WriteMessagesAsync");
#endif
            foreach (var atomicWrites in messages)
            {
                IJournalResponse summary = null;
                var responses = new List<IJournalResponse>();

                foreach (var currentMsg in atomicWrites.Messages)
                {
                    if (currentMsg is AtomicWrite x)
                    {
                        var successMsgs = new List<WriteMessageSuccess>();
                        var batch = new TableBatchOperation();
                        Debug.Assert(currentMsg != null, nameof(currentMsg) + " != null");
                        foreach (var unadapted in (IImmutableList<IPersistentRepresentation>)x.Payload)
                        {
                            try
                            {
                                var persistent = AdaptToJournal(unadapted);
                                batch.Insert(
                                    new PersistentJournalEntry(persistent.PersistenceId,
                                        persistent.SequenceNr, _serialization.PersistentToBytes(persistent),
                                        persistent.Manifest));


                            }
                            catch (Exception ex)
                            {
                                summary = new WriteMessagesFailed(ex);
                                var failResp =
                                        new WriteMessageFailure(unadapted, ex, atomicWrites.ActorInstanceId);
                                responses.Add(failResp);
                                continue;
                            }

                            var successResp = new WriteMessageSuccess(unadapted, atomicWrites.ActorInstanceId);
                            successMsgs.Add(successResp);
                        }

                        if (_log.IsDebugEnabled && _settings.VerboseLogging)
                            _log.Debug("Attempting to write batch of {0} messages to Azure storage", batch.Count);

                        try
                        {
                            var results = await Table.ExecuteBatchAsync(batch);
                        }
                        catch (Exception ex)
                        {
                            summary = new WriteMessagesFailed(ex);
                            atomicWrites.PersistentActor.Tell(summary, ActorRefs.NoSender);
                            foreach (var msg in successMsgs)
                            {
                                responses.Add(new WriteMessageFailure(msg.Persistent, ex, msg.ActorInstanceId));
                            }

                            foreach (var rsp in responses)
                            {
                                atomicWrites.PersistentActor.Tell(rsp, ActorRefs.NoSender);
                            }

                            continue; // break out of this atomic write
                        }

                        summary = summary ?? WriteMessagesSuccessful.Instance;

                        responses.AddRange(successMsgs);
                        atomicWrites.PersistentActor.Tell(summary, ActorRefs.NoSender);
                        foreach (var rsp in responses)
                        {
                            atomicWrites.PersistentActor.Tell(rsp, ActorRefs.NoSender);
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
            return true;
        }

        public async Task<long> ReadHighestSequenceNrAsync(string persistenceId, long fromSequenceNr)
        {
#if DEBUG
            _log.Debug("Entering method ReadHighestSequenceNrAsync");
#endif
            var sequenceNumberQuery = GenerateHighestSequenceNumberQuery(persistenceId, fromSequenceNr);
            var result = await Table.ExecuteQuerySegmentedAsync(sequenceNumberQuery, null);
            long seqNo = 0L;

            do
            {
                if (result.Results.Count > 0)
                    seqNo = Math.Max(seqNo, result.Results.Max(x => x.SeqNo));

                if (result.ContinuationToken != null)
                    result = await Table.ExecuteQuerySegmentedAsync(sequenceNumberQuery, result.ContinuationToken);
            } while (result.ContinuationToken != null);

#if DEBUG
            _log.Debug("Leaving method ReadHighestSequenceNrAsync with SeqNo [{0}] for PersistentId [{1}]", seqNo, persistenceId);
#endif

            return seqNo;
        }

        private static TableQuery<PersistentJournalEntry> GenerateHighestSequenceNumberQuery(string persistenceId, long fromSequenceNr)
        {
            return new TableQuery<PersistentJournalEntry>()
                .Where(TableQuery.CombineFilters(TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, persistenceId),
                    TableOperators.And,
                    TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.GreaterThanOrEqual, fromSequenceNr.ToJournalRowKey())));
        }

        protected async Task DeleteMessagesToAsync(DeleteMessagesTo req)
        {
            try
            {
#if DEBUG
                _log.Debug("Entering method DeleteMessagesToAsync for persistentId [{0}] and up to seqNo [{1}]",
                    req.PersistenceId, req.ToSequenceNr);
#endif
                var deleteQuery = new TableQuery<PersistentJournalEntry>().Where(
                        TableQuery.CombineFilters(
                            TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, req.PersistenceId),
                            TableOperators.And,
                            TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.LessThanOrEqual,
                                req.ToSequenceNr.ToJournalRowKey())))
                    ;

                var nextQuery = Table.ExecuteQuerySegmentedAsync(deleteQuery, null);

                while (nextQuery != null)
                {
                    var queryResults = await nextQuery;

                    if (_log.IsDebugEnabled && _settings.VerboseLogging)
                    {
                        _log.Debug("Have [{0}] messages to delete for entity [{1}]", queryResults.Results.Count,
                            req.PersistenceId);
                    }

                    if (queryResults.ContinuationToken != null) // more data on the wire
                    {
                        nextQuery = Table.ExecuteQuerySegmentedAsync(deleteQuery, queryResults.ContinuationToken);
                    }
                    else
                    {
                        nextQuery = null;
                    }

                    if (queryResults.Results.Count > 0)
                    {
                        var tableBatchOperation = new TableBatchOperation();
                        foreach (var toBeDeleted in queryResults.Results)
                            tableBatchOperation.Delete(toBeDeleted);

                        var deleteTask = Table.ExecuteBatchAsync(tableBatchOperation);

                        await deleteTask;
                    }
                }

                var response = new DeleteMessagesSuccess(req.ToSequenceNr);
                req.PersistentActor.Tell(response);
            }
            catch (Exception ex)
            {
                var response = new DeleteMessagesFailure(ex, req.ToSequenceNr);
                req.PersistentActor.Tell(response, ActorRefs.NoSender);
            }
        }

        public async Task ReplayMessagesAsync(ReplayMessages req, IActorContext context)
        {
#if DEBUG
            _log.Debug("Entering method ReplayMessagesAsync for persistentId [{0}] from seqNo range [{1}, {2}] and taking up to max [{3}]", req.PersistenceId, req.FromSequenceNr, req.ToSequenceNr, req.Max);
#endif
            try
            {
                var highestSequenceNumber = await ReadHighestSequenceNrAsync(req.PersistenceId, req.FromSequenceNr);
                var toSequenceNr = Math.Min(req.ToSequenceNr, highestSequenceNumber);

                var max = toSequenceNr - req.FromSequenceNr;

                var replayQuery = ReplayQuery(req.PersistenceId, req.FromSequenceNr, toSequenceNr);

                var nextTask = Table.ExecuteQuerySegmentedAsync(replayQuery, null);
                var count = 0L;
                while (nextTask != null)
                {
                    var tableQueryResult = await nextTask;

#if DEBUG
                    if (_log.IsDebugEnabled && _settings.VerboseLogging)
                    {
                        _log.Debug("Recovered [{0}] messages for entity [{1}]", tableQueryResult.Results.Count, req.PersistenceId);
                    }
#endif

                    if (tableQueryResult.ContinuationToken != null)
                    {
                        if (_log.IsDebugEnabled && _settings.VerboseLogging)
                        {
                            _log.Debug("Have additional messages to download for entity [{0}]", req.PersistenceId);
                        }
                        // start the next query while we process the results of this one
                        nextTask = Table.ExecuteQuerySegmentedAsync(replayQuery, tableQueryResult.ContinuationToken);
                    }
                    else
                    {
                        if (_log.IsDebugEnabled && _settings.VerboseLogging)
                        {
                            _log.Debug("Completed download of messages for entity [{0}]", req.PersistenceId);
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

                        var deserialized = _serialization.PersistentFromBytes(savedEvent.Payload);
#if DEBUG
                        if (_log.IsDebugEnabled && _settings.VerboseLogging)
                        {
                            _log.Debug("Recovering [{0}] for entity [{1}].", deserialized, savedEvent.PartitionKey);
                        }
#endif
                        foreach (var adapted in AdaptFromJournal(deserialized))
                        {
                            req.PersistentActor.Tell(new ReplayedMessage(adapted), ActorRefs.NoSender);
                        }
                    }
                }

                var response = new RecoverySuccess(highestSequenceNumber);
                req.PersistentActor.Tell(response, ActorRefs.NoSender);
            }
            catch (Exception ex)
            {
                var response = new ReplayMessagesFailure(ex);
                req.PersistentActor.Tell(response, ActorRefs.NoSender);
            }

#if DEBUG
            _log.Debug("Leaving method ReplayMessagesAsync");
#endif
        }

        private static TableQuery<PersistentJournalEntry> ReplayQuery(string persistentId, long fromSequenceNumber,
            long toSequenceNumber)
        {
            return new TableQuery<PersistentJournalEntry>().Where(
                TableQuery.CombineFilters(
                    TableQuery.CombineFilters(
                        TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, persistentId),
                        TableOperators.And,
                        TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.GreaterThanOrEqual,
                            fromSequenceNumber.ToJournalRowKey())), TableOperators.And,
                    TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.LessThanOrEqual,
                        toSequenceNumber.ToJournalRowKey())));
        }

    }
}