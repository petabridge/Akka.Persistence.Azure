// -----------------------------------------------------------------------
// <copyright file="AzureTableStorageJournal.cs" company="Petabridge, LLC">
//      Copyright (C) 2015 - 2018 Petabridge, LLC <https://petabridge.com>
// </copyright>
// -----------------------------------------------------------------------

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
using Akka.Util.Internal;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;
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
        private readonly ILoggingAdapter _log = Context.GetLogger();
        private readonly SerializationHelper _serialization;
        private readonly AzureTableStorageJournalSettings _settings;
        private readonly CloudStorageAccount _storageAccount;
        private readonly Lazy<CloudTable> _tableStorage;

        public AzureTableStorageJournal()
        {
            _settings = AzurePersistence.Get(Context.System).TableSettings;
            _serialization = new SerializationHelper(Context.System);
            _storageAccount = CloudStorageAccount.Parse(_settings.ConnectionString);

            _tableStorage = new Lazy<CloudTable>(() => InitCloudStorage().Result);
        }

        public CloudTable Table => _tableStorage.Value;

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

        protected override void PreStart()
        {
            _log.Debug("Initializing Azure Table Storage...");

            // forces loading of the value
            var name = Table.Name;

            _log.Debug("Successfully started Azure Table Storage!");

            // need to call the base in order to ensure Akka.Persistence starts up correctly
            base.PreStart();
        }

        public override async Task ReplayMessagesAsync(IActorContext context, string persistenceId, long fromSequenceNr,
            long toSequenceNr, long max,
            Action<IPersistentRepresentation> recoveryCallback)
        {
#if DEBUG
            _log.Debug("Entering method ReplayMessagesAsync for persistentId [{0}] from seqNo range [{1}, {2}] and taking up to max [{3}]", persistenceId, fromSequenceNr, toSequenceNr, max);
#endif
            if (max == 0)
                return;

            var replayQuery = ReplayQuery(persistenceId, fromSequenceNr, toSequenceNr);

            var nextTask = Table.ExecuteQuerySegmentedAsync(replayQuery, null);
            var count = 0L;
            while (nextTask != null)
            {
                var tableQueryResult = await nextTask;

#if DEBUG
                if (_log.IsDebugEnabled && _settings.VerboseLogging)
                {
                    _log.Debug("Recovered [{0}] messages for entity [{1}]", tableQueryResult.Results.Count, persistenceId);
                }
#endif

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
#if DEBUG
                    if (_log.IsDebugEnabled && _settings.VerboseLogging)
                    {
                        _log.Debug("Recovering [{0}] for entity [{1}].", deserialized, savedEvent.PartitionKey);
                    }
#endif
                    recoveryCallback(deserialized);

                }
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

        public override async Task<long> ReadHighestSequenceNrAsync(string persistenceId, long fromSequenceNr)
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

                if(result.ContinuationToken != null)
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

        protected override async Task<IImmutableList<Exception>> WriteMessagesAsync(IEnumerable<AtomicWrite> messages)
        {
            try
            {
#if DEBUG
                _log.Debug("Entering method WriteMessagesAsync");
#endif
                var exceptions = ImmutableList<Exception>.Empty;
                using (var atomicWrites = messages.GetEnumerator())
                {
                    var batch = new TableBatchOperation();
                    while (atomicWrites.MoveNext())
                    {
                        Debug.Assert(atomicWrites.Current != null, "atomicWrites.Current != null");

                        
                        foreach(var currentMsg in atomicWrites.Current.Payload
                            .AsInstanceOf<IImmutableList<IPersistentRepresentation>>())
                        {

                                Debug.Assert(currentMsg != null, nameof(currentMsg) + " != null");

                                batch.Insert(
                                    new PersistentJournalEntry(currentMsg.PersistenceId,
                                        currentMsg.SequenceNr, _serialization.PersistentToBytes(currentMsg),
                                        currentMsg.Manifest));
                            
                        }
                    }

                    try
                    {
                            if (_log.IsDebugEnabled && _settings.VerboseLogging)
                                _log.Debug("Attempting to write batch of {0} messages to Azure storage", batch.Count);

                        var results = await Table.ExecuteBatchAsync(batch,
                            new TableRequestOptions { MaximumExecutionTime = _settings.RequestTimeout },
                            new OperationContext());

                        if (_log.IsDebugEnabled && _settings.VerboseLogging)
                            foreach (var r in results)
                                _log.Debug("Azure table storage wrote entity [{0}] with status code [{1}]", r.Etag,
                                    r.HttpStatusCode);
                    }
                    catch (Exception ex)
                    {
                        exceptions = exceptions.Add(ex);
                    }
                }

#if DEBUG
                _log.Debug("Leaving method WriteMessagesAsync");
                foreach (var ex in exceptions)
                {
                    _log.Error(ex, "recorded exception during write");
                }
#endif

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

        protected override async Task DeleteMessagesToAsync(string persistenceId, long toSequenceNr)
        {
#if DEBUG
            _log.Debug("Entering method DeleteMessagesToAsync for persistentId [{0}] and up to seqNo [{1}]", persistenceId, toSequenceNr);
#endif
            var deleteQuery = new TableQuery<PersistentJournalEntry>().Where(
                    TableQuery.CombineFilters(
                        TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, persistenceId),
                        TableOperators.And,
                        TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.LessThanOrEqual,
                            toSequenceNr.ToJournalRowKey())))
                ;

            var nextQuery = Table.ExecuteQuerySegmentedAsync(deleteQuery, null);

            while (nextQuery != null)
            {
                var queryResults = await nextQuery;

                if (_log.IsDebugEnabled && _settings.VerboseLogging)
                {
                    _log.Debug("Have [{0}] messages to delete for entity [{1}]", queryResults.Results.Count, persistenceId);
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
            

           

#if DEBUG
            _log.Debug("Leaving method DeleteMessagesToAsync for persistentId [{0}] and up to seqNo [{1}]", persistenceId, toSequenceNr);
#endif
        }
    }
}