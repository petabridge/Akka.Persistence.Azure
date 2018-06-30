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
            var cts = new CancellationTokenSource(_settings.ConnectTimeout);
            if (await tableRef.CreateIfNotExistsAsync(new TableRequestOptions(), op, cts.Token))
                _log.Info("Created Azure Cloud Table", _settings.TableName);
            else
                _log.Info("Successfully connected to existing table", _settings.TableName);

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
            if (max == 0)
                return;

            var replayQuery = ReplayQuery(persistenceId, fromSequenceNr, toSequenceNr, (int) max);

            var nextTask = Table.ExecuteQuerySegmentedAsync(replayQuery, null);

            while (nextTask != null)
            {
                var tableQueryResult = await nextTask;

                // we have results
                if (tableQueryResult.Results.Count > 0)
                {
                    // and we have more data waiting on the wire
                    if (tableQueryResult.ContinuationToken != null)
                        nextTask = Table.ExecuteQuerySegmentedAsync(replayQuery, tableQueryResult.ContinuationToken);
                    else
                        nextTask = null; // query is finished

                    foreach (var savedEvent in tableQueryResult.Results)
                        recoveryCallback(_serialization.PersistentFromBytes(savedEvent.Payload));
                }
            }
        }

        private static TableQuery<PersistentJournalEntry> ReplayQuery(string persistentId, long fromSequenceNumber,
            long toSequenceNumber, int max)
        {
            return new TableQuery<PersistentJournalEntry>().Where(
                TableQuery.CombineFilters(
                    TableQuery.CombineFilters(
                        TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, persistentId),
                        TableOperators.And,
                        TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.GreaterThanOrEqual,
                            fromSequenceNumber.ToJournalRowKey())), TableOperators.And,
                    TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.LessThanOrEqual,
                        toSequenceNumber.ToJournalRowKey()))).Take(max);
        }

        public override async Task<long> ReadHighestSequenceNrAsync(string persistenceId, long fromSequenceNr)
        {
            var result = await Table.ExecuteQuerySegmentedAsync(
                new TableQuery<PersistentJournalEntry>()
                    .Where(TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, persistenceId))
                    .Take(1),
                null);

            if (result.Results.Count > 0)
                return result.Results.First().SeqNo;
            return 0L;
        }

        protected override async Task<IImmutableList<Exception>> WriteMessagesAsync(IEnumerable<AtomicWrite> messages)
        {
            var exceptions = ImmutableList<Exception>.Empty;
            using (var atomicWrites = messages.GetEnumerator())
            {
                while (atomicWrites.MoveNext())
                {
                    Debug.Assert(atomicWrites.Current != null, "atomicWrites.Current != null");

                    var batch = new TableBatchOperation();
                    using (var persistentMsgs = atomicWrites.Current.Payload
                        .AsInstanceOf<IImmutableList<IPersistentRepresentation>>().GetEnumerator())
                    {
                        while (persistentMsgs.MoveNext())
                        {
                            var currentMsg = persistentMsgs.Current;

                            Debug.Assert(currentMsg != null, nameof(currentMsg) + " != null");

                            batch.Insert(
                                new PersistentJournalEntry(currentMsg.PersistenceId,
                                    currentMsg.SequenceNr, _serialization.PersistentToBytes(currentMsg),
                                    currentMsg.Manifest));
                        }
                    }

                    try
                    {
                        var results = await Table.ExecuteBatchAsync(batch,
                            new TableRequestOptions {MaximumExecutionTime = _settings.RequestTimeout},
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
            }

            return exceptions;
        }

        protected override async Task DeleteMessagesToAsync(string persistenceId, long toSequenceNr)
        {
            var deleteQuery = new TableQuery<PersistentJournalEntry>().Where(
                    TableQuery.CombineFilters(
                        TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, persistenceId),
                        TableOperators.And,
                        TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.LessThanOrEqual,
                            toSequenceNr.ToJournalRowKey())))
                .Select(new[] {"PartitionKey", "RowKey"});

            async Task DeleteRows(Task<TableQuerySegment<PersistentJournalEntry>> queryTask)
            {
                var queryResults = await queryTask;
                if (queryResults.Results.Count > 0)
                {
                    var tableBatchOperation = new TableBatchOperation();
                    foreach (var toBeDeleted in queryResults.Results)
                        tableBatchOperation.Delete(toBeDeleted);

                    var deleteTask = Table.ExecuteBatchAsync(tableBatchOperation);
                    if (queryResults.ContinuationToken != null) // more data on the wire
                    {
                        var nextQuery = Table.ExecuteQuerySegmentedAsync(deleteQuery, queryResults.ContinuationToken);
                        await DeleteRows(nextQuery);
                    }

                    await deleteTask;
                }
            }

            await DeleteRows(Table.ExecuteQuerySegmentedAsync(deleteQuery, null));
        }
    }
}