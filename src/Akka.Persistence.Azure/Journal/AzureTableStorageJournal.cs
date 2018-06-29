using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Akka.Actor;
using Akka.Event;
using Akka.Persistence.Journal;
using Akka.Util.Internal;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;
using Debug = System.Diagnostics.Debug;

namespace Akka.Persistence.Azure.Journal
{
    public class AzureTableStorageJournal : AsyncWriteJournal
    {
        private readonly SerializationHelper _serialization;
        private readonly AzureTableStorageJournalSettings _settings;
        private readonly CloudStorageAccount _storageAccount;
        private readonly Lazy<CloudTable> _tableStorage;
        private readonly ILoggingAdapter _log = Context.GetLogger();

        public CloudTable Table => _tableStorage.Value;

        public AzureTableStorageJournal(AzureTableStorageJournalSettings settings)
        {
            _settings = settings;
            _serialization = new SerializationHelper(Context.System);
            _storageAccount = CloudStorageAccount.Parse(settings.ConnectionString);

            _tableStorage = new Lazy<CloudTable>(() => InitCloudStorage().Result);
        }

        private async Task<CloudTable> InitCloudStorage()
        {
            var tableClient = _storageAccount.CreateCloudTableClient();
            var tableRef = tableClient.GetTableReference(_settings.TableName);
            var op = new OperationContext();
            var cts = new CancellationTokenSource(_settings.ConnectTimeout);
            if (await tableRef.CreateIfNotExistsAsync(new TableRequestOptions(), op, cts.Token))
            {
                _log.Info("Created Azure Cloud Table", _settings.TableName);
            }
            else
            {
                _log.Info("Successfully connected to existing table", _settings.TableName);
            }

            return tableRef;
        }

        public override async Task ReplayMessagesAsync(IActorContext context, string persistenceId, long fromSequenceNr, long toSequenceNr, long max,
            Action<IPersistentRepresentation> recoveryCallback)
        {
            if (max == 0)
                return;
            throw new NotImplementedException();

        }

        public override Task<long> ReadHighestSequenceNrAsync(string persistenceId, long fromSequenceNr)
        {
            //Table.ExecuteAsync(TableOperation.Retrieve<PersistentJournalEntry>(persistenceId, ))
            throw new NotImplementedException();
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
                                    currentMsg.SequenceNr, _serialization.PersistentToBytes(currentMsg), currentMsg.Manifest));
                        }
                    }

                    try
                    {
                        var results = await Table.ExecuteBatchAsync(batch,
                            new TableRequestOptions() {MaximumExecutionTime = _settings.RequestTimeout},
                            new OperationContext());

                        if (_log.IsDebugEnabled && _settings.VerboseLogging)
                        {
                            foreach (var r in results)
                            {
                                _log.Debug("Azure table storage wrote entity [{0}] with status code [{1}]", r.Etag, r.HttpStatusCode);
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        exceptions = exceptions.Add(ex);
                    }
                }
            }

            return exceptions;
        }

        protected override Task DeleteMessagesToAsync(string persistenceId, long toSequenceNr)
        {
            throw new NotImplementedException();
        }
    }
}
