using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Akka.Actor;
using Akka.Event;
using Akka.Persistence.Journal;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;

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

        protected override Task<IImmutableList<Exception>> WriteMessagesAsync(IEnumerable<AtomicWrite> messages)
        {
            throw new NotImplementedException();
            foreach (var write in messages)
            {

            }
        }

        protected override Task DeleteMessagesToAsync(string persistenceId, long toSequenceNr)
        {
            throw new NotImplementedException();
        }
    }
}
