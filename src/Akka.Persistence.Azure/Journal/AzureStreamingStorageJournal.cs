using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Akka.Actor;
using Akka.Event;
using Akka.Persistence.Journal;
using Akka.Streams;
using Akka.Streams.Dsl;
using Akka.Streams.Implementation.Fusing;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;

namespace Akka.Persistence.Azure.Journal
{
    public class AzureStreamingStorageJournal : WriteJournalBase
    {
        private readonly ILoggingAdapter _log = Context.GetLogger();
        private readonly SerializationHelper _serialization;
        private readonly AzureTableStorageJournalSettings _settings;
        private readonly CloudStorageAccount _storageAccount;
        private readonly Lazy<CloudTable> _tableStorage;

        private long _resequencerCounter = 1L;
        private IActorRef _resequencer;
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
            throw new NotImplementedException();
        }

        private void HandleReplay(ReplayMessages replay)
        {
            
        }

        private void HandleWrite(WriteMessages write)
        {
            
        }

        protected override void PreStart()
        {
            _log.Debug("Initializing Azure Table Storage...");

            // forces loading of the value
            var name = Table.Name;

            _log.Debug("Successfully started Azure Table Storage!");

            _resequencer = Context.ActorOf(Props.Create(() => new Resequencer()));

            // need to call the base in order to ensure Akka.Persistence starts up correctly
            base.PreStart();
        }

        private static IActorRef CreateWriteStream(IActorRefFactory factory, IActorRef resequencer)
        {
            var source = Source.ActorRef<WriteWithCounter>(10000, OverflowStrategy.Fail);
            
        }

        internal sealed class WriteWithCounter
        {
            public WriteWithCounter(AtomicWrite write, long sequenceCounterNumber)
            {
                Write = write;
                SequenceCounterNumber = sequenceCounterNumber;
            }

            public AtomicWrite Write { get; }

            public long SequenceCounterNumber { get; }
        }

        internal sealed class Desequenced
        {
            public Desequenced(object message, long sequenceNr, IActorRef target, IActorRef sender)
            {
                Message = message;
                SequenceNr = sequenceNr;
                Target = target;
                Sender = sender;
            }

            public object Message { get; }

            public long SequenceNr { get; }

            public IActorRef Target { get; }

            public IActorRef Sender { get; }
        }

        internal class Resequencer : ActorBase
        {
            private readonly Dictionary<long, Desequenced> _delayed = new Dictionary<long, Desequenced>();
            private long _delivered = 0L;

            protected override bool Receive(object message)
            {
                if (message is Desequenced d)
                {
                    do
                    {
                        d = Resequence(d);
                    } while (d != null);
                    return true;
                }
                return false;
            }

            private Desequenced Resequence(Desequenced desequenced)
            {
                if (desequenced.SequenceNr == _delivered + 1)
                {
                    _delivered = desequenced.SequenceNr;
                    desequenced.Target.Tell(desequenced.Message, desequenced.Sender);
                }
                else
                {
                    _delayed.Add(desequenced.SequenceNr, desequenced);
                }

                var delivered = _delivered + 1;
                if (_delayed.TryGetValue(delivered, out Desequenced d))
                {
                    _delayed.Remove(delivered);
                    return d;
                }

                return null;
            }
        }
    }
}