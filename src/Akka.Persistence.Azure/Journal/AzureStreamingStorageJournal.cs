using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Akka.Actor;
using Akka.Event;
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

            // need to call the base in order to ensure Akka.Persistence starts up correctly
            base.PreStart();
        }

        private IActorRef CreateWriteStream()
        {
            var source = Source.ActorRef<WriteMessages>(10000, OverflowStrategy.Fail);
            var graph = source
                .GroupedWithin(20, TimeSpan.FromMilliseconds(50))
                .SelectAsync(10, writes =>
                {

                })
        }

        protected async Task<IEnumerable<IJournalResponse>> WriteMessagesAsync(IEnumerable<WriteMessages> messages)
        {
            IJournalResponse summary = null;
            var responses = new List<IJournalResponse>();

 
#if DEBUG
                _log.Debug("Entering method WriteMessagesAsync");
#endif
                var exceptions = ImmutableList<Exception>.Empty;
                foreach (var atomicWrites in messages)
                {
                    
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
                                    var failResp =
                                        new WriteMessageRejected(unadapted, ex, atomicWrites.ActorInstanceId);
                                    responses.Add(failResp);
                                    continue;
                                }

                                var successResp = new WriteMessageSuccess(unadapted, atomicWrites.ActorInstanceId);
                                successMsgs.Add(successResp);
                            }

                            if (_log.IsDebugEnabled && _settings.VerboseLogging)
                                _log.Debug("Attempting to write batch of {0} messages to Azure storage", batch.Count);

                            var results = await Table.ExecuteBatchAsync(batch);
                            
                            responses.AddRange(successMsgs);
                        }
                    }

                    try
                    {
                       

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
   
        }
    }
}