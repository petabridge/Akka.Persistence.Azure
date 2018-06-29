using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Text;
using System.Threading.Tasks;
using Akka.Actor;
using Akka.Persistence.Journal;

namespace Akka.Persistence.Azure.Journal
{
    public class AzureTableStorageJournal : AsyncWriteJournal
    {
        private readonly SerializationHelper _serialization;

        public override Task ReplayMessagesAsync(IActorContext context, string persistenceId, long fromSequenceNr, long toSequenceNr, long max,
            Action<IPersistentRepresentation> recoveryCallback)
        {
            throw new NotImplementedException();
        }

        public override Task<long> ReadHighestSequenceNrAsync(string persistenceId, long fromSequenceNr)
        {
            throw new NotImplementedException();
        }

        protected override Task<IImmutableList<Exception>> WriteMessagesAsync(IEnumerable<AtomicWrite> messages)
        {
            throw new NotImplementedException();
        }

        protected override Task DeleteMessagesToAsync(string persistenceId, long toSequenceNr)
        {
            throw new NotImplementedException();
        }
    }
}
