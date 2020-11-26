
using Akka.Actor;

namespace Akka.Persistence.Azure.Query.Publishers
{
    internal sealed class CurrentAllEventsPublisher : AbstractAllEventsPublisher
    {
        public CurrentAllEventsPublisher(long fromOffset, int maxBufferSize, string writeJournalPluginId)
            : base(fromOffset, maxBufferSize, writeJournalPluginId)
        { }

        private long _toOffset = long.MaxValue;
        protected override long ToOffset => _toOffset;

        protected override void ReceiveInitialRequest()
        {
            Replay();
        }

        protected override void ReceiveIdleRequest()
        {
            Buffer.DeliverBuffer(TotalDemand);
            if (Buffer.IsEmpty && CurrentOffset > ToOffset)
                OnCompleteThenStop();
            else
                Self.Tell(AllEventsPublisher.Continue.Instance);
        }

        protected override void ReceiveRecoverySuccess(long highestOrderingNr)
        {
            Buffer.DeliverBuffer(TotalDemand);

            if (highestOrderingNr < ToOffset)
                _toOffset = highestOrderingNr;

            if (Buffer.IsEmpty && CurrentOffset >= ToOffset)
                OnCompleteThenStop();
            else
                Self.Tell(AllEventsPublisher.Continue.Instance);

            Context.Become(Idle);
        }

    }
}
