using Akka.Actor;
using System;

namespace Akka.Persistence.Azure.Query.Publishers
{
    internal sealed class LiveAllEventsPublisher : AbstractAllEventsPublisher
    {
        private readonly ICancelable _tickCancelable;
        public LiveAllEventsPublisher(long fromOffset, TimeSpan refreshInterval, int maxBufferSize, string writeJournalPluginId)
            : base(fromOffset, maxBufferSize, writeJournalPluginId)
        {
            _tickCancelable = Context.System.Scheduler.ScheduleTellRepeatedlyCancelable(refreshInterval, refreshInterval, Self, AllEventsPublisher.Continue.Instance, Self);
        }

        protected override long ToOffset => long.MaxValue;

        protected override void PostStop()
        {
            _tickCancelable.Cancel();
            base.PostStop();
        }

        protected override void ReceiveInitialRequest()
        {
            JournalRef.Tell(SubscribeNewEvents.Instance);
            Replay();
        }

        protected override void ReceiveIdleRequest()
        {
            Buffer.DeliverBuffer(TotalDemand);
            if (Buffer.IsEmpty && CurrentOffset > ToOffset)
                OnCompleteThenStop();
        }

        protected override void ReceiveRecoverySuccess(long highestOrderingNr)
        {
            Buffer.DeliverBuffer(TotalDemand);
            if (Buffer.IsEmpty && CurrentOffset > ToOffset)
                OnCompleteThenStop();

            Context.Become(Idle);
        }

    }
}
