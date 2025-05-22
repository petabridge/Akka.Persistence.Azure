// -----------------------------------------------------------------------
// <copyright file="LiveEventsByPersistenceIdPublisher.cs" company="Petabridge, LLC">
//      Copyright (C) 2015 - 2023 Petabridge, LLC <https://petabridge.com>
// </copyright>
// -----------------------------------------------------------------------

using System;
using Akka.Actor;

namespace Akka.Persistence.Azure.Query.Publishers
{
    internal sealed class LiveEventsByPersistenceIdPublisher : AbstractEventsByPersistenceIdPublisher
    {
        private readonly ICancelable _tickCancelable;

        public LiveEventsByPersistenceIdPublisher(string persistenceId, long fromSequenceNr, long toSequenceNr, int maxBufferSize, IActorRef journalRef, TimeSpan refreshInterval)
            : base(persistenceId, fromSequenceNr, toSequenceNr, maxBufferSize, journalRef)
        {
            _tickCancelable = Context.System.Scheduler.ScheduleTellRepeatedlyCancelable(refreshInterval, refreshInterval, Self, EventsByPersistenceIdPublisher.Continue.Instance, Self);
        }

        protected override void PostStop()
        {
            _tickCancelable.Cancel();
            base.PostStop();
        }

        protected override void ReceiveInitialRequest()
        {
            Replay();
        }

        protected override void ReceiveIdleRequest()
        {
            Buffer.DeliverBuffer(TotalDemand);
            if (Buffer.IsEmpty && CurrentSequenceNr > ToSequenceNr)
                OnCompleteThenStop();
        }

        protected override void ReceiveRecoverySuccess(long highestSequenceNr)
        {
            Buffer.DeliverBuffer(TotalDemand);
            if (Buffer.IsEmpty && CurrentSequenceNr > ToSequenceNr)
                OnCompleteThenStop();

            Context.Become(Idle);
        }
    }
}