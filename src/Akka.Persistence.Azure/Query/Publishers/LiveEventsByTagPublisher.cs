// -----------------------------------------------------------------------
// <copyright file="LiveEventsByTagPublisher.cs" company="Petabridge, LLC">
//      Copyright (C) 2015 - 2023 Petabridge, LLC <https://petabridge.com>
// </copyright>
// -----------------------------------------------------------------------

using System;
using Akka.Actor;

namespace Akka.Persistence.Azure.Query.Publishers
{
    internal sealed class LiveEventsByTagPublisher : AbstractEventsByTagPublisher
    {
        private readonly ICancelable _tickCancelable;

        public LiveEventsByTagPublisher(
            string tag, 
            long fromOffset, 
            long toOffset, 
            TimeSpan refreshInterval, 
            int maxBufferSize, 
            IActorRef journalRef)
            : base(tag, fromOffset, maxBufferSize, journalRef)
        {
            ToOffset = toOffset;
            _tickCancelable = Context.System.Scheduler.ScheduleTellRepeatedlyCancelable(refreshInterval, refreshInterval, Self, EventsByTagPublisher.Continue.Instance, Self);
        }

        protected override long ToOffset { get; }

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
            if (Buffer.IsEmpty && CurrentOffset > ToOffset)
                OnCompleteThenStop();
        }

        protected override void ReceiveRecoverySuccess(bool completed)
        {
            Buffer.DeliverBuffer(TotalDemand);
            if (Buffer.IsEmpty && CurrentOffset > ToOffset)
                OnCompleteThenStop();

            Context.Become(Idle);
        }
    }
}