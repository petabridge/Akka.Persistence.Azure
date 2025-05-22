// -----------------------------------------------------------------------
// <copyright file="CurrentEventsByTagPublisher.cs" company="Petabridge, LLC">
//      Copyright (C) 2015 - 2023 Petabridge, LLC <https://petabridge.com>
// </copyright>
// -----------------------------------------------------------------------

using Akka.Actor;

namespace Akka.Persistence.Azure.Query.Publishers
{
    internal sealed class CurrentEventsByTagPublisher 
        : AbstractEventsByTagPublisher
    {
        private bool _completed;

        public CurrentEventsByTagPublisher(
            string tag, 
            long fromOffset, 
            long toOffset, 
            int maxBufferSize, 
            IActorRef journalRef)
            : base(tag, fromOffset, maxBufferSize, journalRef)
        {
            ToOffset = toOffset;
        }

        protected override long ToOffset { get; }

        protected override void ReceiveIdleRequest()
        {
            Buffer.DeliverBuffer(TotalDemand);

            if (Buffer.IsEmpty && _completed)
                OnCompleteThenStop();
            else
                Self.Tell(EventsByTagPublisher.Continue.Instance);
        }

        protected override void ReceiveInitialRequest()
        {
            Replay();
        }

        protected override void ReceiveRecoverySuccess(bool completed)
        {
            if (completed)
                _completed = true;
            
            Buffer.DeliverBuffer(TotalDemand);

            if (Buffer.IsEmpty && _completed)
                OnCompleteThenStop();
            else
                Self.Tell(EventsByTagPublisher.Continue.Instance);

            Context.Become(Idle);
        }
    }
}