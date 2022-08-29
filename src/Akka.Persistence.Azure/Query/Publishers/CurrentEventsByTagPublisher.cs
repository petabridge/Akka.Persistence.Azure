// -----------------------------------------------------------------------
// <copyright file="CurrentEventsByTagPublisher.cs" company="Petabridge, LLC">
//      Copyright (C) 2015 - 2022 Petabridge, LLC <https://petabridge.com>
// </copyright>
// -----------------------------------------------------------------------

using Akka.Actor;

namespace Akka.Persistence.Azure.Query.Publishers
{
    internal sealed class CurrentEventsByTagPublisher 
        : AbstractEventsByTagPublisher
    {
        private long _toOffset;

        public CurrentEventsByTagPublisher(
            string tag, 
            long fromOffset, 
            long toOffset, 
            int maxBufferSize, 
            string writeJournalPluginId)
            : base(tag, fromOffset, maxBufferSize, writeJournalPluginId)
        {
            _toOffset = toOffset;
        }

        protected override long ToOffset => _toOffset;

        protected override void ReceiveIdleRequest()
        {
            Buffer.DeliverBuffer(TotalDemand);

            if (Buffer.IsEmpty && CurrentOffset > ToOffset)
                OnCompleteThenStop();
            else
                Self.Tell(EventsByTagPublisher.Continue.Instance);
        }

        protected override void ReceiveInitialRequest()
        {
            Replay();
        }

        protected override void ReceiveRecoverySuccess(long highestSequenceNr)
        {
            Buffer.DeliverBuffer(TotalDemand);

            if (highestSequenceNr < ToOffset)
                _toOffset = highestSequenceNr;

            if (Buffer.IsEmpty && CurrentOffset > ToOffset)
                OnCompleteThenStop();
            else
                Self.Tell(EventsByTagPublisher.Continue.Instance);

            Context.Become(Idle);
        }
    }
}