// -----------------------------------------------------------------------
// <copyright file="AbstractEventsByTagPublisher.cs" company="Petabridge, LLC">
//      Copyright (C) 2015 - 2022 Petabridge, LLC <https://petabridge.com>
// </copyright>
// -----------------------------------------------------------------------

using Akka.Actor;
using Akka.Event;
using Akka.Persistence.Query;
using Akka.Streams.Actors;

namespace Akka.Persistence.Azure.Query.Publishers
{
    internal abstract class AbstractEventsByTagPublisher : ActorPublisher<EventEnvelope>
    {
        private ILoggingAdapter _log;

        protected readonly DeliveryBuffer<EventEnvelope> Buffer;
        protected readonly IActorRef JournalRef;
        protected long CurrentOffset;
        protected AbstractEventsByTagPublisher(string tag, long fromOffset, int maxBufferSize, string writeJournalPluginId)
        {
            Tag = tag;
            CurrentOffset = FromOffset = fromOffset;
            MaxBufferSize = maxBufferSize;
            WriteJournalPluginId = writeJournalPluginId;
            Buffer = new DeliveryBuffer<EventEnvelope>(OnNext);
            JournalRef = Persistence.Instance.Apply(Context.System).JournalFor(writeJournalPluginId);
        }

        protected ILoggingAdapter Log => _log ?? (_log = Context.GetLogger());
        protected string Tag { get; }
        protected long FromOffset { get; }
        protected abstract long ToOffset { get; }
        protected int MaxBufferSize { get; }
        protected string WriteJournalPluginId { get; }

        protected bool IsTimeForReplay => (Buffer.IsEmpty || Buffer.Length <= MaxBufferSize / 2) && (CurrentOffset <= ToOffset);

        protected abstract void ReceiveInitialRequest();
        protected abstract void ReceiveIdleRequest();
        protected abstract void ReceiveRecoverySuccess(long highestSequenceNr);

        protected override bool Receive(object message)
        {
            switch (message)
            {
                case Request _:
                    ReceiveInitialRequest();
                    return true;
                case EventsByTagPublisher.Continue _:
                    // no-op
                    return true;
                case Cancel _:
                    Context.Stop(Self);
                    return true;
                default:
                    return false;
            }
        }

        protected bool Idle(object message)
        {
            switch (message)
            {
                case EventsByTagPublisher.Continue _:
                    if (IsTimeForReplay) 
                        Replay();
                    return true;
                case TaggedEventAppended _:
                    if (IsTimeForReplay) 
                        Replay();
                    return true;
                case Request _:
                    ReceiveIdleRequest();
                    return true;
                case Cancel _:
                    Context.Stop(Self);
                    return true;
                default:
                    return false;
            }
        }

        protected void Replay()
        {
            var limit = MaxBufferSize - Buffer.Length;
            Log.Debug("request replay for tag [{0}] from [{1}] to [{2}] limit [{3}]", Tag, CurrentOffset, ToOffset, limit);
            JournalRef.Tell(new ReplayTaggedMessages(CurrentOffset, ToOffset, limit, Tag, Self));
            Context.Become(Replaying);
        }

        protected bool Replaying(object message)
        {
            switch (message)
            {
                case ReplayedTaggedMessage replayed:
                    Buffer.Add(new EventEnvelope(
                        offset: new Sequence(replayed.Offset),
                        persistenceId: replayed.Persistent.PersistenceId,
                        sequenceNr: replayed.Persistent.SequenceNr,
                        timestamp: replayed.Persistent.Timestamp,
                        @event: replayed.Persistent.Payload));

                    CurrentOffset = replayed.Offset;
                    Buffer.DeliverBuffer(TotalDemand);
                    return true;
                
                case RecoverySuccess success:
                    Log.Debug("replay completed for tag [{0}], currOffset [{1}]", Tag, CurrentOffset);
                    ReceiveRecoverySuccess(success.HighestSequenceNr);
                    return true;
                
                case ReplayMessagesFailure failure:
                    Log.Debug("replay failed for tag [{0}], due to [{1}]", Tag, failure.Cause.Message);
                    Buffer.DeliverBuffer(TotalDemand);
                    OnErrorThenStop(failure.Cause);
                    return true;
                
                case Request _:
                    Buffer.DeliverBuffer(TotalDemand);
                    return true;
                
                case EventsByTagPublisher.Continue _:
                    return true;
                
                case TaggedEventAppended _:
                    return true;
                
                case Cancel _:
                    Context.Stop(Self);
                    return true;
                
                default:
                    return false;
            }
        }
    }
}