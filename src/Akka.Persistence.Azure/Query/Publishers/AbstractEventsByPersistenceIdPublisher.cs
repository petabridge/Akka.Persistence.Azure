// -----------------------------------------------------------------------
// <copyright file="AbstractEventsByPersistenceIdPublisher.cs" company="Petabridge, LLC">
//      Copyright (C) 2015 - 2022 Petabridge, LLC <https://petabridge.com>
// </copyright>
// -----------------------------------------------------------------------

using Akka.Actor;
using Akka.Event;
using Akka.Persistence.Query;
using Akka.Streams.Actors;

namespace Akka.Persistence.Azure.Query.Publishers
{
    internal abstract class AbstractEventsByPersistenceIdPublisher : ActorPublisher<EventEnvelope>
    {
        private ILoggingAdapter _log;

        protected DeliveryBuffer<EventEnvelope> Buffer;
        protected readonly IActorRef JournalRef;
        protected long CurrentSequenceNr;

        protected AbstractEventsByPersistenceIdPublisher(string persistenceId, long fromSequenceNr, long toSequenceNr, int maxBufferSize, string writeJournalPluginId)
        {
            PersistenceId = persistenceId;
            CurrentSequenceNr = FromSequenceNr = fromSequenceNr;
            ToSequenceNr = toSequenceNr;
            MaxBufferSize = maxBufferSize;
            WriteJournalPluginId = writeJournalPluginId;
            Buffer = new DeliveryBuffer<EventEnvelope>(OnNext);

            JournalRef = Persistence.Instance.Apply(Context.System).JournalFor(writeJournalPluginId);
        }

        protected ILoggingAdapter Log => _log ?? (_log = Context.GetLogger());
        protected string PersistenceId { get; }
        protected long FromSequenceNr { get; }
        protected long ToSequenceNr { get; set; }
        protected int MaxBufferSize { get; }
        protected string WriteJournalPluginId { get; }

        protected bool IsTimeForReplay => (Buffer.IsEmpty || Buffer.Length <= MaxBufferSize / 2) && (CurrentSequenceNr <= ToSequenceNr);

        protected abstract void ReceiveInitialRequest();
        protected abstract void ReceiveIdleRequest();
        protected abstract void ReceiveRecoverySuccess(long highestSequenceNr);

        protected override bool Receive(object message)
        {
            return Init(message);
        }

        protected bool Init(object message)
        {
            switch (message)
            {
                case EventsByPersistenceIdPublisher.Continue _:
                    // no-op
                    return true;
                case Request _:
                    ReceiveInitialRequest();
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
                case EventsByPersistenceIdPublisher.Continue _:
                    if (IsTimeForReplay) 
                        Replay();
                    return true;
                case EventAppended _:
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
            Log.Debug("request replay for persistenceId [{0}] from [{1}] to [{2}] limit [{3}]", PersistenceId, CurrentSequenceNr, ToSequenceNr, limit);
            JournalRef.Tell(new ReplayMessages(CurrentSequenceNr, ToSequenceNr, limit, PersistenceId, Self));
            Context.Become(Replaying(limit));
        }

        protected Receive Replaying(int limit)
        {
            bool Receive(object message)
            {
                switch (message)
                {
                    case ReplayedMessage replayed:
                        var seqNr = replayed.Persistent.SequenceNr;
                        Buffer.Add(new EventEnvelope(
                            offset: new Sequence(seqNr),
                            persistenceId: PersistenceId,
                            sequenceNr: seqNr,
                            @event: replayed.Persistent.Payload,
                            timestamp: replayed.Persistent.Timestamp));
                        CurrentSequenceNr = seqNr + 1;
                        Buffer.DeliverBuffer(TotalDemand);
                        return true;
                    case RecoverySuccess success:
                        Log.Debug("replay completed for persistenceId [{0}], currSeqNo [{1}]", PersistenceId, CurrentSequenceNr);
                        ReceiveRecoverySuccess(success.HighestSequenceNr);
                        return true;
                    case ReplayMessagesFailure failure:
                        Log.Debug("replay failed for persistenceId [{0}], due to [{1}]", PersistenceId, failure.Cause.Message);
                        Buffer.DeliverBuffer(TotalDemand);
                        OnErrorThenStop(failure.Cause);
                        return true;
                    case Request _:
                        Buffer.DeliverBuffer(TotalDemand);
                        return true;
                    case EventsByPersistenceIdPublisher.Continue _:
                        // skip during replay
                        return true;
                    case EventAppended _:
                        // skip during replay
                        return true;
                    case Cancel _:
                        Context.Stop(Self);
                        return true;
                    default:
                        return false;
                }
            }

            return Receive;
        }
    }
}