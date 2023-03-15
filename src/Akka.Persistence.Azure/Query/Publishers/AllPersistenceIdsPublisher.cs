// -----------------------------------------------------------------------
// <copyright file="AllPersistenceIdsPublisher.cs" company="Petabridge, LLC">
//      Copyright (C) 2015 - 2023 Petabridge, LLC <https://petabridge.com>
// </copyright>
// -----------------------------------------------------------------------

using Akka.Actor;
using Akka.Streams.Actors;

namespace Akka.Persistence.Azure.Query.Publishers
{
    internal sealed class AllPersistenceIdsPublisher : ActorPublisher<string>
    {
        public static Props Props(bool liveQuery, string writeJournalPluginId)
        {
            return Actor.Props.Create(() => new AllPersistenceIdsPublisher(liveQuery, writeJournalPluginId));
        }

        private readonly bool _liveQuery;
        private readonly IActorRef _journalRef;
        private readonly DeliveryBuffer<string> _buffer;

        public AllPersistenceIdsPublisher(bool liveQuery, string writeJournalPluginId)
        {
            _liveQuery = liveQuery;
            _buffer = new DeliveryBuffer<string>(OnNext);
            _journalRef = Persistence.Instance.Apply(Context.System).JournalFor(writeJournalPluginId);
        }

        protected override bool Receive(object message)
        {
            switch (message)
            {
                case Request _:
                    _journalRef.Tell(SubscribeAllPersistenceIds.Instance);
                    Become(Active);
                    return true;
                
                case Cancel _:
                    Context.Stop(Self);
                    return true;
                
                default:
                    return false;
            }
        }

        private bool Active(object message)
        {
            switch (message)
            {
                case CurrentPersistenceIds current:
                    _buffer.AddRange(current.AllPersistenceIds);
                    _buffer.DeliverBuffer(TotalDemand);

                    if (!_liveQuery && _buffer.IsEmpty)
                        OnCompleteThenStop();
                    return true;
                
                case PersistenceIdAdded added:
                    if (_liveQuery)
                    {
                        _buffer.Add(added.PersistenceId);
                        _buffer.DeliverBuffer(TotalDemand);
                    }
                    return true;
                
                case Request _:
                    _buffer.DeliverBuffer(TotalDemand);
                    if (!_liveQuery && _buffer.IsEmpty)
                        OnCompleteThenStop();
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
