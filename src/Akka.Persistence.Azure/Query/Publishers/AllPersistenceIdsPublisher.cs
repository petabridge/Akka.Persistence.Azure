// -----------------------------------------------------------------------
// <copyright file="AllPersistenceIdsPublisher.cs" company="Petabridge, LLC">
//      Copyright (C) 2015 - 2023 Petabridge, LLC <https://petabridge.com>
// </copyright>
// -----------------------------------------------------------------------

using System;
using Akka.Actor;
using Akka.Event;
using Akka.Persistence.Journal;
using Akka.Streams.Actors;

namespace Akka.Persistence.Azure.Query.Publishers
{
        internal sealed class CurrentPersistenceIdsPublisher : ActorPublisher<string>, IWithUnboundedStash
    {
        public static Props Props(IActorRef writeJournal)
        {
            return Actor.Props.Create(() => new CurrentPersistenceIdsPublisher(writeJournal));
        }

        private readonly IActorRef _journalRef;

        private readonly DeliveryBuffer<string> _buffer;
        private readonly ILoggingAdapter _log;

        public IStash Stash { get; set; }

        public CurrentPersistenceIdsPublisher(IActorRef journalRef)
        {
            _journalRef = journalRef;
            _buffer = new DeliveryBuffer<string>(OnNext);
            _log = Context.GetLogger();
        }

        protected override bool Receive(object message)
        {
            switch (message)
            {
                case Request _:
                    Become(Initializing);
                    _journalRef
                        .Ask<CurrentPersistenceIds>(new MemoryJournal.SelectCurrentPersistenceIds(0, Self))
                        .PipeTo(Self);
                    return true;
                
                case Cancel _:
                    Context.Stop(Self);
                    return true;
                
                default:
                    return false;
            }
        }

        private bool Initializing(object message)
        {
            switch (message)
            {
                case CurrentPersistenceIds current:
                    _buffer.AddRange(current.AllPersistenceIds);
                    _buffer.DeliverBuffer(TotalDemand);

                    if (_buffer.IsEmpty)
                    {
                        OnCompleteThenStop();
                        return true;
                    }

                    Become(Active);
                    Stash.UnstashAll();
                    return true;
                
                case Cancel _:
                    Context.Stop(Self);
                    return true;
                
                case Status.Failure msg:
                    if (msg.Cause is AskTimeoutException e)
                    {
                        _log.Info(e, "Current persistence id query timed out, retrying");
                    }
                    else
                    {
                        _log.Info(msg.Cause, "Current persistence id query failed, retrying");
                    }
                    return true;
                    
                default:
                    Stash.Stash();
                    return true;
            }
        }

        private bool Active(object message)
        {
            switch (message)
            {
                case CurrentPersistenceIds _:
                    // Ignore duplicate CurrentPersistenceIds response
                    return true;
                
                case Request _:
                    _buffer.DeliverBuffer(TotalDemand);
                    if (_buffer.IsEmpty)
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

    internal sealed class LivePersistenceIdsPublisher : ActorPublisher<string>, IWithUnboundedStash, IWithTimers
    {
        private sealed class Continue
        {
            public static readonly Continue Instance = new();

            private Continue() { }
        }

        public static Props Props(TimeSpan refreshInterval, IActorRef writeJournal)
        {
            return Actor.Props.Create(() => new LivePersistenceIdsPublisher(refreshInterval, writeJournal));
        }

        private long _lastOrderingOffset = 0L;
        private readonly TimeSpan _refreshInterval;
        private readonly IActorRef _journalRef;
        private readonly DeliveryBuffer<string> _buffer;
        private readonly ILoggingAdapter _log;

        public IStash Stash { get; set; } = null!;
        public ITimerScheduler Timers { get; set; } = null!;

        public LivePersistenceIdsPublisher(TimeSpan refreshInterval, IActorRef journalRef)
        {
            _journalRef = journalRef;
            _log = Context.GetLogger();
            _refreshInterval = refreshInterval;
            _buffer = new DeliveryBuffer<string>(OnNext);
        }

        protected override void PreStart()
        {
            base.PreStart();
            Timers.StartPeriodicTimer(Continue.Instance, Continue.Instance, _refreshInterval, _refreshInterval, Self);
        }

        protected override void PostStop()
        {
            Timers.CancelAll();
            base.PostStop();
        }

        protected override bool Receive(object message)
        {
            switch (message)
            {
                case Request _:
                    Become(Waiting);
                    _journalRef
                        .Ask<CurrentPersistenceIds>(new SelectCurrentPersistenceIds(_lastOrderingOffset, Self))
                        .PipeTo(Self);
                    return true;
                
                case Continue _:
                    return true;
                
                case Cancel _:
                    Context.Stop(Self);
                    return true;
                
                default:
                    return false;
            }
        }

        private bool Waiting(object message)
        {
            switch (message)
            {
                case CurrentPersistenceIds current:
                    _lastOrderingOffset = current.HighestOrderingNumber;
                    _buffer.AddRange(current.AllPersistenceIds);
                    _buffer.DeliverBuffer(TotalDemand);

                    Become(Active);
                    Stash.UnstashAll();
                    return true;
                
                case Continue _:
                    return true;
                
                case Cancel _:
                    Context.Stop(Self);
                    return true;
                
                case Status.Failure msg:
                    if (msg.Cause is AskTimeoutException e)
                    {
                        _log.Info(e, $"Current persistence id query timed out, retrying. Offset: {_lastOrderingOffset}");
                    }
                    else
                    {
                        _log.Info(msg.Cause, $"Current persistence id query failed, retrying. Offset: {_lastOrderingOffset}");
                    }
                    
                    Become(Active);
                    Stash.UnstashAll();
                    return true;
                    
                default:
                    Stash.Stash();
                    return true;
            }
        }

        private bool Active(object message)
        {
            switch (message)
            {
                case CurrentPersistenceIds _:
                    // Ignore duplicate CurrentPersistenceIds response
                    return true;
                
                case Request _:
                    _buffer.DeliverBuffer(TotalDemand);
                    return true;
                
                case Continue _:
                    return true;
                
                case Cancel _:
                    Context.Stop(Self);
                    return true;
                
                case Status.Failure msg:
                    _log.Info(msg.Cause, "Unexpected failure received");
                    return true;
                
                default:
                    return false;
            }
        }
    }
}
