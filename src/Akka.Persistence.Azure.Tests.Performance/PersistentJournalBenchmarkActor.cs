using Akka.Actor;
using NBench;

namespace Akka.Persistence.Azure.Tests.Performance
{
    public class PersistentJournalBenchmarkActor : ReceivePersistentActor
    {
        private readonly Counter _recoveredMessageCounter;
        private readonly Counter _msgWriteCounter;

        private PersistentBenchmarkMsgs.NotifyWhenCounterHits _target;

        /// <summary>
        /// Our stored value
        /// </summary>
        private int TotalCount { get; set; }

        public PersistentJournalBenchmarkActor(string persistenceId, Counter recoveredMessageCounter, Counter msgWriteCounter)
        {
            PersistenceId = persistenceId;
            _recoveredMessageCounter = recoveredMessageCounter;
            _msgWriteCounter = msgWriteCounter;

            Recover<SnapshotOffer>(offer =>
            {
                if (offer.Snapshot is int i)
                {
                    TotalCount = i;
                }
                _recoveredMessageCounter.Increment();
            });

            Recover<int>(i =>
            {
                TotalCount += i;
                _recoveredMessageCounter.Increment();
            });

            Command<int>(i =>
            {
                Persist(i, i1 =>
                {
                    _msgWriteCounter.Increment();
                    if (_target != null && _target.Target >= TotalCount)
                    {
                        _target.Subscriber.Tell(TotalCount);
                    }
                });
            });

            Command<PersistentBenchmarkMsgs.NotifyWhenCounterHits>(n =>
            {
                if (TotalCount >= n.Target)
                {
                    _target.Subscriber.Tell(TotalCount);
                }
                _target = n;
            });

            Command<PersistentBenchmarkMsgs.RecoveryComplete>(r =>
            {
                Sender.Tell(r);
            });
        }

        public override string PersistenceId { get; }
    }
}