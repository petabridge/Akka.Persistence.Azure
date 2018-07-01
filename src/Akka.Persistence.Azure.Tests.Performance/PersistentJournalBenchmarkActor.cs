using System.Linq;
using Akka.Actor;
using Akka.Event;
using NBench;

namespace Akka.Persistence.Azure.Tests.Performance
{
    public class PersistentJournalBenchmarkActor : ReceivePersistentActor
    {
        private readonly Counter _recoveredMessageCounter;
        private readonly Counter _msgWriteCounter;

        private readonly ILoggingAdapter _log = Context.GetLogger();

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
                foreach (var n in Enumerable.Range(0, i))
                {
                    Persist(n, i1 => { Increment(i1); });
                }
            });

            Command<PersistentBenchmarkMsgs.NotifyWhenCounterHits>(n =>
            {
                _target = n;
                TellTargetWhenReady();
            });

            Command<PersistentBenchmarkMsgs.RecoveryComplete>(r =>
            {
                Sender.Tell(r);
            });
        }

        private void Increment(int i1)
        {
            _msgWriteCounter.Increment();
            TotalCount += i1;
            TellTargetWhenReady();
        }

        private void TellTargetWhenReady()
        {
            if (_target != null && _target.Target <= TotalCount)
            {
                _log.Info("Notifying that we have hit or exceeded requested target of [{0}] with actual target of [{1}]", _target.Target, TotalCount);
                _target.Subscriber.Tell(TotalCount);
            }
        }

        public override string PersistenceId { get; }
    }
}