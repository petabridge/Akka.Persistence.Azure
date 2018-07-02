using System.Linq;
using Akka.Actor;
using Akka.Event;
using NBench;
using static Akka.Persistence.Azure.Tests.Performance.PersistentBenchmarkMsgs;

namespace Akka.Persistence.Azure.Tests.Performance
{
    public class PersistentJournalBenchmarkActor : ReceivePersistentActor
    {
        private readonly ILoggingAdapter _log = Context.GetLogger();

        /// <summary>
        /// Our stored value
        /// </summary>
        private int TotalCount { get; set; }

        public PersistentJournalBenchmarkActor(string persistenceId)
        {
            PersistenceId = persistenceId;

            Recover<Stored>(i =>
            {
                TotalCount += i.Value;
            });

            Command<Store>(store =>
            {
                Persist(new Stored(store.Value), s =>
                {
                    TotalCount += s.Value;
                });
            });

            Command<Init>(i =>
            {
                var sender = Sender;
                Persist(new Stored(0), s =>
                {
                    TotalCount += s.Value;
                    sender.Tell(PersistentBenchmarkMsgs.Done.Instance);
                });
            });

            Command<Finish>(r =>
            {
                Sender.Tell(new Finished(TotalCount));
            });
        }

        public override string PersistenceId { get; }
    }
}