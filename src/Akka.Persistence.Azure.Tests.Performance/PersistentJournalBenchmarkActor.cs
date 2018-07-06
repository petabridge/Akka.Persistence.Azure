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

        private int Target { get; set; }

        public PersistentJournalBenchmarkActor(string persistenceId, int target)
        {
            Target = target;
            PersistenceId = persistenceId;

            Recover<Stored>(i =>
            {
                TotalCount += i.Value;
            });

            Command<Store>(store =>
            {
                PersistAsync(new Stored(store.Value), s =>
                {
                    TotalCount += s.Value;
                    Stash.Unstash();
                });
            });

            Command<Init>(i =>
            {
                var sender = Sender;
                PersistAsync(new Stored(0), s =>
                {
                    TotalCount += s.Value;
                    sender.Tell(PersistentBenchmarkMsgs.Done.Instance);
                });
            });

            Command<Finish>(r =>
            {
                if(TotalCount >= Target)
                    Sender.Tell(new Finished(TotalCount));
                else
                    Stash.Stash();
            });
        }

        public override string PersistenceId { get; }
    }
}