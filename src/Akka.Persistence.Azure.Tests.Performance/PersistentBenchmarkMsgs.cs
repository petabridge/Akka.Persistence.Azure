using Akka.Actor;

namespace Akka.Persistence.Azure.Tests.Performance
{
    /// <summary>
    /// Messages used for working with benchmark actors
    /// </summary>
    public static class PersistentBenchmarkMsgs
    {
        public sealed class RecoveryComplete : INoSerializationVerificationNeeded
        {
            public static readonly RecoveryComplete Instance = new RecoveryComplete();
            private RecoveryComplete() { }
        }

        public sealed class ResetCounter : INoSerializationVerificationNeeded
        {
            public static readonly ResetCounter Instance = new ResetCounter();

            private ResetCounter() { }
        }

        public sealed class NotifyWhenCounterHits
        {
            public NotifyWhenCounterHits(int target, IActorRef subscriber)
            {
                Target = target;
                Subscriber = subscriber;
            }

            public int Target { get; }

            public IActorRef Subscriber { get; }
        }
    }
}