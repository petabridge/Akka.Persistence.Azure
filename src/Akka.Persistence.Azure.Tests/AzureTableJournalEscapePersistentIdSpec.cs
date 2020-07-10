using Akka.Actor;
using Xunit;
using Xunit.Abstractions;

namespace Akka.Persistence.Azure.Tests
{
    public class AzureTableJournalEscapePersistentIdSpec : AzureTableJournalSpec
    {
        /// <inheritdoc />
        public AzureTableJournalEscapePersistentIdSpec(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void Journal_should_replay_all_messages1()
        {
            var probe = CreateTestProbe();
            Journal.Tell(new ReplayMessages(1, long.MaxValue, long.MaxValue, Pid, probe.Ref));
            for (int i = 1; i <= 5; i++) probe.ExpectMsg<ReplayedMessage>(m => IsReplayedMessage(m, i));
            probe.ExpectMsg<RecoverySuccess>();
        }
    }
}