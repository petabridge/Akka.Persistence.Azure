using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Akka.Actor;
using Akka.Configuration;
using Akka.Event;
using Akka.Persistence.Azure.Query;
using Akka.Persistence.Azure.Tests.Helper;
using Akka.Persistence.Query;
using Akka.Persistence.TCK.Query;
using Akka.Streams.TestKit;
using FluentAssertions;
using Xunit;
using Xunit.Abstractions;
using Xunit.Sdk;
using static Akka.Persistence.Azure.Tests.Helper.AzureStorageConfigHelper;

namespace Akka.Persistence.Azure.Tests.Query
{
    [Collection("AzureSpecs")]
    public sealed class AzureTableCurrentEventsByTagSpec : CurrentEventsByTagSpec, IClassFixture<AzuriteFixture>
    {
        public AzureTableCurrentEventsByTagSpec(AzuriteFixture fixture, ITestOutputHelper output)
            : base(AzureConfig(fixture.ConnectionString), nameof(AzureTableCurrentEventsByTagSpec), output)
        {
            AzurePersistence.Get(Sys);

            ReadJournal =
                Sys.ReadJournalFor<AzureTableStorageReadJournal>(
                    AzureTableStorageReadJournal.Identifier);

            output.WriteLine("Current table: {0}", TableName);

            var x = Sys.ActorOf(JournalTestActor.Props("x"));
            x.Tell("warm-up");
            ExpectMsg("warm-up-done", TimeSpan.FromSeconds(10));

        }

        public static string TableName { get; private set; }

        [Fact(DisplayName = "Read journal should be able to retrieve all tagged persisted messages beyond the first 1000 messages")]
        public async Task RetrieveAllTaggedRows()
        {
            if (ReadJournal is not ICurrentEventsByTagQuery queries)
                throw IsTypeException.ForMismatchedType(nameof(ICurrentEventsByTagQuery), ReadJournal?.GetType().Name ?? "null");

            var allMessages = Enumerable.Range(0, 2000)
                .Select(i => $"green {i}").ToArray();
            var a = Sys.ActorOf(TestPersistenceActor.Props("a"));
            foreach (var str in allMessages)
            {
                a.Tell(str);
            }
            ReceiveN(2000);

            var probe = queries.CurrentEventsByTag("green", Offset.NoOffset())
                .RunWith(this.SinkProbe<EventEnvelope>(), Materializer);

            await probe.ExpectSubscriptionAsync();
            probe.Request(3000);
            
            var received = new List<string>();
            foreach (var _ in Enumerable.Range(0, 2000))
            {
                var env = await probe.ExpectNextAsync();
                received.Add((string)env.Event);
            }

            await probe.ExpectCompleteAsync();
            received.Should().BeEquivalentTo(allMessages);
        }

        private sealed class TestPersistenceActor : UntypedPersistentActor
        {
            public static Props Props(string persistenceId) => Actor.Props.Create(() => new TestPersistenceActor(persistenceId));

            public TestPersistenceActor(string persistenceId)
            {
                PersistenceId = persistenceId;
            }

            public override string PersistenceId { get; }

            protected override void OnRecover(object message)
            {
            }

            protected override void OnCommand(object message)
            {
                switch (message)
                {
                    case string cmd:
                        var sender = Sender;
                        Persist(cmd, e => sender.Tell($"{e}-done"));
                        break;
                    default:
                        Unhandled(message);
                        break;
                }
            }
        }
    }
}