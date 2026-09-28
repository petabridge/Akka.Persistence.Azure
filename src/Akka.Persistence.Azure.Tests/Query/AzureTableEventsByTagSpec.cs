using System;
using System.Collections.Generic;
using Akka.Actor;
using Akka.Configuration;
using Akka.Persistence.Azure.Query;
using Akka.Persistence.Azure.Tests.Helper;
using Akka.Persistence.Journal;
using Akka.Persistence.Query;
using Akka.Persistence.TCK.Query;
using Akka.Streams.TestKit;
using Xunit;
using static Akka.Persistence.Azure.Tests.Helper.AzureStorageConfigHelper;

namespace Akka.Persistence.Azure.Tests.Query
{
    [Collection("AzureSpecs")]
    public sealed class AzureTableEventsByTagSpec : EventsByTagSpec
    {
        public AzureTableEventsByTagSpec(AzuriteFixture fixture, ITestOutputHelper output)
            : base(AzureConfig(fixture.ConnectionString), nameof(AzureTableEventsByTagSpec), output)
        {
            AzurePersistence.Get(Sys);

            ReadJournal =
                Sys.ReadJournalFor<AzureTableStorageReadJournal>(
                    AzureTableStorageReadJournal.Identifier);

            var x = Sys.ActorOf(JournalTestActor.Props("x"));
            x.Tell("warm-up");
            ExpectMsg("warm-up-done", TimeSpan.FromSeconds(60));
        }

        [Fact]
        public void ReadJournal_should_delete_EventTags_index_items()
        {
            var queries = ReadJournal as IEventsByTagQuery;

            var b = Sys.ActorOf(JournalTestActor.Props("b"));
            var d = Sys.ActorOf(JournalTestActor.Props("d"));

            b.Tell("a black car");
            ExpectMsg("a black car-done", null, null, TestContext.Current.CancellationToken);

            var blackSrc = queries.EventsByTag("black", offset: Offset.NoOffset());
            var probe = blackSrc.RunWith(this.SinkProbe<EventEnvelope>(), Materializer);
            probe.Request(2);
            probe.ExpectNext<EventEnvelope>(p => p.PersistenceId == "b" && p.SequenceNr == 1L && p.Event.Equals("a black car"), TestContext.Current.CancellationToken);
            probe.ExpectNoMsg(TimeSpan.FromMilliseconds(100), TestContext.Current.CancellationToken);

            d.Tell("a black dog");
            ExpectMsg("a black dog-done", null, null, TestContext.Current.CancellationToken);
            d.Tell("a black night");
            ExpectMsg("a black night-done", null, null, TestContext.Current.CancellationToken);

            probe.ExpectNext<EventEnvelope>(p => p.PersistenceId == "d" && p.SequenceNr == 1L && p.Event.Equals("a black dog"), TestContext.Current.CancellationToken);
            probe.ExpectNoMsg(TimeSpan.FromMilliseconds(100), TestContext.Current.CancellationToken);
            probe.Request(10);
            probe.ExpectNext<EventEnvelope>(p => p.PersistenceId == "d" && p.SequenceNr == 2L && p.Event.Equals("a black night"), TestContext.Current.CancellationToken);

            b.Tell(new JournalTestActor.DeleteCommand(1));
            AwaitAssert(() => ExpectMsg("1-deleted", null, null, TestContext.Current.CancellationToken), null, null, TestContext.Current.CancellationToken);

            d.Tell(new JournalTestActor.DeleteCommand(2));
            AwaitAssert(() => ExpectMsg("2-deleted", null, null, TestContext.Current.CancellationToken), null, null, TestContext.Current.CancellationToken);

            probe.Request(10);
            probe.ExpectNoMsg(TimeSpan.FromMilliseconds(100), TestContext.Current.CancellationToken);

            probe.Cancel();
        }
    }
}