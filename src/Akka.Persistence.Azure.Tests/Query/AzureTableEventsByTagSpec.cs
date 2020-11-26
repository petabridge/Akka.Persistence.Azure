using System;
using System.Collections.Generic;
using Akka.Actor;
using Akka.Configuration;
using Akka.Persistence.Azure.Query;
using Akka.Persistence.Azure.TestHelpers;
using Akka.Persistence.Azure.Tests.Helper;
using Akka.Persistence.Journal;
using Akka.Persistence.Query;
using Akka.Persistence.TCK.Query;
using Akka.Streams.TestKit;
using Xunit;
using Xunit.Abstractions;

namespace Akka.Persistence.Azure.Tests.Query
{
    [Collection("AzureQuery")]
    public sealed class AzureTableEventsByTagSpec
        : EventsByTagSpec, IClassFixture<AzureEmulatorFixture>
    {
        public AzureTableEventsByTagSpec(ITestOutputHelper output)
            : base(Config(), nameof(AzureTableEventsByTagSpec), output)
        {
            AzurePersistence.Get(Sys);

            ReadJournal =
                Sys.ReadJournalFor<AzureTableStorageReadJournal>(
                    AzureTableStorageReadJournal.Identifier);

            var x = Sys.ActorOf(JournalTestActor.Props("x"));
            x.Tell("warm-up");
            ExpectMsg("warm-up-done", TimeSpan.FromSeconds(60));
        }

        public static string TableName { get; private set; }

        public static Config Config()
        {
            var azureConfig =
                !string.IsNullOrEmpty(Environment.GetEnvironmentVariable("AZURE_CONNECTION_STR"))
                    ? AzureStorageConfigHelper.AzureConfig(Environment.GetEnvironmentVariable("AZURE_CONNECTION_STR"))
                    : AzureStorageConfigHelper.AzureConfig(AzureEmulatorFixture.GenerateConnStr());

            TableName = azureConfig.GetString("akka.persistence.journal.azure-table.table-name");

            return azureConfig;
        }

        [Fact]
        public void ReadJournal_should_delete_EventTags_index_items()
        {
            var queries = ReadJournal as IEventsByTagQuery;

            var b = Sys.ActorOf(JournalTestActor.Props("b"));
            var d = Sys.ActorOf(JournalTestActor.Props("d"));

            b.Tell("a black car");
            ExpectMsg("a black car-done");

            var blackSrc = queries.EventsByTag("black", offset: Offset.NoOffset());
            var probe = blackSrc.RunWith(this.SinkProbe<EventEnvelope>(), Materializer);
            probe.Request(2);
            probe.ExpectNext<EventEnvelope>(p => p.PersistenceId == "b" && p.SequenceNr == 1L && p.Event.Equals("a black car"));
            probe.ExpectNoMsg(TimeSpan.FromMilliseconds(100));

            d.Tell("a black dog");
            ExpectMsg("a black dog-done");
            d.Tell("a black night");
            ExpectMsg("a black night-done");

            probe.ExpectNext<EventEnvelope>(p => p.PersistenceId == "d" && p.SequenceNr == 1L && p.Event.Equals("a black dog"));
            probe.ExpectNoMsg(TimeSpan.FromMilliseconds(100));
            probe.Request(10);
            probe.ExpectNext<EventEnvelope>(p => p.PersistenceId == "d" && p.SequenceNr == 2L && p.Event.Equals("a black night"));

            b.Tell(new JournalTestActor.DeleteCommand(1));
            AwaitAssert(() => ExpectMsg("1-deleted"));

            d.Tell(new JournalTestActor.DeleteCommand(2));
            AwaitAssert(() => ExpectMsg("2-deleted"));

            probe.Request(10);
            probe.ExpectNoMsg(TimeSpan.FromMilliseconds(100));

            probe.Cancel();
        }
    }
}