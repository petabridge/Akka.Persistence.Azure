using Akka.Actor;
using Akka.Configuration;
using Akka.Persistence.Azure.Query;
using Akka.Persistence.Azure.TestHelpers;
using Akka.Persistence.Azure.Tests.Helper;
using Akka.Persistence.Query;
using Akka.Persistence.TCK.Query;
using Akka.Streams.TestKit;
using System;
using Xunit;
using Xunit.Abstractions;

namespace Akka.Persistence.Azure.Tests.Query
{
    [Collection("AzureQuery")]
    public sealed class AzureTableAllEvents : AllEventsSpec, IClassFixture<AzureEmulatorFixture>
    {
        public AzureTableAllEvents(ITestOutputHelper output)
            : base(Config(), nameof(AzureTablePersistenceIdsSpec), output)
        {
            AzurePersistence.Get(Sys);

            ReadJournal =
                Sys.ReadJournalFor<AzureTableStorageReadJournal>(
                    AzureTableStorageReadJournal.Identifier);

            output.WriteLine("Current table: {0}", TableName);
        }

        public static string TableName { get; private set; }

        public static Config Config()
        {
            var azureConfig =
                !string.IsNullOrEmpty(Environment.GetEnvironmentVariable("AZURE_CONNECTION_STR"))
                    ? AzureStorageConfigHelper.AzureConfig(Environment.GetEnvironmentVariable("AZURE_CONNECTION_STR"))
                    : AzureStorageConfigHelper.AzureConfig(AzureEmulatorFixture.ConnectionString);

            TableName = azureConfig.GetString("akka.persistence.journal.azure-table.table-name");

            return azureConfig;
        }
        [Fact]
        public override void ReadJournal_query_AllEvents_should_find_new_events()
        {
            var queries = ReadJournal as IAllEventsQuery;
            var a = Sys.ActorOf(Query.TestActor.Props("a"));
            var b = Sys.ActorOf(Query.TestActor.Props("b"));

            a.Tell("hello");
            ExpectMsg("hello-done");

            var eventSrc = queries.AllEvents(NoOffset.Instance);
            var probe = eventSrc.RunWith(this.SinkProbe<EventEnvelope>(), Materializer);
            probe.Request(2);
            probe.ExpectNext<EventEnvelope>(p => p.PersistenceId == "a" && p.SequenceNr == 1L && p.Event.Equals("hello"));
            probe.ExpectNoMsg(TimeSpan.FromMilliseconds(100));

            a.Tell("world");
            ExpectMsg("world-done");
            b.Tell("test");
            ExpectMsg("test-done");

            probe.ExpectNext<EventEnvelope>(p => p.PersistenceId == "a" && p.SequenceNr == 2L && p.Event.Equals("world"));

            probe.ExpectNoMsg(TimeSpan.FromMilliseconds(100));

            probe.Request(10);
            probe.ExpectNext<EventEnvelope>(p => p.PersistenceId == "b" && p.SequenceNr == 1L && p.Event.Equals("test"));
            probe.Cancel();
        }
        [Fact]
        public override void ReadJournal_query_AllEvents_should_find_events_from_offset_exclusive()
        {
            var queries = ReadJournal as IAllEventsQuery;

            var a = Sys.ActorOf(Query.TestActor.Props("a"));
            var b = Sys.ActorOf(Query.TestActor.Props("b"));
            var c = Sys.ActorOf(Query.TestActor.Props("c"));

            a.Tell("keep");
            ExpectMsg("keep-done");
            a.Tell("calm");
            ExpectMsg("calm-done");
            b.Tell("and");
            ExpectMsg("and-done");
            a.Tell("keep");
            ExpectMsg("keep-done");
            a.Tell("streaming");
            ExpectMsg("streaming-done");

            var eventSrc1 = queries.AllEvents(NoOffset.Instance);
            var probe1 = eventSrc1.RunWith(this.SinkProbe<EventEnvelope>(), Materializer);
            probe1.Request(4);
            probe1.ExpectNext<EventEnvelope>(p => p.PersistenceId == "a" && p.SequenceNr == 1L && p.Event.Equals("keep"));
            probe1.ExpectNext<EventEnvelope>(p => p.PersistenceId == "a" && p.SequenceNr == 2L && p.Event.Equals("calm"));
            probe1.ExpectNext<EventEnvelope>(p => p.PersistenceId == "b" && p.SequenceNr == 1L && p.Event.Equals("and"));
            var offs = probe1.ExpectNext<EventEnvelope>(p => p.PersistenceId == "a" && p.SequenceNr == 3L && p.Event.Equals("keep")).Offset;
            probe1.Cancel();

            var eventSrc2 = queries.AllEvents(offs);
            var probe2 = eventSrc2.RunWith(this.SinkProbe<EventEnvelope>(), Materializer);
            probe2.Request(10);

            b.Tell("new");
            ExpectMsg("new-done");
            c.Tell("events");
            ExpectMsg("events-done");

            // everything before "streaming" are not included, since exclusive offset
            probe2.ExpectNext<EventEnvelope>(p => p.PersistenceId == "a" && p.SequenceNr == 4L && p.Event.Equals("streaming"));
            probe2.ExpectNext<EventEnvelope>(p => p.PersistenceId == "b" && p.SequenceNr == 2L && p.Event.Equals("new"));
            probe2.ExpectNext<EventEnvelope>(p => p.PersistenceId == "c" && p.SequenceNr == 1L && p.Event.Equals("events"));
            probe2.Cancel();
        }
    }
}
