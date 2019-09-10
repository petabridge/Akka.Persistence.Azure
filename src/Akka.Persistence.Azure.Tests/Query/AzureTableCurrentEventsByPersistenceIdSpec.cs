using System;
using Akka.Actor;
using Akka.Configuration;
using Akka.Persistence.Azure.Query;
using Akka.Persistence.Azure.TestHelpers;
using Akka.Persistence.Azure.Tests.Helper;
using Akka.Persistence.Query;
using Akka.Persistence.TCK.Query;
using Akka.Streams.Dsl;
using Akka.Streams.TestKit;
using Akka.Util.Internal;
using Xunit;
using Xunit.Abstractions;

namespace Akka.Persistence.Azure.Tests.Query
{
    [Collection("AzureQuery")]
    public sealed class AzureTableCurrentEventsByPersistenceIdSpec
        : CurrentEventsByPersistenceIdSpec
    {
        public AzureTableCurrentEventsByPersistenceIdSpec(ITestOutputHelper output)
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
                    : AzureStorageConfigHelper.AzureConfig(WindowsAzureStorageEmulatorFixture.GenerateConnStr());

            TableName = azureConfig.GetString("akka.persistence.journal.azure-table.table-name");

            return azureConfig;
        }

        [Fact]
        public override void ReadJournal_CurrentEventsByPersistenceId_should_return_empty_stream_for_cleaned_journal_from_0_to_MaxLong()
        {
            var queries = ReadJournal.AsInstanceOf<ICurrentEventsByPersistenceIdQuery>();
            var pref = Setup("g1");

            pref.Tell(new JournalTestActor.DeleteCommand(3));
            AwaitAssert(() => ExpectMsg("3-deleted"));

            var src = queries.CurrentEventsByPersistenceId("g1", 0, long.MaxValue);
            src.Select(x => x.Event).RunWith(this.SinkProbe<object>(), Materializer).Request(1).ExpectComplete();
        }

        [Fact]
        public override void ReadJournal_CurrentEventsByPersistenceId_should_return_remaining_values_after_partial_journal_cleanup()
        {
            var queries = ReadJournal.AsInstanceOf<ICurrentEventsByPersistenceIdQuery>();
            var pref = Setup("h");

            pref.Tell(new JournalTestActor.DeleteCommand(2));
            AwaitAssert(() => ExpectMsg("2-deleted"));

            var src = queries.CurrentEventsByPersistenceId("h", 0L, long.MaxValue);
            src.Select(x => x.Event).RunWith(this.SinkProbe<object>(), Materializer)
                .Request(1)
                .ExpectNext("h-3")
                .ExpectComplete();
        }

        //[Fact]
        //public void ReadJournal_CurrentEventsByPersistenceId_failing_tests_combined_will_pass()
        //{
        //    var queries = ReadJournal.AsInstanceOf<ICurrentEventsByPersistenceIdQuery>();

        //    var pref1 = Setup("h");
        //    pref1.Tell(new JournalTestActor.DeleteCommand(2));
        //    AwaitAssert(() => ExpectMsg("2-deleted"));
        //    var src1 = queries.CurrentEventsByPersistenceId("h", 0L, long.MaxValue);
        //    src1.Select(x => x.Event).RunWith(this.SinkProbe<object>(), Materializer)
        //        .Request(1)
        //        .ExpectNext("h-3")
        //        .ExpectComplete();



        //    var pref = Setup("g1");
        //    pref.Tell(new JournalTestActor.DeleteCommand(3));
        //    AwaitAssert(() => ExpectMsg("3-deleted"));
        //    var src = queries.CurrentEventsByPersistenceId("g1", 0, long.MaxValue);
        //    src.Select(x => x.Event).RunWith(this.SinkProbe<object>(), Materializer)
        //        .Request(1)
        //        .ExpectComplete();


        //    //var queries = ReadJournal.AsInstanceOf<ICurrentEventsByPersistenceIdQuery>();
        //    //var pref2 = Setup("rak");

        //    //pref2.Tell(new JournalTestActor.DeleteCommand(2));
        //    //AwaitAssert(() => ExpectMsg("2-deleted"));

        //    //var src2 = queries.CurrentEventsByPersistenceId("rak", 0L, long.MaxValue);
        //    //src2.Select(x => x.Event).RunWith(this.SinkProbe<object>(), Materializer)
        //    //    .Request(1)
        //    //    .ExpectNext("rak-3")
        //    //    .ExpectComplete();
        //}


        private IActorRef Setup(string persistenceId)
        {
            var pref = SetupEmpty(persistenceId);

            pref.Tell(persistenceId + "-1");
            pref.Tell(persistenceId + "-2");
            pref.Tell(persistenceId + "-3");

            ExpectMsg(persistenceId + "-1-done");
            ExpectMsg(persistenceId + "-2-done");
            ExpectMsg(persistenceId + "-3-done");
            return pref;
        }

        private IActorRef SetupEmpty(string persistenceId)
        {
            return Sys.ActorOf(JournalTestActor.Props(persistenceId));
        }
    }
}