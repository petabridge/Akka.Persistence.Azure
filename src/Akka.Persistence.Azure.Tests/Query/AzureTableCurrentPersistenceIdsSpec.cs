using System;
using System.Diagnostics;
using System.Linq;
using Akka.Actor;
using Akka.Configuration;
using Akka.Persistence.Azure.Query;
using Akka.Persistence.Azure.TestHelpers;
using Akka.Persistence.Azure.Tests.Helper;
using Akka.Persistence.Query;
using Akka.Persistence.TCK.Query;
using Akka.Streams.TestKit;
using Akka.Util.Internal;
using Xunit;
using Xunit.Abstractions;

namespace Akka.Persistence.Azure.Tests.Query
{
    [Collection("AzureQuery")]
    public sealed class AzureTableCurrentPersistenceIdsSpec
        : CurrentPersistenceIdsSpec
    {
        private readonly ITestOutputHelper _output;

        public AzureTableCurrentPersistenceIdsSpec(ITestOutputHelper output)
            : base(Config(), nameof(AzureTablePersistenceIdsSpec), output)
        {
            _output = output;
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
                    : AzureStorageConfigHelper.AzureConfig(AzuriteEmulatorFixture.GenerateConnStr());

            TableName = azureConfig.GetString("akka.persistence.journal.azure-table.table-name");

            return azureConfig;
        }

        public override void ReadJournal_query_CurrentPersistenceIds_should_not_see_new_events_after_complete()
        {
            var queries = ReadJournal.AsInstanceOf<ICurrentPersistenceIdsQuery>();

            Setup("a", 1);
            Setup("b", 1);
            Setup("c", 1);

            var greenSrc = queries.CurrentPersistenceIds();
            var probe = greenSrc.RunWith(this.SinkProbe<string>(), Materializer);
            var firstTwo = probe.Request(2).ExpectNextN(2);
            Assert.Empty(firstTwo.Except(new[] { "a", "b", "c" }).ToArray());

            var last = new[] { "a", "b", "c" }.Except(firstTwo).First();
            Setup("d", 1);

            probe.ExpectNoMsg(TimeSpan.FromMilliseconds(100));
            probe.Request(5)
                .ExpectNext(last)
                .ExpectComplete();
        }

        private IActorRef Setup(string persistenceId, int n)
        {
            var sw = Stopwatch.StartNew();
            var pref = Sys.ActorOf(JournalTestActor.Props(persistenceId));
            for (int i = 1; i <= n; i++)
            {
                pref.Tell($"{persistenceId}-{i}");
                ExpectMsg($"{persistenceId}-{i}-done", TimeSpan.FromSeconds(10), $"{persistenceId}-{i}-done");
            }
            _output.WriteLine(sw.ElapsedMilliseconds.ToString());
            return pref;
        }
    }
}