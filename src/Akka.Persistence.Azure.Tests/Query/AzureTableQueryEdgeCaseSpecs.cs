using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Akka.Actor;
using Akka.Configuration;
using Akka.Persistence.Azure.Query;
using Akka.Persistence.Azure.TestHelpers;
using Akka.Persistence.Azure.Tests.Helper;
using Akka.Persistence.Journal;
using Akka.Persistence.Query;
using Akka.Streams;
using Akka.Util.Internal;
using FluentAssertions;
using Xunit;
using Xunit.Abstractions;

namespace Akka.Persistence.Azure.Tests.Query
{
    [Collection("AzureQuery")]
    public class AzureTableQueryEdgeCaseSpecs : Akka.TestKit.Xunit2.TestKit
    {
        public static readonly AtomicCounter Counter = new AtomicCounter(0);
        private readonly ITestOutputHelper _output;

        protected AzureTableStorageReadJournal ReadJournal { get; }

        protected IMaterializer Materializer { get; }

        public class RealMsg
        {
            public RealMsg(string msg)
            {
                Msg = msg;
            }
            public string Msg { get; }
        }

        public const int MessageCount = 20;

        public AzureTableQueryEdgeCaseSpecs(ITestOutputHelper output)
            : base(Config(), nameof(AzureTableQueryEdgeCaseSpecs), output)
        {
            _output = output;
            Materializer = Sys.Materializer();
            ReadJournal = Sys.ReadJournalFor<AzureTableStorageReadJournal>(AzureTableStorageReadJournal.Identifier);
        }

        /// <summary>
        /// Reproduction spec for https://github.com/akkadotnet/Akka.Persistence.MongoDB/issues/61
        /// </summary>
        [Fact(Skip = "Need to fix this in https://github.com/petabridge/Akka.Persistence.Azure/issues/107")]
        public async Task Bug61_Events_Recovered_By_Id_Should_Match_Tag()
        {
            var actor = Sys.ActorOf(TagActor.Props("x"));

            actor.Tell(MessageCount);
            ExpectMsg($"{MessageCount}-done", TimeSpan.FromSeconds(20));

            var eventsById = await ReadJournal.CurrentEventsByPersistenceId("x", 0L, long.MaxValue)
                .RunAggregate(ImmutableHashSet<EventEnvelope>.Empty, (agg, e) => agg.Add(e), Materializer);

            eventsById.Count.Should().Be(MessageCount);

            var eventsByTag = await ReadJournal.CurrentEventsByTag(typeof(RealMsg).Name)
                .RunAggregate(ImmutableHashSet<EventEnvelope>.Empty, (agg, e) => agg.Add(e), Materializer);

            eventsByTag.Count.Should().Be(MessageCount, "All events should be loaded by tag");

            eventsById.All(x => x.Event is RealMsg).Should().BeTrue("Expected all events by id to be RealMsg");
            eventsByTag.All(x => x.Event is RealMsg).Should().BeTrue("Expected all events by tag to be RealMsg");
        }

        /// <summary>
        /// Reproduction spec for https://github.com/akkadotnet/Akka.Persistence.MongoDB/issues/80
        /// </summary>
        [Fact(Skip = "Need to fix this in https://github.com/petabridge/Akka.Persistence.Azure/issues/107")]
        public void Bug80_CurrentEventsByTag_should_Recover_until_end()
        {
            var actor = Sys.ActorOf(TagActor.Props("y"));
            var msgCount = 1200;
            actor.Tell(msgCount);
            ExpectMsg($"{msgCount}-done", TimeSpan.FromSeconds(20));

            var eventsByTag = ReadJournal.CurrentEventsByTag(typeof(RealMsg).Name)
                .RunForeach(e => TestActor.Tell(e), Materializer);

            ReceiveN(msgCount);
        }

        /// <summary>
        /// Making sure EventsByTag didn't break during implementation of https://github.com/akkadotnet/Akka.Persistence.MongoDB/issues/80
        /// </summary>
        [Fact(Skip = "Need to fix this in https://github.com/petabridge/Akka.Persistence.Azure/issues/107")]
        public void Bug80_AllEventsByTag_should_Recover_all_messages()
        {
            var actor = Sys.ActorOf(TagActor.Props("y"));
            var msgCount = 1200;
            actor.Tell(msgCount);
            ExpectMsg($"{msgCount}-done", TimeSpan.FromSeconds(20));

            var eventsByTag = ReadJournal.EventsByTag(typeof(RealMsg).Name)
                .RunForeach(e => TestActor.Tell(e), Materializer);

            // can't do this because Offset isn't IComparable
            // ReceiveN(msgCount).Cast<EventEnvelope>().Select(x => x.Offset).Should().BeInAscendingOrder();
            ReceiveN(msgCount);

            // should receive more messages after the fact
            actor.Tell(msgCount);
            ExpectMsg($"{msgCount}-done", TimeSpan.FromSeconds(20));
            ReceiveN(msgCount);
        }

        private class TagActor : ReceivePersistentActor
        {
            public static Props Props(string id)
            {
                return Akka.Actor.Props.Create(() => new TagActor(id));
            }

            public TagActor(string id)
            {
                PersistenceId = id;

                Command<int>(i =>
                {
                    var msgs = new List<RealMsg>();
                    foreach (var n in Enumerable.Range(0, i))
                    {
                        msgs.Add(new RealMsg(i.ToString()));
                    }
                    PersistAll(msgs, m =>
                    {
                        if (LastSequenceNr >= i)
                        {
                            Sender.Tell($"{i}-done");
                        }
                    });
                });

                Command<RealMsg>(r =>
                {
                    Persist(r, e =>
                    {
                        Sender.Tell($"{e.Msg}-done");
                    });
                });
            }

            public override string PersistenceId { get; }
        }

        private class EventTagger : IWriteEventAdapter
        {
            public string DefaultTag { get; }

            public EventTagger()
            {
                DefaultTag = "accounts";
            }

            public string Manifest(object evt)
            {
                return string.Empty;
            }

            public object ToJournal(object evt)
            {
                return new Tagged(evt, ImmutableHashSet<string>.Empty.Add(DefaultTag).Add(evt.GetType().Name));
            }
        }

        public static string TableName { get; private set; }

        public static Config Config()
        {
            var cosmosString = Environment.GetEnvironmentVariable("AZURE_COSMOSDB_CONNECTION_STR");
            var blobString = Environment.GetEnvironmentVariable("AZURE_BLOB_CONNECTION_STR");

            if (string.IsNullOrWhiteSpace(cosmosString))
                cosmosString = EmulatorFixture.CosmosConnStr();

            if (string.IsNullOrWhiteSpace(blobString))
                blobString = EmulatorFixture.StorageConnStr();

            var azureConfig = AzureStorageConfigHelper.AzureConfig(cosmosString, blobString);

            TableName = azureConfig.GetString("akka.persistence.journal.azure-table.table-name");

            return azureConfig;
        }
    }
}
