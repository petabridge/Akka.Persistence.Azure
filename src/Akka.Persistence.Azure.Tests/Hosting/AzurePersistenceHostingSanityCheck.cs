using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Akka.Actor;
using Akka.Event;
using Akka.Hosting;
using Akka.Persistence.Azure.Hosting;
using Akka.Persistence.Azure.Journal;
using Akka.Persistence.Azure.Snapshot;
using Akka.Persistence.Azure.Tests.Helper;
using Akka.Persistence.Hosting;
using Akka.TestKit.Xunit2.Internals;
using Azure.Data.Tables;
using Azure.Identity;
using Azure.Storage.Blobs;
using FluentAssertions;
using FluentAssertions.Extensions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Xunit;
using Xunit.Abstractions;

namespace Akka.Persistence.Azure.Tests.Hosting
{
    [Collection("AzureSpecs")]
    public class AzurePersistenceHostingSanityCheck
    {
        public enum StartMethod
        {
            ConnectionString,
            TokenCredential,
            ServiceClient
        }
        
        private static async Task<IHost> StartHost(Action<AkkaConfigurationBuilder> testSetup, StartMethod startMethod)
        {
            var conn = Environment.GetEnvironmentVariable("AZURE_CONNECTION_STR") ?? "UseDevelopmentStorage=true";
            await DbUtils.CleanupCloudTable(conn);
            
            var host = new HostBuilder()
                .ConfigureServices(collection =>
                {
                    var tableClient = new TableServiceClient(conn);
                    var blobClient = new BlobServiceClient(conn);
                    collection.AddAkka("MyActorSys", builder =>
                    {
                        switch (startMethod)
                        {
                            case StartMethod.ConnectionString:
                                builder.WithAzurePersistence(conn);
                                break;
                            case StartMethod.TokenCredential: // Doesn't work with Azurite
                                var credential = new DefaultAzureCredential();
                                builder
                                    .WithAzureTableJournal(opt =>
                                    {
                                        opt.ServiceUri = new Uri("http://127.0.0.1:10002");
                                        opt.AzureCredential = credential;
                                    }, true)
                                    .WithAzureBlobsSnapshotStore(opt =>
                                    {
                                        opt.ServiceUri = new Uri("http://127.0.0.1:10000");
                                        opt.AzureCredential = credential;
                                    }, true);
                                break;
                            case StartMethod.ServiceClient:
                                builder.WithAzurePersistence(
                                    tableServiceClientFactory: () => tableClient,
                                    blobServiceClientFactory: () => blobClient
                                );
                                break;
                            default:
                                throw new ArgumentOutOfRangeException(nameof(startMethod));
                        }
                        testSetup(builder);
                    });
                }).Build();

            await host.StartAsync();
            return host;
        }

        public sealed class MyPersistenceActor : ReceivePersistentActor
        {
            private List<int> _values = new List<int>();

            public MyPersistenceActor(string persistenceId)
            {
                PersistenceId = persistenceId;

                Recover<SnapshotOffer>(offer =>
                {
                    if (offer.Snapshot is IEnumerable<int> ints)
                    {
                        _values = new List<int>(ints);
                    }
                });

                Recover<int>(i => { _values.Add(i); });

                Command<int>(i =>
                {
                    Persist(i, i1 =>
                    {
                        _values.Add(i);
                        if (LastSequenceNr % 2 == 0)
                        {
                            SaveSnapshot(_values);
                        }

                        Sender.Tell("ACK");
                    });
                });

                Command<string>(str => str.Equals("getall"), s => { Sender.Tell(_values.ToArray()); });

                Command<SaveSnapshotSuccess>(s => { });
            }

            public override string PersistenceId { get; }
        }

        private readonly ITestOutputHelper _output;

        public AzurePersistenceHostingSanityCheck(ITestOutputHelper output)
        {
            _output = output;
        }

        [Theory]
        [InlineData(StartMethod.ConnectionString)]
        // [InlineData(StartMethod.TokenCredential)] // Doesn't work with Azurite
        [InlineData(StartMethod.ServiceClient)]
        public async Task ShouldLaunchAzurePersistence(StartMethod startMethod)
        {
           // arrange
            using var host = await StartHost(builder => {
            
                    builder.StartActors((system, registry) =>
                    {
                        var myActor = system.ActorOf(Props.Create(() => new MyPersistenceActor("ac1")), "actor1");
                        registry.Register<MyPersistenceActor>(myActor);
                    })
                    .WithActors((system, registry) =>
                    {
                        var extSystem = (ExtendedActorSystem)system;
                        var logger = extSystem.SystemActorOf(Props.Create(() => new TestOutputLogger(_output)), "log-test");
                        logger.Tell(new InitializeLogger(system.EventStream));
                    });
            }, startMethod);

            var actorSystem = host.Services.GetRequiredService<ActorSystem>();
            var actorRegistry = host.Services.GetRequiredService<ActorRegistry>();
            var myPersistentActor = actorRegistry.Get<MyPersistenceActor>();
            
            // act
            var resp1 = await myPersistentActor.Ask<string>(1, TimeSpan.FromSeconds(3));
            var resp2 = await myPersistentActor.Ask<string>(2, TimeSpan.FromSeconds(3));
            var snapshot = await myPersistentActor.Ask<int[]>("getall", TimeSpan.FromSeconds(3));

            // assert
            snapshot.Should().BeEquivalentTo(new[] {1, 2});

            // kill + recreate actor with same PersistentId
            await myPersistentActor.GracefulStop(TimeSpan.FromSeconds(3));
            var myPersistentActor2 = actorSystem.ActorOf(Props.Create(() => new MyPersistenceActor("ac1")), "actor1a");
            
            var snapshot2 = await myPersistentActor2.Ask<int[]>("getall", TimeSpan.FromSeconds(3));
            snapshot2.Should().BeEquivalentTo(new[] {1, 2});
            
            // validate configs
            var config = actorSystem.Settings.Config;
            config.GetString("akka.persistence.journal.plugin").Should().Be("akka.persistence.journal.azure-table");
            config.GetString("akka.persistence.snapshot-store.plugin").Should().Be("akka.persistence.snapshot-store.azure-blob-store");
        }

        [Fact(DisplayName = "Multiple journal and snapshot options should work")]
        public async Task ShouldHandleMultiOptions()
        {
            var snapshotOptions1 = new AzureBlobSnapshotOptions(true)
            {
                ConnectionString = "UseDevelopmentStorage=true",
                Identifier = "azure-blob-store",
                ContainerName = "akka-persistence-default-container",
                AutoInitialize = true
            };
            var snapshotOptions2 = new AzureBlobSnapshotOptions(false)
            {
                ConnectionString = "UseDevelopmentStorage=true",
                Identifier = "azure-sharding-blob-store",
                ContainerName = "akka-persistence-sharding-container",
                AutoInitialize = true
            };
            var journalOptions1 = new AzureTableStorageJournalOptions(true)
            {
                ConnectionString = "UseDevelopmentStorage=true",
                Identifier = "azure-table",
                TableName = "AkkaPersistenceDefaultTable",
                AutoInitialize = true
            };
            var journalOptions2 = new AzureTableStorageJournalOptions(false)
            {
                ConnectionString = "UseDevelopmentStorage=true",
                Identifier = "azure-sharding-table",
                TableName = "AkkaPersistenceShardingTable",
                AutoInitialize = true
            };

            var host = Host.CreateDefaultBuilder()
                .ConfigureServices((context, services) =>
                {
                    services.AddAkka(nameof(AzurePersistenceHostingSanityCheck), (builder, provider) =>
                    {
                        builder
                            .WithJournal(journalOptions1)
                            .WithJournal(journalOptions2)
                            .WithSnapshot(snapshotOptions1)
                            .WithSnapshot(snapshotOptions2);
                    });
                })
                .Build();
            
            try
            {
                await host.StartAsync();
                var sys = host.Services.GetRequiredService<ActorSystem>();

                var config = sys.Settings.Config;

                config.GetString("akka.persistence.journal.plugin").Should().Be("akka.persistence.journal.azure-table");
                config.GetString("akka.persistence.snapshot-store.plugin").Should().Be("akka.persistence.snapshot-store.azure-blob-store");
                
                config.GetConfig("akka.persistence.journal.azure-table").Should().NotBeNull();
                config.GetConfig("akka.persistence.journal.azure-sharding-table").Should().NotBeNull();
                config.GetConfig("akka.persistence.snapshot-store.azure-blob-store").Should().NotBeNull();
                config.GetConfig("akka.persistence.snapshot-store.azure-sharding-blob-store").Should().NotBeNull();

                var persistence = Persistence.Instance.Apply(sys);
                
                var journal = persistence.JournalFor(null);
                ((RepointableActorRef)journal).Underlying.Props.Type.Should().Be(typeof(AzureTableStorageJournal));
                var journalSettings = await journal.Ask<AzureTableStorageJournalSettings>(GetSettings.Instance, 3.Seconds());
                journalSettings.TableName.Should().Be("AkkaPersistenceDefaultTable");

                var snapshot = persistence.SnapshotStoreFor(null);
                ((RepointableActorRef)snapshot).Underlying.Props.Type.Should().Be(typeof(AzureBlobSnapshotStore));
                var snapshotSettings = await snapshot.Ask<AzureBlobSnapshotStoreSettings>(GetSettings.Instance);
                snapshotSettings.ContainerName.Should().Be("akka-persistence-default-container");
            }
            finally
            {
                host.Dispose();
            }
        }
    }
}