using System;
using System.Threading.Tasks;
using Akka.Actor;
using Akka.Hosting;
using Akka.Persistence.Azure.Hosting;
using Akka.Persistence.Azure.Journal;
using Akka.Persistence.Azure.Snapshot;
using Akka.Persistence.Azure.Tests.Helper;
using Azure.Data.Tables;
using Azure.Storage.Blobs;
using Xunit;

namespace Akka.Persistence.Azure.Tests.Hosting;

[Collection("AzureSpecs")]
public class MultiOptionsSanityCheck: Akka.Hosting.TestKit.TestKit
{
    private bool _snapshotFactory1Called;
    private bool _snapshotFactory2Called;
    private bool _journalFactory1Called;
    private bool _journalFactory2Called;

    private readonly AzureBlobSnapshotOptions _snapshotOptions1;
    private readonly AzureBlobSnapshotOptions _snapshotOptions2;
    private readonly AzureTableStorageJournalOptions _journalOptions1;
    private readonly AzureTableStorageJournalOptions _journalOptions2;
    private readonly string _connectionString;

    public MultiOptionsSanityCheck(AzuriteFixture fixture, ITestOutputHelper output) : base(output: output)
    {
        _connectionString = fixture.ConnectionString;
        _snapshotOptions1 = new AzureBlobSnapshotOptions(true)
        {
            ConnectionString = "Nonsense snapshot connection string 1, should not be used",
            Identifier = "azure-blob-store",
            ContainerName = "akka-persistence-default-container",
            AutoInitialize = true,
            BlobServiceClientFactory = SnapshotClientFactory1
        };
        _snapshotOptions2 = new AzureBlobSnapshotOptions(false)
        {
            ConnectionString = "Nonsense snapshot connection string 2, should not be used",
            Identifier = "azure-sharding-blob-store",
            ContainerName = "akka-persistence-sharding-container",
            AutoInitialize = true,
            BlobServiceClientFactory = SnapshotClientFactory2
        };
        _journalOptions1 = new AzureTableStorageJournalOptions(true)
        {
            ConnectionString = "Nonsense journal connection string 1, should not be used",
            Identifier = "azure-table",
            TableName = "AkkaPersistenceDefaultTable",
            AutoInitialize = true,
            TableServiceClientFactory = JournalClientFactory1
        };
        _journalOptions2 = new AzureTableStorageJournalOptions(false)
        {
            ConnectionString = "Nonsense journal connection string 2, should not be used",
            Identifier = "azure-sharding-table",
            TableName = "AkkaPersistenceShardingTable",
            AutoInitialize = true,
            TableServiceClientFactory = JournalClientFactory2
        };
    }
    
    protected override void ConfigureAkka(AkkaConfigurationBuilder builder, IServiceProvider provider)
    {
        builder
            .WithAzureTableJournal(_journalOptions1)
            .WithAzureTableJournal(_journalOptions2)
            .WithAzureBlobsSnapshotStore(_snapshotOptions1)
            .WithAzureBlobsSnapshotStore(_snapshotOptions2);
    }
    
    [Fact(DisplayName = "Multiple journal and snapshot options should work")]
    public async Task ShouldHandleMultiOptions()
    {
            var config = Sys.Settings.Config;

            Assert.Equal("akka.persistence.journal.azure-table", config.GetString("akka.persistence.journal.plugin"));
            Assert.Equal("akka.persistence.snapshot-store.azure-blob-store", config.GetString("akka.persistence.snapshot-store.plugin"));
            
            Assert.NotNull(config.GetConfig("akka.persistence.journal.azure-table"));
            Assert.NotNull(config.GetConfig("akka.persistence.journal.azure-sharding-table"));
            Assert.NotNull(config.GetConfig("akka.persistence.snapshot-store.azure-blob-store"));
            Assert.NotNull(config.GetConfig("akka.persistence.snapshot-store.azure-sharding-blob-store"));

            var persistence = Persistence.Instance.Apply(Sys);
            
            var defaultJournal = persistence.JournalFor(null);
            Assert.Equal(typeof(AzureTableStorageJournal), ((RepointableActorRef)defaultJournal).Underlying.Props.Type);
            
            // wait until journal actor is ready
            defaultJournal.Tell(new Identify(null), TestActor);
            await ExpectMsgAsync<ActorIdentity>();
            Assert.True(_journalFactory1Called);
            
            var journal1 = persistence.JournalFor(_journalOptions1.PluginId);
            Assert.True(journal1.Equals(defaultJournal));

            var defaultSnapshot = persistence.SnapshotStoreFor(null);
            Assert.Equal(typeof(AzureBlobSnapshotStore), ((RepointableActorRef)defaultSnapshot).Underlying.Props.Type);
            
            // wait until snapshot actor is ready
            defaultSnapshot.Tell(new Identify(null), TestActor);
            await ExpectMsgAsync<ActorIdentity>();
            Assert.True(_snapshotFactory1Called);

            var snapshot1 = persistence.SnapshotStoreFor(_snapshotOptions1.PluginId);
            Assert.True(snapshot1.Equals(defaultSnapshot));
            
            var journal2 = persistence.JournalFor(_journalOptions2.PluginId);
            Assert.Equal(typeof(AzureTableStorageJournal), ((RepointableActorRef)journal2).Underlying.Props.Type);
            
            // wait until journal actor is ready
            journal2.Tell(new Identify(null), TestActor);
            await ExpectMsgAsync<ActorIdentity>();
            Assert.True(_journalFactory2Called);

            var snapshot2 = persistence.SnapshotStoreFor(_snapshotOptions2.PluginId);
            Assert.Equal(typeof(AzureBlobSnapshotStore), ((RepointableActorRef)snapshot2).Underlying.Props.Type);
            
            // wait until snapshot actor is ready
            snapshot2.Tell(new Identify(null), TestActor);
            await ExpectMsgAsync<ActorIdentity>();
            Assert.True(_snapshotFactory2Called);
    }
    
    private BlobServiceClient SnapshotClientFactory1()
    {
        _snapshotFactory1Called = true;
        return new BlobServiceClient(connectionString: _connectionString);
    }

    private BlobServiceClient SnapshotClientFactory2()
    {
        _snapshotFactory2Called = true;
        return new BlobServiceClient(connectionString: _connectionString);
    }

    private TableServiceClient JournalClientFactory1()
    {
        _journalFactory1Called = true;
        return new TableServiceClient(connectionString: _connectionString);
    }

    private TableServiceClient JournalClientFactory2()
    {
        _journalFactory2Called = true;
        return new TableServiceClient(connectionString: _connectionString);
    }
}
