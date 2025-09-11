using System;
using System.Threading.Tasks;
using Akka.Actor;
using Akka.Hosting;
using Akka.Persistence.Azure.Hosting;
using Akka.Persistence.Azure.Journal;
using Akka.Persistence.Azure.Snapshot;
using Azure.Data.Tables;
using Azure.Storage.Blobs;
using FluentAssertions;
using Xunit;
using Xunit.Abstractions;

namespace Akka.Persistence.Azure.Tests.Hosting;

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

    public MultiOptionsSanityCheck(ITestOutputHelper output) : base(output: output)
    {
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

            config.GetString("akka.persistence.journal.plugin").Should().Be("akka.persistence.journal.azure-table");
            config.GetString("akka.persistence.snapshot-store.plugin").Should().Be("akka.persistence.snapshot-store.azure-blob-store");
            
            config.GetConfig("akka.persistence.journal.azure-table").Should().NotBeNull();
            config.GetConfig("akka.persistence.journal.azure-sharding-table").Should().NotBeNull();
            config.GetConfig("akka.persistence.snapshot-store.azure-blob-store").Should().NotBeNull();
            config.GetConfig("akka.persistence.snapshot-store.azure-sharding-blob-store").Should().NotBeNull();

            var persistence = Persistence.Instance.Apply(Sys);
            
            var defaultJournal = persistence.JournalFor(null);
            ((RepointableActorRef)defaultJournal).Underlying.Props.Type.Should().Be(typeof(AzureTableStorageJournal));
            
            // wait until journal actor is ready
            defaultJournal.Tell(new Identify(null), TestActor);
            await ExpectMsgAsync<ActorIdentity>();
            _journalFactory1Called.Should().BeTrue();
            
            var journal1 = persistence.JournalFor(_journalOptions1.PluginId);
            journal1.Equals(defaultJournal).Should().BeTrue();

            var defaultSnapshot = persistence.SnapshotStoreFor(null);
            ((RepointableActorRef)defaultSnapshot).Underlying.Props.Type.Should().Be(typeof(AzureBlobSnapshotStore));
            
            // wait until snapshot actor is ready
            defaultSnapshot.Tell(new Identify(null), TestActor);
            await ExpectMsgAsync<ActorIdentity>();
            _snapshotFactory1Called.Should().BeTrue();

            var snapshot1 = persistence.SnapshotStoreFor(_snapshotOptions1.PluginId);
            snapshot1.Equals(defaultSnapshot).Should().BeTrue();
            
            var journal2 = persistence.JournalFor(_journalOptions2.PluginId);
            ((RepointableActorRef)journal2).Underlying.Props.Type.Should().Be(typeof(AzureTableStorageJournal));
            
            // wait until journal actor is ready
            journal2.Tell(new Identify(null), TestActor);
            await ExpectMsgAsync<ActorIdentity>();
            _journalFactory2Called.Should().BeTrue();

            var snapshot2 = persistence.SnapshotStoreFor(_snapshotOptions2.PluginId);
            ((RepointableActorRef)snapshot2).Underlying.Props.Type.Should().Be(typeof(AzureBlobSnapshotStore));
            
            // wait until snapshot actor is ready
            snapshot2.Tell(new Identify(null), TestActor);
            await ExpectMsgAsync<ActorIdentity>();
            _snapshotFactory2Called.Should().BeTrue();
    }
    
    private BlobServiceClient SnapshotClientFactory1()
    {
        _snapshotFactory1Called = true;
        return new BlobServiceClient(connectionString: "UseDevelopmentStorage=true");
    }
    
    private BlobServiceClient SnapshotClientFactory2()
    {
        _snapshotFactory2Called = true;
        return new BlobServiceClient(connectionString: "UseDevelopmentStorage=true");
    }

    private TableServiceClient JournalClientFactory1()
    {
        _journalFactory1Called = true;
        return new TableServiceClient(connectionString: "UseDevelopmentStorage=true");
    }
    
    private TableServiceClient JournalClientFactory2()
    {
        _journalFactory2Called = true;
        return new TableServiceClient(connectionString: "UseDevelopmentStorage=true");
    }
}