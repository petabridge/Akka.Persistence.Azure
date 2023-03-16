#### 1.5.1 March 16 2023 ####

* [Update Akka.NET v1.5.1](https://github.com/akkadotnet/akka.net/releases/tag/1.5.1)
* [Bump Azure.Storage.Blobs to 12.15.0](https://github.com/petabridge/Akka.Persistence.Azure/pull/287)
* [Add multi journal support](https://github.com/petabridge/Akka.Persistence.Azure/pull/272)

**Multi Journal Support**

You can now add more than one Akka.Persistence.Azure settings and use them singularly for different Akka plugins.

In the example below, we set up two separate journal options with two distinct identifier, one is being used as default persistence plugin, and the other are being used as the journal for cluster sharding.

```csharp
var persistenceJournal = new AzureTableStorageJournalOptions(true)
{
    Identifier = "azure-journal",
    ConnectionString = connectionString
};
var shardJournal = new AzureTableStorageJournalOptions(false)
{
    Identifier = "azure-shard-journal",
    ConnectionString = shardConnectionString
};
builder
    .WithClustering()
    .WithAzureTableJournal(persistenceJournal)
    .WithAzureTableJournal(shardJournal)
    .WithAzureBlobsSnapshotStore(new AzureBlobSnapshotOptions
    {
        ConnectionString = connectionString
    })
    .WithShardRegion<ShardRegionKey>(
        "region-1",
        Customer.Props,
        new MessageExtractor(10),
        new ShardOptions
        {
            JournalOptions = shardJournal,
            StateStoreMode = StateStoreMode.Persistence
        });
```

#### 1.5.0 March 02 2023 ####

* [Targets Akka.NET v1.5.0](https://github.com/akkadotnet/akka.net/releases/tag/1.5.0)
* [Targets Akka.Hosting v1.5.0](https://github.com/akkadotnet/Akka.Hosting/releases/tag/1.5.0)

#### 0.9.2 September 27 2022 ####

* [Bump Akka.NET version from 1.4.40 to 1.4.43](https://github.com/akkadotnet/akka.net/releases/tag/1.4.43)
* [Bump Akka.Persistence.Hosting from 0.4.2 to 0.4.3](https://github.com/petabridge/Akka.Persistence.Azure/pull/245)
* [Bump Azure.Identity from 1.6.1 to 1.7.0](https://github.com/petabridge/Akka.Persistence.Azure/pull/247)
* [Clean up all async operations during actor stop](https://github.com/petabridge/Akka.Persistence.Azure/pull/249)
* [Refactor `DefaultAzureCredential` to `TokenCredential`](https://github.com/petabridge/Akka.Persistence.Azure/pull/250)
* [Chunk batch transactions to 100 item chunk batches](https://github.com/petabridge/Akka.Persistence.Azure/pull/254)

#### 0.9.1 August 29 2022 ####

* [Bump Akka.NET version from 1.4.39 to 1.4.40](https://github.com/akkadotnet/akka.net/releases/tag/1.4.40)
* [Bump Akka.Persistence.Hosting version from 0.4.1 to 0.4.2](https://github.com/petabridge/Akka.Persistence.Azure/pull/233)
* [Bump Azure.Storage.Blobs version from 12.12.0 to 12.13.1](https://github.com/petabridge/Akka.Persistence.Azure/pull/234)
* [Bump Azure.Identity version from 1.6.0 to 1.6.1](https://github.com/petabridge/Akka.Persistence.Azure/pull/231)
* [Added programmatic Setup classes](https://github.com/petabridge/Akka.Persistence.Azure/pull/235)
* [Update Akka.Hosting support to support `DefaultAzureCredential`](https://github.com/petabridge/Akka.Persistence.Azure/pull/237)

New Setup classes are added to allow programmatic setup of the journal table and snapshot-store blog storage; these setup classes supports `DefaultAzureCredential`. Note that to use `DefaultAzureCredential` from the `Azure.Identity` package, you need to provide both service URI and credential.

```csharp
var host = new HostBuilder()
    .ConfigureServices(collection =>
    {
        collection.AddAkka("MyActorSys", builder =>
        {
            var credentials = new DefaultAzureCredential();
            
            // Programatically setup the journal table
            builder.WithAzureTableJournal(setup => {
                setup.TableName = "myazuretable";
                setup.ServiceUri = new Uri("https://{account_name}.table.core.windows.net");
                setup.DefaultAzureCredential = credentials;
                // Optional TableClientOptions
                setup.TableClientOptions = new TableClientOptions(); 
            });
            
            // Programatically setup the snapshot-store blob container
            builder.WithAzureBlobsSnapshotStore(setup => {
                setup.ContainerName = "myAzureBlobContainer";
                setup.ServiceUri = new Uri("https://{account_name}.blob.core.windows.net");
                setup.DefaultAzureCredential = credentials;
                // Optional BlobClientOptions
                setup.BlobClientOptions = new BlobClientOptions(); 
            });
            
            builder.StartActors((system, registry) =>
            {
                var myActor = system.ActorOf(Props.Create(() => new MyPersistenceActor("ac1")), "actor1");
                registry.Register<MyPersistenceActor>(myActor);
            });
        });
    }).Build();
```

A few convenience `Akka.Hosting` extension methods are also added as a shortcut:
```csharp
var host = new HostBuilder()
    .ConfigureServices(collection =>
    {
        collection.AddAkka("MyActorSys", builder =>
        {
            var credentials = new DefaultAzureCredential();
            
            // Add the journal table
            builder.WithAzureTableJournal(
                serviceUri: new Uri("https://{account_name}.table.core.windows.net"),
                defaultAzureCredential: credentials);
            
            // Add the snapshot-store blob container
            builder.WithAzureBlobsSnapshotStore(
                serviceUri: new Uri("https://{account_name}.blob.core.windows.net"),
                defaultAzureCredential: credentials);
            
            builder.StartActors((system, registry) =>
            {
                var myActor = system.ActorOf(Props.Create(() => new MyPersistenceActor("ac1")), "actor1");
                registry.Register<MyPersistenceActor>(myActor);
            });
        });
    }).Build();
```

#### 0.9.0 July 21 2022 ####
Added [Akka.Hosting](https://github.com/akkadotnet/Akka.Hosting) support to Akka.Persistence.Azure, which you can activate via the following:

First, install the `Akka.Persistence.Azure.Hosting` NuGet package:

```shell
PS> install-package Akka.Persistence.Azure.Hosting

```

Next, add the `WithAzurePersistence` method calls to your `AkkaConfigurationBuilder` (from Akka.Hosting):

```csharp
var conn = Environment.GetEnvironmentVariable("AZURE_CONNECTION_STR");
var host = new HostBuilder()
    .ConfigureServices(collection =>
    {
        collection.AddAkka("MyActorSys", builder =>
        {
        	// enables both journal and snapshot store
            builder.WithAzurePersistence(conn);
            builder.StartActors((system, registry) =>
            {
                var myActor = system.ActorOf(Props.Create(() => new MyPersistenceActor("ac1")), "actor1");
                registry.Register<MyPersistenceActor>(myActor);
            });
        });
    }).Build();

await host.StartAsync();
return host;
```

You can also call the following methods to activate the journal / snapshot stores independently:

* ` WithAzureTableJournal`
* `WithAzureBlobsSnapshotStore`

#### 0.8.4 June 2 2022 ####
* Upgraded to [Akka.NET 1.4.39](https://github.com/akkadotnet/akka.net/releases/tag/1.4.39)
* [Update Azure.Identity to 1.6.0](https://github.com/petabridge/Akka.Persistence.Azure/pull/205)
* [Update System.Linq.Async to 6.0.1](https://github.com/petabridge/Akka.Persistence.Azure/pull/198)
* [Upgrade `Microsoft.Azure.Consmos.Table` to `Azure.Data.Tables` 12.5.0](https://github.com/petabridge/Akka.Persistence.Azure/pull/207)

#### 0.8.3 September 9 2021 ####
* Upgraded to [Akka.NET 1.4.25](https://github.com/akkadotnet/akka.net/releases/tag/1.4.25)
* [Update Azure.Identity to 1.4.1](https://github.com/petabridge/Akka.Persistence.Azure/pull/176)
* [Update Azure.Storage.Blobs to 12.10.0](https://github.com/petabridge/Akka.Persistence.Azure/pull/183)

#### 0.8.2 April 20 2021 ####
**Release of Akka.Persistence.Azure**

- [Fix settings throws on null or empty connection string ](https://github.com/petabridge/Akka.Persistence.Azure/pull/163)
