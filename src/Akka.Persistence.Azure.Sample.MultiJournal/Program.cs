// -----------------------------------------------------------------------
//  <copyright file="Program.cs" company="Akka.NET Project">
//      Copyright (C) 2013-2023 .NET Foundation <https://github.com/akkadotnet/akka.net>
//  </copyright>
// -----------------------------------------------------------------------

using Akka.Actor;
using Akka.Cluster;
using Akka.Cluster.Hosting;
using Akka.Cluster.Sharding;
using Akka.Hosting;
using Akka.Persistence.Azure.Hosting;
using Akka.Persistence.Azure.Sample.MultiJournal;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

const string shardConnectionString = "UseDevelopmentStorage=true";
const string connectionString = "UseDevelopmentStorage=true";

using var host = new HostBuilder()
    .ConfigureLogging(builder =>
    {
        builder.AddConsole();
    })
    .ConfigureServices((context, services) =>
    {
        services.AddAkka("multi-journal", (builder, provider) =>
        {
            var persistenceJournal = new AzureTableStorageJournalOptions(true)
            {
                Identifier = "azure-journal",
                ConnectionString = connectionString, 
                AutoInitialize = true
            };
            var shardJournal = new AzureTableStorageJournalOptions(false)
            {
                Identifier = "azure-shard-journal",
                ConnectionString = shardConnectionString, 
                AutoInitialize = true
            };
            builder
                .WithClustering()
                .WithAzureTableJournal(persistenceJournal)
                .WithAzureTableJournal(shardJournal)
                .WithAzureBlobsSnapshotStore(new AzureBlobSnapshotOptions
                {
                    ConnectionString = connectionString, 
                    AutoInitialize = true
                })
                .WithShardRegion<ShardRegionKey>(
                    "region-1",
                    Customer.Props,
                    new MessageExtractor(10),
                    new ShardOptions
                    {
                        JournalOptions = shardJournal,
                        StateStoreMode = StateStoreMode.Persistence
                    })
                .WithActors((system, registry, resolver) =>
                {
                    var actor = system.ActorOf(resolver.Props<Purchaser>());
                    registry.Register<Purchaser>(actor);
                });
        });
    }).Build();

await host.StartAsync();

var sys = host.Services.GetRequiredService<ActorSystem>();
var cluster = Cluster.Get(sys);
var registry = host.Services.GetRequiredService<ActorRegistry>();
var purchaser = registry.Get<Purchaser>();
cluster.RegisterOnMemberUp(() =>
{
    purchaser.Tell("start");
});
cluster.Join(cluster.SelfAddress);
    
Console.ReadKey();

await host.StopAsync();