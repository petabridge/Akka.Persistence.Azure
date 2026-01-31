// -----------------------------------------------------------------------
// <copyright file="Program.cs" company="Petabridge, LLC">
//      Copyright (C) 2015 - 2025 Petabridge, LLC <https://petabridge.com>
// </copyright>
// -----------------------------------------------------------------------

using Akka.Actor;
using Akka.Hosting;
using Akka.Persistence.Azure.Hosting;
using Akka.Persistence.Azure.Sample.Aspire.Actors;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

var builder = Host.CreateApplicationBuilder(args);

// IMPORTANT: Get connection strings from Aspire-injected configuration
// Aspire provides these as "ConnectionStrings:{resource-name}" in the configuration
var blobConnectionString = builder.Configuration.GetConnectionString("persistence-blobs");
var tableConnectionString = builder.Configuration.GetConnectionString("persistence-tables");

// Log the connection strings for debugging the bug
// (This is the connection that was reported to fail with Azurite)
Console.WriteLine("=== Aspire Connection String Debug ===");
Console.WriteLine($"Blob Storage Connection String: {blobConnectionString}");
Console.WriteLine($"Table Storage Connection String: {tableConnectionString}");
Console.WriteLine("======================================");

if (string.IsNullOrEmpty(blobConnectionString))
{
    throw new InvalidOperationException(
        "Blob storage connection string not found. " +
        "Make sure to run this application through the Aspire AppHost.");
}

if (string.IsNullOrEmpty(tableConnectionString))
{
    throw new InvalidOperationException(
        "Table storage connection string not found. " +
        "Make sure to run this application through the Aspire AppHost.");
}

builder.Services.AddAkka("aspire-sample", (akkaBuilder, provider) =>
{
    // Configure Azure Table Storage for journal (event store)
    var journalOptions = new AzureTableStorageJournalOptions(isDefault: true)
    {
        ConnectionString = tableConnectionString,
        TableName = "AkkaJournal",
        AutoInitialize = true
    };
    
    // Configure Azure Blob Storage for snapshots
    var snapshotOptions = new AzureBlobSnapshotOptions
    {
        ConnectionString = blobConnectionString,
        ContainerName = "akka-snapshots",
        AutoInitialize = true
    };
    
    akkaBuilder
        .ConfigureLoggers(logBuilder =>
        {
            logBuilder.LogLevel = Akka.Event.LogLevel.InfoLevel;
            logBuilder.AddLoggerFactory();
        })
        .WithAzureTableJournal(journalOptions)
        .WithAzureBlobsSnapshotStore(snapshotOptions)
        .WithActors((system, registry, resolver) =>
        {
            // Create a counter actor to test persistence
            var counter = system.ActorOf(CounterActor.Props("counter-1"), "counter");
            registry.Register<CounterActor>(counter);
        });
});

var host = builder.Build();

// Run the host in background
await host.StartAsync();

// Get the actor system and registry
var actorSystem = host.Services.GetRequiredService<ActorSystem>();
var registry = host.Services.GetRequiredService<ActorRegistry>();
var counter = registry.Get<CounterActor>();
var logger = host.Services.GetRequiredService<ILoggerFactory>().CreateLogger("Program");

logger.LogInformation("Akka.NET Actor System started. Testing persistence with Aspire + Azurite...");

// Perform some operations to test both journal and snapshot store
logger.LogInformation("Sending increment commands to test journal persistence...");

for (var i = 1; i <= 7; i++)
{
    counter.Tell(new CounterCommands.Increment(i));
    await Task.Delay(500); // Small delay to see the logs
}

// Query the current count
var count = await counter.Ask<int>(new CounterCommands.GetCount(), TimeSpan.FromSeconds(5));
logger.LogInformation("Current counter value: {Count}", count);

// Keep the application running to observe the Aspire dashboard
logger.LogInformation("Press Ctrl+C to exit...");

await host.WaitForShutdownAsync();
