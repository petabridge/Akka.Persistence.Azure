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

// Get connection strings from Aspire-injected configuration
// Aspire provides these as "ConnectionStrings:{resource-name}" in the configuration
var blobConnectionString = builder.Configuration.GetConnectionString("persistence-blobs");
var tableConnectionString = builder.Configuration.GetConnectionString("persistence-tables");

// Log the connection strings for debugging
Console.WriteLine("=== Aspire Connection String Debug ===");
Console.WriteLine($"Blob Storage Connection String:");
Console.WriteLine($"  {blobConnectionString}");
Console.WriteLine();
Console.WriteLine($"Table Storage Connection String:");
Console.WriteLine($"  {tableConnectionString}");
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
    akkaBuilder
        .ConfigureLoggers(logBuilder =>
        {
            logBuilder.LogLevel = Akka.Event.LogLevel.InfoLevel;
            logBuilder.AddLoggerFactory();
        })
        // Use separate connection strings for journal and snapshot store
        // This is the recommended approach when using Aspire because Aspire provides
        // different endpoint URLs for blob and table storage
        .WithAzureTableJournal(
            connectionString: tableConnectionString!,
            autoInitialize: true,
            tableName: "AkkaJournal")
        .WithAzureBlobsSnapshotStore(
            connectionString: blobConnectionString!,
            autoInitialize: true,
            containerName: "akka-snapshots")
        .WithActors((system, registry, resolver) =>
        {
            var counter = system.ActorOf(CounterActor.Props("counter-1"), "counter");
            registry.Register<CounterActor>(counter);
        });
});

var host = builder.Build();
await host.StartAsync();

var actorSystem = host.Services.GetRequiredService<ActorSystem>();
var registry = host.Services.GetRequiredService<ActorRegistry>();
var counter = registry.Get<CounterActor>();
var logger = host.Services.GetRequiredService<ILoggerFactory>().CreateLogger("Program");

logger.LogInformation("Akka.NET Actor System started. Testing persistence with Aspire + Azurite...");
logger.LogInformation("Sending increment commands to test journal persistence...");

for (var i = 1; i <= 7; i++)
{
    counter.Tell(new CounterCommands.Increment(i));
    await Task.Delay(500);
}

var count = await counter.Ask<int>(new CounterCommands.GetCount(), TimeSpan.FromSeconds(5));
logger.LogInformation("Current counter value: {Count}", count);
logger.LogInformation("Press Ctrl+C to exit...");

await host.WaitForShutdownAsync();
