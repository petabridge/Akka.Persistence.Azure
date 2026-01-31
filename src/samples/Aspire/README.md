# Akka.Persistence.Azure Aspire Sample

This sample demonstrates how to use **Akka.Persistence.Azure** with [.NET Aspire](https://learn.microsoft.com/en-us/dotnet/aspire/) for local development using the [Azurite](https://learn.microsoft.com/en-us/azure/storage/common/storage-use-azurite) emulator.

## Prerequisites

- [.NET 8.0 SDK](https://dotnet.microsoft.com/download/dotnet/8.0) or later
- [Docker](https://www.docker.com/get-started) (for running Azurite via Aspire)
- [Aspire CLI](https://learn.microsoft.com/en-us/dotnet/aspire/fundamentals/dotnet-aspire-cli) (optional, for `aspire run`)

## Projects

| Project | Description |
|---------|-------------|
| `Akka.Persistence.Azure.Sample.Aspire.AppHost` | Aspire orchestration project that configures Azurite and launches the sample |
| `Akka.Persistence.Azure.Sample.Aspire` | Akka.NET application with a persistent `CounterActor` |

## Running the Sample

### Option 1: Using `dotnet run`

```bash
cd src/samples/Aspire/Akka.Persistence.Azure.Sample.Aspire.AppHost
dotnet run
```

### Option 2: Using Aspire CLI

```bash
cd src/samples/Aspire/Akka.Persistence.Azure.Sample.Aspire.AppHost
aspire run
```

## What to Expect

When you run the sample:

1. **Aspire Dashboard** - A browser window will open with the Aspire dashboard (or you'll see a login URL in the console)
2. **Azurite Container** - Docker will start an Azurite container for Azure Storage emulation
3. **Sample Application** - The Akka.NET application will start and:
   - Print the connection strings it received from Aspire
   - Create an Azure Table for the journal
   - Create an Azure Blob Container for snapshots
   - Run a `CounterActor` that increments a counter 7 times
   - Save a snapshot after every 5 events
   - Display the final counter value (should be 28)

### Sample Output

```
=== Aspire Connection String Debug ===
Blob Storage Connection String: DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=...;BlobEndpoint=http://127.0.0.1:XXXXX/devstoreaccount1;
Table Storage Connection String: DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=...;TableEndpoint=http://127.0.0.1:XXXXX/devstoreaccount1;
======================================
[INFO] Created Azure Cloud Table
[INFO] Created Azure Blob Container akka-snapshots
[INFO] Recovery completed. Current count: 0
[INFO] Incremented by 1. New count: 1
[INFO] Incremented by 2. New count: 3
...
[INFO] Saving snapshot after 5 events
[INFO] Snapshot saved successfully at sequence 5
...
Current counter value: 28
Press Ctrl+C to exit...
```

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                     Aspire AppHost                          │
│  ┌────────────────────┐    ┌─────────────────────────────┐  │
│  │  Azure Storage     │    │  Akka.NET Sample App        │  │
│  │  (Azurite)         │◄───│                             │  │
│  │  ├─ Blob (10000)   │    │  ┌─────────────────────┐    │  │
│  │  ├─ Queue (10001)  │    │  │   CounterActor      │    │  │
│  │  └─ Table (10002)  │    │  │   (Persistent)      │    │  │
│  └────────────────────┘    │  └─────────────────────┘    │  │
│                            └─────────────────────────────┘  │
└─────────────────────────────────────────────────────────────┘
```

## Key Code Patterns

### AppHost Configuration (Program.cs)

```csharp
var azureStorage = builder.AddAzureStorage("storage")
    .RunAsEmulator();

var blobStorage = azureStorage.AddBlobs("persistence-blobs");
var tableStorage = azureStorage.AddTables("persistence-tables");

builder.AddProject<Projects.Akka_Persistence_Azure_Sample_Aspire>("akka-sample")
    .WithReference(blobStorage)
    .WithReference(tableStorage)
    .WaitFor(azureStorage);
```

### Reading Connection Strings from Aspire

```csharp
var blobConnectionString = builder.Configuration.GetConnectionString("persistence-blobs");
var tableConnectionString = builder.Configuration.GetConnectionString("persistence-tables");
```

### Configuring Akka.Persistence.Azure

```csharp
var journalOptions = new AzureTableStorageJournalOptions(isDefault: true)
{
    ConnectionString = tableConnectionString,
    TableName = "AkkaJournal",
    AutoInitialize = true
};

var snapshotOptions = new AzureBlobSnapshotOptions
{
    ConnectionString = blobConnectionString,
    ContainerName = "akka-snapshots",
    AutoInitialize = true
};

akkaBuilder
    .WithAzureTableJournal(journalOptions)
    .WithAzureBlobsSnapshotStore(snapshotOptions);
```

## Troubleshooting

### Docker Not Running

If you see errors about Docker, ensure Docker Desktop (or Docker Engine on Linux) is running.

### Port Conflicts

Aspire dynamically assigns ports for Azurite. If you have another Azurite instance running, you may see port conflicts. Stop other instances or let Aspire manage the ports.

### Connection String Issues

The sample prints the connection strings it receives. If you see issues connecting, verify that:
- The connection string includes `BlobEndpoint=` and `TableEndpoint=` pointing to localhost
- The ports in the connection string match the running Azurite container

## MCP Integration

This repository includes MCP (Model Context Protocol) configuration for AI assistants:
- `.mcp.json` - Claude Code configuration
- `opencode.jsonc` - OpenCode configuration

These enable AI assistants to interact with Aspire resources during development.
