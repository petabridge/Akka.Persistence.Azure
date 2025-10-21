using Akka.Configuration;
using Akka.Persistence.Azure.Tests.Helper;
using Akka.Persistence.TCK.Snapshot;
using Xunit;
using Xunit.Abstractions;
using static Akka.Persistence.Azure.Tests.Helper.AzureStorageConfigHelper;

namespace Akka.Persistence.Azure.Tests;

[Collection("AzureSpecs")]
public class AzureSnapshotStoreCustomFolderSaveSnapshotSpec: SnapshotStoreSaveSnapshotSpec, IClassFixture<AzuriteFixture>
{
    private static Config CreateCustomConfig(string connectionString)
    {
        return ConfigurationFactory
            .ParseString("akka.persistence.snapshot-store.azure-blob-store.folders = \"folder-1/folder-2\"")
            .WithFallback(AzureConfig(connectionString));
    }

    public AzureSnapshotStoreCustomFolderSaveSnapshotSpec(AzuriteFixture fixture, ITestOutputHelper output)
        : base(CreateCustomConfig(fixture.ConnectionString), nameof(AzureSnapshotStoreCustomFolderSaveSnapshotSpec), output)
    {
        AzurePersistence.Get(Sys);
    }
    
}