using Akka.Configuration;
using Akka.Persistence.TCK.Snapshot;
using Xunit;
using Xunit.Abstractions;
using static Akka.Persistence.Azure.Tests.Helper.AzureStorageConfigHelper;

namespace Akka.Persistence.Azure.Tests;

[Collection("AzureSpecs")]
public class AzureSnapshotStoreCustomFolderSaveSnapshotSpec: SnapshotStoreSaveSnapshotSpec
{
    private static readonly Config CustomConfig = ConfigurationFactory
        .ParseString("akka.persistence.snapshot-store.azure-blob-store.folders = \"folder-1/folder-2\"")
        .WithFallback(AzureConfig());
    
    public AzureSnapshotStoreCustomFolderSaveSnapshotSpec(ITestOutputHelper output) 
        : base(CustomConfig, nameof(AzureSnapshotStoreCustomFolderSaveSnapshotSpec), output)
    {
        AzurePersistence.Get(Sys);
    }
    
}