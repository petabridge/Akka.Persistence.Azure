using Akka.Persistence.TCK.Snapshot;
using Xunit;
using Xunit.Abstractions;
using static Akka.Persistence.Azure.Tests.Helper.AzureStorageConfigHelper;

namespace Akka.Persistence.Azure.Tests;


[Collection("AzureSpecs")]
public class AzureSnapshotStoreSaveSnapshotSpec: SnapshotStoreSaveSnapshotSpec
{
    public AzureSnapshotStoreSaveSnapshotSpec(ITestOutputHelper output) 
        : base(AzureConfig(), nameof(AzureBlobSnapshotStoreSpec), output)
    {
        AzurePersistence.Get(Sys);
    }
    
}