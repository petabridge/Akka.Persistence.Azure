using Akka.Persistence.Azure.Tests.Helper;
using Akka.Persistence.TCK.Snapshot;
using Xunit;
using Xunit.Abstractions;
using static Akka.Persistence.Azure.Tests.Helper.AzureStorageConfigHelper;

namespace Akka.Persistence.Azure.Tests;


[Collection("AzureSpecs")]
public class AzureSnapshotStoreSaveSnapshotSpec: SnapshotStoreSaveSnapshotSpec
{
    public AzureSnapshotStoreSaveSnapshotSpec(AzuriteFixture fixture, ITestOutputHelper output)
        : base(AzureConfig(fixture.ConnectionString), nameof(AzureBlobSnapshotStoreSpec), output)
    {
        AzurePersistence.Get(Sys);
    }
    
}