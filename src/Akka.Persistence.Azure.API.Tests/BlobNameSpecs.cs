using Akka.Persistence.Azure.Snapshot;
using Akka.Persistence.Azure.Util;

namespace Akka.Persistence.Azure.API.Tests;

public class BlobNameSpecs
{
    [Fact]
    public async Task VerifyToJournalRowKey()
    {
        var metadata = new SnapshotMetadata("persistenceId", 100, new DateTime(2000, 1, 1));
        await Verify(metadata.ToSnapshotBlobId(string.Empty));
        
        var verifiedToJournalRowKey = File.ReadAllText("./verify/BlobNameSpecs.VerifyToJournalRowKey.DotNet.verified.txt");
        Assert.Equal(verifiedToJournalRowKey, metadata.ToSnapshotBlobId("  "));
        Assert.Equal(verifiedToJournalRowKey, metadata.ToSnapshotBlobId(null));
        Assert.Equal(verifiedToJournalRowKey, metadata.ToSnapshotBlobId(AzureBlobSnapshotStoreSettings.SanitizeFolder("/")));
        Assert.Equal(verifiedToJournalRowKey, metadata.ToSnapshotBlobId(AzureBlobSnapshotStoreSettings.SanitizeFolder("/ ")));
        Assert.Equal(verifiedToJournalRowKey, metadata.ToSnapshotBlobId(AzureBlobSnapshotStoreSettings.SanitizeFolder(" /")));
    }
    
    [Fact]
    public async Task VerifyToSnapshotSearchQuery()
    {
        await Verify(SeqNoHelper.ToSnapshotSearchQuery("persistenceId", string.Empty));
        
        var verifiedToSnapshotSearchQuery = File.ReadAllText("./verify/BlobNameSpecs.VerifyToSnapshotSearchQuery.DotNet.verified.txt");
        Assert.Equal(verifiedToSnapshotSearchQuery, SeqNoHelper.ToSnapshotSearchQuery("persistenceId", "  "));
        Assert.Equal(verifiedToSnapshotSearchQuery, SeqNoHelper.ToSnapshotSearchQuery("persistenceId", null));
        Assert.Equal(verifiedToSnapshotSearchQuery, SeqNoHelper.ToSnapshotSearchQuery("persistenceId", AzureBlobSnapshotStoreSettings.SanitizeFolder("/")));
        Assert.Equal(verifiedToSnapshotSearchQuery, SeqNoHelper.ToSnapshotSearchQuery("persistenceId", AzureBlobSnapshotStoreSettings.SanitizeFolder(" /")));
        Assert.Equal(verifiedToSnapshotSearchQuery, SeqNoHelper.ToSnapshotSearchQuery("persistenceId", AzureBlobSnapshotStoreSettings.SanitizeFolder("/ ")));
    }
}