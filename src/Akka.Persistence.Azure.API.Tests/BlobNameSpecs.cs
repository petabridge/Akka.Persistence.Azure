using Akka.Persistence.Azure.Util;

namespace Akka.Persistence.Azure.API.Tests;

public class BlobNameSpecs
{
    [Fact]
    public async Task VerifyToJournalRowKey()
    {
        var metadata = new SnapshotMetadata("persistenceId", 100, new DateTime(2000, 1, 1));
        await Verify(metadata.ToSnapshotBlobId());
    }
    
    [Fact]
    public async Task VerifyToSnapshotSearchQuery()
    {
        await Verify(SeqNoHelper.ToSnapshotSearchQuery("persistenceId"));
    }
    
}