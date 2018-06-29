using System;
using System.Threading.Tasks;
using Akka.Persistence.Snapshot;

namespace Akka.Persistence.Azure.Snapshot
{
    public class AzureBlobSnapshotStore : SnapshotStore
    {
        protected override Task<SelectedSnapshot> LoadAsync(string persistenceId, SnapshotSelectionCriteria criteria)
        {
            throw new NotImplementedException();
        }

        protected override Task SaveAsync(SnapshotMetadata metadata, object snapshot)
        {
            throw new NotImplementedException();
        }

        protected override Task DeleteAsync(SnapshotMetadata metadata)
        {
            throw new NotImplementedException();
        }

        protected override Task DeleteAsync(string persistenceId, SnapshotSelectionCriteria criteria)
        {
            throw new NotImplementedException();
        }
    }
}
