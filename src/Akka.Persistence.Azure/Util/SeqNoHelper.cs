using System;
using System.Collections.Generic;
using System.Text;

namespace Akka.Persistence.Azure.Util
{
    /// <summary>
    /// INTERNAL API.
    ///
    /// Used to help convert sequence numbers for use inside Windows Azure table and blob storage.
    /// </summary>
    internal static class SeqNoHelper
    {
        /// <summary>
        /// Converts a long integer sequence number into a string that can be sorted in lexicographical
        /// order, which is what is used by Windows Azure Table Storage to determine which rows are returned
        /// first during a table storage query.
        /// </summary>
        /// <param name="seqNo">The sequence number.</param>
        /// <returns>A fixed length, padded integer string representation.</returns>
        /// <remarks>
        /// See http://blog.smarx.com/posts/using-numbers-as-keys-in-windows-azure for more detail on how this works.
        /// </remarks>
        public static string ToJournalRowKey(this long seqNo)
        {
            return $"{seqNo:d19}";
        }

        /// <summary>
        /// Converts a <see cref="SnapshotMetadata"/> object into a Uri-friendly string that can be sorted
        /// into lexicographical order for a particular snapshot.
        /// </summary>
        /// <param name="metadata">The metadata used for the current snapshot.</param>
        /// <returns>A Uri-friendly snapshot blob name.</returns>
        public static string ToSnapshotBlobId(this SnapshotMetadata metadata)
        {
            return $"snapshot-{Uri.EscapeDataString(metadata.PersistenceId)}-{metadata.SequenceNr.ToJournalRowKey()}";
        }

        /// <summary>
        /// Used to help search for the most recent snapshot in Azure Blob storage.
        /// </summary>
        /// <param name="persistentId">The ID of the persistent entity.</param>
        /// <returns>The prefix of a blob store search string.</returns>
        public static string ToSnapshotSearchQuery(string persistentId)
        {
            return $"snapshot-{Uri.EscapeDataString(persistentId)}";
        }
    }
}
