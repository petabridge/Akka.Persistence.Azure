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
    }
}
