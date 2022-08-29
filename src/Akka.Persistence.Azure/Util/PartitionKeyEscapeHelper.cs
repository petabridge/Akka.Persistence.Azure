// -----------------------------------------------------------------------
// <copyright file="PartitionKeyEscapeHelper.cs" company="Petabridge, LLC">
//      Copyright (C) 2015 - 2022 Petabridge, LLC <https://petabridge.com>
// </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;

namespace Akka.Persistence.Azure.Util
{
    /// <summary>
    /// PartitionKeyEscapeHelper
    /// </summary>
    public static class PartitionKeyEscapeHelper
    {
        /// <summary>
        /// Sequence we need to escape
        /// </summary>
        private const string InvalidSequence = "/";
        /// <summary>
        /// Sequence we use to escape invalid chars
        /// </summary>
        /// <remarks>
        /// Using $ here to resolve https://github.com/petabridge/Akka.Persistence.Azure/issues/98
        /// Actor names never start with $ sign, which helps to decode encoded keys
        /// </remarks>
        private const string EscapeSequence = "$";
        
        /// <summary>
        /// Escape special characters in partition key
        /// </summary>
        /// <returns>Escaped partition key</returns>
        public static string Escape(string partitionKey)
        {
            var escapedKey = partitionKey;
            // First, replate escape sequence in case if it is used in original key
            escapedKey = escapedKey.Replace(EscapeSequence, EscapeSequence + EscapeSequence);
                
            // Now escape special chars
            escapedKey = escapedKey.Replace(InvalidSequence, EscapeSequence);
            
            return escapedKey;
        }

        /// <summary>
        /// Unescape previously escaped partition key
        /// </summary>
        /// <returns>Original, unescaped partition key</returns>
        public static string Unescape(string escapedKey)
        {
            var originalKey = escapedKey;
            var uuid = Guid.NewGuid().ToString("N");
            // Do not touch duplicated escape sequence - we will replace them later
            originalKey = originalKey.Replace(EscapeSequence + EscapeSequence, uuid);
                
            // Restore escaped characters
            originalKey = originalKey.Replace(EscapeSequence, InvalidSequence);
                
            // Now it is safe to decode duplicated sequences
            originalKey = originalKey.Replace(uuid, EscapeSequence);
            
            return originalKey;
        }
    }
}