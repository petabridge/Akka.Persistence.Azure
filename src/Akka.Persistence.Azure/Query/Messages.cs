// -----------------------------------------------------------------------
// <copyright file="Messages.cs" company="Petabridge, LLC">
//      Copyright (C) 2015 - 2023 Petabridge, LLC <https://petabridge.com>
// </copyright>
// -----------------------------------------------------------------------

using Akka.Actor;
using Akka.Event;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;

namespace Akka.Persistence.Azure.Query
{
    [Serializable]
    public sealed class SelectCurrentPersistenceIds : IJournalRequest
    {
        public IActorRef ReplyTo { get; }
        public long Offset { get; }

        public SelectCurrentPersistenceIds(long offset, IActorRef replyTo)
        {
            Offset = offset;
            ReplyTo = replyTo;
        }
    }

    public sealed class CurrentPersistenceIds : IDeadLetterSuppression
    {
        public readonly IEnumerable<string> AllPersistenceIds;

        /// <summary>
        /// TBD
        /// </summary>
        /// <param name="allPersistenceIds">TBD</param>
        public CurrentPersistenceIds(IEnumerable<string> allPersistenceIds)
        {
            AllPersistenceIds = allPersistenceIds.ToImmutableHashSet();
        }
    }

    /// <summary>
    /// TBD
    /// </summary>
    public sealed class EventAppended : IDeadLetterSuppression
    {
        /// <summary>
        /// TBD
        /// </summary>
        public readonly string PersistenceId;

        /// <summary>
        /// TBD
        /// </summary>
        /// <param name="persistenceId">TBD</param>
        public EventAppended(string persistenceId)
        {
            PersistenceId = persistenceId;
        }
    }

    public sealed class PersistenceIdAdded : IDeadLetterSuppression
    {
        /// <summary>
        /// TBD
        /// </summary>
        public readonly string PersistenceId;

        /// <summary>
        /// TBD
        /// </summary>
        /// <param name="persistenceId">TBD</param>
        public PersistenceIdAdded(string persistenceId)
        {
            PersistenceId = persistenceId;
        }
    }

    /// <summary>
    /// TBD
    /// </summary>
    public sealed class ReplayedTaggedMessage : INoSerializationVerificationNeeded, IDeadLetterSuppression
    {
        /// <summary>
        /// TBD
        /// </summary>
        public readonly long Offset;

        /// <summary>
        /// TBD
        /// </summary>
        public readonly IPersistentRepresentation Persistent;

        /// <summary>
        /// TBD
        /// </summary>
        public readonly string Tag;

        /// <summary>
        /// TBD
        /// </summary>
        /// <param name="persistent">TBD</param>
        /// <param name="tag">TBD</param>
        /// <param name="offset">TBD</param>
        public ReplayedTaggedMessage(IPersistentRepresentation persistent, string tag, long offset)
        {
            Persistent = persistent;
            Tag = tag;
            Offset = offset;
        }
    }

    /// <summary>
    /// TBD
    /// </summary>
    public sealed class ReplayTaggedMessages : IJournalRequest
    {
        /// <summary>
        /// TBD
        /// </summary>
        public readonly long FromOffset;

        /// <summary>
        /// TBD
        /// </summary>
        public readonly long Max;

        /// <summary>
        /// TBD
        /// </summary>
        public readonly IActorRef ReplyTo;

        /// <summary>
        /// TBD
        /// </summary>
        public readonly string Tag;

        /// <summary>
        /// TBD
        /// </summary>
        public readonly long ToOffset;

        /// <summary>
        /// Initializes a new instance of the <see cref="ReplayTaggedMessages"/> class.
        /// </summary>
        /// <param name="fromOffset">TBD</param>
        /// <param name="toOffset">TBD</param>
        /// <param name="max">TBD</param>
        /// <param name="tag">TBD</param>
        /// <param name="replyTo">TBD</param>
        /// <exception cref="ArgumentException">
        /// This exception is thrown for a number of reasons. These include the following:
        /// <ul>
        /// <li>The specified <paramref name="fromOffset"/> is less than zero.</li>
        /// <li>The specified <paramref name="toOffset"/> is less than or equal to zero.</li>
        /// <li>The specified <paramref name="max"/> is less than or equal to zero.</li>
        /// </ul>
        /// </exception>
        /// <exception cref="ArgumentNullException">
        /// This exception is thrown when the specified <paramref name="tag"/> is null or empty.
        /// </exception>
        public ReplayTaggedMessages(long fromOffset, long toOffset, long max, string tag, IActorRef replyTo)
        {
            if (fromOffset < 0) throw new ArgumentException("From offset may not be a negative number", nameof(fromOffset));
            if (toOffset <= 0) throw new ArgumentException("To offset must be a positive number", nameof(toOffset));
            if (max <= 0) throw new ArgumentException("Maximum number of replayed messages must be a positive number", nameof(max));
            if (string.IsNullOrEmpty(tag)) throw new ArgumentNullException(nameof(tag), "Replay tagged messages require a tag value to be provided");

            FromOffset = fromOffset;
            ToOffset = toOffset;
            Max = max;
            Tag = tag;
            ReplyTo = replyTo;
        }
    }

    public sealed class ReplayTaggedMessageSuccess
    {
        public ReplayTaggedMessageSuccess(bool completed)
        {
            Completed = completed;
        }

        public bool Completed { get; }
    }
    
    /// <summary>
    /// TBD
    /// </summary>
    public sealed class TaggedEventAppended : IDeadLetterSuppression
    {
        /// <summary>
        /// TBD
        /// </summary>
        public readonly string Tag;

        /// <summary>
        /// TBD
        /// </summary>
        /// <param name="tag">TBD</param>
        public TaggedEventAppended(string tag)
        {
            Tag = tag;
        }
    }
}