// -----------------------------------------------------------------------
// <copyright file="EventsByTagPublisher.cs" company="Petabridge, LLC">
//      Copyright (C) 2015 - 2023 Petabridge, LLC <https://petabridge.com>
// </copyright>
// -----------------------------------------------------------------------

using System;
using Akka.Actor;

namespace Akka.Persistence.Azure.Query.Publishers
{
    internal static class EventsByTagPublisher
    {
        public sealed class Continue
        {
            public static readonly Continue Instance = new Continue();

            private Continue()
            {
            }
        }

        public static Props Props(string tag, long fromOffset, long toOffset, TimeSpan? refreshInterval, int maxBufferSize, string writeJournalPluginId)
        {
            return refreshInterval.HasValue
                ? Actor.Props.Create(() => new LiveEventsByTagPublisher(tag, fromOffset, toOffset, refreshInterval.Value, maxBufferSize, writeJournalPluginId))
                : Actor.Props.Create(() => new CurrentEventsByTagPublisher(tag, fromOffset, toOffset, maxBufferSize, writeJournalPluginId));
        }
    }
}