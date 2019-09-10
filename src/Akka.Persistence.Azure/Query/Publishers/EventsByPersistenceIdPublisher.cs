using System;
using Akka.Actor;

namespace Akka.Persistence.Azure.Query.Publishers
{
    internal static class EventsByPersistenceIdPublisher
    {
        public sealed class Continue
        {
            public static readonly Continue Instance = new Continue();

            private Continue()
            {
            }
        }

        public static Props Props(string persistenceId, long fromSequenceNr, long toSequenceNr, TimeSpan? refreshDuration, int maxBufferSize, string writeJournalPluginId)
        {
            return refreshDuration.HasValue
                ? Actor.Props.Create(() => new LiveEventsByPersistenceIdPublisher(persistenceId, fromSequenceNr, toSequenceNr, maxBufferSize, writeJournalPluginId, refreshDuration.Value))
                : Actor.Props.Create(() => new CurrentEventsByPersistenceIdPublisher(persistenceId, fromSequenceNr, toSequenceNr, maxBufferSize, writeJournalPluginId));
        }
    }
}