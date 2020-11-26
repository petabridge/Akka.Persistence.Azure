using Akka.Actor;
using System;

namespace Akka.Persistence.Azure.Query.Publishers
{
    internal sealed class AllEventsPublisher
    {
        [Serializable]
        public sealed class Continue
        {
            public static readonly Continue Instance = new Continue();

            private Continue() { }
        }

        public static Props Props(long fromOffset, TimeSpan? refreshInterval, int maxBufferSize, string writeJournalPluginId)
        {
            return refreshInterval.HasValue
                ? Actor.Props.Create(() => new LiveAllEventsPublisher(fromOffset, refreshInterval.Value, maxBufferSize, writeJournalPluginId))
                : Actor.Props.Create(() => new CurrentAllEventsPublisher(fromOffset, maxBufferSize, writeJournalPluginId));
        }

    }
}
