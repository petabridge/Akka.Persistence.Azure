using Akka.Actor;
using Akka.Configuration;
using Akka.Persistence.Query;
using Hocon;

namespace Akka.Persistence.Azure.Query
{
    public class AzureTableStorageReadJournalProvider : IReadJournalProvider
    {
        private readonly ExtendedActorSystem _system;
        private readonly Config _config;

        public AzureTableStorageReadJournalProvider(ExtendedActorSystem system, Config config)
        {
            _system = system;
            _config = config;
        }

        public IReadJournal GetReadJournal()
        {
            return new AzureTableStorageReadJournal(_system, _config);
        }
    }
}