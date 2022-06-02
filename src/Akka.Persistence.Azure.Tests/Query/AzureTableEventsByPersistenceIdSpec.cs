using System;
using Akka.Configuration;
using Akka.Persistence.Azure.Query;
using Akka.Persistence.Azure.Tests.Helper;
using Akka.Persistence.Query;
using Akka.Persistence.TCK.Query;
using Xunit;
using Xunit.Abstractions;
using static Akka.Persistence.Azure.Tests.Helper.AzureStorageConfigHelper;

namespace Akka.Persistence.Azure.Tests.Query
{
    [Collection("AzureSpecs")]
    public sealed class AzureTableEventsByPersistenceIdSpec : EventsByPersistenceIdSpec
    {
        public AzureTableEventsByPersistenceIdSpec(ITestOutputHelper output)
            : base(AzureConfig(), nameof(AzureTableEventsByPersistenceIdSpec), output)
        {
            AzurePersistence.Get(Sys);

            ReadJournal =
                Sys.ReadJournalFor<AzureTableStorageReadJournal>(
                    AzureTableStorageReadJournal.Identifier);
        }
    }
}