using Akka.Configuration;
using Akka.Persistence.Azure.Query;
using Akka.Persistence.Azure.Tests.Helper;
using Akka.Persistence.Query;
using Akka.Persistence.TCK.Query;
using System;
using Xunit;
using Xunit.Abstractions;
using static Akka.Persistence.Azure.Tests.Helper.AzureStorageConfigHelper;

namespace Akka.Persistence.Azure.Tests.Query
{
    [Collection("AzureSpecs")]
    public sealed class AzureTableCurrentEventsByPersistenceIdSpec : CurrentEventsByPersistenceIdSpec
    {
        public AzureTableCurrentEventsByPersistenceIdSpec(ITestOutputHelper output)
            : base(AzureConfig(), nameof(AzureTablePersistenceIdsSpec), output)
        {
            AzurePersistence.Get(Sys);

            ReadJournal =
                Sys.ReadJournalFor<AzureTableStorageReadJournal>(
                    AzureTableStorageReadJournal.Identifier);

            output.WriteLine("Current table: {0}", TableName);
        }

        public static string TableName { get; private set; }
    }
}