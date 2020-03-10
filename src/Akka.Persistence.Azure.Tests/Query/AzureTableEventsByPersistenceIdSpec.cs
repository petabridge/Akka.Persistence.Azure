using System;
using Akka.Configuration;
using Akka.Persistence.Azure.Query;
using Akka.Persistence.Azure.TestHelpers;
using Akka.Persistence.Azure.Tests.Helper;
using Akka.Persistence.Query;
using Akka.Persistence.TCK.Query;
using Xunit;
using Xunit.Abstractions;

namespace Akka.Persistence.Azure.Tests.Query
{
    [Collection("AzureQuery")]
    public sealed class AzureTableEventsByPersistenceIdSpec
        : EventsByPersistenceIdSpec
    {
        public AzureTableEventsByPersistenceIdSpec(ITestOutputHelper output)
            : base(Config(), nameof(AzureTablePersistenceIdsSpec), output)
        {
            AzurePersistence.Get(Sys);

            ReadJournal =
                Sys.ReadJournalFor<AzureTableStorageReadJournal>(
                    AzureTableStorageReadJournal.Identifier);

            output.WriteLine("Current table: {0}", TableName);
        }

        public static string TableName { get; private set; }

        public static Config Config()
        {
            var azureConfig =
                !string.IsNullOrEmpty(Environment.GetEnvironmentVariable("AZURE_CONNECTION_STR"))
                    ? AzureStorageConfigHelper.AzureConfig(Environment.GetEnvironmentVariable("AZURE_CONNECTION_STR"))
                    : AzureStorageConfigHelper.AzureConfig(WindowsAzureStorageEmulatorFixture.GenerateConnStr());

            TableName = azureConfig.GetString("akka.persistence.journal.azure-table.table-name");

            return azureConfig;
        }
    }
}