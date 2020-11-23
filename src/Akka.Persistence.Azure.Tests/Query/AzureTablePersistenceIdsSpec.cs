using Akka.Configuration;
using Akka.Persistence.Azure.Query;
using Akka.Persistence.Azure.TestHelpers;
using Akka.Persistence.Query;
using Akka.Persistence.TCK.Query;
using System;
using Xunit;
using Xunit.Abstractions;
using static Akka.Persistence.Azure.Tests.Helper.AzureStorageConfigHelper;

namespace Akka.Persistence.Azure.Tests.Query
{
    [Collection("AzureQuery")]
    public sealed class AzureTablePersistenceIdsSpec
        : PersistenceIdsSpec
    {
        public AzureTablePersistenceIdsSpec(ITestOutputHelper output)
            : base(Config(), nameof(AzureTablePersistenceIdsSpec), output)
        {
            AzurePersistence.Get(Sys);
            var conf = AzurePersistence.DefaultConfig;

            ReadJournal =
                Sys.ReadJournalFor<AzureTableStorageReadJournal>(
                    AzureTableStorageReadJournal.Identifier);
        }

        public static string TableName { get; private set; }

        public static Config Config()
        {
            var azureConfig =
                !string.IsNullOrEmpty(Environment.GetEnvironmentVariable("AZURE_CONNECTION_STR"))
                    ? AzureConfig(Environment.GetEnvironmentVariable("AZURE_CONNECTION_STR"))
                    : AzureConfig(WindowsAzureStorageEmulatorFixture.GenerateConnStr());

            TableName = azureConfig.GetString("akka.persistence.journal.azure-table.table-name");

            return azureConfig;
        }
    }
}