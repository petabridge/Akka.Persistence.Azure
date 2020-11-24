using Akka.Configuration;
using Akka.Persistence.Azure.Query;
using Akka.Persistence.Azure.TestHelpers;
using Akka.Persistence.Azure.Tests.Helper;
using Akka.Persistence.Query;
using Akka.Persistence.TCK.Query;
using System;
using Xunit;
using Xunit.Abstractions;

namespace Akka.Persistence.Azure.Tests.Query
{
    [Collection("AzureQuery")]
    public sealed class AzureTableCurrentEventsByPersistenceIdSpec
        : CurrentEventsByPersistenceIdSpec
    {
        public AzureTableCurrentEventsByPersistenceIdSpec(ITestOutputHelper output)
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
            var cosmosString = Environment.GetEnvironmentVariable("AZURE_COSMOSDB_CONNECTION_STR");
            var blobString = Environment.GetEnvironmentVariable("AZURE_BLOB_CONNECTION_STR");

            if (string.IsNullOrWhiteSpace(cosmosString))
                cosmosString = AzureCosmosDbEmulatorFixture.GenerateConnStr();

            if (string.IsNullOrWhiteSpace(blobString))
                blobString = AzuriteEmulatorFixture.GenerateConnStr();

            var azureConfig = AzureStorageConfigHelper.AzureConfig(cosmosString, blobString);

            TableName = azureConfig.GetString("akka.persistence.journal.azure-table.table-name");

            return azureConfig;
        }
    }
}