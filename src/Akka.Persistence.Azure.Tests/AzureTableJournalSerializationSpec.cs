// -----------------------------------------------------------------------
// <copyright file="AzureTableJournalSerializationSpec.cs" company="Petabridge, LLC">
//      Copyright (C) 2015 - 2019 Petabridge, LLC <https://petabridge.com>
// </copyright>
// -----------------------------------------------------------------------

using System;
using Akka.Configuration;
using Akka.Persistence.Azure.TestHelpers;
using Akka.Persistence.Azure.Tests.Helper;
using Akka.Persistence.TCK.Serialization;
using Xunit;
using Xunit.Abstractions;

namespace Akka.Persistence.Azure.Tests
{
    [Collection("AzureJournal")]
    public class AzureTableJournalSerializationSpec : JournalSerializationSpec
    {
        public AzureTableJournalSerializationSpec(ITestOutputHelper output)
            : base(Config(), nameof(AzureTableJournalSerializationSpec), output)
        {
            AzurePersistence.Get(Sys);
            output.WriteLine("Current table: {0}", TableName);
        }

        [Fact(Skip= "https://github.com/akkadotnet/akka.net/issues/3965")]
        public override void Journal_should_serialize_Persistent_with_EventAdapter_manifest()
        {

        }

        public static string TableName { get; private set; }

        public static Config Config()
        {
            var cosmosString = Environment.GetEnvironmentVariable("AZURE_COSMOSDB_CONNECTION_STR");
            var blobString = Environment.GetEnvironmentVariable("AZURE_BLOB_CONNECTION_STR");

            if (string.IsNullOrWhiteSpace(cosmosString))
                cosmosString = EmulatorFixture.CosmosConnStr();

            if (string.IsNullOrWhiteSpace(blobString))
                blobString = EmulatorFixture.StorageConnStr();

            var azureConfig = AzureStorageConfigHelper.AzureConfig(cosmosString, blobString);

            TableName = azureConfig.GetString("akka.persistence.journal.azure-table.table-name");

            return azureConfig;
        }

        protected override void Dispose(bool disposing)
        {
            base.Dispose(disposing);
            if (DbUtils.CleanupCloudTable(EmulatorFixture.ConnectionStrings["Cosmos"], TableName).Wait(TimeSpan.FromSeconds(3)))
            {
                Log.Info("Successfully deleted table [{0}] after test run.", TableName);
            }
            else
            {
                Log.Error("Unable to delete table [{0}] after test run.", TableName);
            }
        }
    }
}