// -----------------------------------------------------------------------
// <copyright file="AzureTableJournalSpec.cs" company="Petabridge, LLC">
//      Copyright (C) 2015 - 2018 Petabridge, LLC <https://petabridge.com>
// </copyright>
// -----------------------------------------------------------------------

using Akka.Configuration;
using Akka.Persistence.Azure.TestHelpers;
using Akka.Persistence.TCK.Journal;
using System;
using Xunit;
using Xunit.Abstractions;
using static Akka.Persistence.Azure.Tests.Helper.AzureStorageConfigHelper;

namespace Akka.Persistence.Azure.Tests
{
    [Collection("AzureJournal")]
    public class AzureTableJournalSpec : JournalSpec
    {
        private ITestOutputHelper _output;

        public AzureTableJournalSpec(AzureCosmosDbEmulatorFixture fixture,  ITestOutputHelper output)
            : base(TestConfig(), nameof(AzureTableJournalSpec), output)
        {
            AzurePersistence.Get(Sys);
            DbUtils.Initialize(fixture);
            _output = output;
            Initialize();

            output.WriteLine("Current table: {0}", TableName);
        }

        public static string TableName { get; private set; }

        public static Config TestConfig()
        {
            var cosmosString = Environment.GetEnvironmentVariable("AZURE_COSMOSDB_CONNECTION_STR");
            var blobString = Environment.GetEnvironmentVariable("AZURE_BLOB_CONNECTION_STR");

            if (string.IsNullOrWhiteSpace(cosmosString))
                cosmosString = AzureCosmosDbEmulatorFixture.GenerateConnStr();

            if (string.IsNullOrWhiteSpace(blobString))
                blobString = AzureStorageEmulatorFixture.GenerateConnStr();

            var azureConfig = AzureConfig(cosmosString, blobString);

            TableName = azureConfig.GetString("akka.persistence.journal.azure-table.table-name");

            return azureConfig;
        }

        protected override void Dispose(bool disposing)
        {
            base.Dispose(disposing);
            if (DbUtils.CleanupCloudTable(TableName).Wait(TimeSpan.FromSeconds(3)))
            {
                _output.WriteLine("Successfully deleted table [{0}] after test run.", TableName);
            }
            else
            {
                _output.WriteLine("Unable to delete table [{0}] after test run.", TableName);
            }
        }
    }
}