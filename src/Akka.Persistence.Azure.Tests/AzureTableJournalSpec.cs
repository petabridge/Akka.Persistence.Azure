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

        public AzureTableJournalSpec(ITestOutputHelper output)
            : base(Config(), nameof(AzureTableJournalSpec), output)
        {
            AzurePersistence.Get(Sys);
            _output = output;
            Initialize();

            output.WriteLine("Current table: {0}", TableName);
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

        protected override void Dispose(bool disposing)
        {
            base.Dispose(disposing);
            if (DbUtils.CleanupCloudTable(AzurePersistence.Get(Sys).TableSettings.ConnectionString, TableName).Wait(TimeSpan.FromSeconds(3)))
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