// -----------------------------------------------------------------------
// <copyright file="AzureTableJournalSpec.cs" company="Petabridge, LLC">
//      Copyright (C) 2015 - 2018 Petabridge, LLC <https://petabridge.com>
// </copyright>
// -----------------------------------------------------------------------

using System;
using System.Threading.Tasks;
using Akka.Actor;
using Akka.Configuration;
using Akka.Persistence.Azure.TestHelpers;
using Akka.Persistence.TCK.Journal;
using Xunit;
using Xunit.Abstractions;
using static Akka.Persistence.Azure.Tests.AzureStorageConfigHelper;

namespace Akka.Persistence.Azure.Tests
{
    [Collection("AzureJournal")]
    public class AzureTableJournalSpec : JournalSpec
    {
        public static string TableName { get; private set; }

        public AzureTableJournalSpec(ITestOutputHelper output) : base(Config(), nameof(AzureTableJournalSpec),
            output)
        {
            AzurePersistence.Get(Sys);
            Initialize();
        }

        public static Config Config()
        {
            var config =
                !string.IsNullOrEmpty(Environment.GetEnvironmentVariable("AZURE_CONNECTION_STR"))
                    ? AzureConfig(Environment.GetEnvironmentVariable("AZURE_CONNECTION_STR"))
                    : AzureConfig(WindowsAzureStorageEmulatorFixture.GenerateConnStr());

            TableName = config.GetString("akka.persistence.journal.azure-table.table-name");

            return config;
        }

        protected override void Dispose(bool disposing)
        {
            base.Dispose(disposing);
            if (DbUtils.CleanupCloudTable(AzurePersistence.Get(Sys).TableSettings.ConnectionString, TableName).Wait(TimeSpan.FromSeconds(3)))
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