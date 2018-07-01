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
using Akka.Util.Internal;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;
using Xunit;
using Xunit.Abstractions;

namespace Akka.Persistence.Azure.Tests
{
    [Collection("AzureJournal")]
    public class AzureTableJournalSpec : JournalSpec
    {
        public static AtomicCounter TableVersionCounter = new AtomicCounter(0);
        public static string TableName { get; private set; }

        public AzureTableJournalSpec(ITestOutputHelper output) : base(JournalConfig(), nameof(AzureTableJournalSpec),
            output)
        {
            AzurePersistence.Get(Sys);
            Initialize();
        }

        public static Config JournalConfig()
        {
            if (!string.IsNullOrEmpty(Environment.GetEnvironmentVariable("AZURE_CONNECTION_STR")))
                return JournalConfig(Environment.GetEnvironmentVariable("AZURE_CONNECTION_STR"));

            return JournalConfig(WindowsAzureStorageEmulatorFixture.GenerateConnStr());
        }

        public static Config JournalConfig(string connectionString)
        {
            TableName = "TestTable" + TableVersionCounter.IncrementAndGet();

            return ConfigurationFactory.ParseString(
                    @"akka.loglevel = DEBUG
                akka.persistence.journal.plugin = ""akka.persistence.journal.azure-table""
                akka.persistence.journal.azure-table.connection-string=""" + connectionString + @"""
                akka.persistence.journal.azure-table.verbose-logging = on
                akka.test.single-expect-default = 3s")
                .WithFallback("akka.persistence.journal.azure-table.table-name=" + TableName);
        }

        protected override void Dispose(bool disposing)
        {
            base.Dispose(disposing);
            if (DbUtils.CleanupCloudTable(Sys, TableName).Wait(TimeSpan.FromSeconds(3)))
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