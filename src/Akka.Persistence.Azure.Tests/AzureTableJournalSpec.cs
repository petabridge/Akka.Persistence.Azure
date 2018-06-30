// -----------------------------------------------------------------------
// <copyright file="AzureTableJournalSpec.cs" company="Petabridge, LLC">
//      Copyright (C) 2015 - 2018 Petabridge, LLC <https://petabridge.com>
// </copyright>
// -----------------------------------------------------------------------

using System;
using Akka.Configuration;
using Akka.Persistence.Azure.TestHelpers;
using Akka.Persistence.TCK.Journal;
using Xunit;
using Xunit.Abstractions;

namespace Akka.Persistence.Azure.Tests
{
    [Collection("AzureJournal")]
    public class AzureTableJournalSpec : JournalSpec
    {
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
            return ConfigurationFactory.ParseString(
                    @"
                akka.loglevel = INFO
                akka.persistence.journal.plugin = ""akka.persistence.journal.azure-table""
                akka.persistence.journal.azure-table.connection-string=""" + connectionString + @"""")
                .WithFallback("akka.persistence.journal.azure-table.table-name=" + "akkatest");
        }
    }
}