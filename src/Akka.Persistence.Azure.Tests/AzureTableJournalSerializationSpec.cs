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
    public class AzureTableJournalSerializationSpec : JournalSerializationSpec, IClassFixture<AzureEmulatorFixture>
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
            var azureConfig =
                !string.IsNullOrEmpty(Environment.GetEnvironmentVariable("AZURE_CONNECTION_STR"))
                    ? AzureStorageConfigHelper.AzureConfig(Environment.GetEnvironmentVariable("AZURE_CONNECTION_STR"))
                    : AzureStorageConfigHelper.AzureConfig(AzureEmulatorFixture.GenerateConnStr());

            TableName = azureConfig.GetString("akka.persistence.journal.azure-table.table-name");

            return azureConfig;
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