using System;
using System.Collections.Generic;
using System.Text;
using Akka.Configuration;

namespace Akka.Persistence.Azure.Tests
{
    public static class AzureStorageConfigHelper
    {
        public static Config AzureConfig(string connectionString)
        {
            var tableName = "t" + Guid.NewGuid();
            var containerName = "testcontainer" + Guid.NewGuid();

            return ConfigurationFactory.ParseString(
                    @"akka.loglevel = DEBUG
                akka.log-config-on-start = off
                akka.persistence.journal.plugin = ""akka.persistence.journal.azure-table""
                akka.persistence.journal.azure-table.connection-string=""" + connectionString + @"""
                akka.persistence.snapshot-store.azure-blob-store.connection-string=""" + connectionString + @"""
                akka.persistence.snapshot-store.plugin = ""akka.persistence.snapshot-store.azure-blob-store""
                akka.persistence.journal.azure-table.verbose-logging = on
                akka.test.single-expect-default = 3s")
                .WithFallback("akka.persistence.journal.azure-table.table-name=" + tableName)
                .WithFallback("akka.persistence.snapshot-store.azure-blob-store.container-name=" + containerName);
        }

    }
}
