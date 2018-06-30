// -----------------------------------------------------------------------
// <copyright file="AzureBlobStorageSpec.cs" company="Petabridge, LLC">
//      Copyright (C) 2015 - 2018 Petabridge, LLC <https://petabridge.com>
// </copyright>
// -----------------------------------------------------------------------

using System;
using Akka.Configuration;
using Akka.Persistence.Azure.TestHelpers;
using Akka.Persistence.TCK.Snapshot;
using Xunit;
using Xunit.Abstractions;

namespace Akka.Persistence.Azure.Tests
{
    [Collection("AzureSnapshot")]
    public class AzureBlobStorageSpec : SnapshotStoreSpec
    {
        public AzureBlobStorageSpec(ITestOutputHelper output) : base(SnapshotStoreConfig(),
            nameof(AzureTableJournalSpec), output)
        {
            AzurePersistence.Get(Sys);
            Initialize();
        }

        public static Config SnapshotStoreConfig()
        {
            if (!string.IsNullOrEmpty(Environment.GetEnvironmentVariable("AZURE_CONNECTION_STR")))
                return SnapshotStoreConfig(Environment.GetEnvironmentVariable("AZURE_CONNECTION_STR"));

            return SnapshotStoreConfig(WindowsAzureStorageEmulatorFixture.GenerateConnStr());
        }

        public static Config SnapshotStoreConfig(string connectionString)
        {
            return ConfigurationFactory.ParseString(
                    @"akka.persistence.snapshot-store.azure-blob-store.connection-string=""+ connectionString +""")
                .WithFallback("akka.persistence.snapshot-store.azure-blob-store.container-name=" + Guid.NewGuid());
        }
    }
}