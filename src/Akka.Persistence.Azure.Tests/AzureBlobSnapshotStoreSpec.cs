// -----------------------------------------------------------------------
// <copyright file="AzureBlobSnapshotStoreSpec.cs" company="Petabridge, LLC">
//      Copyright (C) 2015 - 2018 Petabridge, LLC <https://petabridge.com>
// </copyright>
// -----------------------------------------------------------------------

using System;
using Akka.Configuration;
using Akka.Persistence.Azure.TestHelpers;
using Akka.Persistence.TCK.Snapshot;
using Xunit;
using Xunit.Abstractions;
using static Akka.Persistence.Azure.Tests.Helper.AzureStorageConfigHelper;

namespace Akka.Persistence.Azure.Tests
{
    [Collection("AzureSnapshot")]
    public class AzureBlobSnapshotStoreSpec : SnapshotStoreSpec, IClassFixture<EmulatorFixture>
    {
        public AzureBlobSnapshotStoreSpec(EmulatorFixture fixture, ITestOutputHelper output) : base(Config(),
            nameof(AzureTableJournalSpec), output)
        {
            AzurePersistence.Get(Sys);
            Initialize();
        }

        public static Config Config()
        {
            var cosmosString = Environment.GetEnvironmentVariable("AZURE_COSMOSDB_CONNECTION_STR");
            var blobString = Environment.GetEnvironmentVariable("AZURE_BLOB_CONNECTION_STR");

            if (string.IsNullOrWhiteSpace(cosmosString))
                cosmosString = EmulatorFixture.CosmosConnStr();

            if (string.IsNullOrWhiteSpace(blobString))
                blobString = EmulatorFixture.StorageConnStr();

            return AzureConfig(cosmosString, blobString);
        }
    }
}