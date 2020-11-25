// -----------------------------------------------------------------------
// <copyright file="AzureBlobSnapshotStoreSerializationSpec.cs" company="Petabridge, LLC">
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
    [Collection("AzureSnapshot")]
    public class AzureBlobSnapshotStoreSerializationSpec : SnapshotStoreSerializationSpec, IClassFixture<EmulatorFixture>
    {
        public AzureBlobSnapshotStoreSerializationSpec(EmulatorFixture fixture, ITestOutputHelper output) : base(Config(),
            nameof(AzureTableJournalSpec), output)
        {
           AzurePersistence.Get(Sys);
        }

        public static Config Config()
        {
            var cosmosString = Environment.GetEnvironmentVariable("AZURE_COSMOSDB_CONNECTION_STR");
            var blobString = Environment.GetEnvironmentVariable("AZURE_BLOB_CONNECTION_STR");
            
            if (string.IsNullOrWhiteSpace(cosmosString))
                cosmosString = EmulatorFixture.CosmosConnStr();

            if (string.IsNullOrWhiteSpace(blobString))
                blobString = EmulatorFixture.StorageConnStr();

            return AzureStorageConfigHelper.AzureConfig(EmulatorFixture.AzuriteConnStr());

        }
    }
}