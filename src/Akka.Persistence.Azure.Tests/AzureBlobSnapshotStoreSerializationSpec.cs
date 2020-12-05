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
    public class AzureBlobSnapshotStoreSerializationSpec : SnapshotStoreSerializationSpec    
    {
        public AzureBlobSnapshotStoreSerializationSpec(ITestOutputHelper output) : base(Config(),
            nameof(AzureTableJournalSpec), output)
        {
            AzurePersistence.Get(Sys);
        }

        public static Config Config()
        {
            if (!string.IsNullOrEmpty(Environment.GetEnvironmentVariable("AZURE_CONNECTION_STR")))
                return AzureStorageConfigHelper.AzureConfig(Environment.GetEnvironmentVariable("AZURE_CONNECTION_STR"));

            return AzureStorageConfigHelper.AzureConfig(AzureEmulatorFixture.GenerateConnStr());
        }
    }
}