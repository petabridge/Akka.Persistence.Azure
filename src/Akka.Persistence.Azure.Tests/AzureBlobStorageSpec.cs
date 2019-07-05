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
using static Akka.Persistence.Azure.Tests.AzureStorageConfigHelper;

namespace Akka.Persistence.Azure.Tests
{
    [Collection("AzureSnapshot")]
    public class AzureBlobStorageSpec : SnapshotStoreSpec
    {
        public AzureBlobStorageSpec(ITestOutputHelper output) : base(Config(),
            nameof(AzureTableJournalSpec), output)
        {
            AzurePersistence.Get(Sys);
            Initialize();
        }

        public static Config Config()
        {
            if (!string.IsNullOrEmpty(Environment.GetEnvironmentVariable("AZURE_CONNECTION_STR")))
                return AzureConfig(Environment.GetEnvironmentVariable("AZURE_CONNECTION_STR"));

            return AzureConfig(WindowsAzureStorageEmulatorFixture.GenerateConnStr());
        }
    }
}