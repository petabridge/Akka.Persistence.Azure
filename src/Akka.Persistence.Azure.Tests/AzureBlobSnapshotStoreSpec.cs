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
    public class AzureBlobSnapshotStoreSpec : SnapshotStoreSpec, IClassFixture<AzureEmulatorFixture>
    {
        public AzureBlobSnapshotStoreSpec(ITestOutputHelper output) : base(Config(),
            nameof(AzureTableJournalSpec), output)
        {
            AzurePersistence.Get(Sys);
            Initialize();
        }

        public static Config Config()
        {
            if (!string.IsNullOrEmpty(Environment.GetEnvironmentVariable("AZURE_CONNECTION_STR")))
                return AzureConfig(Environment.GetEnvironmentVariable("AZURE_CONNECTION_STR"));

            return AzureConfig(AzureEmulatorFixture.ConnectionString);
        }
    }
}