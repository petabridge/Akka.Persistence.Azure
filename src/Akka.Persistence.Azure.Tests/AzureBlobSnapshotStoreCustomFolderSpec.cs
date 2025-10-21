// -----------------------------------------------------------------------
// <copyright file="AzureBlobSnapshotStoreSpec.cs" company="Petabridge, LLC">
//      Copyright (C) 2015 - 2023 Petabridge, LLC <https://petabridge.com>
// </copyright>
// -----------------------------------------------------------------------

using System;
using Akka.Configuration;
using Akka.Persistence.Azure.Snapshot;
using Akka.Persistence.Azure.Tests.Helper;
using Akka.Persistence.TCK.Snapshot;
using FluentAssertions;
using Xunit;
using Xunit.Abstractions;
using static Akka.Persistence.Azure.Tests.Helper.AzureStorageConfigHelper;

namespace Akka.Persistence.Azure.Tests
{
    [Collection("AzureSpecs")]
    public class AzureBlobSnapshotStoreCustomFolderSpec : SnapshotStoreSpec, IClassFixture<AzuriteFixture>
    {
        private static Config CreateCustomConfig(string connectionString)
        {
            return ConfigurationFactory
                .ParseString("akka.persistence.snapshot-store.azure-blob-store.folders = \"/folder-1/folder-2/\"")
                .WithFallback(AzureConfig(connectionString));
        }

        public AzureBlobSnapshotStoreCustomFolderSpec(AzuriteFixture fixture, ITestOutputHelper output)
            : base(CreateCustomConfig(fixture.ConnectionString), nameof(AzureBlobSnapshotStoreCustomFolderSpec), output)
        {
            AzurePersistence.Get(Sys);
            Initialize();
        }

        [Fact]
        public void ConfigTest()
        {
            var settings = AzureBlobSnapshotStoreSettings.Create(Sys);
            settings.Folders.Should().Be("folder-1/folder-2");
            
            settings = AzureBlobSnapshotStoreSettings.Create(Sys.Settings.Config.GetConfig(AzureBlobSnapshotStoreSettings.SnapshotStoreConfigPath));
            settings.Folders.Should().Be("folder-1/folder-2");
        }
    }
}