// -----------------------------------------------------------------------
// <copyright file="AzurePersistenceConfigSpec.cs" company="Petabridge, LLC">
//      Copyright (C) 2015 - 2018 Petabridge, LLC <https://petabridge.com>
// </copyright>
// -----------------------------------------------------------------------

using System;
using Akka.Configuration;
using Akka.Persistence.Azure.Journal;
using Akka.Persistence.Azure.Query;
using Akka.Persistence.Azure.Snapshot;
using Azure.Storage.Blobs.Models;
using FluentAssertions;
using Xunit;

namespace Akka.Persistence.Azure.Tests
{
    public class AzurePersistenceConfigSpec
    {
        [Fact]
        public void ShouldLoadDefaultConfig()
        {
            var defaultConfig = AzurePersistence.DefaultConfig;
            defaultConfig.HasPath("akka.persistence.journal.azure-table").Should().BeTrue();
            defaultConfig.HasPath("akka.persistence.snapshot-store.azure-blob-store").Should().BeTrue();
        }

        [Fact]
        public void ShouldParseDefaultSnapshotConfig()
        {
            var blobSettings =
                AzureBlobSnapshotStoreSettings.Create(
                    ConfigurationFactory.ParseString(@"akka.persistence.snapshot-store.azure-blob-store{
                        connection-string = foo
                        container-name = bar
                    }").WithFallback(AzurePersistence.DefaultConfig)
                        .GetConfig("akka.persistence.snapshot-store.azure-blob-store"));

            blobSettings.ContainerName.Should().Be("bar");
            blobSettings.ConnectionString.Should().Be("foo");
            blobSettings.ConnectTimeout.Should().Be(TimeSpan.FromSeconds(3));
            blobSettings.RequestTimeout.Should().Be(TimeSpan.FromSeconds(3));
            blobSettings.VerboseLogging.Should().BeFalse();
            blobSettings.ContainerPublicAccessType.Should().Be(PublicAccessType.BlobContainer);
        }
        
        [Fact]
        public void ShouldProvideDefaultContainerNameValue()
        {
            var blobSettings =
                AzureBlobSnapshotStoreSettings.Create(
                    ConfigurationFactory.ParseString(@"akka.persistence.snapshot-store.azure-blob-store{
                        connection-string = foo
                    }").WithFallback(AzurePersistence.DefaultConfig)
                        .GetConfig("akka.persistence.snapshot-store.azure-blob-store"));

            blobSettings.ContainerName.Should().Be("akka-persistence-default-container");
        }

        [Fact]
        public void ShouldParseTableConfig()
        {
            var tableSettings =
                AzureTableStorageJournalSettings.Create(
                    ConfigurationFactory.ParseString(@"akka.persistence.journal.azure-table{
                        connection-string = foo
                        table-name = bar
                    }").WithFallback(AzurePersistence.DefaultConfig)
                        .GetConfig("akka.persistence.journal.azure-table"));

            tableSettings.TableName.Should().Be("bar");
            tableSettings.ConnectionString.Should().Be("foo");
            tableSettings.ConnectTimeout.Should().Be(TimeSpan.FromSeconds(3));
            tableSettings.RequestTimeout.Should().Be(TimeSpan.FromSeconds(3));
            tableSettings.VerboseLogging.Should().BeFalse();
        }

        [Fact]
        public void ShouldProvideDefaultTableNameValue()
        {
            var tableSettings =
                AzureTableStorageJournalSettings.Create(
                    ConfigurationFactory.ParseString(@"akka.persistence.journal.azure-table{
                        connection-string = foo
                    }").WithFallback(AzurePersistence.DefaultConfig)
                        .GetConfig("akka.persistence.journal.azure-table"));
            tableSettings.TableName.Should().Be("AkkaPersistenceDefaultTable");
        }

        [Theory]
        [InlineData("fo", "Invalid table name length")]
        [InlineData("1foo", "Invalid table name")]
        [InlineData("tables", "Reserved table name")]
        public void ShouldThrowArgumentExceptionForIllegalTableNames(string tableName, string reason)
        {
            Action createJournalSettings = () => AzureTableStorageJournalSettings.Create(
                    ConfigurationFactory.ParseString(@"akka.persistence.journal.azure-table{
                        connection-string = foo
                        table-name = " + tableName + @" 
                    }").WithFallback(AzurePersistence.DefaultConfig)
                        .GetConfig("akka.persistence.journal.azure-table"));
            createJournalSettings.Should().Throw<ArgumentException>(reason);
        }
        
        [Theory]
        [InlineData("ba", "Invalid container name length")]
        [InlineData("bar--table", "Invalid container name")]
        public void ShouldThrowArgumentExceptionForIllegalContainerNames(string containerName, string reason)
        {
            Action createSnapshotSettings = () =>
                AzureBlobSnapshotStoreSettings.Create(
                    ConfigurationFactory.ParseString(@"akka.persistence.snapshot-store.azure-blob-store{
                        connection-string = foo
                        container-name = " + containerName + @"
                    }").WithFallback(AzurePersistence.DefaultConfig)
                        .GetConfig("akka.persistence.snapshot-store.azure-blob-store"));

            createSnapshotSettings.Should().Throw<ArgumentException>(reason);
        }
    }
}