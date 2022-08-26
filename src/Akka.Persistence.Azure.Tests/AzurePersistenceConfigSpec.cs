// -----------------------------------------------------------------------
// <copyright file="AzurePersistenceConfigSpec.cs" company="Petabridge, LLC">
//      Copyright (C) 2015 - 2022 Petabridge, LLC <https://petabridge.com>
// </copyright>
// -----------------------------------------------------------------------

using System;
using Akka.Configuration;
using Akka.Persistence.Azure.Journal;
using Akka.Persistence.Azure.Query;
using Akka.Persistence.Azure.Snapshot;
using Azure.Data.Tables;
using Azure.Identity;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using FluentAssertions;
using FluentAssertions.Extensions;
using Xunit;

namespace Akka.Persistence.Azure.Tests
{
    public class AzurePersistenceConfigSpec
    {
        private const string SnapshotStorePath = "akka.persistence.snapshot-store.azure-blob-store";
        private const string JournalPath = "akka.persistence.journal.azure-table";

        private static readonly AzureBlobSnapshotStoreSettings DefaultSnapshotSettings =
            AzureBlobSnapshotStoreSettings.Create(AzurePersistence.DefaultConfig.GetConfig(SnapshotStorePath));

        private static readonly AzureTableStorageJournalSettings DefaultJournalSettings =
            AzureTableStorageJournalSettings.Create(AzurePersistence.DefaultConfig.GetConfig(JournalPath));
        
        [Fact]
        public void ShouldLoadDefaultConfig()
        {
            AzurePersistence.DefaultConfig.HasPath(SnapshotStorePath).Should().BeTrue();
            AzurePersistence.DefaultConfig.HasPath(JournalPath).Should().BeTrue();
            AzurePersistence.DefaultConfig.HasPath(AzureTableStorageReadJournal.Identifier).Should().BeTrue();
        }

        [Fact]
        public void ShouldParseDefaultSnapshotConfig()
        {
            var settings =
                AzureBlobSnapshotStoreSettings.Create(AzurePersistence.DefaultConfig.GetConfig(SnapshotStorePath));

            settings.ConnectionString.Should().BeEmpty();
            settings.ContainerName.Should().Be("akka-persistence-default-container");
            settings.ConnectTimeout.Should().Be(3.Seconds());
            settings.RequestTimeout.Should().Be(3.Seconds());
            settings.VerboseLogging.Should().BeFalse();
            settings.Development.Should().BeFalse();
            settings.AutoInitialize.Should().BeTrue();
            settings.ContainerPublicAccessType.Should().Be(PublicAccessType.None);
            settings.ServiceUri.Should().BeNull();
            settings.DefaultAzureCredential.Should().BeNull();
            settings.BlobClientOptions.Should().BeNull();
        }
        
        [Fact(DisplayName = "AzureBlobSnapshotStoreSettings With overrides should override default values")]
        public void SnapshotSettingsWithMethodsTest()
        {
            var uri = new Uri("https://whatever.com");
            var credentials = new DefaultAzureCredential();
            var options = new BlobClientOptions();
            var settings = DefaultSnapshotSettings
                    .WithConnectionString("abc")
                    .WithContainerName("bcd")
                    .WithConnectTimeout(1.Seconds())
                    .WithRequestTimeout(2.Seconds())
                    .WithVerboseLogging(true)
                    .WithDevelopment(true)
                    .WithAutoInitialize(false)
                    .WithContainerPublicAccessType(PublicAccessType.Blob)
                    .WithAzureCredential(uri, credentials, options);

            settings.ConnectionString.Should().Be("abc");
            settings.ContainerName.Should().Be("bcd");
            settings.ConnectTimeout.Should().Be(1.Seconds());
            settings.RequestTimeout.Should().Be(2.Seconds());
            settings.VerboseLogging.Should().BeTrue();
            settings.Development.Should().BeTrue();
            settings.AutoInitialize.Should().BeFalse();
            settings.ContainerPublicAccessType.Should().Be(PublicAccessType.Blob);
            settings.ServiceUri.Should().Be(uri);
            settings.DefaultAzureCredential.Should().Be(credentials);
            settings.BlobClientOptions.Should().Be(options);
        }

        [Fact(DisplayName = "AzureBlobSnapshotStoreSetup should override settings values")]
        public void SnapshotSetupTest()
        {
            var uri = new Uri("https://whatever.com");
            var credentials = new DefaultAzureCredential();
            var options = new BlobClientOptions();
            var setup = new AzureBlobSnapshotSetup
            {
                ConnectionString = "abc",
                ContainerName = "bcd",
                ConnectTimeout = 1.Seconds(),
                RequestTimeout = 2.Seconds(),
                VerboseLogging = true,
                Development = true,
                AutoInitialize = false,
                ContainerPublicAccessType = PublicAccessType.Blob,
                ServiceUri = uri,
                DefaultAzureCredential = credentials,
                BlobClientOptions = options
            };

            var settings = setup.Apply(DefaultSnapshotSettings);
            
            settings.ConnectionString.Should().Be("abc");
            settings.ContainerName.Should().Be("bcd");
            settings.ConnectTimeout.Should().Be(1.Seconds());
            settings.RequestTimeout.Should().Be(2.Seconds());
            settings.VerboseLogging.Should().BeTrue();
            settings.Development.Should().BeTrue();
            settings.AutoInitialize.Should().BeFalse();
            settings.ContainerPublicAccessType.Should().Be(PublicAccessType.Blob);
            settings.ServiceUri.Should().Be(uri);
            settings.DefaultAzureCredential.Should().Be(credentials);
            settings.BlobClientOptions.Should().Be(options);
        }

        [Fact]
        public void ShouldParseTableConfig()
        {
            var settings = DefaultJournalSettings;

            settings.ConnectionString.Should().BeEmpty();
            settings.TableName.Should().Be("AkkaPersistenceDefaultTable");
            settings.ConnectTimeout.Should().Be(3.Seconds());
            settings.RequestTimeout.Should().Be(3.Seconds());
            settings.VerboseLogging.Should().BeFalse();
            settings.Development.Should().BeFalse();
            settings.AutoInitialize.Should().BeTrue();
            settings.ServiceUri.Should().BeNull();
            settings.DefaultAzureCredential.Should().BeNull();
            settings.TableClientOptions.Should().BeNull();
        }

        [Fact(DisplayName = "AzureTableStorageJournalSettings With overrides should override default values")]
        public void JournalSettingsWithMethodsTest()
        {
            var uri = new Uri("https://whatever.com");
            var credentials = new DefaultAzureCredential();
            var options = new TableClientOptions();
            var settings = DefaultJournalSettings
                    .WithConnectionString("abc")
                    .WithTableName("bcd")
                    .WithConnectTimeout(1.Seconds())
                    .WithRequestTimeout(2.Seconds())
                    .WithVerboseLogging(true)
                    .WithDevelopment(true)
                    .WithAutoInitialize(false)
                    .WithAzureCredential(uri, credentials, options);

            settings.ConnectionString.Should().Be("abc");
            settings.TableName.Should().Be("bcd");
            settings.ConnectTimeout.Should().Be(1.Seconds());
            settings.RequestTimeout.Should().Be(2.Seconds());
            settings.VerboseLogging.Should().BeTrue();
            settings.Development.Should().BeTrue();
            settings.AutoInitialize.Should().BeFalse();
            settings.ServiceUri.Should().Be(uri);
            settings.DefaultAzureCredential.Should().Be(credentials);
            settings.TableClientOptions.Should().Be(options);
        }

        [Fact(DisplayName = "AzureTableStorageJournalSetup should override settings values")]
        public void JournalSetupTest()
        {
            var uri = new Uri("https://whatever.com");
            var credentials = new DefaultAzureCredential();
            var options = new TableClientOptions();
            var setup = new AzureTableStorageJournalSetup
            {
                ConnectionString = "abc",
                TableName = "bcd",
                ConnectTimeout = 1.Seconds(),
                RequestTimeout = 2.Seconds(),
                VerboseLogging = true,
                Development = true,
                AutoInitialize = false,
                ServiceUri = uri,
                DefaultAzureCredential = credentials,
                TableClientOptions = options
            };

            var settings = setup.Apply(DefaultJournalSettings);
            
            settings.ConnectionString.Should().Be("abc");
            settings.TableName.Should().Be("bcd");
            settings.ConnectTimeout.Should().Be(1.Seconds());
            settings.RequestTimeout.Should().Be(2.Seconds());
            settings.VerboseLogging.Should().BeTrue();
            settings.Development.Should().BeTrue();
            settings.AutoInitialize.Should().BeFalse();
            settings.ServiceUri.Should().Be(uri);
            settings.DefaultAzureCredential.Should().Be(credentials);
            settings.TableClientOptions.Should().Be(options);
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