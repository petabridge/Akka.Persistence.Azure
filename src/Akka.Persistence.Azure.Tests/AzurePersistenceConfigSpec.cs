// -----------------------------------------------------------------------
// <copyright file="AzurePersistenceConfigSpec.cs" company="Petabridge, LLC">
//      Copyright (C) 2015 - 2023 Petabridge, LLC <https://petabridge.com>
// </copyright>
// -----------------------------------------------------------------------

using System;
using Akka.Configuration;
using Akka.Persistence.Azure.Hosting;
using Akka.Persistence.Azure.Journal;
using Akka.Persistence.Azure.Query;
using Akka.Persistence.Azure.Snapshot;
using Azure.Data.Tables;
using Azure.Identity;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Xunit;
#pragma warning disable CS0618 // Type or member is obsolete

namespace Akka.Persistence.Azure.Tests
{
    public class AzurePersistenceConfigSpec
    {
        private static readonly AzureBlobSnapshotStoreSettings DefaultSnapshotSettings =
            AzureBlobSnapshotStoreSettings.Create(AzurePersistence.DefaultConfig
                .GetConfig(AzureBlobSnapshotStoreSettings.SnapshotStoreConfigPath));

        private static readonly AzureTableStorageJournalSettings DefaultJournalSettings =
            AzureTableStorageJournalSettings.Create(AzurePersistence.DefaultConfig
                .GetConfig(AzureTableStorageJournalSettings.JournalConfigPath));
        
        [Fact]
        public void ShouldLoadDefaultConfig()
        {
            Assert.True(AzurePersistence.DefaultConfig.HasPath(AzureBlobSnapshotStoreSettings.SnapshotStoreConfigPath));
            Assert.True(AzurePersistence.DefaultConfig.HasPath(AzureTableStorageJournalSettings.JournalConfigPath));
            Assert.True(AzurePersistence.DefaultConfig.HasPath(AzureTableStorageReadJournal.Identifier));
        }

        [Fact]
        public void ShouldParseDefaultSnapshotConfig()
        {
            var settings = DefaultSnapshotSettings;

            Assert.Empty(settings.ConnectionString);
            Assert.Equal("akka-persistence-default-container", settings.ContainerName);
            Assert.Equal(TimeSpan.FromSeconds(3), settings.ConnectTimeout);
            Assert.Equal(TimeSpan.FromSeconds(3), settings.RequestTimeout);
            Assert.False(settings.VerboseLogging);
            Assert.False(settings.Development);
            Assert.True(settings.AutoInitialize);
            Assert.Equal(PublicAccessType.None, settings.ContainerPublicAccessType);
            Assert.Null(settings.ServiceUri);
            Assert.Null(settings.AzureCredential);
            Assert.Null(settings.BlobClientOptions);
            Assert.Null(settings.BlobServiceClientFactory);
        }
        
        [Fact(DisplayName = "AzureBlobSnapshotStoreSettings With overrides should override default values")]
        public void SnapshotSettingsWithMethodsTest()
        {
            var uri = new Uri("https://whatever.com");
            var credentials = new DefaultAzureCredential();
            var client = new BlobServiceClient(uri, credentials);
            var options = new BlobClientOptions();
            var settings = DefaultSnapshotSettings
                    .WithConnectionString("abc")
                    .WithContainerName("bcd")
                    .WithConnectTimeout(TimeSpan.FromSeconds(1))
                    .WithRequestTimeout(TimeSpan.FromSeconds(2))
                    .WithVerboseLogging(true)
                    .WithDevelopment(true)
                    .WithAutoInitialize(false)
                    .WithContainerPublicAccessType(PublicAccessType.Blob)
                    .WithAzureCredential(uri, credentials, options)
                    .WithBlobServiceClientFactory(() => client);

            Assert.Equal("abc", settings.ConnectionString);
            Assert.Equal("bcd", settings.ContainerName);
            Assert.Equal(TimeSpan.FromSeconds(1), settings.ConnectTimeout);
            Assert.Equal(TimeSpan.FromSeconds(2), settings.RequestTimeout);
            Assert.True(settings.VerboseLogging);
            Assert.False(settings.Development);
            Assert.False(settings.AutoInitialize);
            Assert.Equal(PublicAccessType.Blob, settings.ContainerPublicAccessType);
            Assert.Equal(uri, settings.ServiceUri);
            Assert.Equal(credentials, settings.AzureCredential);
            Assert.Equal(options, settings.BlobClientOptions);
            Assert.NotNull(settings.BlobServiceClientFactory);
            Assert.Equal(client, settings.BlobServiceClientFactory!.Invoke());
        }

        [Fact(DisplayName = "AzureBlobSnapshotStoreSetup should override settings values")]
        public void SnapshotSetupTest()
        {
            var uri = new Uri("https://whatever.com");
            var credentials = new DefaultAzureCredential();
            var client = new BlobServiceClient(uri, credentials);
            var options = new BlobClientOptions();
            var setup = new AzureBlobSnapshotSetup
            {
                ConnectionString = "abc",
                ContainerName = "bcd",
                ConnectTimeout = TimeSpan.FromSeconds(1),
                RequestTimeout = TimeSpan.FromSeconds(2),
                VerboseLogging = true,
                Development = true,
                AutoInitialize = false,
                ContainerPublicAccessType = PublicAccessType.Blob,
                ServiceUri = uri,
                AzureCredential = credentials,
                BlobClientOptions = options,
                BlobServiceClientFactory = () => client,
            };

            var settings = setup.Apply(DefaultSnapshotSettings);
            
            Assert.Equal("abc", settings.ConnectionString);
            Assert.Equal("bcd", settings.ContainerName);
            Assert.Equal(TimeSpan.FromSeconds(1), settings.ConnectTimeout);
            Assert.Equal(TimeSpan.FromSeconds(2), settings.RequestTimeout);
            Assert.True(settings.VerboseLogging);
            Assert.False(settings.Development);
            Assert.False(settings.AutoInitialize);
            Assert.Equal(PublicAccessType.Blob, settings.ContainerPublicAccessType);
            Assert.Equal(uri, settings.ServiceUri);
            Assert.Equal(credentials, settings.AzureCredential);
            Assert.Equal(options, settings.BlobClientOptions);
            Assert.NotNull(settings.BlobServiceClientFactory);
            Assert.Equal(client, settings.BlobServiceClientFactory!.Invoke());
        }

        [Fact(DisplayName = "AzureBlobSnapshotStoreOptions should override settings values")]
        public void SnapshotOptionsTest()
        {
            var uri = new Uri("https://whatever.com");
            var credentials = new DefaultAzureCredential();
            var client = new BlobServiceClient(uri, credentials);
            var blobOptions = new BlobClientOptions();
            var options = new AzureBlobSnapshotOptions(false, "abcd")
            {
                ConnectionString = "abc",
                ContainerName = "bcd",
                ConnectTimeout = TimeSpan.FromSeconds(1),
                RequestTimeout = TimeSpan.FromSeconds(2),
                VerboseLogging = true,
                Development = true,
                AutoInitialize = false,
                ContainerPublicAccessType = PublicAccessType.Blob,
                ServiceUri = uri,
                AzureCredential = credentials,
                BlobClientOptions = blobOptions,
                BlobServiceClientFactory = () => client
            };

            var settings = AzureBlobSnapshotStoreSettings.Create(
                options.ToConfig().WithFallback(options.DefaultConfig).GetConfig(options.PluginId));

            var setup = new AzureBlobSnapshotSetup();
            options.Apply(setup);
            settings = setup.Apply(settings);
            
            Assert.Equal("abc", settings.ConnectionString);
            Assert.Equal("bcd", settings.ContainerName);
            Assert.Equal(TimeSpan.FromSeconds(1), settings.ConnectTimeout);
            Assert.Equal(TimeSpan.FromSeconds(2), settings.RequestTimeout);
            Assert.True(settings.VerboseLogging);
            Assert.False(settings.Development);
            Assert.False(settings.AutoInitialize);
            Assert.Equal(PublicAccessType.Blob, settings.ContainerPublicAccessType);
            Assert.Equal(uri, settings.ServiceUri);
            Assert.Equal(credentials, settings.AzureCredential);
            Assert.Equal(blobOptions, settings.BlobClientOptions);
            Assert.NotNull(settings.BlobServiceClientFactory);
            Assert.Equal(client, settings.BlobServiceClientFactory!.Invoke());
        }

        [Fact]
        public void ShouldParseTableConfig()
        {
            var settings = DefaultJournalSettings;

            Assert.Empty(settings.ConnectionString);
            Assert.Equal("AkkaPersistenceDefaultTable", settings.TableName);
            Assert.Equal(TimeSpan.FromSeconds(3), settings.ConnectTimeout);
            Assert.Equal(TimeSpan.FromSeconds(3), settings.RequestTimeout);
            Assert.False(settings.VerboseLogging);
            Assert.False(settings.Development);
            Assert.True(settings.AutoInitialize);
            Assert.Null(settings.ServiceUri);
            Assert.Null(settings.AzureCredential);
            Assert.Null(settings.TableClientOptions);
            Assert.Null(settings.TableServiceClientFactory);
        }

        [Fact(DisplayName = "AzureTableStorageJournalSettings With overrides should override default values")]
        public void JournalSettingsWithMethodsTest()
        {
            var uri = new Uri("https://whatever.com");
            var credentials = new DefaultAzureCredential();
            var client = new TableServiceClient(uri, credentials);
            var options = new TableClientOptions();
            var settings = DefaultJournalSettings
                    .WithConnectionString("abc")
                    .WithTableName("bcd")
                    .WithConnectTimeout(TimeSpan.FromSeconds(1))
                    .WithRequestTimeout(TimeSpan.FromSeconds(2))
                    .WithVerboseLogging(true)
                    .WithDevelopment(true)
                    .WithAutoInitialize(false)
                    .WithAzureCredential(uri, credentials, options)
                    .WithTableServiceClientFactory(() => client);

            Assert.Equal("abc", settings.ConnectionString);
            Assert.Equal("bcd", settings.TableName);
            Assert.Equal(TimeSpan.FromSeconds(1), settings.ConnectTimeout);
            Assert.Equal(TimeSpan.FromSeconds(2), settings.RequestTimeout);
            Assert.True(settings.VerboseLogging);
            Assert.False(settings.Development);
            Assert.False(settings.AutoInitialize);
            Assert.Equal(uri, settings.ServiceUri);
            Assert.Equal(credentials, settings.AzureCredential);
            Assert.Equal(options, settings.TableClientOptions);
            Assert.NotNull(settings.TableServiceClientFactory);
            Assert.Equal(client, settings.TableServiceClientFactory!.Invoke());
        }

        [Fact(DisplayName = "AzureTableStorageJournalSetup should override settings values")]
        public void JournalSetupTest()
        {
            var uri = new Uri("https://whatever.com");
            var credentials = new DefaultAzureCredential();
            var client = new TableServiceClient(uri, credentials);
            var options = new TableClientOptions();
            var setup = new AzureTableStorageJournalSetup
            {
                ConnectionString = "abc",
                TableName = "bcd",
                ConnectTimeout = TimeSpan.FromSeconds(1),
                RequestTimeout = TimeSpan.FromSeconds(2),
                VerboseLogging = true,
                Development = true,
                AutoInitialize = false,
                ServiceUri = uri,
                AzureCredential = credentials,
                TableClientOptions = options,
                TableServiceClientFactory = () => client
            };

            var settings = setup.Apply(DefaultJournalSettings);
            
            Assert.Equal("abc", settings.ConnectionString);
            Assert.Equal("bcd", settings.TableName);
            Assert.Equal(TimeSpan.FromSeconds(1), settings.ConnectTimeout);
            Assert.Equal(TimeSpan.FromSeconds(2), settings.RequestTimeout);
            Assert.True(settings.VerboseLogging);
            Assert.False(settings.Development);
            Assert.False(settings.AutoInitialize);
            Assert.Equal(uri, settings.ServiceUri);
            Assert.Equal(credentials, settings.AzureCredential);
            Assert.Equal(options, settings.TableClientOptions);
            Assert.NotNull(settings.TableServiceClientFactory);
            Assert.Equal(client, settings.TableServiceClientFactory!.Invoke());
        }
        
        [Fact(DisplayName = "AzureTableStorageJournalOptions should override settings values")]
        public void JournalOptionsTest()
        {
            var uri = new Uri("https://whatever.com");
            var credentials = new DefaultAzureCredential();
            var client = new TableServiceClient(uri, credentials);
            var clientOptions = new TableClientOptions();
            var options = new AzureTableStorageJournalOptions(false, "abcd")
            {
                ConnectionString = "abc",
                TableName = "bcd",
                ConnectTimeout = TimeSpan.FromSeconds(1),
                RequestTimeout = TimeSpan.FromSeconds(2),
                VerboseLogging = true,
                Development = true,
                AutoInitialize = false,
                ServiceUri = uri,
                AzureCredential = credentials,
                TableClientOptions = clientOptions,
                TableServiceClientFactory = () => client
            };

            var settings = AzureTableStorageJournalSettings.Create(
                options.ToConfig().WithFallback(options.DefaultConfig).GetConfig(options.PluginId));

            var setup = new AzureTableStorageJournalSetup();
            options.Apply(setup);
            settings = setup.Apply(settings);

            Assert.Equal("abc", settings.ConnectionString);
            Assert.Equal("bcd", settings.TableName);
            Assert.Equal(TimeSpan.FromSeconds(1), settings.ConnectTimeout);
            Assert.Equal(TimeSpan.FromSeconds(2), settings.RequestTimeout);
            Assert.True(settings.VerboseLogging);
            Assert.False(settings.Development);
            Assert.False(settings.AutoInitialize);
            Assert.Equal(uri, settings.ServiceUri);
            Assert.Equal(credentials, settings.AzureCredential);
            Assert.Equal(clientOptions, settings.TableClientOptions);
            Assert.NotNull(settings.TableServiceClientFactory);
            Assert.Equal(client, settings.TableServiceClientFactory!.Invoke());
        }
        
        [Theory]
        [InlineData("fo")]
        [InlineData("1foo")]
        [InlineData("tables")]
        public void ShouldThrowArgumentExceptionForIllegalTableNames(string tableName)
        {
            Action createJournalSettings = () => AzureTableStorageJournalSettings.Create(
                    ConfigurationFactory.ParseString(@"akka.persistence.journal.azure-table{
                        connection-string = foo
                        table-name = " + tableName + @" 
                    }").WithFallback(AzurePersistence.DefaultConfig)
                        .GetConfig("akka.persistence.journal.azure-table"));
            Assert.Throws<ArgumentException>(() => createJournalSettings());
        }
        
        [Theory]
        [InlineData("ba")]
        [InlineData("bar--table")]
        public void ShouldThrowArgumentExceptionForIllegalContainerNames(string containerName)
        {
            Action createSnapshotSettings = () =>
                AzureBlobSnapshotStoreSettings.Create(
                    ConfigurationFactory.ParseString(@"akka.persistence.snapshot-store.azure-blob-store{
                        connection-string = foo
                        container-name = " + containerName + @"
                    }").WithFallback(AzurePersistence.DefaultConfig)
                        .GetConfig("akka.persistence.snapshot-store.azure-blob-store"));

            Assert.Throws<ArgumentException>(() => createSnapshotSettings());
        }
    }
}
