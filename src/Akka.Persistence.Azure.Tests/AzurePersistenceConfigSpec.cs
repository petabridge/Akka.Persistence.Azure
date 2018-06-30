using System;
using System.Collections.Generic;
using System.Text;
using Akka.Configuration;
using Akka.Persistence.Azure.Journal;
using Akka.Persistence.Azure.Snapshot;
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
        }
    }
}
