using Akka.Configuration;
using Akka.Persistence.Azure.Tests.Helper;
using Akka.Persistence.TCK.Snapshot;
using Azure.Storage.Blobs;
using Xunit;
using Xunit.Abstractions;
using static Akka.Persistence.Azure.Tests.Helper.AzureStorageConfigHelper;

namespace Akka.Persistence.Azure.Tests
{
    [Collection("AzureSpecs")]
    public class Issue159Spec: SnapshotStoreSpec
    {
        private static Config Config(string connectionString)
        {
            return ConfigurationFactory.ParseString(@"
akka {
    loglevel = DEBUG
    log-config-on-start = off
    test.single-expect-default = 30s

    persistence {
        publish-plugin-commands = on

        journal {
            plugin = ""akka.persistence.journal.azure-table""

            azure-table {
                connection-string = """ + connectionString + @"""
                connect-timeout = 3s
                request-timeout = 3s
                verbose-logging = on
            }
        }

        query {
            journal {
                azure-table {
                    write-plugin = ""akka.persistence.journal.azure-table""
                    refresh-interval = 1s
		            max-buffer-size = 150
                }
            }
        }

        snapshot-store {
            plugin = ""akka.persistence.snapshot-store.azure-blob-store""
            azure-blob-store {
                class = ""Akka.Persistence.Azure.Snapshot.AzureBlobSnapshotStore, Akka.Persistence.Azure""
                connection-string = """ + connectionString + @"""
                container-name = ""default""
                connect-timeout = 3s
                request-timeout = 3s
                verbose-logging = on
                plugin-dispatcher = ""akka.actor.default-dispatcher""
            }
        }
    }
}");
        }

        public Issue159Spec(ITestOutputHelper output)
            : base(AzureConfig(Config), nameof(Issue159Spec), output)
        {
            var extension = AzurePersistence.Get(Sys);

            var service = new BlobServiceClient(extension.BlobSettings.ConnectionString);
            var containerClient = service.GetBlobContainerClient(extension.BlobSettings.ContainerName);

            // The thing we're testing, the container already exist on the account
            if (!containerClient.Exists())
                containerClient.Create(extension.BlobSettings.ContainerPublicAccessType);

            // empty the container, possible junk from previous tests
            foreach (var blob in containerClient.GetBlobs())
            {
                containerClient.GetBlobClient(blob.Name).Delete();
            }

            Initialize();
        }
    }
}
