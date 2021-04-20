using System;
using System.Collections.Generic;
using System.Text;
using Akka.Configuration;
using Akka.Persistence.TCK.Snapshot;
using Azure.Storage.Blobs;
using Xunit;
using Xunit.Abstractions;

namespace Akka.Persistence.Azure.Tests
{
    public class Issue159Spec: SnapshotStoreSpec
    {
        private static readonly Config Config = ConfigurationFactory.ParseString(@"
akka {
    loglevel = DEBUG
    log-config-on-start = off
    test.single-expect-default = 30s

    persistence {
        publish-plugin-commands = on

        journal {
            plugin = ""akka.persistence.journal.azure-table""

            azure-table {
                connection-string = ""UseDevelopmentStorage=true""
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
        }
    }
}

akka.persistence.snapshot-store.azure-blob-store {
    class = ""Akka.Persistence.Azure.Snapshot.AzureBlobSnapshotStore, Akka.Persistence.Azure""
    connection-string = ""UseDevelopmentStorage=true""
    container-name = ""default""
    connect-timeout = 3s
    request-timeout = 3s
    verbose-logging = on
    plugin-dispatcher = ""akka.actor.default-dispatcher""
}
");

        public Issue159Spec(ITestOutputHelper output)
            : base(Config, nameof(Issue159Spec), output)
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
