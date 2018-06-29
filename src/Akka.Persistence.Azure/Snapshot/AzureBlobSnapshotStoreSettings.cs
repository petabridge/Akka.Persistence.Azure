using Akka.Configuration;

namespace Akka.Persistence.Azure.Snapshot
{
    /// <summary>
    /// Configuration settings for the <see cref="AzureBlobSnapshotStore"/>
    /// </summary>
    public sealed class AzureBlobSnapshotStoreSettings
    {
        public AzureBlobSnapshotStoreSettings(string connectionString, string containerName)
        {
            ConnectionString = connectionString;
            ContainerName = containerName;
        }

        /// <summary>
        /// The connection string for connecting to Windows Azure blob storage account.
        /// </summary>
        public string ConnectionString { get; }

        /// <summary>
        /// The table of the container we'll be using to serialize these blobs.
        /// </summary>
        public string ContainerName { get; }

        /// <summary>
        /// Creates an <see cref="AzureBlobSnapshotStoreSettings"/> instance using the 
        /// `akka.persistence.snapshot-store.azure-blob-store` HOCON configuration section.
        /// </summary>
        /// <param name="config">The `akka.persistence.snapshot-store.azure-blob-store` HOCON section.</param>
        /// <returns>A new settings instance.</returns>
        public static AzureBlobSnapshotStoreSettings Create(Config config)
        {
            var connectionString = config.GetString("connection-string");
            var containerName = config.GetString("container-name");
            return new AzureBlobSnapshotStoreSettings(connectionString, containerName);
        }
    }
}
