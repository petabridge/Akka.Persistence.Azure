using Akka.Configuration;
using System;

namespace Akka.Persistence.Azure.Snapshot
{
    /// <summary>
    /// Configuration settings for the <see cref="AzureBlobSnapshotStore"/>
    /// </summary>
    public sealed class AzureBlobSnapshotStoreSettings
    {
        public AzureBlobSnapshotStoreSettings(string connectionString, string containerName, 
            TimeSpan connectTimeout, TimeSpan requestTimeout, bool verboseLogging)
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
        /// Initial timeout to use when connecting to Azure Table Storage for the first time.
        /// </summary>
        public TimeSpan ConnectTimeout { get; }

        /// <summary>
        /// Timeouts for individual read, write, and delete requests to Azure Table Storage.
        /// </summary>
        public TimeSpan RequestTimeout { get; }

        /// <summary>
        /// For debugging purposes only. Logs every individual operation to Azure table storage.
        /// </summary>
        public bool VerboseLogging { get; }

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
            var connectTimeout = config.GetTimeSpan("connect-timeout", TimeSpan.FromSeconds(3));
            var requestTimeout = config.GetTimeSpan("request-timeout", TimeSpan.FromSeconds(3));
            var verbose = config.GetBoolean("verbose-logging", false);
            return new AzureBlobSnapshotStoreSettings(connectionString, containerName, connectTimeout, requestTimeout, verbose);
        }
    }
}
