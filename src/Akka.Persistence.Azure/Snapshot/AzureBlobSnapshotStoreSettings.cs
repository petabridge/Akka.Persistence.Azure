// -----------------------------------------------------------------------
// <copyright file="AzureBlobSnapshotStoreSettings.cs" company="Petabridge, LLC">
//      Copyright (C) 2015 - 2018 Petabridge, LLC <https://petabridge.com>
// </copyright>
// -----------------------------------------------------------------------

using System;
using Akka.Configuration;
using Akka.Persistence.Azure.Util;
using Azure.Storage.Blobs.Models;

namespace Akka.Persistence.Azure.Snapshot
{
    /// <summary>
    ///     Configuration settings for the <see cref="AzureBlobSnapshotStore" />.
    ///     Loads settings from the `akka.persistence.snapshot-store.azure-blob-store` HOCON section.
    /// </summary>
    public sealed class AzureBlobSnapshotStoreSettings
    {
        [Obsolete]
        public AzureBlobSnapshotStoreSettings(
            string connectionString, 
            string containerName,
            TimeSpan connectTimeout, 
            TimeSpan requestTimeout, 
            bool verboseLogging, 
            bool development,
            bool autoInitialize)
            : this(connectionString, containerName, connectTimeout, requestTimeout, verboseLogging, development, autoInitialize, PublicAccessType.BlobContainer)
        { }

        public AzureBlobSnapshotStoreSettings(
            string connectionString, 
            string containerName,
            TimeSpan connectTimeout, 
            TimeSpan requestTimeout, 
            bool verboseLogging, 
            bool development, 
            bool autoInitialize, 
            PublicAccessType containerPublicAccessType)
        {
            if (string.IsNullOrWhiteSpace(containerName))
                throw new ConfigurationException("[AzureBlobSnapshotStore] Container name is null or empty.");

            NameValidator.ValidateContainerName(containerName);
            ConnectionString = connectionString;
            ContainerName = containerName;
            RequestTimeout = requestTimeout;
            ConnectTimeout = connectTimeout;
            VerboseLogging = verboseLogging;
            Development = development;
            AutoInitialize = autoInitialize;
            ContainerPublicAccessType = containerPublicAccessType;
        }

        /// <summary>
        ///     The connection string for connecting to Windows Azure blob storage account.
        /// </summary>
        public string ConnectionString { get; }

        /// <summary>
        ///     The table of the container we'll be using to serialize these blobs.
        /// </summary>
        public string ContainerName { get; }

        /// <summary>
        ///     Initial timeout to use when connecting to Azure Container Storage for the first time.
        /// </summary>
        public TimeSpan ConnectTimeout { get; }

        /// <summary>
        ///     Timeouts for individual read, write, and delete requests to Azure Container Storage.
        /// </summary>
        public TimeSpan RequestTimeout { get; }

        /// <summary>
        ///     For debugging purposes only. Logs every individual operation to Azure table storage.
        /// </summary>
        public bool VerboseLogging { get; }

        public bool Development { get; }

        public bool AutoInitialize { get; }
        
        public PublicAccessType ContainerPublicAccessType { get; }

        /// <summary>
        ///     Creates an <see cref="AzureBlobSnapshotStoreSettings" /> instance using the
        ///     `akka.persistence.snapshot-store.azure-blob-store` HOCON configuration section.
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
            var development = config.GetBoolean("development", false);
            var autoInitialize = config.GetBoolean("auto-initialize", true);

            var accessType = config.GetString("container-public-access-type", PublicAccessType.BlobContainer.ToString());

            if (!Enum.TryParse<PublicAccessType>(accessType, true, out var containerPublicAccessType))
                throw new ConfigurationException(
                    "Invalid [container-public-access-type] value. Valid values are 'None', 'Blob', and 'BlobContainer'");

            return new AzureBlobSnapshotStoreSettings(
                connectionString, 
                containerName, 
                connectTimeout, 
                requestTimeout,
                verbose,
                development,
                autoInitialize,
                containerPublicAccessType);
        }
    }
}