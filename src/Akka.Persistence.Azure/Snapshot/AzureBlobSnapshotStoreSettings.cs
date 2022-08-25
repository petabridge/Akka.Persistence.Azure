// -----------------------------------------------------------------------
// <copyright file="AzureBlobSnapshotStoreSettings.cs" company="Petabridge, LLC">
//      Copyright (C) 2015 - 2018 Petabridge, LLC <https://petabridge.com>
// </copyright>
// -----------------------------------------------------------------------

using System;
using Akka.Actor;
using Akka.Configuration;
using Akka.Persistence.Azure.Util;
using Azure.Identity;
using Azure.Storage.Blobs;
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

        [Obsolete]
        public AzureBlobSnapshotStoreSettings(
            string connectionString, 
            string containerName,
            TimeSpan connectTimeout, 
            TimeSpan requestTimeout, 
            bool verboseLogging, 
            bool development, 
            bool autoInitialize, 
            PublicAccessType containerPublicAccessType)
            : this(
                connectionString: connectionString,
                containerName: containerName,
                connectTimeout: connectTimeout,
                requestTimeout: requestTimeout,
                verboseLogging: verboseLogging,
                development: development,
                autoInitialize: autoInitialize,
                containerPublicAccessType: containerPublicAccessType,
                serviceUri: null,
                defaultAzureCredential: null,
                blobClientOption: null)
        { }

        public AzureBlobSnapshotStoreSettings(
            string connectionString, 
            string containerName,
            TimeSpan connectTimeout, 
            TimeSpan requestTimeout, 
            bool verboseLogging, 
            bool development, 
            bool autoInitialize, 
            PublicAccessType containerPublicAccessType,
            Uri serviceUri,
            DefaultAzureCredential defaultAzureCredential,
            BlobClientOptions blobClientOption)
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
            ServiceUri = serviceUri;
            DefaultAzureCredential = defaultAzureCredential;
            BlobClientOptions = blobClientOption;
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

        /// <summary>
        ///     Flag that we're running in development mode. When this is set, <see cref="DefaultAzureCredential"/> and
        ///     <see cref="ConnectionString"/> will be ignored, replaced with "UseDevelopmentStorage=true" for local
        ///     connection to Azurite.
        /// </summary>
        public bool Development { get; }

        /// <summary>
        ///     Automatically create the Blog Storage container if no existing Blob container is found
        /// </summary>
        public bool AutoInitialize { get; }
        
        /// <summary>
        ///     The public access type of the auto-initialized Blob Storage container
        /// </summary>
        public PublicAccessType ContainerPublicAccessType { get; }

        /// <summary>
        /// A <see cref="Uri"/> referencing the blob service.
        /// This is likely to be similar to "https://{account_name}.blob.core.windows.net".
        /// </summary>
        public Uri ServiceUri { get; }

        /// <summary>
        /// The <see cref="DefaultAzureCredential"/> used to sign API requests.
        /// </summary>
        public DefaultAzureCredential DefaultAzureCredential { get; }

        /// <summary>
        /// Optional client options that define the transport pipeline policies for authentication,
        /// retries, etc., that are applied to every request.
        /// </summary>
        public BlobClientOptions BlobClientOptions { get; }

        public AzureBlobSnapshotStoreSettings WithConnectionString(string connectionString)
            => Copy(connectionString: connectionString);
        public AzureBlobSnapshotStoreSettings WithContainerName(string containerName)
            => Copy(containerName: containerName);
        public AzureBlobSnapshotStoreSettings WithConnectTimeout(TimeSpan connectTimeout)
            => Copy(connectTimeout: connectTimeout);
        public AzureBlobSnapshotStoreSettings WithRequestTimeout(TimeSpan requestTimeout)
            => Copy(requestTimeout: requestTimeout);
        public AzureBlobSnapshotStoreSettings WithVerboseLogging(bool verboseLogging)
            => Copy(verboseLogging: verboseLogging);
        public AzureBlobSnapshotStoreSettings WithDevelopment(bool development)
            => Copy(development: development);
        public AzureBlobSnapshotStoreSettings WithAutoInitialize(bool autoInitialize)
            => Copy(autoInitialize: autoInitialize);
        public AzureBlobSnapshotStoreSettings WithContainerPublicAccessType(PublicAccessType containerPublicAccessType)
            => Copy(containerPublicAccessType: containerPublicAccessType);
        public AzureBlobSnapshotStoreSettings WithAzureCredential(
            Uri serviceUri,
            DefaultAzureCredential defaultAzureCredential,
            BlobClientOptions blobClientOption = null)
            => Copy(
                serviceUri: serviceUri,
                defaultAzureCredential: defaultAzureCredential,
                blobClientOption: blobClientOption);
        
        private AzureBlobSnapshotStoreSettings Copy(
            string connectionString = null,
            string containerName = null,
            TimeSpan? connectTimeout = null,
            TimeSpan? requestTimeout = null,
            bool? verboseLogging = null,
            bool? development = null,
            bool? autoInitialize = null,
            PublicAccessType? containerPublicAccessType = null,
            Uri serviceUri = null,
            DefaultAzureCredential defaultAzureCredential = null,
            BlobClientOptions blobClientOption = null)
            => new AzureBlobSnapshotStoreSettings(
                connectionString ?? ConnectionString,
                containerName ?? ContainerName,
                connectTimeout ?? ConnectTimeout,
                requestTimeout ?? RequestTimeout,
                verboseLogging ?? VerboseLogging,
                development ?? Development,
                autoInitialize ?? AutoInitialize,
                containerPublicAccessType ?? ContainerPublicAccessType,
                serviceUri ?? ServiceUri,
                defaultAzureCredential ?? DefaultAzureCredential,
                blobClientOption ?? BlobClientOptions);
        
        /// <summary>
        ///     Creates an <see cref="AzureBlobSnapshotStoreSettings" /> instance using the
        ///     `akka.persistence.snapshot-store.azure-blob-store` HOCON configuration section inside
        ///     the <see cref="ActorSystem"/> settings.
        /// </summary>
        /// <param name="system">The <see cref="ActorSystem"/> to extract the configuration from</param>
        /// <returns>A new settings instance.</returns>
        public static AzureBlobSnapshotStoreSettings Create(ActorSystem system)
        {
            var config = system.Settings.Config.GetConfig("akka.persistence.snapshot-store.azure-blob-store");
            if (config is null)
                throw new ConfigurationException(
                    "Could not find HOCON config at path 'akka.persistence.snapshot-store.azure-blob-store'");
            return Create(config);
        }
        
        /// <summary>
        ///     Creates an <see cref="AzureBlobSnapshotStoreSettings" /> instance using the
        ///     `akka.persistence.snapshot-store.azure-blob-store` HOCON configuration section.
        /// </summary>
        /// <param name="config">The `akka.persistence.snapshot-store.azure-blob-store` HOCON section.</param>
        /// <returns>A new settings instance.</returns>
        public static AzureBlobSnapshotStoreSettings Create(Config config)
        {
            if (config is null)
                throw new ArgumentNullException(nameof(config));
            
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