// -----------------------------------------------------------------------
// <copyright file="AzureBlobSnapshotStoreSettings.cs" company="Petabridge, LLC">
//      Copyright (C) 2015 - 2023 Petabridge, LLC <https://petabridge.com>
// </copyright>
// -----------------------------------------------------------------------

using System;
using Akka.Actor;
using Akka.Configuration;
using Akka.Persistence.Azure.Util;
using Azure.Core;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;

#nullable enable
namespace Akka.Persistence.Azure.Snapshot
{
    /// <summary>
    ///     Configuration settings for the <see cref="AzureBlobSnapshotStore" />.
    ///     Loads settings from the `akka.persistence.snapshot-store.azure-blob-store` HOCON section.
    /// </summary>
    public sealed class AzureBlobSnapshotStoreSettings
    {
        public const string SnapshotStoreConfigPath = "akka.persistence.snapshot-store.azure-blob-store";
        
        // ReSharper disable IntroduceOptionalParameters.Global
        [Obsolete(message:"Use constructor with containerPublicAccessType, serviceUri, defaultAzureCredential, tableClientOptions, and blobServiceClient argument instead.")]
        public AzureBlobSnapshotStoreSettings(
            string? connectionString, 
            string containerName,
            TimeSpan connectTimeout, 
            TimeSpan requestTimeout, 
            bool verboseLogging, 
            bool development,
            bool autoInitialize)
            : this(
                connectionString: connectionString,
                containerName: containerName,
                connectTimeout: connectTimeout,
                requestTimeout: requestTimeout,
                verboseLogging: verboseLogging,
                development: development,
                autoInitialize: autoInitialize,
                containerPublicAccessType: PublicAccessType.BlobContainer,
                serviceUri: null,
                defaultAzureCredential: null,
                blobClientOption: null, 
                blobServiceClientFactory: null)
        { }

        [Obsolete(message:"Use constructor with serviceUri, defaultAzureCredential, tableClientOptions, and blobServiceClient argument instead.")]
        public AzureBlobSnapshotStoreSettings(
            string? connectionString, 
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
                blobClientOption: null, 
                blobServiceClientFactory: null)
        { }

        [Obsolete(message:"Use constructor with blobServiceClient argument instead.")]
        public AzureBlobSnapshotStoreSettings(
            string connectionString, 
            string containerName,
            TimeSpan connectTimeout, 
            TimeSpan requestTimeout, 
            bool verboseLogging, 
            bool development, 
            bool autoInitialize, 
            PublicAccessType containerPublicAccessType,
            Uri? serviceUri,
            TokenCredential? defaultAzureCredential,
            BlobClientOptions? blobClientOption)
            : this(
                connectionString: connectionString,
                containerName: containerName,
                connectTimeout: connectTimeout,
                requestTimeout: requestTimeout,
                verboseLogging: verboseLogging,
                development: development,
                autoInitialize: autoInitialize,
                containerPublicAccessType: containerPublicAccessType,
                serviceUri: serviceUri,
                defaultAzureCredential: defaultAzureCredential,
                blobClientOption: blobClientOption,
                blobServiceClientFactory: null)
        { }

        [Obsolete(message:"Use constructor with folders argument instead.")]
        public AzureBlobSnapshotStoreSettings(
            string? connectionString, 
            string containerName,
            TimeSpan connectTimeout, 
            TimeSpan requestTimeout, 
            bool verboseLogging, 
            bool development, 
            bool autoInitialize, 
            PublicAccessType containerPublicAccessType,
            Uri? serviceUri,
            TokenCredential? defaultAzureCredential,
            BlobClientOptions? blobClientOption,
            Func<BlobServiceClient>? blobServiceClientFactory)
            : this(
                connectionString: connectionString,
                containerName: containerName,
                folders: string.Empty,
                connectTimeout: connectTimeout,
                requestTimeout: requestTimeout,
                verboseLogging: verboseLogging,
                development: development,
                autoInitialize: autoInitialize,
                containerPublicAccessType: containerPublicAccessType,
                serviceUri: serviceUri,
                defaultAzureCredential: defaultAzureCredential,
                blobClientOption: blobClientOption,
                blobServiceClientFactory: blobServiceClientFactory)
        { }
        // ReSharper restore IntroduceOptionalParameters.Global
        
        public AzureBlobSnapshotStoreSettings(
            string? connectionString, 
            string containerName,
            string folders,
            TimeSpan connectTimeout, 
            TimeSpan requestTimeout, 
            bool verboseLogging, 
            bool development, 
            bool autoInitialize, 
            PublicAccessType containerPublicAccessType,
            Uri? serviceUri,
            TokenCredential? defaultAzureCredential,
            BlobClientOptions? blobClientOption,
            Func<BlobServiceClient>? blobServiceClientFactory)
        {
            if (string.IsNullOrWhiteSpace(containerName))
                throw new ConfigurationException("[AzureBlobSnapshotStore] Container name is null or empty.");

            NameValidator.ValidateContainerName(containerName);
            ConnectionString = connectionString;
            ContainerName = containerName;
            Folders = folders;
            RequestTimeout = requestTimeout;
            ConnectTimeout = connectTimeout;
            VerboseLogging = verboseLogging;
            AutoInitialize = autoInitialize;
            ContainerPublicAccessType = containerPublicAccessType;
            ServiceUri = serviceUri;
            AzureCredential = defaultAzureCredential;
            BlobClientOptions = blobClientOption;
            BlobServiceClientFactory = blobServiceClientFactory;
        }

        /// <summary>
        ///     The connection string for connecting to Windows Azure blob storage account.
        /// </summary>
        public string? ConnectionString { get; }

        /// <summary>
        ///     The table of the container we'll be using to serialize these blobs.
        /// </summary>
        public string ContainerName { get; }
        
        /// <summary>
        ///     The "folder" or "directory" where snapshot files will be stored.
        ///     Note that Azure Blob Storage does not implement a true folder tree structure,
        ///     "folder" names are actually a simple prefix to the blob file name.
        /// </summary>
        /// <example>
        ///     If you set this setting to "folder1/folder2", then the snapshots will be stored as:
        ///         /{account name}/akka-persistence-default-container/folder1/folder2/snapshot-{persistence id}-{sequence number}
        /// </example>
        public string Folders { get; }

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
        ///     Flag that we're running in development mode. When this is set, <see cref="TokenCredential"/> and
        ///     <see cref="ConnectionString"/> will be ignored, replaced with "UseDevelopmentStorage=true" for local
        ///     connection to Azurite.
        /// </summary>
        [Obsolete(message: "The Development property is not being applied anymore. Please set ConnectionString to 'UseDevelopmentStorage=true' instead.")]
        public bool Development => false;

        /// <summary>
        ///     Automatically create the Blog Storage container if no existing Blob container is found
        /// </summary>
        public bool AutoInitialize { get; }
        
        /// <summary>
        ///     The public access type of the auto-initialized Blob Storage container
        /// </summary>
        public PublicAccessType ContainerPublicAccessType { get; }

        /// <summary>
        ///     A <see cref="Uri"/> referencing the blob service.
        ///     This is likely to be similar to "https://{account_name}.blob.core.windows.net".
        /// </summary>
        public Uri? ServiceUri { get; }

        /// <summary>
        ///     The <see cref="TokenCredential"/> used to sign API requests.
        /// </summary>
        [Obsolete(message:"Use AzureCredential instead")]
        public TokenCredential? DefaultAzureCredential => AzureCredential;

        /// <summary>
        ///     The <see cref="TokenCredential"/> used to sign API requests.
        /// </summary>
        public TokenCredential? AzureCredential { get; }
        
        /// <summary>
        ///     Optional client options that define the transport pipeline policies for authentication,
        ///     retries, etc., that are applied to every request.
        /// </summary>
        public BlobClientOptions? BlobClientOptions { get; }
        
        /// <summary>
        ///     A function that returns an Azure <see cref="BlobServiceClient"/> to be used by the snapshot store.
        ///     When set, this will override any connection string or token credential in this setup.
        /// </summary>
        public Func<BlobServiceClient>? BlobServiceClientFactory { get; }

        /// <summary>
        ///     Creates a BlobServiceClient using the configured connection settings.
        ///     Priority order: BlobServiceClientFactory > ServiceUri + AzureCredential > ConnectionString
        /// </summary>
        /// <returns>A configured BlobServiceClient instance</returns>
        /// <exception cref="ConfigurationException">Thrown when no valid connection method is configured</exception>
        public BlobServiceClient CreateBlobServiceClient()
        {
            if (BlobServiceClientFactory != null)
            {
                return BlobServiceClientFactory.Invoke();
            }

            if (ServiceUri != null && AzureCredential != null)
            {
                return new BlobServiceClient(
                    serviceUri: ServiceUri,
                    credential: AzureCredential,
                    options: BlobClientOptions);
            }

            if (!string.IsNullOrWhiteSpace(ConnectionString))
            {
                return new BlobServiceClient(connectionString: ConnectionString);
            }

            throw new ConfigurationException(
                "No connection method configured. ConnectionString, AzureCredential, or BlobServiceClient " +
                "must be specified.");
        }

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
        [Obsolete(message: "The Development property is not being applied anymore. Please set ConnectionString to 'UseDevelopmentStorage=true' instead.")]
        public AzureBlobSnapshotStoreSettings WithDevelopment(bool development) => this;
        public AzureBlobSnapshotStoreSettings WithAutoInitialize(bool autoInitialize)
            => Copy(autoInitialize: autoInitialize);
        public AzureBlobSnapshotStoreSettings WithContainerPublicAccessType(PublicAccessType containerPublicAccessType)
            => Copy(containerPublicAccessType: containerPublicAccessType);
        public AzureBlobSnapshotStoreSettings WithAzureCredential(
            Uri serviceUri,
            TokenCredential defaultAzureCredential,
            BlobClientOptions? blobClientOption = null)
            => Copy(
                serviceUri: serviceUri,
                azureCredential: defaultAzureCredential,
                blobClientOption: blobClientOption);
        public AzureBlobSnapshotStoreSettings WithBlobServiceClientFactory(Func<BlobServiceClient> blobServiceClient)
            => Copy(blobServiceClientFactory: blobServiceClient);
        
        private AzureBlobSnapshotStoreSettings Copy(
            string? connectionString = null,
            string? containerName = null,
            string? folders = null,
            TimeSpan? connectTimeout = null,
            TimeSpan? requestTimeout = null,
            bool? verboseLogging = null,
            bool? autoInitialize = null,
            PublicAccessType? containerPublicAccessType = null,
            Uri? serviceUri = null,
            TokenCredential? azureCredential = null,
            BlobClientOptions? blobClientOption = null,
            Func<BlobServiceClient>? blobServiceClientFactory = null)
            => new (
                connectionString: connectionString ?? ConnectionString,
                containerName: containerName ?? ContainerName,
                folders: folders ?? Folders,
                connectTimeout: connectTimeout ?? ConnectTimeout,
                requestTimeout: requestTimeout ?? RequestTimeout,
                verboseLogging: verboseLogging ?? VerboseLogging,
                development: false,
                autoInitialize: autoInitialize ?? AutoInitialize,
                containerPublicAccessType: containerPublicAccessType ?? ContainerPublicAccessType,
                serviceUri: serviceUri ?? ServiceUri,
                defaultAzureCredential: azureCredential ?? AzureCredential,
                blobClientOption: blobClientOption ?? BlobClientOptions,
                blobServiceClientFactory: blobServiceClientFactory ?? BlobServiceClientFactory);
        
        /// <summary>
        ///     Creates an <see cref="AzureBlobSnapshotStoreSettings" /> instance using the
        ///     `akka.persistence.snapshot-store.azure-blob-store` HOCON configuration section inside
        ///     the <see cref="ActorSystem"/> settings.
        /// </summary>
        /// <param name="system">The <see cref="ActorSystem"/> to extract the configuration from</param>
        /// <returns>A new settings instance.</returns>
        public static AzureBlobSnapshotStoreSettings Create(ActorSystem system)
        {
            var config = system.Settings.Config.GetConfig(SnapshotStoreConfigPath);
            if (config is null)
                throw new ConfigurationException($"Could not find HOCON config at path '{SnapshotStoreConfigPath}'");
            return Create(config);
        }
        
        public static string SanitizeFolder(string? folder)
            => folder?.Trim().Trim('/') ?? string.Empty;
        
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
            var folders = SanitizeFolder(config.GetString("folders"));
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
                connectionString: connectionString, 
                containerName: containerName, 
                folders: folders,
                connectTimeout: connectTimeout, 
                requestTimeout: requestTimeout,
                verboseLogging: verbose,
                development: development,
                autoInitialize: autoInitialize,
                containerPublicAccessType: containerPublicAccessType,
                serviceUri: null,
                defaultAzureCredential: null,
                blobClientOption: null, 
                blobServiceClientFactory: null);
        }
    }
}