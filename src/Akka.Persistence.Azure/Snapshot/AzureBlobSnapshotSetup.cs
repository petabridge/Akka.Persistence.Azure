// -----------------------------------------------------------------------
// <copyright file="AzureBlobSnapshotSetup.cs" company="Petabridge, LLC">
//      Copyright (C) 2015 - 2022 Petabridge, LLC <https://petabridge.com>
// </copyright>
// -----------------------------------------------------------------------

using System;
using Akka.Actor.Setup;
using Azure.Core;
using Azure.Identity;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;

namespace Akka.Persistence.Azure.Snapshot
{
    /// <summary>
    ///     Setup class for <see cref="AzureBlobSnapshotStore"/>.
    ///     Any populated properties will override its respective HOCON setting.
    /// </summary>
    public sealed class AzureBlobSnapshotSetup : Setup
    {
        /// <summary>
        ///     Create a new <see cref="AzureBlobSnapshotSetup"/>
        /// </summary>
        /// <param name="serviceUri">
        ///     A <see cref="Uri"/> referencing the blob service.
        ///     This is likely to be similar to "https://{account_name}.blob.core.windows.net".
        /// </param>
        /// <param name="defaultAzureCredential">
        ///     The <see cref="TokenCredential"/> used to sign requests.
        /// </param>
        /// <param name="blobClientOptions">
        ///     Optional client options that define the transport pipeline policies for authentication,
        ///     retries, etc., that are applied to every request.
        /// </param>
        /// <returns>A new <see cref="AzureBlobSnapshotSetup"/> instance</returns>
        public static AzureBlobSnapshotSetup Create(
            Uri serviceUri, 
            TokenCredential defaultAzureCredential,
            BlobClientOptions blobClientOptions = default)
            => new AzureBlobSnapshotSetup
            {
                ServiceUri = serviceUri,
                AzureCredential = defaultAzureCredential,
                BlobClientOptions = blobClientOptions
            };

        /// <summary>
        ///     The connection string for connecting to Windows Azure blob storage account.
        /// </summary>
        public string ConnectionString { get; set; }

        /// <summary>
        ///     The table of the container we'll be using to serialize these blobs.
        /// </summary>
        public string ContainerName { get; set; }

        /// <summary>
        ///     Initial timeout to use when connecting to Azure Container Storage for the first time.
        /// </summary>
        public TimeSpan? ConnectTimeout { get; set; }

        /// <summary>
        ///     Timeouts for individual read, write, and delete requests to Azure Container Storage.
        /// </summary>
        public TimeSpan? RequestTimeout { get; set; }

        /// <summary>
        ///     For debugging purposes only. Logs every individual operation to Azure table storage.
        /// </summary>
        public bool? VerboseLogging { get; set; }

        /// <summary>
        ///     Flag that we're running in development mode. When this is set, <see cref="TokenCredential"/> and
        ///     <see cref="ConnectionString"/> will be ignored, replaced with "UseDevelopmentStorage=true" for local
        ///     connection to Azurite.
        /// </summary>
        public bool? Development { get; set; }

        /// <summary>
        ///     Automatically create the Blog Storage container if no existing Blob container is found
        /// </summary>
        public bool? AutoInitialize { get; set; }
        
        /// <summary>
        ///     The public access type of the auto-initialized Blob Storage container
        /// </summary>
        public PublicAccessType? ContainerPublicAccessType { get; set; }

        /// <summary>
        ///     A <see cref="Uri"/> referencing the blob service.
        ///     This is likely to be similar to "https://{account_name}.blob.core.windows.net".
        /// </summary>
        public Uri ServiceUri { get; set; }

        /// <summary>
        ///     The <see cref="TokenCredential"/> used to sign API requests.
        /// </summary>
        [Obsolete(message:"Use AzureCredential instead")]
        public TokenCredential DefaultAzureCredential
        {
            get => AzureCredential;
            set => AzureCredential = value;
        }

        /// <summary>
        ///     The <see cref="TokenCredential"/> used to sign API requests.
        /// </summary>
        public TokenCredential AzureCredential { get; set; }

        /// <summary>
        ///     Optional client options that define the transport pipeline policies for authentication,
        ///     retries, etc., that are applied to every request.
        /// </summary>
        public BlobClientOptions BlobClientOptions { get; set; }

        internal AzureBlobSnapshotStoreSettings Apply(AzureBlobSnapshotStoreSettings settings)
        {
            if (ConnectionString != null)
                settings = settings.WithConnectionString(ConnectionString);
            if (ContainerName != null)
                settings = settings.WithContainerName(ContainerName);
            if (ConnectTimeout != null)
                settings = settings.WithConnectTimeout(ConnectTimeout.Value);
            if (RequestTimeout != null)
                settings = settings.WithRequestTimeout(RequestTimeout.Value);
            if (VerboseLogging != null)
                settings = settings.WithVerboseLogging(VerboseLogging.Value);
            if (Development != null)
                settings = settings.WithDevelopment(Development.Value);
            if (AutoInitialize != null)
                settings = settings.WithAutoInitialize(AutoInitialize.Value);
            if (ContainerPublicAccessType != null)
                settings = settings.WithContainerPublicAccessType(ContainerPublicAccessType.Value);
            if (ServiceUri != null && AzureCredential != null)
                settings = settings.WithAzureCredential(ServiceUri, AzureCredential, BlobClientOptions);

            return settings;
        }
    }
}
