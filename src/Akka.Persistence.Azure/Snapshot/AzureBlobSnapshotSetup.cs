using System;
using System.Collections.Generic;
using System.Text;
using Akka.Actor.Setup;
using Azure.Identity;
using Azure.Storage.Blobs;

namespace Akka.Persistence.Azure.Snapshot
{
    public class AzureBlobSnapshotSetup : Setup
    {
        /// <summary>
        /// Create a new <see cref="AzureBlobSnapshotSetup"/>
        /// </summary>
        /// <param name="serviceUri">
        /// A <see cref="Uri"/> referencing the blob service.
        /// This is likely to be similar to "https://{account_name}.blob.core.windows.net".
        /// </param>
        /// <param name="defaultAzureCredential">
        /// The <see cref="DefaultAzureCredential"/> used to sign requests.
        /// </param>
        /// <param name="blobClientOptions">
        /// Optional client options that define the transport pipeline policies for authentication,
        /// retries, etc., that are applied to every request.
        /// </param>
        /// <returns>A new <see cref="AzureBlobSnapshotSetup"/> instance</returns>
        public static AzureBlobSnapshotSetup Create(
            Uri serviceUri, 
            DefaultAzureCredential defaultAzureCredential,
            BlobClientOptions blobClientOptions = default)
            => new AzureBlobSnapshotSetup(serviceUri, defaultAzureCredential, blobClientOptions);

        private AzureBlobSnapshotSetup(
            Uri serviceUri, 
            DefaultAzureCredential azureCredential,
            BlobClientOptions blobClientOptions)
        {
            ServiceUri = serviceUri;
            DefaultAzureCredential = azureCredential;
            BlobClientOptions = blobClientOptions;
        }

        public Uri ServiceUri { get; }

        public DefaultAzureCredential DefaultAzureCredential { get; }

        public BlobClientOptions BlobClientOptions { get; }
    }
}
