// -----------------------------------------------------------------------
// <copyright file="AzureBlobSnapshotStoreConnectivityCheck.cs" company="Petabridge, LLC">
//      Copyright (C) 2015 - 2023 Petabridge, LLC <https://petabridge.com>
// </copyright>
// -----------------------------------------------------------------------

using System;
using System.Threading;
using System.Threading.Tasks;
using Akka.Hosting;
using Azure.Core;
using Azure.Storage.Blobs;
using Microsoft.Extensions.Diagnostics.HealthChecks;

#nullable enable
namespace Akka.Persistence.Azure.Hosting
{
    /// <summary>
    /// Health check that verifies connectivity to the Azure Blob Storage instance used by the snapshot store.
    /// This is a liveness check that proactively verifies backend connectivity.
    /// </summary>
    public sealed class AzureBlobSnapshotStoreConnectivityCheck : IAkkaHealthCheck
    {
        private readonly BlobContainerClient _blobContainerClient;
        private readonly string _snapshotStoreId;

        public AzureBlobSnapshotStoreConnectivityCheck(
            string? connectionString,
            Uri? serviceUri,
            TokenCredential? azureCredential,
            BlobClientOptions? blobClientOptions,
            Func<BlobServiceClient>? blobServiceClientFactory,
            string containerName,
            string snapshotStoreId)
        {
            _snapshotStoreId = snapshotStoreId ?? throw new ArgumentNullException(nameof(snapshotStoreId));

            if (string.IsNullOrWhiteSpace(containerName))
                throw new ArgumentNullException(nameof(containerName));

            // Create client once and cache it - matches the pattern used in AzureBlobSnapshotStore
            // Priority: Factory > ServiceUri + Credential > ConnectionString
            BlobServiceClient blobServiceClient;
            if (blobServiceClientFactory != null)
            {
                blobServiceClient = blobServiceClientFactory();
            }
            else if (serviceUri != null && azureCredential != null)
            {
                blobServiceClient = new BlobServiceClient(
                    serviceUri: serviceUri,
                    credential: azureCredential,
                    options: blobClientOptions);
            }
            else if (!string.IsNullOrWhiteSpace(connectionString))
            {
                blobServiceClient = new BlobServiceClient(connectionString);
            }
            else
            {
                throw new ArgumentException(
                    "At least one of ConnectionString, ServiceUri + AzureCredential, or BlobServiceClientFactory must be provided",
                    nameof(connectionString));
            }

            // Get the specific container client - this is what the snapshot store uses for all operations
            _blobContainerClient = blobServiceClient.GetBlobContainerClient(containerName);
        }

        public async Task<HealthCheckResult> CheckHealthAsync(
            AkkaHealthCheckContext context,
            CancellationToken cancellationToken = default)
        {
            try
            {
                // Use the same check as the snapshot store's InitCloudStorage() method
                // Check if the container exists - this verifies connectivity to Azure Blob Storage
                // This uses container-level permissions (same as snapshot store initialization) rather than
                // service-level properties permissions
                // Successfully executing this check proves connectivity, regardless of whether the container exists
                await _blobContainerClient.ExistsAsync(cancellationToken);

                return HealthCheckResult.Healthy($"Azure Blob snapshot store '{_snapshotStoreId}' connection successful");
            }
            catch (OperationCanceledException)
            {
                return HealthCheckResult.Unhealthy($"Azure Blob snapshot store '{_snapshotStoreId}' connectivity check timed out");
            }
            catch (Exception ex)
            {
                return HealthCheckResult.Unhealthy($"Azure Blob snapshot store '{_snapshotStoreId}' connection failed", ex);
            }
        }
    }
}
