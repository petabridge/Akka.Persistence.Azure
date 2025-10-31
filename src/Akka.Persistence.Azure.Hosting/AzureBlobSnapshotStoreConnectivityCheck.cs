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
        private readonly BlobServiceClient _blobServiceClient;
        private readonly string _snapshotStoreId;

        public AzureBlobSnapshotStoreConnectivityCheck(
            string? connectionString,
            Uri? serviceUri,
            TokenCredential? azureCredential,
            BlobClientOptions? blobClientOptions,
            Func<BlobServiceClient>? blobServiceClientFactory,
            string snapshotStoreId)
        {
            _snapshotStoreId = snapshotStoreId ?? throw new ArgumentNullException(nameof(snapshotStoreId));

            // Create client once and cache it - matches the pattern used in AzureBlobSnapshotStore
            // Priority: Factory > ServiceUri + Credential > ConnectionString
            if (blobServiceClientFactory != null)
            {
                _blobServiceClient = blobServiceClientFactory();
            }
            else if (serviceUri != null && azureCredential != null)
            {
                _blobServiceClient = new BlobServiceClient(
                    serviceUri: serviceUri,
                    credential: azureCredential,
                    options: blobClientOptions);
            }
            else if (!string.IsNullOrWhiteSpace(connectionString))
            {
                _blobServiceClient = new BlobServiceClient(connectionString);
            }
            else
            {
                throw new ArgumentException(
                    "At least one of ConnectionString, ServiceUri + AzureCredential, or BlobServiceClientFactory must be provided",
                    nameof(connectionString));
            }
        }

        public async Task<HealthCheckResult> CheckHealthAsync(
            AkkaHealthCheckContext context,
            CancellationToken cancellationToken = default)
        {
            try
            {
                // Use the cached client instead of creating a new one on each check
                // This prevents authentication storms and rate limiting issues
                await _blobServiceClient.GetPropertiesAsync(cancellationToken);

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
