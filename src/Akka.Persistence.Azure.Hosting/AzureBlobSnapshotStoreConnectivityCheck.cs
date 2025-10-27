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
        private readonly string? _connectionString;
        private readonly Uri? _serviceUri;
        private readonly TokenCredential? _azureCredential;
        private readonly BlobClientOptions? _blobClientOptions;
        private readonly Func<BlobServiceClient>? _blobServiceClientFactory;
        private readonly string _snapshotStoreId;

        public AzureBlobSnapshotStoreConnectivityCheck(
            string? connectionString,
            Uri? serviceUri,
            TokenCredential? azureCredential,
            BlobClientOptions? blobClientOptions,
            Func<BlobServiceClient>? blobServiceClientFactory,
            string snapshotStoreId)
        {
            _connectionString = connectionString;
            _serviceUri = serviceUri;
            _azureCredential = azureCredential;
            _blobClientOptions = blobClientOptions;
            _blobServiceClientFactory = blobServiceClientFactory;
            _snapshotStoreId = snapshotStoreId ?? throw new ArgumentNullException(nameof(snapshotStoreId));
        }

        public async Task<HealthCheckResult> CheckHealthAsync(
            AkkaHealthCheckContext context,
            CancellationToken cancellationToken = default)
        {
            try
            {
                BlobServiceClient client;

                // Priority: Factory > ServiceUri + Credential > ConnectionString
                // This matches the priority order in AzureBlobSnapshotStore
                if (_blobServiceClientFactory != null)
                {
                    client = _blobServiceClientFactory();
                }
                else if (_serviceUri != null && _azureCredential != null)
                {
                    client = new BlobServiceClient(
                        serviceUri: _serviceUri,
                        credential: _azureCredential,
                        options: _blobClientOptions);
                }
                else if (!string.IsNullOrWhiteSpace(_connectionString))
                {
                    client = new BlobServiceClient(_connectionString);
                }
                else
                {
                    return HealthCheckResult.Unhealthy(
                        $"Azure Blob snapshot store '{_snapshotStoreId}' connectivity check failed: no valid connection configuration provided");
                }

                // Perform a lightweight connectivity check
                await client.GetPropertiesAsync(cancellationToken);

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
