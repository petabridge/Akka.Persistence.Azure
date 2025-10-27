// -----------------------------------------------------------------------
// <copyright file="AzureTableJournalConnectivityCheck.cs" company="Petabridge, LLC">
//      Copyright (C) 2015 - 2023 Petabridge, LLC <https://petabridge.com>
// </copyright>
// -----------------------------------------------------------------------

using System;
using System.Threading;
using System.Threading.Tasks;
using Akka.Hosting;
using Azure.Core;
using Azure.Data.Tables;
using Microsoft.Extensions.Diagnostics.HealthChecks;

#nullable enable
namespace Akka.Persistence.Azure.Hosting
{
    /// <summary>
    /// Health check that verifies connectivity to the Azure Table Storage instance used by the journal.
    /// This is a liveness check that proactively verifies backend connectivity.
    /// </summary>
    public sealed class AzureTableJournalConnectivityCheck : IAkkaHealthCheck
    {
        private readonly string? _connectionString;
        private readonly Uri? _serviceUri;
        private readonly TokenCredential? _azureCredential;
        private readonly TableClientOptions? _tableClientOptions;
        private readonly Func<TableServiceClient>? _tableServiceClientFactory;
        private readonly string _journalId;

        public AzureTableJournalConnectivityCheck(
            string? connectionString,
            Uri? serviceUri,
            TokenCredential? azureCredential,
            TableClientOptions? tableClientOptions,
            Func<TableServiceClient>? tableServiceClientFactory,
            string journalId)
        {
            _connectionString = connectionString;
            _serviceUri = serviceUri;
            _azureCredential = azureCredential;
            _tableClientOptions = tableClientOptions;
            _tableServiceClientFactory = tableServiceClientFactory;
            _journalId = journalId ?? throw new ArgumentNullException(nameof(journalId));
        }

        public async Task<HealthCheckResult> CheckHealthAsync(
            AkkaHealthCheckContext context,
            CancellationToken cancellationToken = default)
        {
            try
            {
                TableServiceClient client;

                // Priority: Factory > ConnectionString > ServiceUri + Credential
                if (_tableServiceClientFactory != null)
                {
                    client = _tableServiceClientFactory();
                }
                else if (!string.IsNullOrWhiteSpace(_connectionString))
                {
                    client = new TableServiceClient(_connectionString, _tableClientOptions);
                }
                else if (_serviceUri != null && _azureCredential != null)
                {
                    client = new TableServiceClient(_serviceUri, _azureCredential, _tableClientOptions);
                }
                else
                {
                    return HealthCheckResult.Unhealthy(
                        $"Azure Table journal '{_journalId}' connectivity check failed: no valid connection configuration provided");
                }

                // Perform a lightweight connectivity check
                await client.GetPropertiesAsync(cancellationToken);

                return HealthCheckResult.Healthy($"Azure Table journal '{_journalId}' connection successful");
            }
            catch (OperationCanceledException)
            {
                return HealthCheckResult.Unhealthy($"Azure Table journal '{_journalId}' connectivity check timed out");
            }
            catch (Exception ex)
            {
                return HealthCheckResult.Unhealthy($"Azure Table journal '{_journalId}' connection failed", ex);
            }
        }
    }
}
