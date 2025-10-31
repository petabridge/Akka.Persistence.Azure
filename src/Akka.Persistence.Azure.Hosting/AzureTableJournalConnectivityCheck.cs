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
        private readonly TableServiceClient _tableServiceClient;
        private readonly string _journalId;

        public AzureTableJournalConnectivityCheck(
            string? connectionString,
            Uri? serviceUri,
            TokenCredential? azureCredential,
            TableClientOptions? tableClientOptions,
            Func<TableServiceClient>? tableServiceClientFactory,
            string journalId)
        {
            _journalId = journalId ?? throw new ArgumentNullException(nameof(journalId));

            // Create client once and cache it - matches the pattern used in AzureTableStorageJournal
            // Priority: Factory > ServiceUri + Credential > ConnectionString
            if (tableServiceClientFactory != null)
            {
                _tableServiceClient = tableServiceClientFactory();
            }
            else if (serviceUri != null && azureCredential != null)
            {
                _tableServiceClient = new TableServiceClient(
                    endpoint: serviceUri,
                    tokenCredential: azureCredential,
                    options: tableClientOptions);
            }
            else if (!string.IsNullOrWhiteSpace(connectionString))
            {
                _tableServiceClient = new TableServiceClient(connectionString);
            }
            else
            {
                throw new ArgumentException(
                    "At least one of ConnectionString, ServiceUri + AzureCredential, or TableServiceClientFactory must be provided",
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
                await _tableServiceClient.GetPropertiesAsync(cancellationToken);

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
