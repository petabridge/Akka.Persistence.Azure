// -----------------------------------------------------------------------
// <copyright file="AzureConnectivityCheckExtensions.cs" company="Petabridge, LLC">
//      Copyright (C) 2015 - 2023 Petabridge, LLC <https://petabridge.com>
// </copyright>
// -----------------------------------------------------------------------

using System;
using Akka.Hosting;
using Akka.Persistence.Hosting;
using Microsoft.Extensions.Diagnostics.HealthChecks;

#nullable enable
namespace Akka.Persistence.Azure.Hosting
{
    /// <summary>
    /// Extension methods for Azure persistence connectivity checks
    /// </summary>
    public static class AzureConnectivityCheckExtensions
    {
        /// <summary>
        /// Adds a connectivity check for the Azure Table Storage journal.
        /// This is a liveness check that proactively verifies database connectivity.
        /// </summary>
        /// <param name="builder">The journal builder</param>
        /// <param name="unHealthyStatus">The status to return when check fails. Defaults to Unhealthy.</param>
        /// <param name="name">Optional name for the health check. Defaults to "Akka.Persistence.Azure.Journal.{id}.Connectivity"</param>
        /// <param name="tags">Optional tags for the health check. Defaults to ["akka", "persistence", "azure", "journal", "connectivity"]</param>
        /// <returns>The journal builder for chaining</returns>
        /// <remarks>
        /// Requires Akka.Hosting 1.5.55.1 or later. Options are accessed from builder.Options automatically.
        /// </remarks>
        public static AkkaPersistenceJournalBuilder WithConnectivityCheck(
            this AkkaPersistenceJournalBuilder builder,
            HealthStatus unHealthyStatus = HealthStatus.Unhealthy,
            string? name = null,
            string[]? tags = null)
        {
            if (!(builder.Options is AzureTableStorageJournalOptions journalOptions))
            {
                throw new InvalidOperationException(
                    "WithConnectivityCheck requires AzureTableStorageJournalOptions. " +
                    "Ensure you're using this with WithAzureTableJournal() or pass options explicitly using the overload.");
            }

            return WithConnectivityCheck(builder, journalOptions, unHealthyStatus, name, tags);
        }

        /// <summary>
        /// Adds a connectivity check for the Azure Table Storage journal.
        /// This is a liveness check that proactively verifies database connectivity.
        /// </summary>
        /// <param name="builder">The journal builder</param>
        /// <param name="journalOptions">The journal options containing connection details</param>
        /// <param name="unHealthyStatus">The status to return when check fails. Defaults to Unhealthy.</param>
        /// <param name="name">Optional name for the health check. Defaults to "Akka.Persistence.Azure.Journal.{id}.Connectivity"</param>
        /// <param name="tags">Optional tags for the health check. Defaults to ["akka", "persistence", "azure", "journal", "connectivity"]</param>
        /// <returns>The journal builder for chaining</returns>
        /// <remarks>
        /// This overload is provided for backward compatibility. Consider using the parameterless overload
        /// if you're on Akka.Hosting 1.5.55.1 or later.
        /// </remarks>
        public static AkkaPersistenceJournalBuilder WithConnectivityCheck(
            this AkkaPersistenceJournalBuilder builder,
            AzureTableStorageJournalOptions journalOptions,
            HealthStatus unHealthyStatus = HealthStatus.Unhealthy,
            string? name = null,
            string[]? tags = null)
        {
            if (journalOptions is null)
                throw new ArgumentNullException(nameof(journalOptions));

            if (string.IsNullOrWhiteSpace(journalOptions.ConnectionString)
                && journalOptions.ServiceUri is null
                && journalOptions.TableServiceClientFactory is null)
                throw new ArgumentException(
                    "At least one of ConnectionString, ServiceUri, or TableServiceClientFactory must be set on AzureTableStorageJournalOptions",
                    nameof(journalOptions));

            var registration = new AkkaHealthCheckRegistration(
                name ?? $"Akka.Persistence.Azure.Journal.{journalOptions.Identifier}.Connectivity",
                new AzureTableJournalConnectivityCheck(
                    journalOptions.ConnectionString,
                    journalOptions.ServiceUri,
                    journalOptions.AzureCredential,
                    journalOptions.TableClientOptions,
                    journalOptions.TableServiceClientFactory,
                    journalOptions.TableName ?? "AkkaPersistenceDefaultTable",
                    journalOptions.Identifier),
                unHealthyStatus,
                tags ?? new[] { "akka", "persistence", "azure", "journal", "connectivity" });

            // Use the new WithCustomHealthCheck method from Akka.Hosting 1.5.55
            return builder.WithCustomHealthCheck(registration);
        }

        /// <summary>
        /// Adds a connectivity check for the Azure Blob Storage snapshot store.
        /// This is a liveness check that proactively verifies database connectivity.
        /// </summary>
        /// <param name="builder">The snapshot builder</param>
        /// <param name="unHealthyStatus">The status to return when check fails. Defaults to Unhealthy.</param>
        /// <param name="name">Optional name for the health check. Defaults to "Akka.Persistence.Azure.SnapshotStore.{id}.Connectivity"</param>
        /// <param name="tags">Optional tags for the health check. Defaults to ["akka", "persistence", "azure", "snapshot-store", "connectivity"]</param>
        /// <returns>The snapshot builder for chaining</returns>
        /// <remarks>
        /// Requires Akka.Hosting 1.5.55.1 or later. Options are accessed from builder.Options automatically.
        /// </remarks>
        public static AkkaPersistenceSnapshotBuilder WithConnectivityCheck(
            this AkkaPersistenceSnapshotBuilder builder,
            HealthStatus unHealthyStatus = HealthStatus.Unhealthy,
            string? name = null,
            string[]? tags = null)
        {
            if (!(builder.Options is AzureBlobSnapshotOptions snapshotOptions))
            {
                throw new InvalidOperationException(
                    "WithConnectivityCheck requires AzureBlobSnapshotOptions. " +
                    "Ensure you're using this with WithAzureBlobsSnapshotStore() or pass options explicitly using the overload.");
            }

            return WithConnectivityCheck(builder, snapshotOptions, unHealthyStatus, name, tags);
        }

        /// <summary>
        /// Adds a connectivity check for the Azure Blob Storage snapshot store.
        /// This is a liveness check that proactively verifies database connectivity.
        /// </summary>
        /// <param name="builder">The snapshot builder</param>
        /// <param name="snapshotOptions">The snapshot options containing connection details</param>
        /// <param name="unHealthyStatus">The status to return when check fails. Defaults to Unhealthy.</param>
        /// <param name="name">Optional name for the health check. Defaults to "Akka.Persistence.Azure.SnapshotStore.{id}.Connectivity"</param>
        /// <param name="tags">Optional tags for the health check. Defaults to ["akka", "persistence", "azure", "snapshot-store", "connectivity"]</param>
        /// <returns>The snapshot builder for chaining</returns>
        /// <remarks>
        /// This overload is provided for backward compatibility. Consider using the parameterless overload
        /// if you're on Akka.Hosting 1.5.55.1 or later.
        /// </remarks>
        public static AkkaPersistenceSnapshotBuilder WithConnectivityCheck(
            this AkkaPersistenceSnapshotBuilder builder,
            AzureBlobSnapshotOptions snapshotOptions,
            HealthStatus unHealthyStatus = HealthStatus.Unhealthy,
            string? name = null,
            string[]? tags = null)
        {
            if (snapshotOptions is null)
                throw new ArgumentNullException(nameof(snapshotOptions));

            if (string.IsNullOrWhiteSpace(snapshotOptions.ConnectionString)
                && snapshotOptions.ServiceUri is null
                && snapshotOptions.BlobServiceClientFactory is null)
                throw new ArgumentException(
                    "At least one of ConnectionString, ServiceUri, or BlobServiceClientFactory must be set on AzureBlobSnapshotOptions",
                    nameof(snapshotOptions));

            var registration = new AkkaHealthCheckRegistration(
                name ?? $"Akka.Persistence.Azure.SnapshotStore.{snapshotOptions.Identifier}.Connectivity",
                new AzureBlobSnapshotStoreConnectivityCheck(
                    snapshotOptions.ConnectionString,
                    snapshotOptions.ServiceUri,
                    snapshotOptions.AzureCredential,
                    snapshotOptions.BlobClientOptions,
                    snapshotOptions.BlobServiceClientFactory,
                    snapshotOptions.ContainerName ?? "akka-persistence-default-container",
                    snapshotOptions.Identifier),
                unHealthyStatus,
                tags ?? new[] { "akka", "persistence", "azure", "snapshot-store", "connectivity" });

            // Use the new WithCustomHealthCheck method from Akka.Hosting 1.5.55
            return builder.WithCustomHealthCheck(registration);
        }
    }
}
