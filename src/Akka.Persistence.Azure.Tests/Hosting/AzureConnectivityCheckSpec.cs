// -----------------------------------------------------------------------
// <copyright file="AzureConnectivityCheckSpec.cs" company="Petabridge, LLC">
//      Copyright (C) 2015 - 2023 Petabridge, LLC <https://petabridge.com>
// </copyright>
// -----------------------------------------------------------------------

using System;
using System.Linq;
using System.Threading.Tasks;
using Akka.Hosting;
using Akka.Persistence.Azure.Hosting;
using Akka.Persistence.Azure.Tests.Helper;
using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Hosting;
using Xunit;
using Xunit.Abstractions;

namespace Akka.Persistence.Azure.Tests.Hosting
{
    /// <summary>
    /// Tests for Azure persistence connectivity health checks.
    /// Validates that connectivity checks can detect both healthy and unhealthy Azure connections.
    /// </summary>
    [Collection("AzureSpecs")]
    public class AzureConnectivityCheckSpec : Akka.Hosting.TestKit.TestKit
    {
        private readonly string _connectionString;

        public AzureConnectivityCheckSpec(AzuriteFixture fixture, ITestOutputHelper output)
            : base(nameof(AzureConnectivityCheckSpec), output)
        {
            _connectionString = fixture.ConnectionString;
        }

        protected override void ConfigureServices(HostBuilderContext context, IServiceCollection services)
        {
            base.ConfigureServices(context, services);
            services.AddHealthChecks();
        }

        protected override void ConfigureAkka(AkkaConfigurationBuilder builder, IServiceProvider provider)
        {
            var journalOptions = new AzureTableStorageJournalOptions(isDefault: true)
            {
                ConnectionString = _connectionString,
                TableName = "connectivitytest",
                AutoInitialize = true
            };

            var snapshotOptions = new AzureBlobSnapshotOptions(isDefault: true)
            {
                ConnectionString = _connectionString,
                ContainerName = "connectivity-test-snapshots",
                AutoInitialize = true
            };

            builder
                .WithAzureTableJournal(journalOptions, journal =>
                {
                    journal.WithConnectivityCheck(journalOptions);
                })
                .WithAzureBlobsSnapshotStore(snapshotOptions, snapshot =>
                {
                    snapshot.WithConnectivityCheck(snapshotOptions);
                });
        }

        [Fact]
        public async Task Journal_connectivity_check_should_be_registered()
        {
            // Arrange
            var healthCheckService = Host.Services.GetRequiredService<HealthCheckService>();

            // Act
            var result = await healthCheckService.CheckHealthAsync();

            // Assert
            result.Entries.Keys.Should().Contain(key => key.Contains("Journal") && key.Contains("Connectivity"));
        }

        [Fact]
        public async Task Snapshot_connectivity_check_should_be_registered()
        {
            // Arrange
            var healthCheckService = Host.Services.GetRequiredService<HealthCheckService>();

            // Act
            var result = await healthCheckService.CheckHealthAsync();

            // Assert
            result.Entries.Keys.Should().Contain(key => key.Contains("SnapshotStore") && key.Contains("Connectivity"));
        }

        [Fact]
        public async Task Journal_connectivity_check_should_report_healthy_with_valid_connection()
        {
            // Arrange
            var healthCheckService = Host.Services.GetRequiredService<HealthCheckService>();

            // Act
            var result = await healthCheckService.CheckHealthAsync();

            // Assert
            var journalCheck = result.Entries.First(e => e.Key.Contains("Journal") && e.Key.Contains("Connectivity"));
            journalCheck.Value.Status.Should().Be(HealthStatus.Healthy);
            journalCheck.Value.Description.Should().Contain("connection successful");

            Output?.WriteLine($"Journal connectivity check: {journalCheck.Key}");
            Output?.WriteLine($"Status: {journalCheck.Value.Status}");
            Output?.WriteLine($"Description: {journalCheck.Value.Description}");
        }

        [Fact]
        public async Task Snapshot_connectivity_check_should_report_healthy_with_valid_connection()
        {
            // Arrange
            var healthCheckService = Host.Services.GetRequiredService<HealthCheckService>();

            // Act
            var result = await healthCheckService.CheckHealthAsync();

            // Assert
            var snapshotCheck = result.Entries.First(e => e.Key.Contains("SnapshotStore") && e.Key.Contains("Connectivity"));
            snapshotCheck.Value.Status.Should().Be(HealthStatus.Healthy);
            snapshotCheck.Value.Description.Should().Contain("connection successful");

            Output?.WriteLine($"Snapshot connectivity check: {snapshotCheck.Key}");
            Output?.WriteLine($"Status: {snapshotCheck.Value.Status}");
            Output?.WriteLine($"Description: {snapshotCheck.Value.Description}");
        }

        [Fact]
        public async Task Both_connectivity_checks_should_be_healthy()
        {
            // Arrange
            var healthCheckService = Host.Services.GetRequiredService<HealthCheckService>();

            // Act
            var result = await healthCheckService.CheckHealthAsync();

            // Assert
            var connectivityChecks = result.Entries
                .Where(e => e.Key.Contains("Connectivity"))
                .ToList();

            connectivityChecks.Should().HaveCount(2, "both journal and snapshot connectivity checks should be registered");
            connectivityChecks.Should().OnlyContain(e => e.Value.Status == HealthStatus.Healthy);

            foreach (var check in connectivityChecks)
            {
                Output?.WriteLine($"{check.Key}: {check.Value.Status} - {check.Value.Description}");
            }
        }

        [Fact]
        public async Task Connectivity_checks_should_have_correct_tags()
        {
            // Arrange
            var healthCheckService = Host.Services.GetRequiredService<HealthCheckService>();

            // Act
            var result = await healthCheckService.CheckHealthAsync();

            // Assert
            var journalCheck = result.Entries.First(e => e.Key.Contains("Journal") && e.Key.Contains("Connectivity"));
            journalCheck.Value.Tags.Should().Contain("akka");
            journalCheck.Value.Tags.Should().Contain("persistence");
            journalCheck.Value.Tags.Should().Contain("azure");
            journalCheck.Value.Tags.Should().Contain("journal");
            journalCheck.Value.Tags.Should().Contain("connectivity");

            var snapshotCheck = result.Entries.First(e => e.Key.Contains("SnapshotStore") && e.Key.Contains("Connectivity"));
            snapshotCheck.Value.Tags.Should().Contain("akka");
            snapshotCheck.Value.Tags.Should().Contain("persistence");
            snapshotCheck.Value.Tags.Should().Contain("azure");
            snapshotCheck.Value.Tags.Should().Contain("snapshot-store");
            snapshotCheck.Value.Tags.Should().Contain("connectivity");
        }
    }

    /// <summary>
    /// Tests for connectivity checks with invalid connections.
    /// These tests verify that health checks properly detect connection failures.
    /// </summary>
    public class AzureConnectivityCheckInvalidConnectionSpec : Akka.Hosting.TestKit.TestKit
    {
        public AzureConnectivityCheckInvalidConnectionSpec(ITestOutputHelper output)
            : base(nameof(AzureConnectivityCheckInvalidConnectionSpec), output)
        {
        }

        protected override void ConfigureServices(HostBuilderContext context, IServiceCollection services)
        {
            base.ConfigureServices(context, services);
            services.AddHealthChecks();
        }

        protected override void ConfigureAkka(AkkaConfigurationBuilder builder, IServiceProvider provider)
        {
            // Use an invalid connection string that will fail connectivity checks
            var invalidConnectionString = "DefaultEndpointsProtocol=https;AccountName=invalidaccount;AccountKey=aW52YWxpZGtleQ==;EndpointSuffix=core.windows.net";

            var journalOptions = new AzureTableStorageJournalOptions(isDefault: true)
            {
                ConnectionString = invalidConnectionString,
                TableName = "connectivitytest",
                AutoInitialize = false // Don't auto-initialize with invalid connection
            };

            var snapshotOptions = new AzureBlobSnapshotOptions(isDefault: true)
            {
                ConnectionString = invalidConnectionString,
                ContainerName = "connectivity-test-snapshots",
                AutoInitialize = false // Don't auto-initialize with invalid connection
            };

            builder
                .WithAzureTableJournal(journalOptions, journal =>
                {
                    journal.WithConnectivityCheck(journalOptions, HealthStatus.Degraded);
                })
                .WithAzureBlobsSnapshotStore(snapshotOptions, snapshot =>
                {
                    snapshot.WithConnectivityCheck(snapshotOptions, HealthStatus.Degraded);
                });
        }

        [Fact]
        public async Task Journal_connectivity_check_should_report_unhealthy_with_invalid_connection()
        {
            // Arrange
            var healthCheckService = Host.Services.GetRequiredService<HealthCheckService>();

            // Act
            var result = await healthCheckService.CheckHealthAsync();

            // Assert
            var journalCheck = result.Entries.First(e => e.Key.Contains("Journal") && e.Key.Contains("Connectivity"));
            journalCheck.Value.Status.Should().Be(HealthStatus.Unhealthy, "because the connection is invalid");
            journalCheck.Value.Description.Should().Contain("connection failed");
            journalCheck.Value.Exception.Should().NotBeNull("because the connection should have failed with an exception");

            Output?.WriteLine($"Journal connectivity check: {journalCheck.Key}");
            Output?.WriteLine($"Status: {journalCheck.Value.Status}");
            Output?.WriteLine($"Description: {journalCheck.Value.Description}");
            Output?.WriteLine($"Exception: {journalCheck.Value.Exception?.Message}");
        }

        [Fact]
        public async Task Snapshot_connectivity_check_should_report_unhealthy_with_invalid_connection()
        {
            // Arrange
            var healthCheckService = Host.Services.GetRequiredService<HealthCheckService>();

            // Act
            var result = await healthCheckService.CheckHealthAsync();

            // Assert
            var snapshotCheck = result.Entries.First(e => e.Key.Contains("SnapshotStore") && e.Key.Contains("Connectivity"));
            snapshotCheck.Value.Status.Should().Be(HealthStatus.Unhealthy, "because the connection is invalid");
            snapshotCheck.Value.Description.Should().Contain("connection failed");
            snapshotCheck.Value.Exception.Should().NotBeNull("because the connection should have failed with an exception");

            Output?.WriteLine($"Snapshot connectivity check: {snapshotCheck.Key}");
            Output?.WriteLine($"Status: {snapshotCheck.Value.Status}");
            Output?.WriteLine($"Description: {snapshotCheck.Value.Description}");
            Output?.WriteLine($"Exception: {snapshotCheck.Value.Exception?.Message}");
        }

        [Fact]
        public async Task Overall_health_status_should_be_unhealthy_when_connectivity_fails()
        {
            // Arrange
            var healthCheckService = Host.Services.GetRequiredService<HealthCheckService>();

            // Act
            var result = await healthCheckService.CheckHealthAsync();

            // Assert
            result.Status.Should().Be(HealthStatus.Unhealthy,
                "because at least one health check is unhealthy");

            var connectivityChecks = result.Entries
                .Where(e => e.Key.Contains("Connectivity"))
                .ToList();

            connectivityChecks.Should().HaveCount(2);
            connectivityChecks.Should().OnlyContain(e => e.Value.Status == HealthStatus.Unhealthy);

            foreach (var check in connectivityChecks)
            {
                Output?.WriteLine($"{check.Key}: {check.Value.Status} - {check.Value.Description}");
            }
        }
    }
}
