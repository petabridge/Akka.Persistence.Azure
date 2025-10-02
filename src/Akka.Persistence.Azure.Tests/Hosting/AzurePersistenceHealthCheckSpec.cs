using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Akka.Actor;
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
    [Collection("AzureSpecs")]
    public class AzurePersistenceHealthCheckSpec
    {
        private readonly ITestOutputHelper _output;

        public AzurePersistenceHealthCheckSpec(ITestOutputHelper output)
        {
            _output = output;
        }

        [Fact]
        public async Task Journal_health_check_should_be_registered()
        {
            // Arrange
            var conn = Environment.GetEnvironmentVariable("AZURE_CONNECTION_STR") ?? "UseDevelopmentStorage=true";
            await DbUtils.CleanupCloudTable(conn);

            var host = new HostBuilder()
                .ConfigureServices(collection =>
                {
                    collection.AddHealthChecks();
                    collection.AddAkka("MyActorSys", builder =>
                    {
                        builder.WithAzureTableJournal(
                            connectionString: conn,
                            journalBuilder: journal => journal.WithHealthCheck());
                    });
                })
                .Build();

            await host.StartAsync();

            try
            {
                // Act
                var healthCheckService = host.Services.GetRequiredService<HealthCheckService>();
                var result = await healthCheckService.CheckHealthAsync();

                // Assert
                result.Status.Should().Be(HealthStatus.Healthy);
                result.Entries.Keys.Should().Contain("akka.persistence.journal.azure-table");
            }
            finally
            {
                await host.StopAsync();
                host.Dispose();
            }
        }

        [Fact]
        public async Task Snapshot_health_check_should_be_registered()
        {
            // Arrange
            var conn = Environment.GetEnvironmentVariable("AZURE_CONNECTION_STR") ?? "UseDevelopmentStorage=true";
            await DbUtils.CleanupCloudTable(conn);

            var host = new HostBuilder()
                .ConfigureServices(collection =>
                {
                    collection.AddHealthChecks();
                    collection.AddAkka("MyActorSys", builder =>
                    {
                        builder.WithAzurePersistence(
                            connectionString: conn,
                            snapshotBuilder: snapshot => snapshot.WithHealthCheck());
                    });
                })
                .Build();

            await host.StartAsync();

            try
            {
                // Act
                var healthCheckService = host.Services.GetRequiredService<HealthCheckService>();
                var result = await healthCheckService.CheckHealthAsync();

                // Assert
                result.Status.Should().Be(HealthStatus.Healthy);
                result.Entries.Keys.Should().Contain("akka.persistence.snapshot-store.azure-blob-store");
            }
            finally
            {
                await host.StopAsync();
                host.Dispose();
            }
        }

        [Fact]
        public async Task Both_journal_and_snapshot_health_checks_should_be_registered()
        {
            // Arrange
            var conn = Environment.GetEnvironmentVariable("AZURE_CONNECTION_STR") ?? "UseDevelopmentStorage=true";
            await DbUtils.CleanupCloudTable(conn);

            var host = new HostBuilder()
                .ConfigureServices(collection =>
                {
                    collection.AddHealthChecks();
                    collection.AddAkka("MyActorSys", builder =>
                    {
                        builder.WithAzurePersistence(
                            connectionString: conn,
                            journalBuilder: journal => journal.WithHealthCheck(),
                            snapshotBuilder: snapshot => snapshot.WithHealthCheck());
                    });
                })
                .Build();

            await host.StartAsync();

            try
            {
                // Act
                var healthCheckService = host.Services.GetRequiredService<HealthCheckService>();
                var result = await healthCheckService.CheckHealthAsync();

                // Assert
                result.Status.Should().Be(HealthStatus.Healthy);
                result.Entries.Keys.Should().Contain("akka.persistence.journal.azure-table");
                result.Entries.Keys.Should().Contain("akka.persistence.snapshot-store.azure-blob-store");
            }
            finally
            {
                await host.StopAsync();
                host.Dispose();
            }
        }

        [Fact]
        public async Task Health_check_with_custom_degraded_status_should_work()
        {
            // Arrange
            var conn = Environment.GetEnvironmentVariable("AZURE_CONNECTION_STR") ?? "UseDevelopmentStorage=true";
            await DbUtils.CleanupCloudTable(conn);

            var host = new HostBuilder()
                .ConfigureServices(collection =>
                {
                    collection.AddHealthChecks();
                    collection.AddAkka("MyActorSys", builder =>
                    {
                        builder.WithAzurePersistence(
                            connectionString: conn,
                            journalBuilder: journal => journal.WithHealthCheck(HealthStatus.Degraded),
                            snapshotBuilder: snapshot => snapshot.WithHealthCheck(HealthStatus.Degraded));
                    });
                })
                .Build();

            await host.StartAsync();

            try
            {
                // Act
                var healthCheckService = host.Services.GetRequiredService<HealthCheckService>();
                var result = await healthCheckService.CheckHealthAsync();

                // Assert
                // Health checks should be registered and report as healthy (degraded status is for failures)
                result.Status.Should().Be(HealthStatus.Healthy);
                result.Entries.Keys.Should().Contain("akka.persistence.journal.azure-table");
                result.Entries.Keys.Should().Contain("akka.persistence.snapshot-store.azure-blob-store");
            }
            finally
            {
                await host.StopAsync();
                host.Dispose();
            }
        }

        [Fact]
        public async Task Health_checks_should_pass_after_persistence_operations()
        {
            // Arrange
            var conn = Environment.GetEnvironmentVariable("AZURE_CONNECTION_STR") ?? "UseDevelopmentStorage=true";
            await DbUtils.CleanupCloudTable(conn);

            var host = new HostBuilder()
                .ConfigureServices(collection =>
                {
                    collection.AddHealthChecks();
                    collection.AddAkka("MyActorSys", builder =>
                    {
                        builder.WithAzurePersistence(
                            connectionString: conn,
                            journalBuilder: journal => journal.WithHealthCheck(),
                            snapshotBuilder: snapshot => snapshot.WithHealthCheck());

                        builder.StartActors((system, registry) =>
                        {
                            var myActor = system.ActorOf(Props.Create(() => new MyPersistenceActor("health-check-test")), "test-actor");
                            registry.Register<MyPersistenceActor>(myActor);
                        });
                    });
                })
                .Build();

            await host.StartAsync();

            try
            {
                var actorRegistry = host.Services.GetRequiredService<ActorRegistry>();
                var myPersistentActor = actorRegistry.Get<MyPersistenceActor>();

                // Act - perform persistence operations
                var resp1 = await myPersistentActor.Ask<string>(1, TimeSpan.FromSeconds(5));
                var resp2 = await myPersistentActor.Ask<string>(2, TimeSpan.FromSeconds(5));
                resp1.Should().Be("ACK");
                resp2.Should().Be("ACK");

                // Now check health
                var healthCheckService = host.Services.GetRequiredService<HealthCheckService>();
                var result = await healthCheckService.CheckHealthAsync();

                // Assert
                result.Status.Should().Be(HealthStatus.Healthy);
                result.Entries.Keys.Should().Contain("akka.persistence.journal.azure-table");
                result.Entries.Keys.Should().Contain("akka.persistence.snapshot-store.azure-blob-store");

                // Verify individual entries are healthy
                result.Entries["akka.persistence.journal.azure-table"].Status.Should().Be(HealthStatus.Healthy);
                result.Entries["akka.persistence.snapshot-store.azure-blob-store"].Status.Should().Be(HealthStatus.Healthy);

                // Verify data and descriptions exist
                result.Entries["akka.persistence.journal.azure-table"].Data.Should().NotBeNull();
                result.Entries["akka.persistence.snapshot-store.azure-blob-store"].Data.Should().NotBeNull();
            }
            finally
            {
                await host.StopAsync();
                host.Dispose();
            }
        }

        public sealed class MyPersistenceActor : ReceivePersistentActor
        {
            private List<int> _values = new List<int>();

            public MyPersistenceActor(string persistenceId)
            {
                PersistenceId = persistenceId;

                Recover<SnapshotOffer>(offer =>
                {
                    if (offer.Snapshot is IEnumerable<int> ints)
                    {
                        _values = new List<int>(ints);
                    }
                });

                Recover<int>(i => { _values.Add(i); });

                Command<int>(i =>
                {
                    Persist(i, i1 =>
                    {
                        _values.Add(i);
                        if (LastSequenceNr % 2 == 0)
                        {
                            SaveSnapshot(_values);
                        }

                        Sender.Tell("ACK");
                    });
                });

                Command<string>(str => str.Equals("getall"), s => { Sender.Tell(_values.ToArray()); });

                Command<SaveSnapshotSuccess>(s => { });
            }

            public override string PersistenceId { get; }
        }
    }
}
