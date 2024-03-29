﻿using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Akka.Actor;
using Akka.Event;
using Akka.Hosting;
using Akka.Persistence.Azure.Hosting;
using Akka.Persistence.Azure.Tests.Helper;
using Akka.TestKit.Xunit2.Internals;
using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Xunit;
using Xunit.Abstractions;

namespace Akka.Persistence.Azure.Tests.Hosting
{
    [Collection("AzureSpecs")]
    public class AzurePersistenceHostingSanityCheck
    {
        public static async Task<IHost> StartHost(Action<AkkaConfigurationBuilder> testSetup)
        {
            var conn = Environment.GetEnvironmentVariable("AZURE_CONNECTION_STR") ?? "UseDevelopmentStorage=true";
            await DbUtils.CleanupCloudTable(conn);
            var host = new HostBuilder()
                .ConfigureServices(collection =>
                {
                    collection.AddAkka("MyActorSys", builder =>
                    {
                        builder.WithAzurePersistence(conn);
                        testSetup(builder);
                    });
                }).Build();

            await host.StartAsync();
            return host;
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

        private readonly ITestOutputHelper _output;

        public AzurePersistenceHostingSanityCheck(ITestOutputHelper output)
        {
            _output = output;
        }

        [Fact]
        public async Task ShouldLaunchAzurePersistence()
        {
           // arrange
            using var host = await StartHost(builder => {
            
                    builder.StartActors((system, registry) =>
                    {
                        var myActor = system.ActorOf(Props.Create(() => new MyPersistenceActor("ac1")), "actor1");
                        registry.Register<MyPersistenceActor>(myActor);
                    })
                    .WithActors((system, registry) =>
                    {
                        var extSystem = (ExtendedActorSystem)system;
                        var logger = extSystem.SystemActorOf(Props.Create(() => new TestOutputLogger(_output)), "log-test");
                        logger.Tell(new InitializeLogger(system.EventStream));
                    });
            });

            var actorSystem = host.Services.GetRequiredService<ActorSystem>();
            var actorRegistry = host.Services.GetRequiredService<ActorRegistry>();
            var myPersistentActor = actorRegistry.Get<MyPersistenceActor>();
            
            // act
            var resp1 = await myPersistentActor.Ask<string>(1, TimeSpan.FromSeconds(3));
            var resp2 = await myPersistentActor.Ask<string>(2, TimeSpan.FromSeconds(3));
            var snapshot = await myPersistentActor.Ask<int[]>("getall", TimeSpan.FromSeconds(3));

            // assert
            snapshot.Should().BeEquivalentTo(new[] {1, 2});

            // kill + recreate actor with same PersistentId
            await myPersistentActor.GracefulStop(TimeSpan.FromSeconds(3));
            var myPersistentActor2 = actorSystem.ActorOf(Props.Create(() => new MyPersistenceActor("ac1")), "actor1a");
            
            var snapshot2 = await myPersistentActor2.Ask<int[]>("getall", TimeSpan.FromSeconds(3));
            snapshot2.Should().BeEquivalentTo(new[] {1, 2});
            
            // validate configs
            var config = actorSystem.Settings.Config;
            config.GetString("akka.persistence.journal.plugin").Should().Be("akka.persistence.journal.azure-table");
            config.GetString("akka.persistence.snapshot-store.plugin").Should().Be("akka.persistence.snapshot-store.azure-blob-store");
        }
    }
}