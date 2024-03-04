// -----------------------------------------------------------------------
//   <copyright file="Issue350Spec.cs" company="Petabridge, LLC">
//     Copyright (C) 2015-2023 .NET Petabridge, LLC
//   </copyright>
// -----------------------------------------------------------------------

using System;
using System.Linq;
using System.Threading.Tasks;
using Akka.Configuration;
using Xunit.Abstractions;
using Akka.Actor;
using Akka.Cluster.Sharding;
using Akka.Event;
using Akka.TestKit;
using FluentAssertions;
using FluentAssertions.Extensions;
using Xunit;

namespace Akka.Persistence.Azure.Tests;

public class ClusterShardingSpec: Akka.TestKit.Xunit2.TestKit, IAsyncLifetime
{
    private static Config JournalConfig()
    {
        var connString = Environment.GetEnvironmentVariable("AZURE_CONNECTION_STR");
        if (string.IsNullOrWhiteSpace(connString))
            connString = DefaultConnectionString;

        return JournalConfig(connString);
    }

    private static Config JournalConfig(string connectionString)
    {
        var tableName = $"testtable{Guid.NewGuid().ToString("N")[^8..]}";

        return ConfigurationFactory.ParseString(
                $$"""
                  akka {
                      loglevel = INFO
                      actor.provider = cluster
                      persistence {
                          max-concurrent-recoveries = 25 # since Azure seems to have some trouble at 50
                          journal {
                              plugin = "akka.persistence.journal.azure-table"
                              azure-table {
                                  class = "Akka.Persistence.Azure.Journal.AzureTableStorageJournal, Akka.Persistence.Azure"
                                  connection-string="{{connectionString}}"
                                  verbose-logging = off
                              }
                          }
                          
                          query {
                              journal {
                                  azure-table {
                                      write-plugin = ""akka.persistence.journal.azure-table""
                                      refresh-interval = 1s
                                    max-buffer-size = 150
                                  }
                              }
                          }

                          snapshot-store {
                              plugin = ""akka.persistence.snapshot-store.azure-blob-store""
                              
                              azure-blob-store {
                                  connection-string="{{connectionString}}"
                                  request-timeout = 3s
                              }
                          }
                      }
                  }
                  """)
            .WithFallback($"akka.persistence.journal.azure-table.table-name = {tableName}")
            .WithFallback(AzurePersistence.DefaultConfig);
    }

    private const string DefaultConnectionString = "UseDevelopmentStorage=true";
    private static readonly string PId = $"pId-{Guid.NewGuid().ToString("N")[^8..]}";

    private TestProbe _probe;
    private IActorRef _shardRegion;

    public ClusterShardingSpec(ITestOutputHelper output) : base(JournalConfig(), nameof(ClusterShardingSpec), output)
    {
    }

    [Fact(DisplayName = "Persistent actor in cluster sharding environment should behave correctly")]
    public async Task ResetActorTest()
    {
        // basic test, shard actor should wake up and persist message
        _shardRegion.Tell(new ShardEnvelope(PId, "wake-up"));
        var persistActor = _probe.ExpectMsg<IActorRef>(20.Seconds());
        _probe.Watch(persistActor);
        
        _shardRegion.Tell(new ShardEnvelope(PId, "a"));
        await _probe.ExpectMsgAsync("-a");
        
        _shardRegion.Tell(new ShardEnvelope(PId, "b"));
        await _probe.ExpectMsgAsync("-a-b");
        
        _shardRegion.Tell(new ShardEnvelope(PId, "state"));
        await _probe.ExpectMsgAsync("-a-b");

        _shardRegion.Tell(new ShardEnvelope(PId, "snap"));
        await _probe.ExpectMsgAsync("OK");
        
        _shardRegion.Tell(new ShardEnvelope(PId, "state"));
        await _probe.ExpectMsgAsync("-a-b");
        
        _shardRegion.Tell(new ShardEnvelope(PId, "c"));
        await _probe.ExpectMsgAsync("-a-b-c");
        
        _shardRegion.Tell(new ShardEnvelope(PId, "d"));
        await _probe.ExpectMsgAsync("-a-b-c-d");
        
        // recovery test, shard actor should wake up and recover
        _shardRegion.Tell(new ShardEnvelope(PId, "die"));
        await _probe.ExpectTerminatedAsync(persistActor);

        _shardRegion.Tell(new ShardEnvelope(PId, "wake-up"));
        var newPersistActor = _probe.ExpectMsg<IActorRef>();
        newPersistActor.Should().NotBe(persistActor);
        _probe.Watch(newPersistActor);
        
        _shardRegion.Tell(new ShardEnvelope(PId, "state"));
        await _probe.ExpectMsgAsync("-a-b-c-d");
        
        // recovery test, shard actor should wake up and recover after state reset
        _shardRegion.Tell(new ShardEnvelope(PId, "reset"));
        await _probe.ExpectTerminatedAsync(newPersistActor);
        
        _shardRegion.Tell(new ShardEnvelope(PId, "wake-up"));
        var resetActor = _probe.ExpectMsg<IActorRef>();
        resetActor.Should().NotBe(persistActor);
        resetActor.Should().NotBe(newPersistActor);
        _probe.Watch(resetActor);
        
        _shardRegion.Tell(new ShardEnvelope(PId, "state"));
        await _probe.ExpectMsgAsync(string.Empty);
    }

    [Fact(DisplayName = "Persistent actor in cluster sharding environment should survive repeated resets")]
    public async Task RepeatedResetActorTest()
    {
        IActorRef oldActor = null;

        foreach (var _ in Enumerable.Range(0, 100))
        {
            _shardRegion.Tell(new ShardEnvelope(PId, "wake-up"));
            var persistActor = _probe.ExpectMsg<IActorRef>();
            if (oldActor is not null)
                persistActor.Should().NotBe(oldActor);
            _probe.Watch(persistActor);
        
            _shardRegion.Tell(new ShardEnvelope(PId, "state"));
            await _probe.ExpectMsgAsync(string.Empty);
            
            _shardRegion.Tell(new ShardEnvelope(PId, "a"));
            await _probe.ExpectMsgAsync("-a");
        
            _shardRegion.Tell(new ShardEnvelope(PId, "b"));
            await _probe.ExpectMsgAsync("-a-b");
        
            _shardRegion.Tell(new ShardEnvelope(PId, "state"));
            await _probe.ExpectMsgAsync("-a-b");

            _shardRegion.Tell(new ShardEnvelope(PId, "snap"));
            await _probe.ExpectMsgAsync("OK");
        
            _shardRegion.Tell(new ShardEnvelope(PId, "state"));
            await _probe.ExpectMsgAsync("-a-b");

            oldActor = persistActor;
            _shardRegion.Tell(new ShardEnvelope(PId, "reset"));
            await _probe.ExpectTerminatedAsync(persistActor);
        }
    }
    
    public async Task InitializeAsync()
    {
        _probe = CreateTestProbe();
        
        var sharding = ClusterSharding.Get(Sys);
        _shardRegion = await sharding.StartAsync(
            typeName: "customer",
            entityPropsFactory: id => Props.Create(() => new PersistentActor(id, _probe)),
            settings: ClusterShardingSettings.Create(Sys),
            messageExtractor: new MessageExtractor(10));

        var tcs = new TaskCompletionSource();
        
        var cluster = Cluster.Cluster.Get(Sys);
        cluster.RegisterOnMemberUp(() =>
        {
            tcs.SetResult();
        });
        cluster.Join(cluster.SelfAddress);

        await tcs.Task;
    }

    public Task DisposeAsync()
    {
        return Task.CompletedTask;
    }
    
    private sealed class ShardEnvelope
    {
        public readonly string EntityId;
        public readonly object Payload;

        public ShardEnvelope(string entityId, object payload)
        {
            EntityId = entityId;
            Payload = payload;
        }
    }
    
    private sealed class MessageExtractor : HashCodeMessageExtractor
    {
        public MessageExtractor(int maxNumberOfShards) : base(maxNumberOfShards)
        {
        }

        public override string EntityId(object message)
        {
            switch (message)
            {
                case ShardRegion.StartEntity start: return start.EntityId;
                case ShardEnvelope e: return e.EntityId;
            }

            return null;
        }

        public override object EntityMessage(object message)
        {
            switch (message)
            {
                case ShardEnvelope e: return e.Payload;
                default:
                    return message;
            }
        }
    }    
    
    private class PersistentActor: ReceivePersistentActor
    {
        private string _state = string.Empty;
        private bool _journalDeleted;
        private bool _snapshotDeleted;
        private bool _snapshotting;
        
        public PersistentActor(string persistenceId, IActorRef probe)
        {
            var log = Context.GetLogger();
            PersistenceId = persistenceId;

            Recover<string>(msg => _state = $"{_state}-{msg}");
            
            Recover<SnapshotOffer>(state => _state = (string) state.Snapshot);
            
            Command<DeleteSnapshotsSuccess>(_ =>
            {
                _snapshotDeleted = true;
                if (_journalDeleted)
                {
                    Context.Stop(Self);
                }
            });
            
            Command<DeleteMessagesSuccess>(_ =>
            {
                if (_snapshotting)
                {
                    _snapshotting = false;
                    probe.Tell("OK");
                    return;
                }
                
                _journalDeleted = true;
                if (_snapshotDeleted)
                {
                    Context.Stop(Self);
                }
            });
            
            Command<SaveSnapshotSuccess>(snap =>
            {
                DeleteMessages(snap.Metadata.SequenceNr);
            });
            
            Command<string>(msg =>
            {
                switch (msg)
                {
                    case "wake-up":
                        probe.Tell(Self);
                        break;
                    
                    case "state":
                        probe.Tell(_state);
                        break;
                    
                    case "snap":
                        if (_snapshotting)
                            break;
                        _snapshotting = true;
                        SaveSnapshot(_state);
                        break;
                    
                    case "die":
                        Context.Stop(Self);
                        break;
                    
                    case "reset":
                        DeleteMessages(long.MaxValue);
                        DeleteSnapshots(new SnapshotSelectionCriteria(long.MaxValue));
                        break;
                    
                    default:
                        Persist(msg, persisted =>
                        {
                            _state = $"{_state}-{persisted}";
                            probe.Tell(_state);
                        });
                        break;
                }
            });
        }

        public override string PersistenceId { get; }
    }
}