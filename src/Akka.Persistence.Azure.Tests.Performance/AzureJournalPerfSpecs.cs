// -----------------------------------------------------------------------
// <copyright file="AzureJournalPerfSpecs.cs" company="Petabridge, LLC">
//      Copyright (C) 2015 - 2018 Petabridge, LLC <https://petabridge.com>
// </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Akka.Actor;
using Akka.Configuration;
using Akka.Persistence.Azure.TestHelpers;
using Akka.Util.Internal;
using NBench;

namespace Akka.Persistence.Azure.Tests.Performance
{
    public class AzureJournalPerfSpecs
    {
        public const string RecoveryCounterName = "MsgRecovered";
        private Counter _recoveryCounter;

        public const string WriteCounterName = "MsgPersisted";
        private Counter _writeCounter;

        public static AtomicCounter TableVersionCounter = new AtomicCounter(0);
        public static string TableName { get; private set; }

        public const int PersistentActorCount = 20;
        public const int PersistedMessageCount = 1000;

        public static Config JournalConfig()
        {
            if (!string.IsNullOrEmpty(Environment.GetEnvironmentVariable("AZURE_CONNECTION_STR")))
                return JournalConfig(Environment.GetEnvironmentVariable("AZURE_CONNECTION_STR"));

            return JournalConfig(WindowsAzureStorageEmulatorFixture.GenerateConnStr());
        }

        public static Config JournalConfig(string connectionString)
        {
            TableName = "TestTable" + TableVersionCounter.IncrementAndGet();
            
            return ConfigurationFactory.ParseString(
                    @"akka.loglevel = DEBUG
                akka.log-config-on-start = on
                akka.persistence.journal.azure-table.class = ""Akka.Persistence.Azure.Journal.AzureTableStorageJournal, Akka.Persistence.Azure""
                akka.persistence.journal.plugin = ""akka.persistence.journal.azure-table""
                akka.persistence.journal.azure-table.connection-string=""" + connectionString + @"""
                akka.persistence.journal.azure-table.verbose-logging = on")
                .WithFallback("akka.persistence.journal.azure-table.table-name=" + TableName);
        }

        private ActorSystem ActorSystem { get; set; }

        private Dictionary<string, IActorRef> _persistentActors = new Dictionary<string, IActorRef>();

        [PerfSetup]
        public void Setup(BenchmarkContext context)
        {
            _recoveryCounter = context.GetCounter(RecoveryCounterName);
            _writeCounter = context.GetCounter(WriteCounterName);

            
            ActorSystem = Actor.ActorSystem.Create(nameof(AzureJournalPerfSpecs) + TableVersionCounter.Current, JournalConfig());
            Console.WriteLine(ActorSystem.Settings.Config.ToString());
            foreach (var i in Enumerable.Range(0, PersistentActorCount))
            {
                var id = "persistent" + i;
                var actorRef =
                    ActorSystem.ActorOf(
                        Props.Create(() => new PersistentJournalBenchmarkActor(id, _recoveryCounter, _writeCounter)),
                        id);

                _persistentActors[id] = actorRef;
            }
        }

        [PerfBenchmark(NumberOfIterations = 5, RunMode = RunMode.Iterations, 
            Description = "Write performance spec by 20 persistent actors", SkipWarmups = true)]
        [CounterMeasurement(RecoveryCounterName)]
        [CounterMeasurement(WriteCounterName)]
        [GcMeasurement(GcMetric.TotalCollections, GcGeneration.AllGc)]
        [MemoryMeasurement(MemoryMetric.TotalBytesAllocated)]
        public void BatchJournalWriteSpec(BenchmarkContext context)
        {
            foreach (var i in Enumerable.Range(0, PersistedMessageCount))
            {
                foreach (var actor in _persistentActors)
                {
                    actor.Value.Tell(i);
                }
            }

            using (var cts = new CancellationTokenSource(TimeSpan.FromMinutes(1)))
            {
                var tasks = new List<Task>();
                foreach (var actor in _persistentActors)
                {
                    tasks.Add(actor.Value.Ask<int>(r =>
                        new PersistentBenchmarkMsgs.NotifyWhenCounterHits(PersistedMessageCount, r), null, cts.Token));
                }

                try
                {
                    Task.WaitAll(tasks.ToArray(), cts.Token);
                }
                catch(Exception ex)
                {
                    context.Trace.Error(ex, "Failed to process results after 1 minute");
                    return;
                }
            }
            
        }

        [PerfCleanup]
        public void CleanUp()
        {
            ActorSystem.Terminate().Wait();

            try
            {
                DbUtils.CleanupCloudTable(AzurePersistence.Get(ActorSystem).TableSettings.ConnectionString, TableName).Wait(TimeSpan.FromSeconds(3));
            }
            catch { }
        }
    }
}