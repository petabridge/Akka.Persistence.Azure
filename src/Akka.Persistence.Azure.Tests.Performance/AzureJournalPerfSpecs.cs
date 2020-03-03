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
using Hocon;
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

        public const int PersistentActorCount = 200;
        public const int PersistedMessageCount = 20;

        public static readonly TimeSpan MaxTimeout = TimeSpan.FromMinutes(6);

        public static Config JournalConfig()
        {
            if (!string.IsNullOrEmpty(Environment.GetEnvironmentVariable("AZURE_CONNECTION_STR")))
                return JournalConfig(Environment.GetEnvironmentVariable("AZURE_CONNECTION_STR"));

            return JournalConfig(WindowsAzureStorageEmulatorFixture.GenerateConnStr());
        }

        public static Config JournalConfig(string connectionString)
        {
            TableName = "PerfTestTable" + TableVersionCounter.IncrementAndGet();

            return ConfigurationFactory.ParseString(
                    @"akka.loglevel = INFO
                akka.persistence.max-concurrent-recoveries = 25 # since Azure seems to have some trouble at 50
                akka.persistence.journal.azure-table.class = ""Akka.Persistence.Azure.Journal.AzureTableStorageJournal, Akka.Persistence.Azure""
                akka.persistence.journal.plugin = ""akka.persistence.journal.azure-table""
                akka.persistence.journal.azure-table.connection-string=""" + connectionString + @"""
                akka.persistence.journal.azure-table.verbose-logging = off")
                .WithFallback("akka.persistence.journal.azure-table.table-name=" + TableName);
        }

        private ActorSystem ActorSystem { get; set; }

        private List<IActorRef> _persistentActors = new List<IActorRef>(PersistentActorCount);

        [PerfSetup]
        public void Setup(BenchmarkContext context)
        {
            _recoveryCounter = context.GetCounter(RecoveryCounterName);
            _writeCounter = context.GetCounter(WriteCounterName);


            ActorSystem = Actor.ActorSystem.Create(nameof(AzureJournalPerfSpecs) + TableVersionCounter.Current, JournalConfig());

            foreach (var i in Enumerable.Range(0, PersistentActorCount))
            {
                var id = "persistent" + Guid.NewGuid();
                var actorRef =
                    ActorSystem.ActorOf(
                        Props.Create(() => new PersistentJournalBenchmarkActor(id)),
                        id);

                _persistentActors.Add(actorRef);
            }

            // force the system to initialize
            Task.WaitAll(_persistentActors.Select(a => a.Ask<PersistentBenchmarkMsgs.Done>(PersistentBenchmarkMsgs.Init.Instance)).Cast<Task>().ToArray());
        }

        [PerfBenchmark(NumberOfIterations = 5, RunMode = RunMode.Iterations,
            Description = "Write performance spec by 200 persistent actors", SkipWarmups = true)]
        [CounterMeasurement(RecoveryCounterName)]
        [CounterMeasurement(WriteCounterName)]
        [GcMeasurement(GcMetric.TotalCollections, GcGeneration.AllGc)]
        [MemoryMeasurement(MemoryMetric.TotalBytesAllocated)]
        [TimingMeasurement]
        public void BatchJournalWriteSpec(BenchmarkContext context)
        {
            for (int i = 0; i < PersistedMessageCount; i++)
                for (int j = 0; j < PersistentActorCount; j++)
                {
                    _persistentActors[j].Tell(new PersistentBenchmarkMsgs.Store(1));
                }

            var finished = new Task<PersistentBenchmarkMsgs.Finished>[PersistentActorCount];
            for (int i = 0; i < PersistentActorCount; i++)
            {
                var task = _persistentActors[i]
                    .Ask<PersistentBenchmarkMsgs.Finished>(PersistentBenchmarkMsgs.Finish.Instance, MaxTimeout);

                finished[i] = task;
            }

            Task.WaitAll(finished.Cast<Task>().ToArray());
            foreach (var task in finished.Where(x => x.IsCompleted))
            {
                _writeCounter.Increment(task.Result.State);
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