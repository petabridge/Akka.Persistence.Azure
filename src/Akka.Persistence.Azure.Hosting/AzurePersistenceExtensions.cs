// -----------------------------------------------------------------------
// <copyright file="AzurePersistenceExtensions.cs" company="Petabridge, LLC">
//      Copyright (C) 2015 - 2022 Petabridge, LLC <https://petabridge.com>
// </copyright>
// -----------------------------------------------------------------------

using System;
using Akka.Actor;
using Akka.Configuration;
using Akka.Hosting;
using Akka.Persistence.Azure.Query;
using Akka.Persistence.Hosting;

namespace Akka.Persistence.Azure.Hosting
{
    /// <summary>
    /// Extension methods for Akka.Hosting and Akka.Azure.Persistence
    /// </summary>
    public static class AzurePersistenceExtensions
    {
        public const string DefaultTableName = "AkkaPersistenceDefaultTable";
        public const string DefaultBlobContainerName = "akka-persistence-default-container";
        
        private static string ToHocon(bool b)
        {
            return b ? "on" : "off";
        }


        public static AkkaConfigurationBuilder WithAzureTableJournal(this AkkaConfigurationBuilder builder,
            string connectionString, bool autoInitialize = true, string tableName = DefaultTableName, Action<AkkaPersistenceJournalBuilder> configurator = null)
        {
            Config journalConfiguration = @$"
            akka.persistence {{
                journal {{
                    plugin = ""akka.persistence.journal.azure-table""
                    azure-table {{
                        class = ""Akka.Persistence.Azure.Journal.AzureTableStorageJournal, Akka.Persistence.Azure""
                        connection-string = ""{connectionString}""
                        # the name of the Windows Azure Table used to persist journal events
                        table-name =  ""{tableName}""
                        auto-initialize = {ToHocon(autoInitialize)}
                    }}
                }}
            }}";

            var finalConfig = journalConfiguration;
            builder.AddHocon(finalConfig, HoconAddMode.Prepend);
            
            // PUSH DEFAULT CONFIG TO END
            builder.AddHocon(AzurePersistence.DefaultConfig, HoconAddMode.Append);
            
            if (configurator != null) // configure event adapters
            {
                builder.WithJournal("azure-table", configurator);
            }

            return builder;
        }
        
        public static AkkaConfigurationBuilder WithAzureBlobsSnapshotStore(this AkkaConfigurationBuilder builder,
            string connectionString, bool autoInitialize = true, string containerName = DefaultBlobContainerName)
        {
            Config journalConfiguration = @$"
            akka.persistence {{
                snapshot-store {{
                    plugin = ""akka.persistence.snapshot-store.azure-blob-store""
                    azure-blob-store {{
                        class = ""Akka.Persistence.Azure.Snapshot.AzureBlobSnapshotStore, Akka.Persistence.Azure""
                        connection-string = ""{connectionString}""
                        # the name of the Windows Azure Table used to persist journal events
                        container-name =  ""{containerName}""
                        auto-initialize = {ToHocon(autoInitialize)}
                    }}
                }}
            }}";

            var finalConfig = journalConfiguration;
            builder.AddHocon(finalConfig, HoconAddMode.Prepend);
            
            // PUSH DEFAULT CONFIG TO END
            builder.AddHocon(AzurePersistence.DefaultConfig, HoconAddMode.Append);

            return builder;
        }

        /// <summary>
        /// Adds both AzureTableStorage journal and AzureBlobStorage snapshot-store as the default Akka.Persistence
        /// implementations for a given <see cref="ActorSystem"/>.
        /// </summary>
        /// <param name="builder"></param>
        /// <param name="connectionString"></param>
        /// <param name="autoInitialize"></param>
        /// <param name="containerName"></param>
        /// <param name="tableName"></param>
        /// <param name="configurator"></param>
        /// <returns></returns>
        public static AkkaConfigurationBuilder WithAzurePersistence(this AkkaConfigurationBuilder builder,
            string connectionString, bool autoInitialize = true, string containerName = DefaultBlobContainerName,
            string tableName = DefaultTableName, Action<AkkaPersistenceJournalBuilder> configurator = null)
        {
            builder.WithAzureTableJournal(connectionString, autoInitialize, tableName, configurator);
            builder.WithAzureBlobsSnapshotStore(connectionString, autoInitialize, containerName);

            return builder;
        }
    }
}
