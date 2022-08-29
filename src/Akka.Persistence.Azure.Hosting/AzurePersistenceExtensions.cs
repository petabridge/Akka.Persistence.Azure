// -----------------------------------------------------------------------
// <copyright file="AzurePersistenceExtensions.cs" company="Petabridge, LLC">
//      Copyright (C) 2015 - 2022 Petabridge, LLC <https://petabridge.com>
// </copyright>
// -----------------------------------------------------------------------

using System;
using Akka.Actor;
using Akka.Hosting;
using Akka.Persistence.Azure.Journal;
using Akka.Persistence.Azure.Snapshot;
using Akka.Persistence.Hosting;
using Azure.Data.Tables;
using Azure.Identity;
using Azure.Storage.Blobs;

namespace Akka.Persistence.Azure.Hosting
{
    /// <summary>
    /// Extension methods for Akka.Hosting and Akka.Azure.Persistence
    /// </summary>
    public static class AzurePersistenceExtensions
    {
        public const string DefaultTableName = "AkkaPersistenceDefaultTable";
        public const string DefaultBlobContainerName = "akka-persistence-default-container";
        
        /// <summary>
        ///     Add an AzureTableStorage journal as the default Akka.Persistence
        ///     implementations for a given <see cref="ActorSystem"/>.
        /// </summary>
        /// <param name="builder">
        ///     The <see cref="AkkaConfigurationBuilder"/> builder instance being configured.
        /// </param>
        /// <param name="serviceUri">
        ///     A <see cref="Uri"/> referencing the Azure Storage Table service.
        ///     This is likely to be similar to "https://{account_name}.table.core.windows.net".
        /// </param>
        /// <param name="defaultAzureCredential">
        ///     The <see cref="DefaultAzureCredential"/> used to sign API requests.
        /// </param>
        /// <param name="tableClientOptions">
        ///     Optional client options that define the transport pipeline policies for authentication,
        ///     retries, etc., that are applied to every request.
        /// </param>
        /// <param name="autoInitialize">
        ///     Automatically create the Table Storage table and Storage blob if no existing table is found
        /// </param>
        /// <param name="tableName">
        ///     The Azure table we'll be connecting to.
        /// </param>
        /// <param name="eventAdapterConfigurator">
        ///     A delegate that can be used to configure an <see cref="AkkaPersistenceJournalBuilder"/> instance
        ///     to set up event adapters.
        /// </param>
        /// <returns>
        ///     The same <see cref="AkkaConfigurationBuilder"/> instance originally passed in.
        /// </returns>        
        public static AkkaConfigurationBuilder WithAzureTableJournal(this AkkaConfigurationBuilder builder,
            Uri serviceUri,
            DefaultAzureCredential defaultAzureCredential,
            TableClientOptions tableClientOptions = null,
            bool autoInitialize = true,
            string tableName = DefaultTableName,
            Action<AkkaPersistenceJournalBuilder> eventAdapterConfigurator = null)
        {
            if (serviceUri is null)
                throw new ArgumentNullException(nameof(serviceUri));
            
            if (defaultAzureCredential is null)
                throw new ArgumentNullException(nameof(defaultAzureCredential));
            
            var setup = new AzureTableStorageJournalSetup
            {
                ServiceUri = serviceUri,
                DefaultAzureCredential = defaultAzureCredential,
                TableClientOptions = tableClientOptions,
                AutoInitialize = autoInitialize,
                TableName = tableName
            };
            return WithAzureTableJournal(builder, setup, eventAdapterConfigurator);
        }

        /// <summary>
        ///     Add an AzureTableStorage journal as the default Akka.Persistence
        ///     implementations for a given <see cref="ActorSystem"/>.
        /// </summary>
        /// <param name="builder">
        ///     The <see cref="AkkaConfigurationBuilder"/> builder instance being configured.
        /// </param>
        /// <param name="connectionString">
        ///     The connection string for connecting to Windows Azure table storage.
        /// </param>
        /// <param name="autoInitialize">
        ///     Automatically create the Table Storage table and Storage blob if no existing table is found
        /// </param>
        /// <param name="tableName">
        ///     The Azure table we'll be connecting to.
        /// </param>
        /// <param name="eventAdapterConfigurator">
        ///     A delegate that can be used to configure an <see cref="AkkaPersistenceJournalBuilder"/> instance
        ///     to set up event adapters.
        /// </param>
        /// <returns>
        ///     The same <see cref="AkkaConfigurationBuilder"/> instance originally passed in.
        /// </returns>
        public static AkkaConfigurationBuilder WithAzureTableJournal(this AkkaConfigurationBuilder builder,
            string connectionString, 
            bool autoInitialize = true,
            string tableName = DefaultTableName,
            Action<AkkaPersistenceJournalBuilder> eventAdapterConfigurator = null)
        {
            if (string.IsNullOrWhiteSpace(connectionString))
                throw new ArgumentNullException(nameof(connectionString));
            
            var setup = new AzureTableStorageJournalSetup
            {
                ConnectionString = connectionString,
                AutoInitialize = autoInitialize,
                TableName = tableName
            };
            return WithAzureTableJournal(builder, setup, eventAdapterConfigurator);
        }

        /// <summary>
        ///     Add an AzureTableStorage journal as the default Akka.Persistence
        ///     implementations for a given <see cref="ActorSystem"/>.
        /// </summary>
        /// <param name="builder">
        ///     The <see cref="AkkaConfigurationBuilder"/> builder instance being configured.
        /// </param>
        /// <param name="configure">
        ///     A delegate that can be used to configure an <see cref="AzureTableStorageJournalSetup"/> instance
        ///     to set up the AzureTableStorage journal.
        /// </param>
        /// <param name="eventAdapterConfigurator">
        ///     A delegate that can be used to configure an <see cref="AkkaPersistenceJournalBuilder"/> instance
        ///     to set up event adapters.
        /// </param>
        /// <returns>
        ///     The same <see cref="AkkaConfigurationBuilder"/> instance originally passed in.
        /// </returns>
        public static AkkaConfigurationBuilder WithAzureTableJournal(
            this AkkaConfigurationBuilder builder,
            Action<AzureTableStorageJournalSetup> configure,
            Action<AkkaPersistenceJournalBuilder> eventAdapterConfigurator = null)
        {
            if (configure is null)
                throw new ArgumentNullException(nameof(configure));
            
            var setup = new AzureTableStorageJournalSetup();
            configure(setup);
            return WithAzureTableJournal(builder, setup, eventAdapterConfigurator);
        }

        /// <summary>
        ///     Add an AzureTableStorage journal as the default Akka.Persistence
        ///     implementations for a given <see cref="ActorSystem"/>.
        /// </summary>
        /// <param name="builder">
        ///     The <see cref="AkkaConfigurationBuilder"/> builder instance being configured.
        /// </param>
        /// <param name="setup">
        ///     An <see cref="AzureTableStorageJournalSetup"/> instance that will be used to set up
        ///     the AzureTableStorage journal.
        /// </param>
        /// <param name="eventAdapterConfigurator">
        ///     A delegate that can be used to configure an <see cref="AkkaPersistenceJournalBuilder"/> instance
        ///     to set up event adapters.
        /// </param>
        /// <returns>
        ///     The same <see cref="AkkaConfigurationBuilder"/> instance originally passed in.
        /// </returns>
        public static AkkaConfigurationBuilder WithAzureTableJournal(
            this AkkaConfigurationBuilder builder,
            AzureTableStorageJournalSetup setup,
            Action<AkkaPersistenceJournalBuilder> eventAdapterConfigurator = null)
        {
            if (setup is null)
                throw new ArgumentNullException(nameof(setup));
            
            builder.AddHocon("akka.persistence.journal.plugin = \"akka.persistence.journal.azure-table\"", HoconAddMode.Prepend);
            builder.AddSetup(setup);
            
            // PUSH DEFAULT CONFIG TO END
            builder.AddHocon(AzurePersistence.DefaultConfig, HoconAddMode.Append);
            
            if (eventAdapterConfigurator != null) // configure event adapters
            {
                builder.WithJournal("azure-table", eventAdapterConfigurator);
            }

            return builder;
        }
        
        /// <summary>
        ///     Add an AzureBlobStorage snapshot-store as the default Akka.Persistence
        ///     implementations for a given <see cref="ActorSystem"/>.
        /// </summary>
        /// <param name="builder">
        ///     The <see cref="AkkaConfigurationBuilder"/> builder instance being configured.
        /// </param>
        /// <param name="serviceUri">
        ///     A <see cref="Uri"/> referencing the Azure Storage blob service.
        ///     This is likely to be similar to "https://{account_name}.blob.core.windows.net".
        /// </param>
        /// <param name="defaultAzureCredential">
        ///     The <see cref="DefaultAzureCredential"/> used to sign API requests.
        /// </param>
        /// <param name="blobClientOptions">
        ///     Optional client options that define the transport pipeline policies for authentication,
        ///     retries, etc., that are applied to every request.
        /// </param>
        /// <param name="autoInitialize">
        ///     Automatically create the Table Storage table and Storage blob if no existing table is found
        /// </param>
        /// <param name="containerName">
        ///     The table of the container we'll be using to serialize these blobs.
        /// </param>
        /// <returns>
        ///     The same <see cref="AkkaConfigurationBuilder"/> instance originally passed in.
        /// </returns>
        public static AkkaConfigurationBuilder WithAzureBlobsSnapshotStore(
            this AkkaConfigurationBuilder builder,
            Uri serviceUri,
            DefaultAzureCredential defaultAzureCredential,
            BlobClientOptions blobClientOptions,
            bool autoInitialize = true,
            string containerName = DefaultBlobContainerName)
        {
            if (serviceUri is null)
                throw new ArgumentNullException(nameof(serviceUri));
            
            if (defaultAzureCredential is null)
                throw new ArgumentNullException(nameof(defaultAzureCredential));
            
            var setup = new AzureBlobSnapshotSetup
            {
                ServiceUri = serviceUri,
                DefaultAzureCredential = defaultAzureCredential,
                BlobClientOptions = blobClientOptions,
                AutoInitialize = autoInitialize,
                ContainerName = containerName
            };
            
            return WithAzureBlobsSnapshotStore(builder, setup);
        }

        /// <summary>
        ///     Add an AzureBlobStorage snapshot-store as the default Akka.Persistence
        ///     implementations for a given <see cref="ActorSystem"/>.
        /// </summary>
        /// <param name="builder">
        ///     The <see cref="AkkaConfigurationBuilder"/> builder instance being configured.
        /// </param>
        /// <param name="connectionString">
        ///     The connection string for connecting to Windows Azure table storage.
        /// </param>
        /// <param name="autoInitialize">
        ///     Automatically create the Table Storage table and Storage blob if no existing table is found
        /// </param>
        /// <param name="containerName">
        ///     The table of the container we'll be using to serialize these blobs.
        /// </param>
        /// <returns>
        ///     The same <see cref="AkkaConfigurationBuilder"/> instance originally passed in.
        /// </returns>
        public static AkkaConfigurationBuilder WithAzureBlobsSnapshotStore(
            this AkkaConfigurationBuilder builder,
            string connectionString, 
            bool autoInitialize = true,
            string containerName = DefaultBlobContainerName)
        {
            if (string.IsNullOrWhiteSpace(connectionString))
                throw new ArgumentNullException(nameof(connectionString));

            var setup = new AzureBlobSnapshotSetup
            {
                ConnectionString = connectionString,
                AutoInitialize = autoInitialize,
                ContainerName = containerName
            };

            return WithAzureBlobsSnapshotStore(builder, setup);
        }

        /// <summary>
        ///     Add an AzureBlobStorage snapshot-store as the default Akka.Persistence
        ///     implementations for a given <see cref="ActorSystem"/>.
        /// </summary>
        /// <param name="builder">
        ///     The <see cref="AkkaConfigurationBuilder"/> builder instance being configured.
        /// </param>
        /// <param name="configure">
        ///     A delegate that can be used to configure an <see cref="AzureBlobSnapshotSetup"/> instance
        ///     to set up the AzureBlobStorage snapshot-store.
        /// </param>
        /// <returns>
        ///     The same <see cref="AkkaConfigurationBuilder"/> instance originally passed in.
        /// </returns>
        public static AkkaConfigurationBuilder WithAzureBlobsSnapshotStore(
            this AkkaConfigurationBuilder builder,
            Action<AzureBlobSnapshotSetup> configure)
        {
            if (configure is null)
                throw new ArgumentNullException(nameof(configure));
            
            var setup = new AzureBlobSnapshotSetup();
            configure(setup);
            
            return WithAzureBlobsSnapshotStore(builder, setup);
        }
        
        /// <summary>
        ///     Add an AzureBlobStorage snapshot-store as the default Akka.Persistence
        ///     implementations for a given <see cref="ActorSystem"/>.
        /// </summary>
        /// <param name="builder">
        ///     The <see cref="AkkaConfigurationBuilder"/> builder instance being configured.
        /// </param>
        /// <param name="setup">
        ///     An <see cref="AzureBlobSnapshotSetup"/> instance that will be used to set up
        ///     the AzureBlobStorage snapshot-store.
        /// </param>
        /// <returns>
        ///     The same <see cref="AkkaConfigurationBuilder"/> instance originally passed in.
        /// </returns>
        public static AkkaConfigurationBuilder WithAzureBlobsSnapshotStore(
            this AkkaConfigurationBuilder builder,
            AzureBlobSnapshotSetup setup)
        {
            if (setup is null)
                throw new ArgumentNullException(nameof(setup));

            builder.AddHocon("akka.persistence.snapshot-store.plugin = \"akka.persistence.snapshot-store.azure-blob-store\"", HoconAddMode.Prepend);
            builder.AddSetup(setup);
            
            // PUSH DEFAULT CONFIG TO END
            builder.AddHocon(AzurePersistence.DefaultConfig, HoconAddMode.Append);

            return builder;
        }

        /// <summary>
        ///     Adds both AzureTableStorage journal and AzureBlobStorage snapshot-store as the default Akka.Persistence
        ///     implementations for a given <see cref="ActorSystem"/>.
        /// </summary>
        /// <param name="builder">
        ///     The <see cref="AkkaConfigurationBuilder"/> builder instance being configured.
        /// </param>
        /// <param name="connectionString">
        ///     The connection string for connecting to Windows Azure table storage.
        /// </param>
        /// <param name="autoInitialize">
        ///     Automatically create the Table Storage table and Storage blob if no existing table is found
        /// </param>
        /// <param name="containerName">
        ///     The table of the container we'll be using to serialize these blobs.
        /// </param>
        /// <param name="tableName">
        ///     The Azure table we'll be connecting to.
        /// </param>
        /// <param name="eventAdapterConfigurator">
        ///     A delegate that can be used to configure an <see cref="AkkaPersistenceJournalBuilder"/> instance
        ///     to set up event adapters.
        /// </param>
        /// <returns>
        ///     The same <see cref="AkkaConfigurationBuilder"/> instance originally passed in.
        /// </returns>
        public static AkkaConfigurationBuilder WithAzurePersistence(
            this AkkaConfigurationBuilder builder,
            string connectionString,
            bool autoInitialize = true,
            string containerName = DefaultBlobContainerName,
            string tableName = DefaultTableName,
            Action<AkkaPersistenceJournalBuilder> eventAdapterConfigurator = null)
        {
            builder.WithAzureTableJournal(connectionString, autoInitialize, tableName, eventAdapterConfigurator);
            builder.WithAzureBlobsSnapshotStore(connectionString, autoInitialize, containerName);

            return builder;
        }

        /// <summary>
        ///     Adds both AzureTableStorage journal and AzureBlobStorage snapshot-store as the default Akka.Persistence
        ///     implementations for a given <see cref="ActorSystem"/>.
        /// </summary>
        /// <param name="builder">
        ///     The <see cref="AkkaConfigurationBuilder"/> builder instance being configured.
        /// </param>
        /// <param name="blobStorageServiceUri">
        ///     A <see cref="Uri"/> referencing the Azure Storage blob service.
        ///     This is likely to be similar to "https://{account_name}.blob.core.windows.net".
        /// </param>
        /// <param name="tableStorageServiceUri">
        ///     A <see cref="Uri"/> referencing the Azure Storage Table service.
        ///     This is likely to be similar to "https://{account_name}.table.core.windows.net".
        /// </param>
        /// <param name="defaultAzureCredential">
        ///     The <see cref="DefaultAzureCredential"/> used to sign API requests.
        /// </param>
        /// <param name="blobClientOptions">
        ///     Optional client options that define the transport pipeline policies for authentication,
        ///     retries, etc., that are applied to every request.
        /// </param>
        /// <param name="tableClientOptions">
        ///     Optional client options that define the transport pipeline policies for authentication,
        ///     retries, etc., that are applied to every request.
        /// </param>
        /// <param name="autoInitialize">
        ///     Automatically create the Table Storage table and Storage blob if no existing table is found
        /// </param>
        /// <param name="containerName">
        ///     The table of the container we'll be using to serialize these blobs.
        /// </param>
        /// <param name="tableName">
        ///     The Azure table we'll be connecting to.
        /// </param>
        /// <param name="eventAdapterConfigurator">
        ///     A delegate that can be used to configure an <see cref="AkkaPersistenceJournalBuilder"/> instance
        ///     to set up event adapters.
        /// </param>
        /// <returns>
        ///     The same <see cref="AkkaConfigurationBuilder"/> instance originally passed in.
        /// </returns>
        public static AkkaConfigurationBuilder WithAzurePersistence(
            this AkkaConfigurationBuilder builder,
            Uri blobStorageServiceUri,
            Uri tableStorageServiceUri,
            DefaultAzureCredential defaultAzureCredential,
            BlobClientOptions blobClientOptions = null,
            TableClientOptions tableClientOptions = null,
            bool autoInitialize = true,
            string containerName = DefaultBlobContainerName,
            string tableName = DefaultTableName,
            Action<AkkaPersistenceJournalBuilder> eventAdapterConfigurator = null)
        {
            builder.WithAzureTableJournal(tableStorageServiceUri, defaultAzureCredential, tableClientOptions, autoInitialize, tableName, eventAdapterConfigurator);
            builder.WithAzureBlobsSnapshotStore(blobStorageServiceUri, defaultAzureCredential, blobClientOptions, autoInitialize, containerName);

            return builder;
        }
    }
}
