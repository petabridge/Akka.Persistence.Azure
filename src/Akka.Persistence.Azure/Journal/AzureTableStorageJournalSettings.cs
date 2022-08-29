// -----------------------------------------------------------------------
// <copyright file="AzureTableStorageJournalSettings.cs" company="Petabridge, LLC">
//      Copyright (C) 2015 - 2022 Petabridge, LLC <https://petabridge.com>
// </copyright>
// -----------------------------------------------------------------------

using System;
using System.Linq;
using Akka.Actor;
using Akka.Configuration;
using Akka.Persistence.Azure.Util;
using Azure.Data.Tables;
using Azure.Identity;

namespace Akka.Persistence.Azure.Journal
{
    /// <summary>
    ///     Defines all of the configuration settings used by the `akka.persistence.journal.azure-table` plugin.
    /// </summary>
    public sealed class AzureTableStorageJournalSettings
    {
        private static readonly string[] ReservedTableNames = {"tables"};
        
        [Obsolete]
        public AzureTableStorageJournalSettings(
            string connectionString, 
            string tableName, 
            TimeSpan connectTimeout,
            TimeSpan requestTimeout, 
            bool verboseLogging,
            bool development, 
            bool autoInitialize)
            : this(
                connectionString: connectionString,
                tableName: tableName,
                connectTimeout: connectTimeout,
                requestTimeout: requestTimeout,
                verboseLogging: verboseLogging,
                development: development,
                autoInitialize: autoInitialize,
                serviceUri: null,
                defaultAzureCredential: null,
                tableClientOptions: null)
        { }
            
        public AzureTableStorageJournalSettings(
            string connectionString, 
            string tableName, 
            TimeSpan connectTimeout,
            TimeSpan requestTimeout, 
            bool verboseLogging,
            bool development, 
            bool autoInitialize,
            Uri serviceUri,
            DefaultAzureCredential defaultAzureCredential,
            TableClientOptions tableClientOptions)
        {
            if(string.IsNullOrWhiteSpace(tableName))
                throw new ConfigurationException("[AzureTableStorageJournal] Table name is null or empty.");

            NameValidator.ValidateTableName(tableName);
            
            if (ReservedTableNames.Contains(tableName))
            {
                throw new ArgumentException(
                    "Reserved table name. Check MSDN for more information about valid table naming", nameof(tableName));
            }
            
            ConnectionString = connectionString;
            TableName = tableName;
            ConnectTimeout = connectTimeout;
            RequestTimeout = requestTimeout;
            VerboseLogging = verboseLogging;
            Development = development;
            AutoInitialize = autoInitialize;
            ServiceUri = serviceUri;
            DefaultAzureCredential = defaultAzureCredential;
            TableClientOptions = tableClientOptions;
        }

        /// <summary>
        ///     The connection string for connecting to Windows Azure table storage.
        /// </summary>
        public string ConnectionString { get; }

        /// <summary>
        ///     The table of the table we'll be connecting to.
        /// </summary>
        public string TableName { get; }

        /// <summary>
        ///     Initial timeout to use when connecting to Azure Table Storage for the first time.
        /// </summary>
        public TimeSpan ConnectTimeout { get; }

        /// <summary>
        ///     Timeouts for individual read, write, and delete requests to Azure Table Storage.
        /// </summary>
        public TimeSpan RequestTimeout { get; }

        /// <summary>
        ///     For debugging purposes only. Logs every individual operation to Azure table storage.
        /// </summary>
        public bool VerboseLogging { get; }

        /// <summary>
        ///     Flag that we're running in development mode. When this is set, <see cref="DefaultAzureCredential"/> and
        ///     <see cref="ConnectionString"/> will be ignored, replaced with "UseDevelopmentStorage=true" for local
        ///     connection to Azurite.
        /// </summary>
        public bool Development { get; }
        
        /// <summary>
        ///     Automatically create the Table Storage table if no existing table is found
        /// </summary>
        public bool AutoInitialize { get; }

        /// <summary>
        ///     A <see cref="Uri"/> referencing the Azure Table Storage service.
        ///     This is likely to be similar to "https://{account_name}.table.core.windows.net".
        /// </summary>
        public Uri ServiceUri { get; }

        /// <summary>
        ///     The <see cref="DefaultAzureCredential"/> used to sign API requests.
        /// </summary>
        public DefaultAzureCredential DefaultAzureCredential { get; }

        /// <summary>
        ///     Optional client options that define the transport pipeline policies for authentication,
        ///     retries, etc., that are applied to every request.
        /// </summary>
        public TableClientOptions TableClientOptions { get; }

        public AzureTableStorageJournalSettings WithConnectionString(string connectionString)
            => Copy(connectionString: connectionString);
        public AzureTableStorageJournalSettings WithTableName(string tableName)
            => Copy(tableName: tableName);
        public AzureTableStorageJournalSettings WithConnectTimeout(TimeSpan connectTimeout)
            => Copy(connectTimeout: connectTimeout);
        public AzureTableStorageJournalSettings WithRequestTimeout(TimeSpan requestTimeout)
            => Copy(requestTimeout: requestTimeout);
        public AzureTableStorageJournalSettings WithVerboseLogging(bool verboseLogging)
            => Copy(verboseLogging: verboseLogging);
        public AzureTableStorageJournalSettings WithDevelopment(bool development)
            => Copy(development: development);
        public AzureTableStorageJournalSettings WithAutoInitialize(bool autoInitialize)
            => Copy(autoInitialize: autoInitialize);
        public AzureTableStorageJournalSettings WithAzureCredential(
            Uri serviceUri,
            DefaultAzureCredential defaultAzureCredential,
            TableClientOptions tableClientOptions = null)
            => Copy(
                serviceUri: serviceUri,
                defaultAzureCredential: defaultAzureCredential,
                tableClientOptions: tableClientOptions);
        
        private AzureTableStorageJournalSettings Copy(
            string connectionString = null,
            string tableName = null,
            TimeSpan? connectTimeout = null,
            TimeSpan? requestTimeout = null,
            bool? verboseLogging = null,
            bool? development = null,
            bool? autoInitialize = null,
            Uri serviceUri = null,
            DefaultAzureCredential defaultAzureCredential = null,
            TableClientOptions tableClientOptions = null)
            => new AzureTableStorageJournalSettings(
                connectionString: connectionString ?? ConnectionString,
                tableName: tableName ?? TableName,
                connectTimeout: connectTimeout ?? ConnectTimeout,
                requestTimeout: requestTimeout ?? RequestTimeout,
                verboseLogging: verboseLogging ?? VerboseLogging,
                development: development ?? Development,
                autoInitialize: autoInitialize ?? AutoInitialize,
                serviceUri: serviceUri ?? ServiceUri,
                defaultAzureCredential: defaultAzureCredential ?? DefaultAzureCredential,
                tableClientOptions: tableClientOptions ?? TableClientOptions);

        /// <summary>
        ///     Creates an <see cref="AzureTableStorageJournalSettings" /> instance using the
        ///     `akka.persistence.journal.azure-table` HOCON configuration section inside
        ///     the <see cref="ActorSystem"/> settings.
        /// </summary>
        /// <param name="system">The <see cref="ActorSystem"/> to extract the configuration from</param>
        /// <returns>A new settings instance.</returns>
        public static AzureTableStorageJournalSettings Create(ActorSystem system)
        {
            var config = system.Settings.Config.GetConfig("akka.persistence.journal.azure-table");
            if (config is null)
                throw new ConfigurationException(
                    "Could not find HOCON config at path 'akka.persistence.journal.azure-table'");
            return Create(config);
        }
        
        /// <summary>
        ///     Creates an <see cref="AzureTableStorageJournalSettings" /> instance using the
        ///     `akka.persistence.journal.azure-table` HOCON configuration section.
        /// </summary>
        /// <param name="config">The `akka.persistence.journal.azure-table` HOCON section.</param>
        /// <returns>A new settings instance.</returns>
        public static AzureTableStorageJournalSettings Create(Config config)
        {
            if (config is null)
                throw new ArgumentNullException(nameof(config));
            
            var connectionString = config.GetString("connection-string");
            var tableName = config.GetString("table-name");
            var connectTimeout = config.GetTimeSpan("connect-timeout", TimeSpan.FromSeconds(3));
            var requestTimeout = config.GetTimeSpan("request-timeout", TimeSpan.FromSeconds(3));
            var verbose = config.GetBoolean("verbose-logging", false);
            var development = config.GetBoolean("development", false);
            var autoInitialize = config.GetBoolean("auto-initialize", true);

            return new AzureTableStorageJournalSettings(
                connectionString: connectionString, 
                tableName: tableName, 
                connectTimeout: connectTimeout, 
                requestTimeout: requestTimeout,
                verboseLogging: verbose,
                development: development,
                autoInitialize: autoInitialize,
                serviceUri: null,
                defaultAzureCredential: null,
                tableClientOptions: null);
        }
    }
}