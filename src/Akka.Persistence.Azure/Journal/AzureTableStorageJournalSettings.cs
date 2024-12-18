// -----------------------------------------------------------------------
// <copyright file="AzureTableStorageJournalSettings.cs" company="Petabridge, LLC">
//      Copyright (C) 2015 - 2023 Petabridge, LLC <https://petabridge.com>
// </copyright>
// -----------------------------------------------------------------------

using System;
using System.Linq;
using Akka.Actor;
using Akka.Configuration;
using Akka.Persistence.Azure.Util;
using Azure.Core;
using Azure.Data.Tables;

#nullable enable
namespace Akka.Persistence.Azure.Journal
{
    /// <summary>
    ///     Defines all the configuration settings used by the `akka.persistence.journal.azure-table` plugin.
    /// </summary>
    public sealed class AzureTableStorageJournalSettings
    {
        public const string JournalConfigPath = "akka.persistence.journal.azure-table";
        private static readonly string[] ReservedTableNames = {"tables"};
        
        // ReSharper disable IntroduceOptionalParameters.Global
        [Obsolete(message:"Use constructor with serviceUri, defaultAzureCredential, tableClientOptions, and tableServiceClientFactory argument instead.")]
        public AzureTableStorageJournalSettings(
            string? connectionString, 
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
                tableClientOptions: null, 
                tableServiceClientFactory: null)
        { }
            
        [Obsolete(message:"Use constructor with tableServiceClientFactory argument instead.")]
        public AzureTableStorageJournalSettings(
            string? connectionString,
            string tableName,
            TimeSpan connectTimeout,
            TimeSpan requestTimeout,
            bool verboseLogging,
            bool development,
            bool autoInitialize,
            Uri? serviceUri,
            TokenCredential? defaultAzureCredential,
            TableClientOptions? tableClientOptions)
            : this(
                connectionString: connectionString,
                tableName: tableName,
                connectTimeout: connectTimeout,
                requestTimeout: requestTimeout,
                verboseLogging: verboseLogging,
                development: development,
                autoInitialize: autoInitialize,
                serviceUri: serviceUri,
                defaultAzureCredential: defaultAzureCredential,
                tableClientOptions: tableClientOptions, 
                tableServiceClientFactory: null)
        { }
        // ReSharper restore IntroduceOptionalParameters.Global
        
        public AzureTableStorageJournalSettings(
            string? connectionString, 
            string tableName, 
            TimeSpan connectTimeout,
            TimeSpan requestTimeout, 
            bool verboseLogging,
            bool development, 
            bool autoInitialize,
            Uri? serviceUri,
            TokenCredential? defaultAzureCredential,
            TableClientOptions? tableClientOptions,
            Func<TableServiceClient>? tableServiceClientFactory)
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
            AutoInitialize = autoInitialize;
            ServiceUri = serviceUri;
            AzureCredential = defaultAzureCredential;
            TableClientOptions = tableClientOptions;
            TableServiceClientFactory = tableServiceClientFactory;
        }

        /// <summary>
        ///     The connection string for connecting to Windows Azure table storage.
        /// </summary>
        public string? ConnectionString { get; }

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
        ///     Flag that we're running in development mode. When this is set, <see cref="TokenCredential"/> and
        ///     <see cref="ConnectionString"/> will be ignored, replaced with "UseDevelopmentStorage=true" for local
        ///     connection to Azurite.
        /// </summary>
        [Obsolete(message: "The Development property is not being applied anymore. Please set ConnectionString to 'UseDevelopmentStorage=true' instead.")]
        public bool Development => false;
        
        /// <summary>
        ///     Automatically create the Table Storage table if no existing table is found
        /// </summary>
        public bool AutoInitialize { get; }

        /// <summary>
        ///     A <see cref="Uri"/> referencing the Azure Table Storage service.
        ///     This is likely to be similar to "https://{account_name}.table.core.windows.net".
        /// </summary>
        public Uri? ServiceUri { get; }

        /// <summary>
        ///     The <see cref="TokenCredential"/> used to sign API requests.
        /// </summary>
        [Obsolete(message: "Use AzureCredential instead")]
        public TokenCredential? DefaultAzureCredential => AzureCredential;

        /// <summary>
        ///     The <see cref="TokenCredential"/> used to sign API requests.
        /// </summary>
        public TokenCredential? AzureCredential { get; }

        /// <summary>
        ///     Optional client options that define the transport pipeline policies for authentication,
        ///     retries, etc., that are applied to every request.
        /// </summary>
        public TableClientOptions? TableClientOptions { get; }
        
        /// <summary>
        ///     A factory function that returns an instance of Azure <see cref="TableServiceClient"/> to be used by the journal.
        ///     When set, this will override any connection string or token credential in this setup.
        /// </summary>
        public Func<TableServiceClient>? TableServiceClientFactory { get; }

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
        [Obsolete(message: "The Development property is not being applied anymore. Please set ConnectionString to 'UseDevelopmentStorage=true' instead.")]
        public AzureTableStorageJournalSettings WithDevelopment(bool development) => this;
        public AzureTableStorageJournalSettings WithAutoInitialize(bool autoInitialize)
            => Copy(autoInitialize: autoInitialize);
        public AzureTableStorageJournalSettings WithAzureCredential(
            Uri serviceUri,
            TokenCredential defaultAzureCredential,
            TableClientOptions? tableClientOptions = null)
            => Copy(
                serviceUri: serviceUri,
                azureCredential: defaultAzureCredential,
                tableClientOptions: tableClientOptions);
        public AzureTableStorageJournalSettings WithTableServiceClientFactory(Func<TableServiceClient> tableServiceClientFactory)
            => Copy(tableServiceClientFactory: tableServiceClientFactory);
        
        private AzureTableStorageJournalSettings Copy(
            string? connectionString = null,
            string? tableName = null,
            TimeSpan? connectTimeout = null,
            TimeSpan? requestTimeout = null,
            bool? verboseLogging = null,
            bool? autoInitialize = null,
            Uri? serviceUri = null,
            TokenCredential? azureCredential = null,
            TableClientOptions? tableClientOptions = null,
            Func<TableServiceClient>? tableServiceClientFactory = null)
            => new AzureTableStorageJournalSettings(
                connectionString: connectionString ?? ConnectionString,
                tableName: tableName ?? TableName,
                connectTimeout: connectTimeout ?? ConnectTimeout,
                requestTimeout: requestTimeout ?? RequestTimeout,
                verboseLogging: verboseLogging ?? VerboseLogging,
                development: false,
                autoInitialize: autoInitialize ?? AutoInitialize,
                serviceUri: serviceUri ?? ServiceUri,
                defaultAzureCredential: azureCredential ?? AzureCredential,
                tableClientOptions: tableClientOptions ?? TableClientOptions,
                tableServiceClientFactory: tableServiceClientFactory);

        /// <summary>
        ///     Creates an <see cref="AzureTableStorageJournalSettings" /> instance using the
        ///     `akka.persistence.journal.azure-table` HOCON configuration section inside
        ///     the <see cref="ActorSystem"/> settings.
        /// </summary>
        /// <param name="system">The <see cref="ActorSystem"/> to extract the configuration from</param>
        /// <returns>A new settings instance.</returns>
        public static AzureTableStorageJournalSettings Create(ActorSystem system)
        {
            var config = system.Settings.Config.GetConfig(JournalConfigPath);
            if (config is null)
                throw new ConfigurationException($"Could not find HOCON config at path '{JournalConfigPath}'");
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
                tableClientOptions: null,
                tableServiceClientFactory: null);
        }
    }
}