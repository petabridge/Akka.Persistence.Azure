// -----------------------------------------------------------------------
// <copyright file="AzureTableStorageJournalSettings.cs" company="Petabridge, LLC">
//      Copyright (C) 2015 - 2018 Petabridge, LLC <https://petabridge.com>
// </copyright>
// -----------------------------------------------------------------------

using System;
using System.Linq;
using Akka.Configuration;
using Microsoft.WindowsAzure.Storage;

namespace Akka.Persistence.Azure.Journal
{
    /// <summary>
    ///     Defines all of the configuration settings used by the `akka.persistence.journal.azure-table` plugin.
    /// </summary>
    public sealed class AzureTableStorageJournalSettings
    {
        private static readonly string[] ReservedTableNames = {"tables"};
        
        public AzureTableStorageJournalSettings(
            string connectionString, 
            string tableName, 
            TimeSpan connectTimeout,
            TimeSpan requestTimeout, 
            bool verboseLogging,
            bool development)
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

        public bool Development { get; }

        /// <summary>
        ///     Creates an <see cref="AzureTableStorageJournalSettings" /> instance using the
        ///     `akka.persistence.journal.azure-table` HOCON configuration section.
        /// </summary>
        /// <param name="config">The `akka.persistence.journal.azure-table` HOCON section.</param>
        /// <returns>A new settings instance.</returns>
        public static AzureTableStorageJournalSettings Create(Config config)
        {
            var connectionString = config.GetString("connection-string");
            var tableName = config.GetString("table-name");
            var connectTimeout = config.GetTimeSpan("connect-timeout", TimeSpan.FromSeconds(3));
            var requestTimeout = config.GetTimeSpan("request-timeout", TimeSpan.FromSeconds(3));
            var verbose = config.GetBoolean("verbose-logging", false);
            var development = config.GetBoolean("development", false);

            return new AzureTableStorageJournalSettings(
                connectionString, 
                tableName, 
                connectTimeout, 
                requestTimeout,
                verbose,
                development);
        }
    }
}