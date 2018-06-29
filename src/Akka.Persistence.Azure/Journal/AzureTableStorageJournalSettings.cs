using System;
using Akka.Configuration;

namespace Akka.Persistence.Azure.Journal
{
    /// <summary>
    /// Defines all of the configuration settings used by the `akka.persistence.journal.azure-table` plugin.
    /// </summary>
    public sealed class AzureTableStorageJournalSettings
    {
        public AzureTableStorageJournalSettings(string connectionString, string tableName, TimeSpan connectTimeout, TimeSpan requestTimeout, int batchSize)
        {
            ConnectionString = connectionString;
            TableName = tableName;
            ConnectTimeout = connectTimeout;
            RequestTimeout = requestTimeout;
            BatchSize = batchSize;
        }

        /// <summary>
        /// The connection string for connecting to Windows Azure table storage.
        /// </summary>
        public string ConnectionString { get; }

        /// <summary>
        /// The table of the table we'll be connecting to.
        /// </summary>
        public string TableName { get; }

        /// <summary>
        /// Initial timeout to use when connecting to Azure Table Storage for the first time.
        /// </summary>
        public TimeSpan ConnectTimeout { get; }

        /// <summary>
        /// Timeouts for individual read, write, and delete requests to Azure Table Storage.
        /// </summary>
        public TimeSpan RequestTimeout { get; }

        /// <summary>
        /// The batch size used when writing journal events to Azure table storage.
        /// </summary>
        public int BatchSize { get; }

        /// <summary>
        /// Creates an <see cref="AzureTableStorageJournalSettings"/> instance using the 
        /// `akka.persistence.journal.azure-table` HOCON configuration section.
        /// </summary>
        /// <param name="config">The `akka.persistence.journal.azure-table` HOCON section.</param>
        /// <returns>A new settings instance.</returns>
        public static AzureTableStorageJournalSettings Create(Config config)
        {
            var connectionString = config.GetString("connection-string");
            var tableName = config.GetString("table-name");
            var connectTimeout = config.GetTimeSpan("connect-timeout", TimeSpan.FromSeconds(3));
            var requestTimeout = config.GetTimeSpan("request-timeout", TimeSpan.FromSeconds(3));
            var batchSize = config.GetInt("batch-size", 100);
            return new AzureTableStorageJournalSettings(connectionString, tableName, connectTimeout, requestTimeout, batchSize);
        }
    }
}