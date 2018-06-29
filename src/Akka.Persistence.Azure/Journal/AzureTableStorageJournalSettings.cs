using Akka.Configuration;

namespace Akka.Persistence.Azure.Journal
{
    /// <summary>
    /// Defines all of the configuration settings used by the `akka.persistence.journal.azure-table` plugin.
    /// </summary>
    public sealed class AzureTableStorageJournalSettings
    {
        public AzureTableStorageJournalSettings(string connectionString, string tableName, bool createTableIfNotExists)
        {
            ConnectionString = connectionString;
            TableName = tableName;
            CreateTableIfNotExists = createTableIfNotExists;
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
        /// Defaults to <c>true</c>. When <c>true</c>, we will create <see cref="TableName"/>
        /// if it doesn't already exist.
        /// </summary>
        public bool CreateTableIfNotExists { get; }

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
            var createTableIfNotExists = config.GetBoolean("create-table", true);

            return new AzureTableStorageJournalSettings(connectionString, tableName, createTableIfNotExists);
        }
    }
}