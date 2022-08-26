// -----------------------------------------------------------------------
// <copyright file="AzureTableStorageJournalSetup.cs" company="Petabridge, LLC">
//      Copyright (C) 2015 - 2018 Petabridge, LLC <https://petabridge.com>
// </copyright>
// -----------------------------------------------------------------------

using System;
using Akka.Actor.Setup;
using Azure.Data.Tables;
using Azure.Identity;

namespace Akka.Persistence.Azure.Journal
{
    public sealed class AzureTableStorageJournalSetup : Setup
    {
        /// <summary>
        ///     The connection string for connecting to Windows Azure table storage.
        /// </summary>
        public string ConnectionString { get; set; }

        /// <summary>
        ///     The table of the table we'll be connecting to.
        /// </summary>
        public string TableName { get; set; }

        /// <summary>
        ///     Initial timeout to use when connecting to Azure Table Storage for the first time.
        /// </summary>
        public TimeSpan? ConnectTimeout { get; set; }

        /// <summary>
        ///     Timeouts for individual read, write, and delete requests to Azure Table Storage.
        /// </summary>
        public TimeSpan? RequestTimeout { get; set; }

        /// <summary>
        ///     For debugging purposes only. Logs every individual operation to Azure table storage.
        /// </summary>
        public bool? VerboseLogging { get; set; }

        /// <summary>
        ///     Flag that we're running in development mode. When this is set, <see cref="DefaultAzureCredential"/> and
        ///     <see cref="ConnectionString"/> will be ignored, replaced with "UseDevelopmentStorage=true" for local
        ///     connection to Azurite.
        /// </summary>
        public bool? Development { get; set; }
        
        /// <summary>
        ///     Automatically create the Table Storage table if no existing table is found
        /// </summary>
        public bool? AutoInitialize { get; set; }
        
        /// <summary>
        ///     A <see cref="Uri"/> referencing the Azure Storage Table service.
        ///     This is likely to be similar to "https://{account_name}.table.core.windows.net".
        /// </summary>
        public Uri ServiceUri { get; set; }

        /// <summary>
        ///     The <see cref="DefaultAzureCredential"/> used to sign API requests.
        /// </summary>
        public DefaultAzureCredential DefaultAzureCredential { get; set; }

        /// <summary>
        ///     Optional client options that define the transport pipeline policies for authentication,
        ///     retries, etc., that are applied to every request.
        /// </summary>
        public TableClientOptions TableClientOptions { get; set; }

        internal AzureTableStorageJournalSettings Apply(AzureTableStorageJournalSettings settings)
        {
            if (ConnectionString != null)
                settings = settings.WithConnectionString(ConnectionString);
            if (TableName != null)
                settings = settings.WithTableName(TableName);
            if (ConnectTimeout != null)
                settings = settings.WithConnectTimeout(ConnectTimeout.Value);
            if (RequestTimeout != null)
                settings = settings.WithRequestTimeout(RequestTimeout.Value);
            if (VerboseLogging != null)
                settings = settings.WithVerboseLogging(VerboseLogging.Value);
            if (Development != null)
                settings = settings.WithDevelopment(Development.Value);
            if (AutoInitialize != null)
                settings = settings.WithAutoInitialize(AutoInitialize.Value);
            if (ServiceUri != null && DefaultAzureCredential != null)
                settings = settings.WithAzureCredential(ServiceUri, DefaultAzureCredential, TableClientOptions);

            return settings;
        }
    }
}