// -----------------------------------------------------------------------
// <copyright file="AzureTableStorageJournalSetup.cs" company="Petabridge, LLC">
//      Copyright (C) 2015 - 2022 Petabridge, LLC <https://petabridge.com>
// </copyright>
// -----------------------------------------------------------------------

using System;
using System.Linq;
using System.Text;
using Akka.Configuration;
using Akka.Hosting;
using Akka.Persistence.Azure.Journal;
using Akka.Persistence.Hosting;
using Azure.Core;
using Azure.Data.Tables;

namespace Akka.Persistence.Azure.Hosting
{
    public sealed class AzureTableStorageJournalOptions : JournalOptions
    {
        private static readonly Config Default = AzurePersistence.DefaultConfig
            .GetConfig(AzureTableStorageJournalSettings.JournalConfigPath);
        
        public AzureTableStorageJournalOptions() : this(true)
        {
        }
        
        public AzureTableStorageJournalOptions(bool isDefault, string identifier = "azure-table") : base(isDefault)
        {
            Identifier = identifier;
        }
        
        /// <summary>
        ///     The connection string for connecting to Windows Azure table storage.
        /// </summary>
        public string? ConnectionString { get; set; }

        /// <summary>
        ///     The table of the table we'll be connecting to.
        /// </summary>
        public string? TableName { get; set; }

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
        ///     Flag that we're running in development mode. When this is set, <see cref="TokenCredential"/> and
        ///     <see cref="ConnectionString"/> will be ignored, replaced with "UseDevelopmentStorage=true" for local
        ///     connection to Azurite.
        /// </summary>
        public bool? Development { get; set; }

        public override string Identifier { get; set; }

        protected override Config InternalDefaultConfig => Default;

        /// <summary>
        ///     A <see cref="Uri"/> referencing the Azure Storage Table service.
        ///     This is likely to be similar to "https://{account_name}.table.core.windows.net".
        /// </summary>
        public Uri? ServiceUri { get; set; }

        /// <summary>
        ///     The <see cref="TokenCredential"/> used to sign API requests.
        /// </summary>
        public TokenCredential? AzureCredential { get; set; }

        /// <summary>
        ///     Optional client options that define the transport pipeline policies for authentication,
        ///     retries, etc., that are applied to every request.
        /// </summary>
        public TableClientOptions? TableClientOptions { get; set; }

        protected override StringBuilder Build(StringBuilder sb)
        {
            if(ConnectionString is { })
                sb.AppendLine($"connection-string = {ConnectionString.ToHocon()}");
            
            if(TableName is { })
                sb.AppendLine($"table-name = {TableName.ToHocon()}");
            
            if(ConnectTimeout is { })
                sb.AppendLine($"connect-timeout = {ConnectTimeout.ToHocon()}");
            
            if(RequestTimeout is { })
                sb.AppendLine($"request-timeout = {RequestTimeout.ToHocon()}");
            
            if(VerboseLogging is { })
                sb.AppendLine($"verbose-logging = {VerboseLogging.ToHocon()}");
            
            if(Development is { })
                sb.AppendLine($"development = {Development.ToHocon()}");
            
            return base.Build(sb);
        }

        internal void Apply(AkkaConfigurationBuilder builder)
        {
            if (ServiceUri is null && AzureCredential is null) 
                return;
            
            if (AzureCredential is null || ServiceUri is null)
                throw new ConfigurationException($"Both {nameof(ServiceUri)} and {nameof(AzureCredential)} need to be declared to use {nameof(AzureCredential)}");

            var setup = builder.Setups.FirstOrDefault(s => s is AzureTableStorageJournalSetup) as AzureTableStorageJournalSetup;
            setup ??= new AzureTableStorageJournalSetup();
            Apply(setup);
            
            builder.AddSetup(setup);
        }

        internal void Apply(AzureTableStorageJournalSetup setup)
        {
            setup.ServiceUri = ServiceUri;
            setup.AzureCredential = AzureCredential;
            setup.TableClientOptions = TableClientOptions;
        }
    }
}