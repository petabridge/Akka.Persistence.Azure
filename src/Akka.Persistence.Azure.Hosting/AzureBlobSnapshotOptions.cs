// -----------------------------------------------------------------------
// <copyright file="AzureBlobSnapshotSetup.cs" company="Petabridge, LLC">
//      Copyright (C) 2015 - 2023 Petabridge, LLC <https://petabridge.com>
// </copyright>
// -----------------------------------------------------------------------

using System;
using System.Linq;
using System.Text;
using Akka.Configuration;
using Akka.Hosting;
using Akka.Persistence.Azure.Snapshot;
using Akka.Persistence.Hosting;
using Azure.Core;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;

namespace Akka.Persistence.Azure.Hosting
{
    /// <summary>
    ///     Setup class for <see cref="AzureBlobSnapshotStore"/>.
    ///     Any populated properties will override its respective HOCON setting.
    /// </summary>
    public sealed class AzureBlobSnapshotOptions : SnapshotOptions
    {
        private static readonly Config Default = AzurePersistence.DefaultConfig
            .GetConfig(AzureBlobSnapshotStoreSettings.SnapshotStoreConfigPath);
        
        public AzureBlobSnapshotOptions() : this(true)
        {
        }
        
        public AzureBlobSnapshotOptions(bool isDefault, string identifier = "azure-blob-store") : base(isDefault)
        {
            Identifier = identifier;
        }

        /// <summary>
        ///     The connection string for connecting to Windows Azure blob storage account.
        /// </summary>
        public string? ConnectionString { get; set; }

        /// <summary>
        ///     The table of the container we'll be using to serialize these blobs.
        /// </summary>
        public string? ContainerName { get; set; }

        /// <summary>
        ///     Initial timeout to use when connecting to Azure Container Storage for the first time.
        /// </summary>
        public TimeSpan? ConnectTimeout { get; set; }

        /// <summary>
        ///     Timeouts for individual read, write, and delete requests to Azure Container Storage.
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
        ///     The public access type of the auto-initialized Blob Storage container
        /// </summary>
        public PublicAccessType? ContainerPublicAccessType { get; set; }

        /// <summary>
        ///     A <see cref="Uri"/> referencing the blob service.
        ///     This is likely to be similar to "https://{account_name}.blob.core.windows.net".
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
        public BlobClientOptions? BlobClientOptions { get; set; }

        protected override StringBuilder Build(StringBuilder sb)
        {
            if (ConnectionString is { })
                sb.AppendLine($"connection-string = {ConnectionString.ToHocon()}");

            if (ContainerName is { })
                sb.AppendLine($"container-name = {ContainerName.ToHocon()}");

            if (ConnectTimeout is { })
                sb.AppendLine($"connect-timeout = {ConnectTimeout.ToHocon()}");

            if (RequestTimeout is { })
                sb.AppendLine($"request-timeout = {RequestTimeout.ToHocon()}");

            if (VerboseLogging is { })
                sb.AppendLine($"verbose-logging = {VerboseLogging.ToHocon()}");

            if (Development is { })
                sb.AppendLine($"development = {Development.ToHocon()}");

            if (ContainerPublicAccessType is { })
                sb.AppendLine($"container-public-access-type = {ContainerPublicAccessType.ToString().ToHocon()}");
            
            return base.Build(sb);
        }

        internal void Apply(AkkaConfigurationBuilder builder)
        {
            if (ServiceUri is null && AzureCredential is null) 
                return;
            
            if (AzureCredential is null || ServiceUri is null)
                throw new ConfigurationException($"Both {nameof(ServiceUri)} and {nameof(AzureCredential)} need to be declared to use {nameof(AzureCredential)}");

            var setup = builder.Setups.FirstOrDefault(s => s is AzureBlobSnapshotSetup) as AzureBlobSnapshotSetup;
            setup ??= new AzureBlobSnapshotSetup();
            Apply(setup);
            
            builder.AddSetup(setup);
        }

        internal void Apply(AzureBlobSnapshotSetup setup)
        {
            setup.ServiceUri = ServiceUri;
            setup.AzureCredential = AzureCredential;
            setup.BlobClientOptions = BlobClientOptions;
        }
    }
}
