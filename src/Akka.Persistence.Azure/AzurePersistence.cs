// -----------------------------------------------------------------------
// <copyright file="AzurePersistence.cs" company="Petabridge, LLC">
//      Copyright (C) 2015 - 2023 Petabridge, LLC <https://petabridge.com>
// </copyright>
// -----------------------------------------------------------------------

using Akka.Actor;
using Akka.Configuration;
using Akka.Persistence.Azure.Journal;
using Akka.Persistence.Azure.Snapshot;

namespace Akka.Persistence.Azure
{
    /// <summary>
    ///     Used to configure the <see cref="AzureTableStorageJournal" />
    ///     and <see cref="AzureBlobSnapshotStore" />.
    /// </summary>
    public sealed class AzurePersistence : IExtension
    {
        private readonly ActorSystem _system;

        public AzurePersistence(ActorSystem system, AzureTableStorageJournalSettings tableSettings,
            AzureBlobSnapshotStoreSettings blobSettings)
        {
            _system = system;
            TableSettings = tableSettings;
            BlobSettings = blobSettings;
        }

        public AzureTableStorageJournalSettings TableSettings { get; }

        public AzureBlobSnapshotStoreSettings BlobSettings { get; }

        /// <summary>
        ///     The default HOCON configuration for <see cref="AzurePersistence" />.
        /// </summary>
        public static readonly Config DefaultConfig =
            ConfigurationFactory.FromResource<AzurePersistence>("Akka.Persistence.Azure.reference.conf");

        /// <summary>
        ///     Returns the <see cref="AzurePersistence" /> instance for <see cref="system" />.
        /// </summary>
        /// <param name="system">The current <see cref="ActorSystem" />.</param>
        /// <returns>
        ///     If <see cref="AzurePersistence" /> has already been instantiated, gets the current instance. If not, creates a
        ///     new instance, registers it, and returns it.
        /// </returns>
        public static AzurePersistence Get(ActorSystem system)
        {
            return system.WithExtension<AzurePersistence, AzurePersistenceProvider>();
        }
    }

    /// <summary>
    ///     Used to instantiate the <see cref="AzurePersistence" /> <see cref="ActorSystem" /> extension.
    /// </summary>
    public sealed class AzurePersistenceProvider : ExtensionIdProvider<AzurePersistence>
    {
        public override AzurePersistence CreateExtension(ExtendedActorSystem system)
        {
            system.Settings.InjectTopLevelFallback(AzurePersistence.DefaultConfig);

            var journalSettings =
                AzureTableStorageJournalSettings.Create(
                    system.Settings.Config.GetConfig("akka.persistence.journal.azure-table"));

            var snapshotSettings =
                AzureBlobSnapshotStoreSettings.Create(
                    system.Settings.Config.GetConfig("akka.persistence.snapshot-store.azure-blob-store"));

            return new AzurePersistence(system, journalSettings, snapshotSettings);
        }
    }
}