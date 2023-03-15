// -----------------------------------------------------------------------
// <copyright file="AzureTableStorageReadJournalSetup.cs" company="Petabridge, LLC">
//      Copyright (C) 2015 - 2023 Petabridge, LLC <https://petabridge.com>
// </copyright>
// -----------------------------------------------------------------------

using System;
using Akka.Actor.Setup;

namespace Akka.Persistence.Azure.Query
{
    public sealed class AzureTableStorageReadJournalSetup: Setup
    {
        /// <summary>
        /// How many events to fetch in one query (replay) and keep buffered until they
        /// are delivered downstream.
        /// </summary>
        public int? MaxBufferSize { get; set; }
        
        /// <summary>
        /// The Azure Table write journal is notifying the query side as soon as things
        /// are persisted, but for efficiency reasons the query side retrieves the events 
        /// in batches that sometimes can be delayed up to the configured <see cref="RefreshInterval"/>.
        /// </summary>
        public TimeSpan? RefreshInterval { get; set; }
        
        /// <summary>
        /// Absolute path to the write journal plugin configuration entry that this 
        /// query journal will connect to. 
        /// If undefined (or "") it will connect to the default journal as specified by the
        /// akka.persistence.journal.plugin property.
        /// </summary>
        public string WritePluginId { get; set; }
    }
}