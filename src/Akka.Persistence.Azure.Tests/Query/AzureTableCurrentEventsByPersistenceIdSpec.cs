using Akka.Configuration;
using Akka.Persistence.Azure.Query;
using Akka.Persistence.Azure.Tests.Helper;
using Akka.Persistence.Query;
using Akka.Persistence.TCK.Query;
using System;
using Xunit;
using Xunit.Abstractions;
using static Akka.Persistence.Azure.Tests.Helper.AzureStorageConfigHelper;

namespace Akka.Persistence.Azure.Tests.Query
{
    [Collection("AzureSpecs")]
    public sealed class AzureTableCurrentEventsByPersistenceIdSpec : CurrentEventsByPersistenceIdSpec
    {
        public AzureTableCurrentEventsByPersistenceIdSpec(ITestOutputHelper output)
            : base(AzureConfig(), nameof(AzureTableCurrentEventsByPersistenceIdSpec), output)
        {
            AzurePersistence.Get(Sys);

            ReadJournal =
                Sys.ReadJournalFor<AzureTableStorageReadJournal>(
                    AzureTableStorageReadJournal.Identifier);
        }

        [WindowsFact(SkipUnixReason = "Batch delete is not supported by Azurite in Linux")]
        public override void ReadJournal_CurrentEventsByPersistenceId_should_return_empty_stream_for_cleaned_journal_from_0_to_MaxLong()
        {
            base.ReadJournal_CurrentEventsByPersistenceId_should_return_empty_stream_for_cleaned_journal_from_0_to_MaxLong();
        }

        [WindowsFact(SkipUnixReason = "Batch delete is not supported by Azurite in Linux")]
        public override void ReadJournal_CurrentEventsByPersistenceId_should_return_remaining_values_after_partial_journal_cleanup()
        {
            base.ReadJournal_CurrentEventsByPersistenceId_should_return_remaining_values_after_partial_journal_cleanup();
        }

        [WindowsFact(SkipUnixReason = "Batch delete is not supported by Azurite in Linux")]
        public override void ReadJournal_CurrentEventsByPersistenceId_should_return_empty_stream_for_cleaned_journal_from_0_to_0()
        {
            base.ReadJournal_CurrentEventsByPersistenceId_should_return_empty_stream_for_cleaned_journal_from_0_to_0();
        }
    }
}