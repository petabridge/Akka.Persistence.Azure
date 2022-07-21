using System;
using System.Reflection;
using Akka.Actor;
using Akka.Configuration;
using Akka.Persistence.Azure.Tests.Helper;
using Akka.Persistence.TCK;
using Akka.Persistence.TCK.Journal;
using Akka.TestKit;
using Xunit;
using Xunit.Abstractions;
using static Akka.Persistence.Azure.Tests.Helper.AzureStorageConfigHelper;

namespace Akka.Persistence.Azure.Tests
{
    [Collection("AzureSpecs")]
    public class AzureTableJournalEscapePersistentIdSpec : JournalSpec
    {
        public AzureTableJournalEscapePersistentIdSpec(ITestOutputHelper output) 
            : base(AzureConfig(), nameof(AzureTableJournalEscapePersistentIdSpec), output)
        {
            AzurePersistence.Get(Sys);
            
            // Before storage is initialized, let's set Pid to the value that needs to be encoded
            var persistenceIdUsedForTests = typeof(PluginSpec).GetField($"<{nameof(Pid)}>k__BackingField", BindingFlags.Instance | BindingFlags.NonPublic);
            var currentValue = persistenceIdUsedForTests.GetValue(this).ToString();
            persistenceIdUsedForTests.SetValue(this, $"some/path/to/encode/{currentValue}");
            
            Initialize();
        }

        [WindowsFact(SkipUnixReason = "Batch delete is not supported by Azurite in Linux")]
        public override void Journal_should_not_reset_HighestSequenceNr_after_message_deletion()
        {
            base.Journal_should_not_reset_HighestSequenceNr_after_message_deletion();
        }

        [WindowsFact(SkipUnixReason = "Batch delete is not supported by Azurite in Linux")]
        public override void Journal_should_not_replay_permanently_deleted_messages_on_range_deletion()
        {
            base.Journal_should_not_replay_permanently_deleted_messages_on_range_deletion();
        }

        [WindowsFact(SkipUnixReason = "Batch delete is not supported by Azurite in Linux")]
        public override void Journal_should_not_reset_HighestSequenceNr_after_journal_cleanup()
        {
            base.Journal_should_not_reset_HighestSequenceNr_after_journal_cleanup();
        }
    }
}