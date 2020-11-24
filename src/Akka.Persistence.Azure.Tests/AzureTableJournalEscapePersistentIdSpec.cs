using System;
using System.Reflection;
using Akka.Actor;
using Akka.Configuration;
using Akka.Persistence.Azure.TestHelpers;
using Akka.Persistence.TCK;
using Akka.Persistence.TCK.Journal;
using Akka.TestKit;
using Xunit;
using Xunit.Abstractions;

namespace Akka.Persistence.Azure.Tests
{
    public class AzureTableJournalEscapePersistentIdSpec : AzureTableJournalSpec, IClassFixture<AzureCosmosDbEmulatorFixture>
    {
        public AzureTableJournalEscapePersistentIdSpec(AzureCosmosDbEmulatorFixture fixture, ITestOutputHelper output) : base(fixture, output)
        {
        }

        /// <inheritdoc />
        protected override void PreparePersistenceId(string pid)
        {
            base.PreparePersistenceId(pid);
            
            // Before storage is initialized, let's set Pid to the value that needs to be encoded
            var persistenceIdUsedForTests = typeof(PluginSpec).GetField($"<{nameof(Pid)}>k__BackingField", BindingFlags.Instance | BindingFlags.NonPublic);
            var currentValue = persistenceIdUsedForTests.GetValue(this).ToString();
            persistenceIdUsedForTests.SetValue(this, $"some/path/to/encode/{currentValue}");
        }
    }
}