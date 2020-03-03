using System;
using System.Collections.Generic;
using System.Text;
using Akka.Actor;
using Akka.Configuration;
using Akka.Persistence.Azure.TestHelpers;
using Akka.Persistence.Azure.Tests.Helper;
using FluentAssertions;
using Hocon;
using Xunit;
using Xunit.Abstractions;

namespace Akka.Persistence.Azure.Tests
{
    public class SerializerHelperSpecs : Akka.TestKit.Xunit2.TestKit
    {
        private readonly SerializationHelper _helper;

        public SerializerHelperSpecs(ITestOutputHelper helper) 
            : base(Config(), nameof(SerializerHelperSpecs), output: helper)
        {
            // force Akka.Persistence serializers to be loaded
            AzurePersistence.Get(Sys);
            _helper = new SerializationHelper(Sys);
        }

        public static Config Config()
        {
            var azureConfig =
                !string.IsNullOrEmpty(Environment.GetEnvironmentVariable("AZURE_CONNECTION_STR"))
                    ? AzureStorageConfigHelper.AzureConfig(Environment.GetEnvironmentVariable("AZURE_CONNECTION_STR"))
                    : AzureStorageConfigHelper.AzureConfig(WindowsAzureStorageEmulatorFixture.GenerateConnStr());

            return azureConfig;
        }


        [Fact]
        public void ShouldSerializeAndDeserializePersistentRepresentation()
        {
            var persistentRepresentation = new Persistent("hi", 1L, "aaron");
            var bytes = _helper.PersistentToBytes(persistentRepresentation);
            var deserialized = _helper.PersistentFromBytes(bytes);

            deserialized.Payload.Should().Be(persistentRepresentation.Payload);
            deserialized.Manifest.Should().Be(persistentRepresentation.Manifest);
            deserialized.SequenceNr.Should().Be(persistentRepresentation.SequenceNr);
            deserialized.PersistenceId.Should().Be(persistentRepresentation.PersistenceId);
            deserialized.Sender.Should().Be(persistentRepresentation.Sender);
            deserialized.IsDeleted.Should().BeFalse();
        }
    }
}
