using System;
using System.Collections.Generic;
using System.Text;
using Akka.Actor;
using Akka.Configuration;
using Akka.Persistence.Azure.Tests.Helper;
using FluentAssertions;
using Xunit;
using Xunit.Abstractions;
using static Akka.Persistence.Azure.Tests.Helper.AzureStorageConfigHelper;

namespace Akka.Persistence.Azure.Tests
{
    [Collection("AzureSpecs")]
    public class SerializerHelperSpecs : Akka.TestKit.Xunit2.TestKit
    {
        private readonly SerializationHelper _helper;

        public SerializerHelperSpecs(ITestOutputHelper helper) 
            : base(AzureConfig(), nameof(SerializerHelperSpecs), output: helper)
        {
            // force Akka.Persistence serializers to be loaded
            AzurePersistence.Get(Sys);
            _helper = new SerializationHelper(Sys);
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
