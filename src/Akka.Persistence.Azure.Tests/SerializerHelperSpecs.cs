using System;
using System.Collections.Generic;
using System.Text;
using Akka.Actor;
using Akka.Configuration;
using Akka.Persistence.Azure.Tests.Helper;
using Xunit;
using static Akka.Persistence.Azure.Tests.Helper.AzureStorageConfigHelper;

namespace Akka.Persistence.Azure.Tests
{
    [Collection("AzureSpecs")]
    public class SerializerHelperSpecs : Akka.TestKit.Xunit.TestKit
    {
        private readonly SerializationHelper _helper;

        public SerializerHelperSpecs(AzuriteFixture fixture, ITestOutputHelper helper)
            : base(AzureConfig(fixture.ConnectionString), nameof(SerializerHelperSpecs), output: helper)
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

            Assert.Equal(persistentRepresentation.Payload, deserialized.Payload);
            Assert.Equal(persistentRepresentation.Manifest, deserialized.Manifest);
            Assert.Equal(persistentRepresentation.SequenceNr, deserialized.SequenceNr);
            Assert.Equal(persistentRepresentation.PersistenceId, deserialized.PersistenceId);
            // Sender is not stored in the journal. Akka.NET 1.6 always resolves Persistent through the
            // built-in protobuf serializer, which turns NoSender into dead letters on the way back.
            Assert.True(deserialized.Sender == null || deserialized.Sender.Equals(Sys.DeadLetters));
            Assert.False(deserialized.IsDeleted);
        }
    }
}
