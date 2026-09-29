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
            Assert.Equal(persistentRepresentation.Sender, deserialized.Sender);
            Assert.False(deserialized.IsDeleted);
        }
    }
}
