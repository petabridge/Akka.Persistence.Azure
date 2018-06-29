using System;
using Akka.Actor;

namespace Akka.Persistence.Azure
{
    /// <summary>
    /// INTERNAL API
    /// </summary>
    internal sealed class SerializationHelper
    {
        private readonly ActorSystem _actorSystem;

        public SerializationHelper(ActorSystem actorSystem)
        {
            _actorSystem = actorSystem;
        }

        private readonly Type _persistentRepresentation = typeof(IPersistentRepresentation);

        public byte[] PersistentToBytes(IPersistentRepresentation message)
        {
            /*
             * Implementation note: Akka.NET caches the serialization lookups internally here,
             * so there's no need to do it again.
             */

            var serializer = _actorSystem.Serialization.FindSerializerForType(_persistentRepresentation);
            return serializer.ToBinary(message);
        }

        public IPersistentRepresentation PersistentFromBytes(byte[] bytes)
        {
            /*
             * Implementation note: Akka.NET caches the serialization lookups internally here,
             * so there's no need to do it again.
             */

            var serializer = _actorSystem.Serialization.FindSerializerForType(_persistentRepresentation);
            return serializer.FromBinary<IPersistentRepresentation>(bytes);
        }
    }
}
