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
    }
}
