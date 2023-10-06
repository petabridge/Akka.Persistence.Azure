// -----------------------------------------------------------------------
// <copyright file="MessageExtractor.cs" company="Petabridge, LLC">
//      Copyright (C) 2015 - 2023 Petabridge, LLC <https://petabridge.com>
// </copyright>
// -----------------------------------------------------------------------

using Akka.Cluster.Sharding;

namespace Akka.Persistence.Azure.Sample.MultiJournal;

public sealed class ShardEnvelope
{
    public string EntityId { get; }
    public object Payload { get; }

    public ShardEnvelope(string entityId, object payload)
    {
        EntityId = entityId;
        Payload = payload;
    }
}

public sealed class MessageExtractor : HashCodeMessageExtractor
{
    public MessageExtractor(int maxNumberOfShards) : base(maxNumberOfShards)
    {
    }

    public override string? EntityId(object message)
        => message switch
        {
            ShardRegion.StartEntity start => start.EntityId,
            ShardEnvelope e => e.EntityId,
            _ => null
        };

    public override object EntityMessage(object message)
        => message switch
        {
            ShardEnvelope e => e.Payload,
            _ => message
        };
}
