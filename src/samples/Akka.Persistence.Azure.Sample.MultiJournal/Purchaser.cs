// -----------------------------------------------------------------------
// <copyright file="Purchaser.cs" company="Petabridge, LLC">
//      Copyright (C) 2015 - 2023 Petabridge, LLC <https://petabridge.com>
// </copyright>
// -----------------------------------------------------------------------

using Akka.Actor;
using Akka.Hosting;
using Akka.Util;

namespace Akka.Persistence.Azure.Sample.MultiJournal;

public class Purchaser: UntypedActor, IWithTimers
{
    private const string TimerKey = "timer-key";
    private const string Timer = "timer";
    private readonly string[] _customers;
    private readonly string[] _items;
    private readonly IActorRef _shardRegion;
    private readonly TimeSpan _purchaseInterval = TimeSpan.FromSeconds(3);

    public Purchaser(IRequiredActor<ShardRegionKey> shardActor)
    {
        _customers = new[]
        {
            "Yoda", "Obi-Wan", "Darth Vader", "Princess Leia", 
            "Luke Skywalker", "R2D2", "Han Solo", "Chewbacca", "Jabba"
        };
        
        _items = new[]
        {
            "Yoghurt", "Fruits", "Light Saber", "Fluffy toy", "Dream Catcher", 
            "Candies", "Cigars", "Chicken nuggets", "French fries"
        };

        _shardRegion = shardActor.ActorRef;
    }
    
    public ITimerScheduler Timers { get; set; }

    protected override void OnReceive(object message)
    {
        if (message is not string str) 
            return;

        switch (str)
        {
            case "start":
                Timers.StartPeriodicTimer(TimerKey, Timer, _purchaseInterval);
                return;
            case Timer:
                var customer = PickRandom(_customers);
                var item = PickRandom(_items);
                
                // A shard message needs to be wrapped inside an envelope so the system knows which
                // shard and actor it should route the message to.
                var envelope = new ShardEnvelope(customer, new PurchaseItem(item));
                
                _shardRegion.Tell(envelope);
                return;
        }
    }

    private static T PickRandom<T>(IReadOnlyList<T> items) => items[ThreadLocalRandom.Current.Next(items.Count)];
}