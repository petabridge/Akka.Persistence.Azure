// -----------------------------------------------------------------------
// <copyright file="Customers.cs" company="Petabridge, LLC">
//      Copyright (C) 2015 - 2023 Petabridge, LLC <https://petabridge.com>
// </copyright>
// -----------------------------------------------------------------------

using Akka.Actor;
using Akka.Event;

namespace Akka.Persistence.Azure.Sample.MultiJournal;

public sealed class PurchaseItem
{
    public readonly string ItemName;

    public PurchaseItem(string itemName)
    {
        ItemName = itemName;
    }
}

public class Customer : ReceivePersistentActor
{
    public static Props Props(string id) => Akka.Actor.Props.Create(() => new Customer(id));

    private readonly ILoggingAdapter _log;
    private readonly string _name;
    private List<string> _purchasedItems = new();
    private int _counter;
    
    public Customer(string persistenceId)
    {
        PersistenceId = persistenceId;
        _log = Context.GetLogger();
        _name = Uri.UnescapeDataString(Self.Path.Name);

        Recover<SnapshotOffer>(msg =>
        {
            _purchasedItems = (List<string>) msg.Snapshot;
            _log.Info(
                @$"'{_name}' restored purchases.
All items: [{string.Join(", ", _purchasedItems)}]
--------------------------");
        });
        
        Recover<PurchaseItem>(HandlePurchase);
        
        Command<PurchaseItem>(purchase =>
        {
            Persist(purchase, HandlePurchase);
        });
        
        Command<SaveSnapshotSuccess>(msg =>
        {
            DeleteMessages(msg.Metadata.SequenceNr);
        });
        
        Command<DeleteMessagesSuccess>(_ => { });
    }

    private void HandlePurchase(PurchaseItem item)
    {
        _purchasedItems.Add(item.ItemName);
        _log.Info(
            @$"'{_name}' purchased '{item.ItemName}'.
All items: [{string.Join(", ", _purchasedItems)}]
--------------------------");
        if (_counter > 0 && _counter % 10 == 0)
            SaveSnapshot(_purchasedItems);
        _counter++;
    }

    public override string PersistenceId { get; }
}
