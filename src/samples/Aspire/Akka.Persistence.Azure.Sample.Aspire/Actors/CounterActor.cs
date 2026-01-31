// -----------------------------------------------------------------------
// <copyright file="CounterActor.cs" company="Petabridge, LLC">
//      Copyright (C) 2015 - 2025 Petabridge, LLC <https://petabridge.com>
// </copyright>
// -----------------------------------------------------------------------

using Akka.Actor;
using Akka.Event;
using Akka.Persistence;

namespace Akka.Persistence.Azure.Sample.Aspire.Actors;

/// <summary>
/// Commands for the <see cref="CounterActor"/>
/// </summary>
public static class CounterCommands
{
    public sealed record Increment(int Amount = 1);
    public sealed record Decrement(int Amount = 1);
    public sealed record GetCount;
    public sealed record SaveSnapshot;
}

/// <summary>
/// Events persisted by the <see cref="CounterActor"/>
/// </summary>
public static class CounterEvents
{
    public sealed record CounterIncremented(int Amount);
    public sealed record CounterDecremented(int Amount);
}

/// <summary>
/// Snapshot state for the <see cref="CounterActor"/>
/// </summary>
public sealed record CounterState(int Count = 0)
{
    public CounterState Apply(CounterEvents.CounterIncremented evt) => this with { Count = Count + evt.Amount };
    public CounterState Apply(CounterEvents.CounterDecremented evt) => this with { Count = Count - evt.Amount };
}

/// <summary>
/// A simple persistent actor that maintains a counter.
/// Demonstrates both event persistence (journal) and snapshot persistence (blob storage).
/// </summary>
public sealed class CounterActor : ReceivePersistentActor
{
    private readonly ILoggingAdapter _log = Context.GetLogger();
    private CounterState _state = new();
    private int _eventsSinceLastSnapshot;
    
    // Save a snapshot every 5 events
    private const int SnapshotInterval = 5;

    public override string PersistenceId { get; }

    public CounterActor(string persistenceId)
    {
        PersistenceId = persistenceId;
        
        // Recovery handlers
        Recover<CounterEvents.CounterIncremented>(evt =>
        {
            _state = _state.Apply(evt);
            _eventsSinceLastSnapshot++;
        });
        
        Recover<CounterEvents.CounterDecremented>(evt =>
        {
            _state = _state.Apply(evt);
            _eventsSinceLastSnapshot++;
        });
        
        Recover<SnapshotOffer>(offer =>
        {
            if (offer.Snapshot is CounterState state)
            {
                _state = state;
                _log.Info("Recovered state from snapshot: Count={0}", _state.Count);
            }
        });
        
        Recover<RecoveryCompleted>(_ =>
        {
            _log.Info("Recovery completed. Current count: {0}", _state.Count);
        });
        
        // Command handlers
        Command<CounterCommands.Increment>(cmd =>
        {
            var evt = new CounterEvents.CounterIncremented(cmd.Amount);
            Persist(evt, e =>
            {
                _state = _state.Apply(e);
                _eventsSinceLastSnapshot++;
                _log.Info("Incremented by {0}. New count: {1}", e.Amount, _state.Count);
                MaybeSnapshot();
            });
        });
        
        Command<CounterCommands.Decrement>(cmd =>
        {
            var evt = new CounterEvents.CounterDecremented(cmd.Amount);
            Persist(evt, e =>
            {
                _state = _state.Apply(e);
                _eventsSinceLastSnapshot++;
                _log.Info("Decremented by {0}. New count: {1}", e.Amount, _state.Count);
                MaybeSnapshot();
            });
        });
        
        Command<CounterCommands.GetCount>(_ =>
        {
            Sender.Tell(_state.Count);
        });
        
        Command<CounterCommands.SaveSnapshot>(_ =>
        {
            SaveSnapshot(_state);
        });
        
        Command<SaveSnapshotSuccess>(success =>
        {
            _log.Info("Snapshot saved successfully at sequence {0}", success.Metadata.SequenceNr);
            _eventsSinceLastSnapshot = 0;
            
            // Optionally delete old snapshots to save space
            DeleteSnapshots(new SnapshotSelectionCriteria(success.Metadata.SequenceNr - 1));
        });
        
        Command<SaveSnapshotFailure>(failure =>
        {
            _log.Error(failure.Cause, "Failed to save snapshot at sequence {0}", failure.Metadata.SequenceNr);
        });
        
        Command<DeleteSnapshotsSuccess>(_ =>
        {
            _log.Debug("Old snapshots deleted successfully");
        });
        
        Command<DeleteSnapshotsFailure>(failure =>
        {
            _log.Warning(failure.Cause, "Failed to delete old snapshots");
        });
    }
    
    private void MaybeSnapshot()
    {
        if (_eventsSinceLastSnapshot >= SnapshotInterval)
        {
            _log.Info("Saving snapshot after {0} events", _eventsSinceLastSnapshot);
            SaveSnapshot(_state);
        }
    }
    
    public static Props Props(string persistenceId) => 
        Akka.Actor.Props.Create(() => new CounterActor(persistenceId));
}
