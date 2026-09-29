// -----------------------------------------------------------------------
// <copyright file="AzureBlobSnapshotStoreListingSpec.cs" company="Petabridge, LLC">
//      Copyright (C) 2015 - 2026 Petabridge, LLC <https://petabridge.com>
// </copyright>
// -----------------------------------------------------------------------

using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Akka.Actor;
using Akka.Configuration;
using Akka.Persistence.Azure.Tests.Helper;
using Xunit;
using static Akka.Persistence.Azure.Tests.Helper.AzureStorageConfigHelper;

#nullable enable
namespace Akka.Persistence.Azure.Tests;

/// <summary>
/// Regression tests for how <see cref="Snapshot.AzureBlobSnapshotStore"/> lists blobs:
/// LoadAsync must consider every page of the listing, and both LoadAsync and
/// DeleteAsync(persistenceId, criteria) must only touch blobs of the requested persistence id.
/// </summary>
[Collection("AzureSpecs")]
public class AzureBlobSnapshotStoreListingSpec : Akka.TestKit.Xunit.TestKit
{
    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(30);
    private readonly IActorRef _store;

    public AzureBlobSnapshotStoreListingSpec(AzuriteFixture fixture, ITestOutputHelper output)
        : base(
            ConfigurationFactory.ParseString("akka.loglevel = INFO")
                .WithFallback(AzureConfig(fixture.ConnectionString)),
            nameof(AzureBlobSnapshotStoreListingSpec),
            output)
    {
        AzurePersistence.Get(Sys);
        _store = Persistence.Instance.Apply(Sys).SnapshotStoreFor(null);
    }

    [Fact(DisplayName = "LoadAsync must return the newest snapshot when it is not on the first listing page")]
    public async Task Load_returns_newest_snapshot_beyond_first_page()
    {
        // List Blobs returns at most 5,000 items per page and names sort ascending by sequence
        // number, so snapshot 5,001 is on page 2.
        const int count = 5_001;
        var pid = "paged-" + Guid.NewGuid().ToString("N");

        using var throttle = new SemaphoreSlim(32);
        await Task.WhenAll(Enumerable.Range(1, count).Select(async seqNr =>
        {
            await throttle.WaitAsync(TestContext.Current.CancellationToken);
            try
            {
                await Save(pid, seqNr, "payload-" + seqNr);
            }
            finally
            {
                throttle.Release();
            }
        }));

        var loaded = await LoadLatest(pid);

        Assert.NotNull(loaded);
        Assert.Equal(count, loaded!.Metadata.SequenceNr);
        Assert.Equal("payload-" + count, loaded.Snapshot);
    }

    [Fact(DisplayName = "LoadAsync must not return a snapshot of another persistence id that shares the prefix")]
    public async Task Load_ignores_persistence_id_sharing_the_prefix()
    {
        var pid = "orders-" + Guid.NewGuid().ToString("N");
        var otherPid = pid + "-2";
        await Save(pid, 5, "belongs-to:" + pid);
        await Save(otherPid, 9, "belongs-to:" + otherPid);

        var loaded = await LoadLatest(pid);

        Assert.NotNull(loaded);
        Assert.Equal(5, loaded!.Metadata.SequenceNr);
        Assert.Equal("belongs-to:" + pid, loaded.Snapshot);
    }

    [Fact(DisplayName = "DeleteAsync(persistenceId, criteria) must not delete snapshots of another persistence id that shares the prefix")]
    public async Task Delete_ignores_persistence_id_sharing_the_prefix()
    {
        var pid = "acct-" + Guid.NewGuid().ToString("N") + "-1";
        var otherPid = pid + "0"; // "...-1" vs "...-10"
        await Save(pid, 1, "belongs-to:" + pid);
        await Save(otherPid, 1, "belongs-to:" + otherPid);

        var reply = await _store.Ask<object>(
            new DeleteSnapshots(pid, SnapshotSelectionCriteria.Latest), Timeout, TestContext.Current.CancellationToken);
        Assert.IsType<DeleteSnapshotsSuccess>(reply);

        Assert.Null(await LoadLatest(pid));
        var survivor = await LoadLatest(otherPid);
        Assert.NotNull(survivor);
        Assert.Equal(1, survivor!.Metadata.SequenceNr);
    }

    private async Task Save(string persistenceId, long seqNr, string payload)
    {
        var reply = await _store.Ask<object>(
            new SaveSnapshot(new SnapshotMetadata(persistenceId, seqNr, DateTime.UtcNow), payload), Timeout, TestContext.Current.CancellationToken);
        Assert.IsType<SaveSnapshotSuccess>(reply);
    }

    private async Task<SelectedSnapshot?> LoadLatest(string persistenceId)
    {
        var reply = await _store.Ask<object>(
            new LoadSnapshot(persistenceId, SnapshotSelectionCriteria.Latest, long.MaxValue), Timeout, TestContext.Current.CancellationToken);
        return Assert.IsType<LoadSnapshotResult>(reply).Snapshot;
    }
}
