// -----------------------------------------------------------------------
// <copyright file="AzureBlobSnapshotStoreListingScopeSpec.cs" company="Petabridge, LLC">
//      Copyright (C) 2015 - 2026 Petabridge, LLC <https://petabridge.com>
// </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.Web;
using Akka.Actor;
using Akka.Actor.Setup;
using Akka.Persistence.Azure.Snapshot;
using Akka.Persistence.Azure.Tests.Helper;
using Akka.Persistence.Azure.Util;
using Azure.Core;
using Azure.Core.Pipeline;
using Azure.Storage.Blobs;
using Xunit;
using static Akka.Persistence.Azure.Tests.Helper.AzureStorageConfigHelper;

#nullable enable
namespace Akka.Persistence.Azure.Tests;

/// <summary>
/// The snapshot store lists blobs by prefix. That listing must not cover the blobs of other
/// persistence ids. Otherwise a load or delete for "customer-9" pages through every snapshot of
/// "customer-90", "customer-91", ... and, with 45k such ids, times out.
///
/// These tests record the real List Blobs requests the store sends, so they stay deterministic
/// and do not depend on how fast Azurite answers.
/// </summary>
[Collection("AzureSpecs")]
public class AzureBlobSnapshotStoreListingScopeSpec : Akka.TestKit.Xunit.TestKit
{
    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(30);
    private readonly ListRequestRecorder _recorder;
    private readonly IActorRef _store;

    public AzureBlobSnapshotStoreListingScopeSpec(AzuriteFixture fixture, ITestOutputHelper output)
        : this(fixture, output, new ListRequestRecorder())
    {
    }

    private AzureBlobSnapshotStoreListingScopeSpec(
        AzuriteFixture fixture, ITestOutputHelper output, ListRequestRecorder recorder)
        : base(
            CreateSystem(fixture.ConnectionString, recorder),
            output)
    {
        _recorder = recorder;
        AzurePersistence.Get(Sys);
        _store = Persistence.Instance.Apply(Sys).SnapshotStoreFor(null);
    }

    private static ActorSystem CreateSystem(string connectionString, ListRequestRecorder recorder)
    {
        var options = new BlobClientOptions();
        options.AddPolicy(recorder, HttpPipelinePosition.PerCall);

        var setup = ActorSystemSetup.Create(
            Akka.Actor.BootstrapSetup.Create().WithConfig(AzureConfig(connectionString)),
            AzureBlobSnapshotSetup.Create(() => new BlobServiceClient(connectionString, options)));

        return ActorSystem.Create(nameof(AzureBlobSnapshotStoreListingScopeSpec), setup);
    }

    [Fact(DisplayName = "LoadAsync must not list blobs of persistence ids that merely start with the same text")]
    public async Task Load_listing_does_not_cover_sibling_persistence_ids()
    {
        var (pid, siblings) = await SeedSiblings();
        _recorder.Clear();

        var reply = await _store.Ask<object>(
            new LoadSnapshot(pid, SnapshotSelectionCriteria.Latest, long.MaxValue), Timeout, TestContext.Current.CancellationToken);
        var loaded = Assert.IsType<LoadSnapshotResult>(reply).Snapshot;

        Assert.NotNull(loaded); // sanity: we still find our own snapshot
        AssertListingExcludes(siblings);
    }

    [Fact(DisplayName = "DeleteAsync(persistenceId, criteria) must not list blobs of persistence ids that merely start with the same text")]
    public async Task Delete_listing_does_not_cover_sibling_persistence_ids()
    {
        var (pid, siblings) = await SeedSiblings();
        _recorder.Clear();

        var reply = await _store.Ask<object>(
            new DeleteSnapshots(pid, SnapshotSelectionCriteria.Latest), Timeout, TestContext.Current.CancellationToken);
        Assert.IsType<DeleteSnapshotsSuccess>(reply);

        AssertListingExcludes(siblings);
    }

    /// <summary>
    /// Saves one snapshot for <c>pid</c> and one each for ids that extend it by a single digit,
    /// e.g. "customer-9" vs "customer-90" ... "customer-99". That is the shape in the bug report.
    /// </summary>
    private async Task<(string Pid, IReadOnlyList<string> SiblingBlobNames)> SeedSiblings()
    {
        var pid = "customer-" + Guid.NewGuid().ToString("N") + "-9";
        await Save(pid, 1);

        var siblingBlobNames = new List<string>();
        foreach (var digit in "0123456789")
        {
            var siblingPid = pid + digit;
            await Save(siblingPid, 1);
            siblingBlobNames.Add(new SnapshotMetadata(siblingPid, 1, DateTime.UtcNow).ToSnapshotBlobId(string.Empty));
        }

        return (pid, siblingBlobNames);
    }

    private void AssertListingExcludes(IReadOnlyList<string> siblingBlobNames)
    {
        var prefixes = _recorder.ListPrefixes;
        Assert.NotEmpty(prefixes); // sanity: the recorder saw the store's List Blobs call

        foreach (var prefix in prefixes)
        {
            var covered = siblingBlobNames.Where(name => name.StartsWith(prefix, StringComparison.Ordinal)).ToList();
            Assert.True(
                covered.Count == 0,
                $"List Blobs used prefix '{prefix}', which also covers {covered.Count} blob(s) of other persistence ids, e.g. '{covered.FirstOrDefault()}'.");
        }
    }

    private async Task Save(string persistenceId, long seqNr)
    {
        var reply = await _store.Ask<object>(
            new SaveSnapshot(new SnapshotMetadata(persistenceId, seqNr, DateTime.UtcNow), "payload"), Timeout, TestContext.Current.CancellationToken);
        Assert.IsType<SaveSnapshotSuccess>(reply);
    }

    /// <summary>Records the <c>prefix</c> of every List Blobs request that passes through the pipeline.</summary>
    private sealed class ListRequestRecorder : HttpPipelinePolicy
    {
        private readonly ConcurrentQueue<string> _prefixes = new();

        public IReadOnlyList<string> ListPrefixes => _prefixes.ToArray();

        public void Clear() => _prefixes.Clear();

        public override void Process(HttpMessage message, ReadOnlyMemory<HttpPipelinePolicy> pipeline)
        {
            Record(message);
            ProcessNext(message, pipeline);
        }

        public override ValueTask ProcessAsync(HttpMessage message, ReadOnlyMemory<HttpPipelinePolicy> pipeline)
        {
            Record(message);
            return ProcessNextAsync(message, pipeline);
        }

        private void Record(HttpMessage message)
        {
            var query = HttpUtility.ParseQueryString(message.Request.Uri.Query);
            if (query["comp"] == "list" && query["restype"] == "container")
                _prefixes.Enqueue(query["prefix"] ?? string.Empty);
        }
    }
}
