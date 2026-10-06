#### 1.6.0-beta2 October 6 2026 ####

* Built against [Akka.NET 1.6.0-beta2](https://github.com/akkadotnet/akka.net/releases/tag/1.6.0-beta2) and Akka.Hosting 1.6.0-beta2.
* Both packages now target `net10.0` only, as Akka.NET 1.6 does.

**Breaking changes:**

* `Akka.Persistence.Azure` and `Akka.Persistence.Azure.Hosting` no longer target `netstandard2.0` or `net6.0`. Projects that use them need Akka.NET 1.6 and .NET 10.
* The `System.Linq.Async` package reference is gone. The .NET 10 BCL has its own `System.Linq.AsyncEnumerable`.
* See the [Akka.NET 1.6 breaking changes](https://github.com/akkadotnet/akka.net/blob/dev/BREAKING_CHANGES_V1.6.md) for changes in Akka.NET itself.

No public API or storage format changes. The 1.5.x line continues on the `v1.5` branch.

#### 1.5.71 September 29 2026 ####

* [Fix `AzureBlobSnapshotStore` loading stale or missing snapshots and matching other persistence ids](https://github.com/petabridge/Akka.Persistence.Azure/pull/560)
* [Update Akka.NET v1.5.71](https://github.com/akkadotnet/akka.net/releases/tag/1.5.71)
* [Update Akka.Hosting v1.5.71](https://github.com/akkadotnet/Akka.Hosting/releases/tag/1.5.71) - [#558](https://github.com/petabridge/Akka.Persistence.Azure/pull/558)

**Bug Fixes:**

This release fixes two bugs in `AzureBlobSnapshotStore`, reported by a user.

1. **Loading could return an older snapshot, or none at all.** `LoadAsync` read only the first page of the blob listing. Azure Blob Storage can return an empty or partial first page along with a continuation token, and when that happened the actor recovered as if it had no snapshot, with no error logged. A persistence id with more than 5,000 snapshots also got an older snapshot, since the newest one sits on a later page. Because recovery takes the actor's sequence number from the snapshot when the snapshot is ahead of the journal, a stale or missing snapshot could move the sequence number backwards and cause existing snapshots to be overwritten. `LoadAsync` now reads every page and picks the newest match.
2. **Snapshot operations matched other persistence ids that share the same prefix.** Loading `orders-1` could return the snapshot of `orders-1-2`, and recovery would then skip replaying `orders-1`'s own events that came after its real snapshot. Deleting snapshots for `acct-1` also deleted the snapshots of `acct-10`, `acct-11`, and so on. `LoadAsync` and `DeleteAsync(persistenceId, criteria)` now match only the exact persistence id.

No public API or storage format changes.

**Behavior change:**

`LoadAsync` now reads the full blob listing within the snapshot store's `request-timeout` (default `3s`; `AzureBlobSnapshotOptions.RequestTimeout` in Akka.Hosting). If the listing can't finish in time, loading the snapshot fails and recovery fails with an error, instead of silently recovering without a snapshot. If you keep a very large number of previous blob versions or soft-deleted blobs in your snapshot container, raise `request-timeout` or clean up those old entries.

**Dependency changes:**

Akka.Hosting 1.5.71 raises the minimum versions of some transitive dependencies for users of `Akka.Persistence.Azure.Hosting`:

* `Microsoft.Extensions.Hosting.Abstractions`, `Microsoft.Extensions.Configuration.Abstractions`, `Microsoft.Extensions.Logging.Abstractions`, and `Microsoft.Extensions.Diagnostics.HealthChecks`: 8.0 to 9.0
* `OpenTelemetry`: 1.9 to 1.10

The test suite also moved to xunit v3; this does not affect the published packages.

#### 1.5.60 February 10 2026 ####

* [Update Akka.NET v1.5.60](https://github.com/akkadotnet/akka.net/releases/tag/1.5.60)
* [Update Akka.Hosting v1.5.60](https://github.com/akkadotnet/Akka.Hosting/releases/tag/1.5.60)

#### 1.5.59 January 26 2026 ####

* [Update Akka.NET v1.5.59](https://github.com/akkadotnet/akka.net/releases/tag/1.5.59)
* [Update Akka.Hosting v1.5.59](https://github.com/akkadotnet/Akka.Hosting/releases/tag/1.5.59)

#### 1.5.55.1 November 17 2025 ####

* [Fix Health Check Permission Requirements](https://github.com/petabridge/Akka.Persistence.Azure/pull/543)

This release addresses permission issues in the connectivity health checks introduced in v1.5.55.

**Bug Fix:**

The connectivity health checks were using `GetPropertiesAsync()` operations that require service-level Azure permissions, causing authentication failures for users whose credentials didn't have these elevated permissions. This was separate from the permissions needed by the journal and snapshot store themselves.

**Solution:**

Changed health checks to use the same operations as the actual persistence plugins:

- **Table Journal**: Now uses `TableServiceClient.QueryAsync()` - matching the journal's `IsTableExist()` method
- **Blob Snapshot Store**: Now uses `BlobContainerClient.ExistsAsync()` - matching the snapshot store's `InitCloudStorage()` method

**Impact:**

Health checks now require only the same permissions as normal journal/snapshot store operations. No additional Azure role assignments are needed, and the checks work whether the table/container exists or not (supporting auto-initialize scenarios).

This change is **fully backward compatible** and requires no code changes from users.

#### 1.5.55.1-beta1 October 31 2025 ####

* [Fix authentication storm issue in connectivity health checks](https://github.com/petabridge/Akka.Persistence.Azure/pull/542)

This is a critical bug fix release that addresses authentication issues in the connectivity health checks introduced in v1.5.55.

**Bug Fix:**

The connectivity health checks were creating new Azure SDK client instances on every health check invocation, causing:
- Authentication storms when using Managed Identity or TokenCredential
- Rate limiting from Azure AD token endpoints
- Intermittent 500 errors in readiness probes
- Unnecessary resource allocation and network traffic

**Solution:**

Both `AzureTableJournalConnectivityCheck` and `AzureBlobSnapshotStoreConnectivityCheck` now cache their Azure SDK clients (`TableServiceClient` and `BlobServiceClient`) as instance fields, matching the pattern used by the actual persistence plugins. Clients are created once during health check construction and reused for all subsequent health check invocations.

This change is **fully backward compatible** and requires no code changes from users. The Azure SDK clients handle token refresh internally, so cached clients remain valid for the lifetime of the health check instance.

**Impact:**

For production environments using readiness probes (especially with multiple replicas), this fix eliminates authentication-related health check failures and significantly reduces authentication overhead.

#### 1.5.55 October 27 2025 ####

* [Update Akka.NET v1.5.55](https://github.com/akkadotnet/akka.net/releases/tag/1.5.55)
