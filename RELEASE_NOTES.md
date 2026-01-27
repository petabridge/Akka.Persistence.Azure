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
