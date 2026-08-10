# Physical storage backends

Orleans.SearchableStorage stores its durable layout, partition manifests, bounded journal segments, and two snapshot slots through the Orleans provider registered as `SearchableStorageConstants.PhysicalStorageProviderName`. Application grains continue to reference the searchable provider name; changing the physical provider does not change their `PersistentState` attributes or query calls.

The repository contract suite validates the official Orleans 10.2.2 providers for PostgreSQL, Redis, and Azure Blob Storage. Backend independence means that record, index, journal recovery, compaction, ETag, reactivation, and failure-boundary semantics are identical. It does not make the latency, capacity, backup, or availability properties of those systems identical.

Register exactly one physical provider before the searchable provider. After any physical-provider registration below, configure the searchable provider once:

```csharp
siloBuilder.AddSearchableGrainStorage(
    "Searchable",
    options =>
    {
        options.PartitionCount = 32;
        options.VirtualSlotTargetCount = 16_384;
        options.JournalSegmentCapacity = 64;
        options.MaximumJournalReplayEntries = 4_096;
        options.CompactionThreshold = 1_024;
    });
```

These are the defaults. `VirtualSlotTargetCount` seeds the exact persisted map for that provider
namespace by rounding upward to a multiple of `PartitionCount`; it does not change an existing
version-4 map, although it must remain a valid configured value. The exact per-layout map is capped
at 262,144 owner integers. `PartitionCount`, `JournalSegmentCapacity`, and
`MaximumJournalReplayEntries` are persisted choices and require migration to change.
`CompactionThreshold` is operational, must be positive, and cannot exceed the replay limit.

A valid version-3 layout upgrades with one physical layout CAS. The migration does not write any
partition manifest, journal segment, or snapshot. Every supported backend must therefore preserve
the same stale-ETag and lost-acknowledgement behavior for the layout document as it does for the
partition manifest. The project uses JSON physical serialization; durable C# property names in the
layout state are retained across the version change in addition to Orleans serializer field IDs.
Partition manifests, journal segments, and snapshots remain persistence format 3 after layout
adoption.

The v3-to-v4 transition requires a traffic pause rather than an online mixed-version rollout.
Quiesce searchable storage and query traffic, update every silo and Orleans client, verify that no
version-3 process remains, and keep traffic paused while one normal grain-state storage operation
adopts each provider namespace. Verify that the admin read succeeds and reports epoch 1 before
resuming traffic; the admin path returns a snapshot only for format 4, and it does not perform
adoption. A new storage activation immediately uses routed methods which an old silo does not
implement, and an old activation cannot read the adopted format-4 layout. This release retains
legacy methods for updated consumers and its epoch-1 identity map preserves old modulo placement,
but it does not expose `MoveSlot`. Future ownership changes require a separate coordinated all-v4
protocol gate.

The physical provider must atomically replace or clear one grain-state value subject to its ETag, reject stale ETags, and provide authoritative point reads of durable state after reactivation or retry. No transaction across the manifest, journal, and snapshot states is required; the manifest is the searchable-storage commit point. Do not configure provider TTLs or lifecycle rules which can independently expire layout, manifest, journal, or snapshot state.

Journal segments are bounded by operation count, not serialized bytes, and each snapshot contains
the whole partition. The two snapshot slots bound object count, not object size. The virtual map adds
approximately four raw bytes per slot before serializer overhead and is read as one layout value.
Partition activations share one retained map per provider and silo instead of cloning it per
partition. The keyed storage provider and query/admin clients retain a bounded constant number of
additional process-local snapshots, never one per partition activation.
Choose the initial partition count, virtual-slot target, and journal capacity with the provider's
row/value/blob limits, serialization cost, and activation memory in mind. Repeatable capacity
benchmarks are tracked in [issue #8](https://github.com/Neftedollar/Orleans.SearchableStorage/issues/8).

## PostgreSQL

Reference `Microsoft.Orleans.Persistence.AdoNet` and the Npgsql ADO.NET driver. Install the official Orleans [`PostgreSQL-Main.sql`](https://github.com/dotnet/orleans/blob/v10.2.2/src/AdoNet/Shared/PostgreSQL-Main.sql) and [`PostgreSQL-Persistence.sql`](https://github.com/dotnet/orleans/blob/v10.2.2/src/AdoNet/Orleans.Persistence.AdoNet/PostgreSQL-Persistence.sql) scripts in the target database. The operational SQL copied into this repository retains its pinned source URL and the full upstream MIT notice inline. The repository-wide `eng/orleans-sql.sha256` manifest pins each complete vendored file, including that provenance and license header, and CI checks the manifest before restoring dependencies.

```csharp
siloBuilder.AddAdoNetGrainStorage(
    SearchableStorageConstants.PhysicalStorageProviderName,
    options =>
    {
        options.Invariant = "Npgsql";
        options.ConnectionString = configuration.GetConnectionString("SearchableStorage")!;
        options.DeleteStateOnClear = true;
    });
```

The database schema and the configured search path are deployment concerns. The integration fixture creates an isolated schema per run, loads the scripts from the Orleans 10.2.2 tag, and drops that schema after the contract completes.

## Redis

Reference `Microsoft.Orleans.Persistence.Redis`. Do not configure `EntryExpiry` for durable production data: the Orleans provider documents it as an ephemeral-environment option which can allow state to expire while an activation still exists.

```csharp
siloBuilder.AddRedisGrainStorage(
    SearchableStorageConstants.PhysicalStorageProviderName,
    options =>
    {
        options.ConfigurationOptions = ConfigurationOptions.Parse(
            configuration.GetConnectionString("SearchableStorage")!);
        options.DeleteStateOnClear = true;
    });
```

Redis grain-state keys are scoped by the Orleans service id. Keep `ClusterOptions.ServiceId` stable across restarts which must see the same data. The integration contract creates two keys through the official provider, verifies the provider's state namespace, and derives its cleanup sentinels from those keys so a provider key-format change cannot silently invalidate fixture cleanup. Cleanup deduplicates keys discovered through all endpoints and pipelines bounded batches of single-key `DEL` commands; it does not issue a cross-slot multi-key command, so connection-string overrides may target Redis Cluster as well as standalone Redis.

## Azure Blob Storage

Reference `Microsoft.Orleans.Persistence.AzureStorage`. The same configuration works with an authenticated Azure `BlobServiceClient`; the integration suite supplies an Azurite connection string.

```csharp
siloBuilder.AddAzureBlobGrainStorage(
    SearchableStorageConstants.PhysicalStorageProviderName,
    options =>
    {
        options.BlobServiceClient = new BlobServiceClient(
            configuration.GetConnectionString("SearchableStorage")!);
        options.ContainerName = "searchable-storage";
        options.DeleteStateOnClear = true;
    });
```

Use a dedicated valid Azure container name for the searchable storage namespace and include it in backup and retention policy. Do not apply a lifecycle expiry policy to its durable state blobs.

## Run the backend contract locally

The repository includes pinned PostgreSQL, Redis, and Azurite containers:

```bash
docker compose --file tests/backends.compose.yml up --detach --wait

ORLEANS_SEARCHABLE_STORAGE_RUN_BACKEND_TESTS=true \
  dotnet test tests/Orleans.SearchableStorage.Tests \
  --filter "Category=BackendIntegration"

docker compose --file tests/backends.compose.yml down --volumes
```

The explicit opt-in prevents a normal unit-test run from contacting local infrastructure. With the opt-in enabled, these variables override the compose defaults:

| Variable | Default |
| --- | --- |
| `ORLEANS_SEARCHABLE_STORAGE_POSTGRES_CONNECTION_STRING` | `Host=127.0.0.1;Port=5432;Database=postgres;Username=postgres;Password=postgres` |
| `ORLEANS_SEARCHABLE_STORAGE_REDIS_CONNECTION_STRING` | `127.0.0.1:6379,abortConnect=false` |
| `ORLEANS_SEARCHABLE_STORAGE_AZURE_BLOB_CONNECTION_STRING` | `UseDevelopmentStorage=true` |

The pinned Azurite 3.36.0 container implements API version `2025-11-05`, while Azure.Storage.Blobs 12.27.0 defaults to `2026-02-06`. It therefore uses `--skipApiVersionCheck` so the newer header can reach Azurite's implemented Blob operations. The storage contract still exercises and asserts every operation; this setting does not add unsupported Azure behavior. Remove the flag after the pinned emulator natively supports `2026-02-06`.

Use disposable test resources. The fixtures create unique PostgreSQL schemas, Orleans service ids, and Blob containers, then remove their data after the run. Provider-specific contract cases seed both owned and unrelated sentinels and prove that cleanup removes only the owned namespace. The compose command removes the container volumes as a final cleanup.
