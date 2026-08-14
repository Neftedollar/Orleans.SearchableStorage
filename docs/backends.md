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
        options.Movement.TransferPageRecordLimit = 128;
        options.Movement.TransferPageByteTarget = 256 * 1_024;
        options.Query.ContinuationProtection.CurrentKey =
            new SearchableStorageContinuationKey(
                "searchable-v1",
                Convert.FromBase64String(
                    configuration["SearchableStorage:ContinuationKey"]!));
    });

siloBuilder.AddSearchableStorageState<VacancyState>("Searchable", "vacancy");
```

Public paging requires exactly 32 bytes of secret key material. Configure the same provider-scoped
current/decrypt-only key ring on every silo and external Orleans client which can create or resume a
page. The key is independent of the physical backend and must come from secret configuration; it is
not stored in PostgreSQL, Redis, Blob Storage, layout state, or continuations. Legacy all-results
queries and terminal facets remain token-free; distinct facets use the same key ring for their own
response-family-bound continuation.

These are the defaults. `VirtualSlotTargetCount` seeds the exact persisted map for that provider
namespace by rounding upward to a multiple of `PartitionCount`; it does not change an existing
format-4 or format-5 map, although it must remain a valid configured value. The exact per-layout map
is capped at 262,144 owner integers. `PartitionCount` remains the persisted initial owner count; live
rebalancing changes assignments and may introduce higher owner indices without changing that
identity field. `JournalSegmentCapacity` and `MaximumJournalReplayEntries` are persisted choices and
require migration to change. `CompactionThreshold` and the movement page settings are operational.
Movement pages accept 1–1,024 records and a 1-byte–4-MiB canonical movement-encoding target. That
measure is deterministic replay/page accounting, not actual Orleans wire or physical-provider bytes.

A valid version-3 layout upgrades with one physical layout CAS. The migration does not write any
partition manifest, journal segment, or snapshot. Every supported backend must therefore preserve
the same stale-ETag and lost-acknowledgement behavior for the layout document as it does for the
partition manifest. The project uses JSON physical serialization; durable C# property names in the
layout state are retained across the version change in addition to Orleans serializer field IDs.
Layout adoption itself leaves partition manifests, journal segments, and snapshots untouched.
Only an explicit, quiesced capability sweep upgrades partition persistence. Movement enablement
upgrades a supported format-3 owner to format 4. Managed-schema adoption upgrades a supported
format-3 or format-4 owner directly to format 5. Ordinary activation and mutation do not opt into a
newer capability. Both upgrades retain the bounded journal ring and two snapshot slots; formats 4
and 5 use the lossless snapshot record representation.

The v3-to-v4 transition requires a traffic pause rather than an online mixed-version rollout.
Quiesce searchable storage and query traffic, update every silo and Orleans client, verify that no
version-3 process remains, and keep traffic paused while one normal grain-state storage operation
adopts each provider namespace. Verify that the admin read succeeds and reports epoch 1; the admin
path returns a snapshot only for routing-capable layout format 4 or 5, and it does not perform
adoption. A new storage activation immediately uses routed methods which an old silo does not
implement, and an old activation cannot read the adopted format-4 layout. Updated consumers retain
legacy methods while the epoch-1 identity map preserves old modulo placement. At this point either
resume traffic with movement disabled, or keep it paused if the second gate follows immediately.

Live movement has a second gate. Quiesce searchable traffic if it was resumed, deploy/restart the
movement-capable package on every silo and client, and call `EnableMovementAsync` for each namespace.
The resumable enablement intent upgrades and fences one current owner at a time; only its final layout
CAS publishes movement protocol version 1 and the new epoch. Verify `MovementState == Enabled` before
resuming traffic. Legacy placement-only calls are rejected thereafter, so binary rollback is not
supported. The complete procedure and recovery rules are in the
[live-movement runbook](live-movement.md).

Managed index schemas have a separate `index-schema` control document for every provider/state pair
in the same physical provider. The first rebuild can initialize a fresh layout, upgrades every
current owner to persistence format 5, rebuilds every record for that one state, and then publishes
a provider-wide, one-way capability in the layout. Publication activates no other state: complete
and verify every registered state's rebuild in that same quiesced first-adoption window before
resuming provider traffic or movement. A durable per-rebuild layout intent blocks movement
throughout the owner sweep and record pages, including for later generation changes. Every state
using that provider must be registered on every silo, and every direct query client must declare the
states it uses, before this sweep starts. Against existing data, quiesce the provider before the
first new binary or changed registration is deployed, then keep it paused while every participant
is updated and verified. Admin calls must supply the same CLR type and application version. Old
binaries and schema-unaware clients are unsupported afterwards.

The rebuild uses the existing partition WAL and snapshots, so backend conditional-write and
lost-acknowledgement requirements are unchanged. A control write can fail before commit or commit
before its acknowledgement is lost; retry on a fresh activation must expose the durable cursor or
active generation and resume idempotently. A completed routing-layout change between turns resets
the owner scan and processed count if it occurs before the rebuild acquires its layout maintenance
intent; movement is excluded after that point. Registration and rebuild are explicit: use the
homogeneous, quiesced procedure in
[index-schema-lifecycle.md](index-schema-lifecycle.md). Owner enablement, record scanning, and
provider-capability publication cannot overlap live movement. The following state-control
activation remains protected by the operator's quiescence window.

The control document is not disposable metadata. Include it in the same backup, replication,
retention, and restore boundary as the provider layout, manifests, journals, and snapshots. If only
the control is lost, a restarted or cold-cache participant fails closed against the still-enabled
format-5 namespace. Already validated participants do not poll the control on each operation, so
quiesce and restart them during recovery rather than treating out-of-band deletion as an immediate
distributed fence. After investigation, a quiesced rebuild can recreate the generation only when
the layout has no surviving schema-maintenance intent and every payload which needs reindexing is
still decodable. Mid-rebuild control loss with a surviving intent requires a consistent backup or a
reviewed physical recovery; the public API cannot synthesize its missing identity. The rebuild
deserializes those payloads through
the configured application-state serializer, so a CLR or serializer-breaking deployment must be
corrected before retrying. The schema protocol does not migrate payload serialization.

The bounded query protocol has a separate mixed-version rule and no persistence migration. Before
upgrading from a release which lacks the page RPC, quiesce searchable query traffic, deploy every
partition-hosting silo and built-in query client, distribute the common continuation key ring, and
then resume queries. Point reads, writes, and clears may continue during this query-only rollout.
Never send a page or facet RPC to an old activation and never fall back to the old unbounded query
RPC. Facets require no durable migration: their messages are non-persisted wire protocol, and the
canonical hash-value projection is rebuilt from the same durable records/index entries on activation.

The physical provider must atomically replace or clear one grain-state value subject to its ETag, reject stale ETags, and provide authoritative point reads of durable state after reactivation or retry. No transaction across the manifest, journal, snapshot, and schema-control states is required; the partition manifest is the record/index commit point and the per-state control is the generation commit point. Do not configure provider TTLs or lifecycle rules which can independently expire layout, manifest, journal, snapshot, or `index-schema` control state.

Journal segments have dual hard ceilings of 64 operations and 320 MiB of deterministic canonical
entry bytes; each entry is capped at 5 MiB. A whole-partition snapshot is capped at 1,000,000 records
and 512 MiB of deterministic canonical record bytes. These are logical safety limits, not Orleans-
serialized or provider-physical sizes; a backend may impose lower limits. Movement export, import,
and source-deletion pages remain record-count-bounded and byte-targeted, with one accepted record
allowed to exceed the normal page target. Compaction can still rewrite the whole accepted partition
during a move, and activation still loads a whole snapshot and rebuilds the derived slot index;
provider-native write and latency telemetry must preserve those costs. See
[storage-capacity-limits.md](storage-capacity-limits.md) for exact formulas and rollout preflight.
The virtual map adds
approximately four raw bytes per slot before serializer overhead and is read as one layout value.
Partition activations share one retained map per provider and silo instead of cloning it per
partition. The keyed storage provider and query/admin clients retain a bounded constant number of
additional process-local snapshots, never one per partition activation.
Each partition derives and retains one canonical ordered catalog/posting projection shared by
lookup, paging, and facets; this is activation memory, not physical-backend bytes.
Hash and range scopes share a balanced canonical value projection for facets. Candidate nomination
reads only bucket metadata, while exact filtered probes traverse nominated postings in bounded
slices. No facet candidate, count, cursor, or owner data version is persisted in the backend.
Choose the initial partition count, virtual-slot target, and journal capacity with the provider's
row/value/blob limits, serialization cost, and activation memory in mind. Repeatable capacity
benchmarks are tracked in [issue #8](https://github.com/Neftedollar/Orleans.SearchableStorage/issues/8).

Back up a provider namespace before enabling movement using the physical backend's documented
consistent backup procedure. Never edit layout, manifest, journal, or snapshot values manually.
Movement uses normal conditional whole-state provider writes and therefore inherits each backend's
ETag, lost-acknowledgement, maximum-value-size, retry, and backup behavior. The shared provider
contract executes a live move under concurrent routed writes and continuous exact query/facet reads,
then reactivates both participants against Memory, PostgreSQL, Redis, and Azure Blob/Azurite; this
proves the storage protocol, not equivalent service availability or performance.

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
