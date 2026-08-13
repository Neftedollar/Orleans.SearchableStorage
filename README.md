# Orleans.SearchableStorage

> [!CAUTION]
> **Version 1.0.0-rc.2 is NOT PRODUCTION-QUALIFIED. DO NOT USE THIS PACKAGE IN PRODUCTION.**
> It is a qualification and integration candidate. Controlled 1,000,000-record provider runs and
> a full, non-modelled, external/distributed 10,000,000+ record run are still required before a
> production 1.0 release. See [the release notes](RELEASE_NOTES.md) for the exact evidence status.

Orleans-native persistent grain storage and payload-free secondary indexes.

The project is an early vertical slice with two explicit ownership modes. Integrated mode implements
an `IGrainStorage` provider whose records and local index entries are owned by Orleans grains and
committed together. Index-only mode stores only derived index entries while the application owns its
payload store, update ordering, reconciliation, and hydration. Both modes persist their internal
state through another Orleans storage provider and use the same bounded query API. See the
[index-only guide](docs/index-only-mode.md) for the consistency boundary.

The [1.0 product and query contract](docs/one-zero-contract.md) is the concise matrix of the
implemented product boundary, supported CLR/index/query surface, terminal semantics, failure model,
and current runtime/backend evidence, including the implemented bounded collection-membership slice.
It does not claim that version 1.0 or its SemVer guarantees have already shipped.

For production and contributor navigation, start with the [operations index](docs/operations.md),
the [maintainer guide](docs/maintainers.md), and the [release process](docs/release.md). They connect
the focused product contract to the existing schema, movement, capacity, backend, and query runbooks.
The [prerelease handoff and qualification boundary](docs/qualification-handoff.md) provides the
still-open clean-room human worksheet and defines how an exact NuGet package later becomes the target
of independently reproducible one-million and ten-million-record verification.

## Current semantics

- Hash indexes support scalar exact-value lookup and bounded membership over exact `T[]` and
  `List<T>` properties.
- Range indexes support exact-value and bounded range lookup.
- In integrated mode, a record and all of its local index entries share one journal operation and one
  manifest commit point.
- Index-only mode exposes a keyed writer which extracts marked properties from the supplied state but
  never serializes or retains the payload. Its blind replacements are last-arrival-wins; the caller
  owns cross-store consistency.
- Steady mutations rewrite one bounded journal segment and one constant-size manifest instead of the whole partition.
- A fixed journal ring and a hard replay limit bound recovery work; a mutation is backpressured when compaction cannot make room.
- Compaction publishes immutable whole-partition snapshots through two generation-fenced physical slots.
- Mutations within a partition are serialized by one Orleans grain activation.
- Integrated layout formats 4 and 5 map a fixed per-namespace virtual-slot space to physical owners. Version 5
  keeps the same routing identity and adds the durable managed-schema fence. Version-3 layouts
  first adopt the format-4 identity map without moving records; a separately enabled protocol can
  then move one slot at a time under durable epoch and visibility fences.
- Index-only namespaces use layout and persistence format 6 as a durable mode and downgrade fence;
  full and index-only providers can coexist only under different provider names.
- Routed point operations carry their virtual slot and layout epoch. Each bounded query page fans out
  to every distinct current owner and returns a sorted, distinct `GrainId` prefix.
- A focused `IQueryable<TState>` layer supports indexed comparisons, exact collection `Contains`,
  bounded scalar `WhereIn`, and boolean AND/OR composition.
- Every partition evaluates at most one configured logical-work slice in a non-reentrant turn;
  continuations are stateless, authenticated-encrypted, and bound to the query and routing epoch.
- Scalar-index-only facet terminals provide value-ordered distinct pages, explicit exact or bounded-
  approximate top-N counts, and exact minimum/maximum values without loading record payloads into
  the caller.
- The keyed admin client explicitly enables movement, plans/advances/executes/aborts one durable
  slot move, and computes deterministic minimal-churn rebalance steps. Core storage never starts an
  automatic balancing policy.
- A quiesced, resumable schema rebuild binds index scopes and records to a deterministic generation.
  Its first use enables a provider-wide, one-way capability: thereafter every provider state must
  be declared on every silo, every direct query client must declare each state it uses, and
  current schema-unaware calls fail closed. Older binaries are excluded by the mandatory
  homogeneous rollout because an entirely local contradiction issues no RPC to fence.
- The physical persistence provider remains replaceable through Orleans configuration.

The journal removes partition-sized writes from the mutation path, but it does not make the current
layout an unbounded database. The fixed storage envelope caps individual records and index entries,
one journal entry at 5 MiB of canonical data, and a segment at 64 entries and 320 MiB of aggregate
canonical entry data. Canonical bytes are deterministic logical accounting, not Orleans transport
or physical-provider bytes. A partition activation still loads its whole accepted snapshot into
memory, and compaction still serializes that whole partition; the 1,000,000-record and 512 MiB
canonical snapshot ceilings are safety boundaries, not small latency or transient-memory bounds.
One collection membership scope contributes at most 64 unique canonical entries per record.
Range indexes use logarithmic bucket seeks and incremental bucket updates. One activation-local
ordered catalog/posting projection now serves paging, facets, and legacy materialized queries.
Its postings use compact local record references and inline the normal one-record group; these
references never enter persistence, public results, or continuations. Live updates remain
logarithmic.
The exact accounting, failure behavior, and pre-1.0 rollout procedure are documented in the
[storage capacity envelope](docs/storage-capacity-limits.md).
Every query page still contacts every distinct current owner. Moving slots can change that owner set,
but it does not make a query local or provide a snapshot across partitions. Text search, including
`StartsWith`, composite indexes, and arbitrary LINQ beyond the documented focused subset are not
implemented. Movement pages are count-bounded and byte-targeted, but activation recovery and
compaction still have honest whole-partition boundaries.

## Example

Register one physical provider and the searchable provider in the silo:

```csharp
siloBuilder.AddMemoryGrainStorage(
    SearchableStorageConstants.PhysicalStorageProviderName);

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

siloBuilder.AddSearchableStorageState<VacancyState>(
    "Searchable",
    "vacancy",
    applicationSchemaVersion: 1);
```

The continuation key is an application secret containing exactly 32 decoded bytes. Give the same
provider-scoped key id and material to every silo and external Orleans client which can create or
resume pages; do not commit the material, store it in provider state, or generate a different key in
each process. See [key rotation and rollout](docs/bounded-query-contract.md#continuation-token).

`PartitionCount` seeds the initial physical owners. On first format-4 initialization, the provider
rounds `VirtualSlotTargetCount` up to the smallest multiple of that count and persists the exact
virtual-slot count plus its identity assignment. The target is an initialization seed: changing it
later cannot change an existing map, although it must remain a valid configured value. The exact map
is capped at 262,144 slots. `PartitionCount` remains the immutable initial owner count even after a
rebalance introduces higher owner indices. `JournalSegmentCapacity` and
`MaximumJournalReplayEntries` still require migration to change. `CompactionThreshold` and movement
page limits are operational settings within their documented bounds.

Register every state name which uses the provider, even a state with no indexed properties. In
integrated mode, before first adoption—or after changing an indexed property, its index name or
kind, CLR domain, codec meaning, or the application-owned version—quiesce searchable traffic,
deploy the same declarations everywhere, and
run `ISearchableStorageAdminClient.RebuildIndexSchemaAsync<VacancyState>("vacancy", 1,
cancellationToken)`. Registration is deliberately fail-closed, not a compatibility no-op: that
state's writes, clears, and queries require its declared fingerprint to be active. The first rebuild
can initialize a fresh layout; no dummy read is needed. It also durably enables persistence format 5
across the current owners and scans records only for the requested state. It does not activate other
registered states: rebuild and verify every registered state in the same first-adoption maintenance
window before resuming provider traffic or movement. That provider-wide capability cannot be
disabled. Older binaries and clients are unsupported and must be excluded by the homogeneous
restart because a locally answered contradiction has no RPC to fence. Updated managed writes,
clears, queries, pages, and facets remain blocked until their fingerprint is active. Point reads do
not interpret indexes. Index-only mode uses the same first-adoption gate in format 6, but a later
incompatible declaration requires a new namespace and authoritative replay rather than an in-place
rebuild. Renaming the Orleans persistent state name is a data migration, not a schema
rebuild: old records remain under the old catalog and record keys and cannot be discovered by
rebuilding the new name.
Page and distinct-facet continuations created under an older generation are invalid after the new
fingerprint becomes active; restart those traversals from their first page.
See the [managed index schema runbook](docs/index-schema-lifecycle.md) before adoption.

The memory provider is only an example. PostgreSQL, Redis, Azure Blob Storage, or another Orleans persistence provider can be registered under `SearchableStorageConstants.PhysicalStorageProviderName` without changing application grains. See [physical backend configuration](docs/backends.md) for complete provider examples and operational prerequisites.

Mark indexed state properties and use the searchable provider with normal Orleans persistent state:

```csharp
[GenerateSerializer]
public sealed class VacancyState
{
    [Id(0)]
    [SearchableIndex(SearchableIndexKind.Hash)]
    public string City { get; set; } = string.Empty;

    [Id(1)]
    [SearchableIndex(SearchableIndexKind.Range)]
    public int Salary { get; set; }
}

public sealed class VacancyGrain(
    [PersistentState("vacancy", "Searchable")]
    IPersistentState<VacancyState> state) : Grain
{
    public Task SaveAsync(VacancyState value)
    {
        state.State = value;
        return state.WriteStateAsync();
    }
}
```

Alternatively, keep payloads in another Orleans provider or application database and register a
payload-free index:

```csharp
siloBuilder.AddSearchableIndex("CompanyIndex", options =>
{
    options.PartitionCount = 32;
});
siloBuilder.AddSearchableStorageState<CompanyState>("CompanyIndex", "company");

public sealed class CompanyGrain(
    [PersistentState("company", "ApplicationState")]
    IPersistentState<CompanyState> state,
    [FromKeyedServices("CompanyIndex")]
    ISearchableStorageIndexWriter index) : Grain
{
    public async Task SaveAsync(CompanyState value)
    {
        state.State = value;
        await state.WriteStateAsync();
        await index.UpsertAsync("company", this.GetGrainId(), value);
    }
}
```

`AddSearchableIndex` registers keyed writer, query, and admin services but no `IGrainStorage`.
The writer extracts only `[SearchableIndex]` values from the same `CompanyState` instance; it does
not serialize the object. The payload write and index call are not one transaction, so the caller
must serialize delivery per key or use its own outbox/reconciliation policy. Queries return
`GrainId` values for caller-owned hydration. For an external Orleans client, apply the same
`AddSearchableIndex` and state declarations to its `IServiceCollection` and resolve keyed query or
admin services; the public direct client constructors remain integrated-only. Read the complete
[index-only mode contract](docs/index-only-mode.md) before choosing this ownership model.

Resolve the named query client inside the silo so it shares the provider configuration. Build a
deferred predicate and follow its continuation until the final page:

```csharp
var search = services.GetRequiredKeyedService<ISearchableStorageQueryClient>("Searchable");

var minimumSalary = 5;
var maximumSalary = 8;
var query = search
    .Query<VacancyState>("vacancy")
    .Where(state =>
        state.City == "Helsinki" &&
        state.Salary > minimumSalary &&
        state.Salary <= maximumSalary);

string? continuation = null;
do
{
    var page = await query.ToGrainIdPageAsync(
        new SearchableStorageQueryPageRequest(128, continuation),
        cancellationToken);
    foreach (var grainId in page.Items)
    {
        // Hydrate only the page the application is ready to consume.
    }

    continuation = page.ContinuationToken;
}
while (continuation is not null);
```

For bounded membership, declare only an exact one-dimensional `T[]` or exact `List<T>` property and
use a Hash index. `T` must be one of the supported scalar types (including supported nullable value
types):

```csharp
[GenerateSerializer]
public sealed class CandidateState
{
    [Id(0)]
    [SearchableIndex(SearchableIndexKind.Hash)]
    public string?[] Skills { get; set; } = [];

    [Id(1)]
    [SearchableIndex(SearchableIndexKind.Hash)]
    public List<int?> AudienceIds { get; set; } = [];

    [Id(2)]
    [SearchableIndex(SearchableIndexKind.Hash)]
    public string City { get; set; } = string.Empty;
}

var bySkill = search
    .Query<CandidateState>("candidate")
    .Where(state => Enumerable.Contains(state.Skills, "C#"));

var byAudience = search
    .Query<CandidateState>("candidate")
    .Where(state => state.AudienceIds.Contains(42));

IReadOnlyList<string> selectedCities = ["Haifa", "Tel Aviv"];
var byCity = search
    .Query<CandidateState>("candidate")
    .WhereIn(state => state.City, selectedCities);
```

Register `CandidateState` for the `candidate` state name on every participant and complete its
managed-schema rebuild before executing these queries.
A null collection contributes no entries. Null elements are omitted, an empty string is a normal
indexed value, and duplicate elements are canonically deduplicated and sorted before admission. A
managed write runs the active-schema gate before it reads indexed properties, so that gate can
consult schema/layout authority first. After the gate succeeds, more than 64 unique members in one
scope fails before any partition mutation or WAL authority. In an unmanaged namespace with no
registrations, the same first-write rejection occurs before layout initialization or routing.
Collection properties are predicates only: direct `FindAsync`/`RangeAsync`, `WhereIn`, and facet
selectors stay scalar.

Use the same deferred predicate for facets over one indexed property:

```csharp
var filtered = search
    .Query<VacancyState>("vacancy")
    .Where(state => state.Salary >= minimumSalary);

var cities = await filtered.ToDistinctFacetValuePageAsync(
    state => state.City,
    new SearchableStorageFacetPageRequest(128, continuation),
    cancellationToken);

var topCities = await filtered.ToFacetValueCountsAsync(
    state => state.City,
    new SearchableStorageFacetRequest(
        topN: 10,
        accuracy: SearchableStorageFacetAccuracy.Exact),
    cancellationToken);

var salaryExtrema = await filtered.ToFacetMinMaxAsync(
    state => state.Salary,
    cancellationToken);
```

Distinct values use canonical indexed-value order; follow their opaque continuation until it is
null. Top-N accuracy is deliberately explicit. Every returned count is exact, but an approximate
result can omit a winner: inspect `IsExact` and the inclusive `MaximumOmittedCount` certificate
instead of treating its items as a proven global ranking. Exact top-N and min/max are all-or-throw
under aggregate work, item, byte, and round ceilings. Facets exclude nulls because null values are
not indexed. They are not a cross-partition snapshot; one multi-turn attempt pins each owner to one
data version and restarts once if it changes, while a later distinct continuation remains weakly
consistent like id paging.

The keyed admin client exposes the persisted routing summary without returning its mutable
assignment array:

```csharp
var admin = services.GetRequiredKeyedService<ISearchableStorageAdminClient>("Searchable");
var layout = await admin.GetLayoutAsync(cancellationToken);
```

The result reports the routing epoch, exact virtual-slot count, initial physical count, movement
state, active move summary, and slot count per current owner. It is `null` until a storage operation
or the first managed-schema rebuild initializes the provider namespace. Movement is explicit and
resumable:

```csharp
var enabled = await admin.EnableMovementAsync(cancellationToken);
var move = await admin.PlanMoveAsync(slot: 17, targetPartitionIndex: 40, cancellationToken);

// Exactly one durable transition or bounded record-page payload:
move = await admin.AdvanceMoveAsync(move.MoveId, cancellationToken);

// Client-side loop over the same resumable turns:
move = await admin.ExecuteMoveAsync(move.MoveId, cancellationToken);
```

`AbortMoveAsync` rolls back only before ownership commits. `PlanRebalanceAsync` reports one next
minimal-churn move without persisting an unbounded bulk plan, and `ExecuteRebalanceAsync` repeats
those explicit single moves. See the [live-movement runbook](docs/live-movement.md) before enabling
the protocol; it requires quiescence and a coordinated restart of every participant.

Movement byte targets and progress counters use the protocol's deterministic canonical encoding.
They shape page construction (with the documented oversize-singleton exception) and make replay
accounting stable; they are not measurements or limits of Orleans wire frames, network traffic, or
physical-provider bytes.

The focused query layer accepts direct indexed-property comparisons using `==`, `<`, `<=`, `>`,
and `>=`. Comparisons can be combined with `&&` and `||`, and additional `Where` calls are treated
as `&&`. The other side of a comparison must be a constant or captured value; method calls and
calculations inside the expression are rejected. Relational operators require a range index.
The two collection exceptions are exact `Enumerable.Contains<T>(state.Array, value)` for a direct
SZ-array property and exact `state.List.Contains(value)` for a direct `List<T>` property. Both
require a Hash membership index and a closed scalar operand of the exact element type. Reversing the
shape to `values.Contains(state.Property)`, using an interface or nested collection, or selecting a
collection for `WhereIn` is not supported.
`WhereIn` accepts one direct scalar Hash or Range property plus at most 64 raw non-null values. It
snapshots the input immediately, then the built-in translator canonically deduplicates and sorts the
values into existing exact/OR nodes; an empty input is an empty plan. It adds no general LINQ or
client-side fallback.
Compiler-generated integral and enum promotions are accepted only when they preserve every indexed
value exactly. Conversions of the indexed property which box, narrow, invoke user code, or lose
numeric information are rejected instead of being translated with different CLR semantics. Built-in
conversions on the closed value side are interpreted; user-defined value conversions are rejected.
Supported query traversal and semantic and serialized plans are limited to 64 levels and 256
visited nodes, while property and state-parameter conversion chains are independently capped at 64.
`ToGrainIdPageAsync` and the compatibility `ToGrainIdsAsync` terminal are the execution operations:
synchronous enumeration, projections, ordering, grouping, joins, and other general LINQ operators
throw `NotSupportedException` with a diagnostic. Each page uses a deterministic partition-work,
item, and byte budget. The coordinator merges only a globally safe canonical prefix. A non-terminal
page can therefore be short or empty; only a null continuation means completion. Pages are weakly
consistent rather than a distributed snapshot, so concurrent writes have the precise consequences
documented in the paging contract.

`ToDistinctFacetValuePageAsync`, `ToFacetValueCountsAsync`, and `ToFacetMinMaxAsync` are separate
indexed-only terminals. They reject selectors which do not name one declared hash or range index.
Collection membership indexes cannot be facet selectors.
The built-in provider bounds candidate metadata, exact-count probes, filtered predicate work, and
retry rounds independently from legacy result collection. External LINQ providers opt in through
`ISearchableStorageFacetQueryProvider`; the existing async and paging provider interfaces remain
source and binary independent.

The [1.0 product and query contract](docs/one-zero-contract.md) defines the public product boundary.
The [bounded query and paging contract](docs/bounded-query-contract.md) provides the normative
protocol detail for implemented work accounting, ordered partition prefixes, coordinator merge,
continuation protection, weak consistency, and rollout rules. Continuations contain no
activation-local cursor or buffered result state.

`FindAsync`, `RangeAsync`, and `ToGrainIdsAsync` remain available as all-results compatibility APIs
for known-small results. The built-in client implements them by collecting the same bounded pages
under aggregate work, item, byte, and round ceilings. It returns the complete sorted result or throws
`SearchableStorageQueryLimitExceededException`; it never truncates or falls back to the old
unbounded RPC. `FindAsync` and `RangeAsync` remain on
`ISearchableStorageClient`; the focused LINQ surface is opt-in through
`ISearchableStorageQueryClient : ISearchableStorageClient`. Both keyed registrations resolve the
same built-in client instance. An alternative `IQueryable` implementation can use
`ToGrainIdsAsync` by exposing an `IQueryProvider` which also implements the public
`ISearchableStorageAsyncQueryProvider` terminal contract.

The cancellation token cancels the caller's wait. Orleans partition calls already in flight cannot
be canceled by that local token, so the client observes their eventual completion while returning
cancellation promptly to the caller. No partial page, partial compatibility result, or advanced
continuation is returned.

`SearchableStorageClient` can also be constructed from an `IGrainFactory`, provider name, and initial
partition count. The overload accepting `SearchableStorageQueryOptions` supplies the same page
limits and continuation key ring used by external Orleans clients. Once managed schemas are enabled,
use the overload which also accepts a fully configured `SearchableStorageSchemaRegistry`; the client
captures that registry at construction, and every queried state must match the silo registration.
It reads the persisted virtual map before fan-out. An epoch or ownership mismatch
discards a first-page attempt, refreshes the shared layout cache, and retries once. A resumed page
is pinned to its authenticated layout and throws `SearchableStorageStaleContinuationTokenException`
instead of moving the old frontier into a new epoch; results from different epochs are never merged.

The bounded query and facet RPCs are additive but replace the built-in client's legacy unbounded
query path. For a query-protocol-only rollout, quiesce searchable query traffic, deploy every
partition-hosting silo and query client, configure the same provider key ring wherever public paging
is used, and only then resume queries. Point reads, writes, and clears may continue only when no
layout, movement, or managed-schema migration is being performed. Mixed-version paging/facets and
fallback to an old array-returning RPC are unsupported. Existing continuations cannot be reused as
another response family.

Separately, adopting a version-3 layout into format 4 is not an online mixed-version rollout.
Quiesce searchable storage and query traffic, deploy this package to every silo and Orleans client,
verify that no version-3 process
remains, and keep traffic paused while one normal grain-state storage operation adopts each provider
namespace. Verify that the admin read succeeds for every persisted layout and reports epoch 1; the
admin path returns a snapshot only for routing-capable format 4, 5, or 6. At that point either resume
traffic with movement still disabled, or, if movement is being enabled in the same maintenance
window, keep traffic paused through the second gate. Query and admin reads
deliberately do not perform migration themselves. New routed methods are additive and legacy calls
remain placement-compatible with the epoch-1 identity map on updated processes, but a new storage
activation can otherwise reach an old silo which does not implement those methods. For the second
gate, quiesce traffic if it was resumed, deploy/restart the movement-capable package everywhere, call
`EnableMovementAsync`, require movement protocol version 1 from the admin read, and only then resume.
Once enabled, old placement-only calls are rejected and rolling an older binary back into that
namespace is unsupported.

An integrated managed-schema rebuild can perform that same version-3 layout adoption as its first
quiesced step, so schema adoption does not require a preceding dummy state operation. Its owner
sweep then upgrades supported partition-persistence format 3 or 4 directly to format 5. A fresh
index-only namespace instead initializes format 6 and activates its empty schema before replay.

The provider name identifies a storage namespace. Using another name selects a separate, initially
empty namespace, so renaming a provider requires an explicit migration. The initial
`PartitionCount`, journal segment capacity, maximum replay entries, and layout/persistence formats
are validated within that namespace and must not change without migration. Index names, kinds,
property types, codec versions, and the application schema version are managed persisted schema.
Adding or changing one does not become queryable until the registered generation is rebuilt and
active. Once any state adopts managed schemas, every state using that provider must be registered
on every participant. Null property values are not indexed. Indexed `DateTime`
values must use `DateTimeKind.Utc`. Facet text values are limited to 16,384 bytes of valid strict
UTF-8. The write path does not impose that newer wire constraint, so applications using string or
`char` facets must validate it before writes and rewrite pre-existing overlong or unpaired-surrogate
values; a facet which reaches one throws `SearchableStorageQueryLimitExceededException` without a
partial result rather than truncating or skipping it.

Integrated layout formats 4 and 5 store the same virtual routing identity independently from
integrated partition persistence formats 3, 4, and 5. Layout format 5 appends the provider schema capability and
per-rebuild maintenance intent.
A valid format-3 layout is upgraded in place with one layout compare-and-swap; the seeded identity
map is mathematically equivalent to the old modulo placement for every supported initial partition
count, including non-powers-of-two. No partition manifest, journal segment, snapshot, record, or index
is rewritten by layout adoption. Partition persistence format 4 retains the small manifest, bounded
journal ring, and two whole-partition snapshot slots while adding movement state and lossless
snapshots. Format 5 adds the one-way managed-schema capability and per-record fingerprints. A
quiesced schema-adoption sweep can upgrade a supported format-3 or format-4 owner directly to format
5; a movement-only sweep upgrades format 3 to format 4. Formats 1 and 2 still require an explicit
migration or complete rewrite and are rejected rather than read as fresh state. Backups and
retention must include the per-state `index-schema` control documents as well as layout and
partition data.

Index-only layout and partition-persistence format 6 preserve the same slot-placement algorithm but
use a distinct routing/continuation fingerprint and require payload-free records. Format 6 is a
downgrade fence, not an in-place conversion from integrated formats. An incompatible active
index-only schema requires a new provider namespace and replay from the application's authoritative
payload store.

Facet support does not change a durable record, journal, manifest, snapshot, layout, or write-path
format. On activation, hash scopes now derive the same balanced, canonical value projection already
used for range scopes instead of retaining unordered hash-bucket enumeration as their facet source.
Rebuild and every committed incremental mutation update that single lookup and ordering projection.
Value seek and live add/remove are logarithmic in the number of distinct values;
candidate nomination reads bucket cardinality metadata and does not enumerate posting members.

Index declarations and value accessors are resolved through a cached [PolyType](https://github.com/eiriktsarpalis/PolyType) runtime type model. Complete index scopes are cached per state name, so steady-state writes do not rebuild persisted type identities through reflection. Collection, scalar, and other non-object state shapes remain valid storage values and simply contribute no index entries. Applications only use `SearchableIndexAttribute`; no PolyType attributes or generated witness types are required. This project uses PolyType's reflection provider and does not support Native AOT or trimming.

## Run the API sample

The runnable sample co-hosts an ASP.NET Core minimal API and an Orleans silo:

```bash
dotnet run --project samples/Orleans.SearchableStorage.ApiSample
```

Use [`requests.http`](samples/Orleans.SearchableStorage.ApiSample/requests.http) to write vacancies,
read one by id, execute the `IQueryable` layer, hydrate one bounded id page through application
grains, inspect/resume the managed schema, inspect/enable
movement, manually plan or advance a move/rebalance, and remove a record. The
[sample walkthrough](samples/Orleans.SearchableStorage.ApiSample/README.md)
follows each request from HTTP through the application grain, query plan, searchable provider,
storage-partition grain, and physical provider. The sample registers `VacancyState` and bootstraps
its managed format-5 generation before accepting traffic. That startup shortcut is valid only for
its fresh, single-process, in-memory namespace; production adoption follows the quiesced runbook.

The one-process topology and in-memory physical storage keep the sample easy to run; neither is a library requirement.

## Performance harness

The repository includes process-local BenchmarkDotNet cases and a native Orleans load driver with
deterministic, versioned scenario files. The driver supports a plain-Orleans point-operation baseline,
the searchable provider, closed- and open-loop scheduling, raw mergeable HDR histograms, embedded
Memory smoke runs, and separate Crank silo/client jobs. Pull requests use only functional smoke gates;
no shared-runner latency value is treated as a regression threshold or scalability result.

See the [benchmarking guide](docs/benchmarks.md) for commands, provenance and comparison rules,
execution tiers, current limitations, and why million/billion-record claims require dedicated
capacity runs rather than extrapolation from a small test.

## Backend validation

The test suite defines one reusable storage contract. It runs through a two-silo `TestCluster`,
forces storage-partition reactivation, and injects failures before commit and after commit but before
acknowledgement across journal, manifest, snapshot publication, and cleanup transitions. Recovery is
checked immediately without manually deactivating the failed grain first. The same Memory,
PostgreSQL, Redis, and Azure Blob contract performs a live move under concurrent routed writes and
continuous exact query/facet reads, then verifies final point, index, facet, authority, and
reactivation state. The API sample is also exercised through an in-process HTTP server.

- In-memory: Orleans `Microsoft.Orleans.Persistence.Memory`.
- PostgreSQL: Orleans `Microsoft.Orleans.Persistence.AdoNet` with Npgsql and the official Orleans schema.
- Redis: Orleans `Microsoft.Orleans.Persistence.Redis`.
- Azure Blob Storage: Orleans `Microsoft.Orleans.Persistence.AzureStorage`, exercised against Azurite in CI.

The vendored Orleans 10.2.2 PostgreSQL scripts retain their source and MIT license headers. CI verifies their complete file hashes against `eng/orleans-sql.sha256` before the tests run.

The external backend contract is opt-in locally and runs on every pull request in a dedicated CI job. Start the pinned containers and run it with:

```bash
docker compose --file tests/backends.compose.yml up --detach --wait
ORLEANS_SEARCHABLE_STORAGE_RUN_BACKEND_TESTS=true \
  dotnet test tests/Orleans.SearchableStorage.Tests \
  --filter "Category=BackendIntegration"
docker compose --file tests/backends.compose.yml down --volumes
```

Connection strings can be overridden for an existing test environment. See the [backend guide](docs/backends.md) and [testing strategy](docs/testing.md).

Every pull request requires a dedicated test-sufficiency review in addition to general and domain-specific reviews. See the [testing strategy](docs/testing.md) for the behavioral checklist and test layers.

## Build

The repository pins .NET 10 in `global.json` and Orleans dependencies centrally.

```bash
dotnet restore Orleans.SearchableStorage.slnx
dotnet build Orleans.SearchableStorage.slnx --no-restore
dotnet test Orleans.SearchableStorage.slnx --no-build --collect "XPlat Code Coverage"
```

## Prior art

This project revisits ideas from the archived [Orleans.Indexing 1.5 implementation](https://github.com/OrleansContrib/Orleans.Indexing-1.5) and the paper [Indexing in an Actor-Oriented Database](https://www.cidrdb.org/cidr2017/papers/p29-bernstein-cidr17.pdf). The current design is a new storage-provider implementation for modern Orleans rather than a port of the archived API.

## Contributing and license

See [CONTRIBUTING.md](CONTRIBUTING.md) for engineering and review rules and [docs/architecture.md](docs/architecture.md) for the internal design and consistency boundaries. The project is licensed under the [MIT License](LICENSE).
