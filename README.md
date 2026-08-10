# Orleans.SearchableStorage

Orleans-native persistent grain storage with secondary indexes.

The project is an early vertical slice. It implements an `IGrainStorage` provider whose records and local index entries are owned by Orleans grains and persisted through another Orleans storage provider. Applications continue to use `IPersistentState<T>` and add searchable semantics by marking state properties.

## Current semantics

- Hash indexes support exact-value lookup.
- Range indexes support exact-value and bounded range lookup.
- A record and all of its local index entries share one journal operation and one manifest commit point.
- Steady mutations rewrite one bounded journal segment and one constant-size manifest instead of the whole partition.
- A fixed journal ring and a hard replay limit bound recovery work; a mutation is backpressured when compaction cannot make room.
- Compaction publishes immutable whole-partition snapshots through two generation-fenced physical slots.
- Mutations within a partition are serialized by one Orleans grain activation.
- Layout format 4 maps an immutable per-namespace virtual-slot space to physical partitions. Version-3 layouts adopt the identity map in one layout write without moving records or rewriting partition persistence.
- Routed point operations carry their virtual slot and layout epoch. Queries fan out to each distinct current owner once and return matching `GrainId` values.
- A focused `IQueryable<TState>` layer supports indexed comparisons combined with boolean AND and OR.
- One complete boolean query plan is evaluated in one non-reentrant call per partition.
- The physical persistence provider remains replaceable through Orleans configuration.

The journal removes partition-sized writes from the mutation path, but it does not make the current
layout an unbounded database. A partition activation still loads its whole active snapshot into
memory, compaction still serializes that whole partition, and a configured segment capacity bounds
operations rather than bytes: one large record can still produce a large segment. Range indexes now
use logarithmic bucket seeks and incremental bucket updates. Every query still contacts every
distinct current owner—all initial partitions in this identity-map release—and `ToGrainIdsAsync`
currently has no `Take`, pagination, or result-size limit. Increasing the initial `PartitionCount`
spreads ownership, snapshots, and writes but does not reduce read fan-out. Queries
do not provide a snapshot across partitions. Text search, including `StartsWith`, composite indexes,
arbitrary LINQ beyond the documented focused subset, and live virtual-slot movement are not
implemented yet. This release establishes zero-movement virtual routing; its assignment map remains
the identity map until the separately reviewed move protocol lands.

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
    });
```

`PartitionCount` seeds the initial physical owners. On first format-4 initialization, the provider
rounds `VirtualSlotTargetCount` up to the smallest multiple of that count and persists the exact
virtual-slot count plus its identity assignment. The target is an initialization seed: changing it
later cannot change an existing map, although it must remain a valid configured value. The exact map
is capped at 262,144 slots. `PartitionCount`, `JournalSegmentCapacity`, and
`MaximumJournalReplayEntries` still require migration to change. `CompactionThreshold` is operational
and can be tuned between deployments, but must remain positive and no greater than the replay limit.

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

Resolve the named query client inside the silo so it shares the provider configuration. Build a
deferred predicate and execute it explicitly as a `GrainId` query:

```csharp
var search = services.GetRequiredKeyedService<ISearchableStorageQueryClient>("Searchable");

var minimumSalary = 5;
var maximumSalary = 8;
var matches = await search
    .Query<VacancyState>("vacancy")
    .Where(state =>
        state.City == "Helsinki" &&
        state.Salary > minimumSalary &&
        state.Salary <= maximumSalary)
    .ToGrainIdsAsync(cancellationToken);
```

The keyed admin client exposes the persisted routing summary without returning its mutable
assignment array:

```csharp
var admin = services.GetRequiredKeyedService<ISearchableStorageAdminClient>("Searchable");
var layout = await admin.GetLayoutAsync(cancellationToken);
```

The result reports the routing epoch, exact virtual-slot count, initial physical count, and slot
count per current owner. It is `null` until a storage operation initializes the provider namespace.

The focused query layer accepts direct indexed-property comparisons using `==`, `<`, `<=`, `>`,
and `>=`. Comparisons can be combined with `&&` and `||`, and additional `Where` calls are treated
as `&&`. The other side of a comparison must be a constant or captured value; method calls and
calculations inside the expression are rejected. Relational operators require a range index.
Compiler-generated integral and enum promotions are accepted only when they preserve every indexed
value exactly. Conversions of the indexed property which box, narrow, invoke user code, or lose
numeric information are rejected instead of being translated with different CLR semantics. Built-in
conversions on the closed value side are interpreted; user-defined value conversions are rejected.
Supported query traversal and semantic and serialized plans are limited to 64 levels and 256
visited nodes, while property and state-parameter conversion chains are independently capped at 64.
`ToGrainIdsAsync` is the only execution operation: synchronous enumeration, projections, ordering,
grouping, joins, and other general LINQ operators throw `NotSupportedException` with a diagnostic.
Execution sends the complete translated predicate to each distinct owner once. AND and OR are evaluated
against one serially consistent view inside that partition, then the client merges the partition-local
results into a sorted, distinct list. The merge is not a snapshot across partitions.
Because the result is currently unbounded, callers must use this API only where the expected match
set is operationally bounded by the application until a bounded result protocol is added.

`FindAsync` and `RangeAsync` remain available as lower-level compatibility APIs for callers which
already express one exact lookup or one bounded range directly. They remain on
`ISearchableStorageClient`; the focused LINQ surface is opt-in through
`ISearchableStorageQueryClient : ISearchableStorageClient`. Both keyed registrations resolve the
same built-in client instance. An alternative `IQueryable` implementation can use
`ToGrainIdsAsync` by exposing an `IQueryProvider` which also implements the public
`ISearchableStorageAsyncQueryProvider` terminal contract.

The cancellation token cancels the caller's wait. Orleans partition calls already in flight cannot
be canceled by that local token, so the client observes their eventual completion while returning
cancellation promptly to the caller.

`SearchableStorageClient` can also be constructed from an `IGrainFactory`, provider name, and initial
partition count. It reads the persisted virtual map before fan-out. An epoch or ownership mismatch
discards the complete attempt, refreshes the shared layout cache, and retries once; results from
different epochs are never merged.

This format transition is not an online mixed-version rollout. Quiesce searchable storage and query
traffic, deploy this package to every silo and Orleans client, verify that no version-3 process
remains, and only then resume traffic. The first storage operation can then adopt the layout. New
routed methods are additive and legacy calls remain placement-compatible with the epoch-1 identity
map on updated processes, but a new storage activation can otherwise reach an old silo which does
not implement those methods. Live ownership movement is not exposed by this release and requires a
separate coordinated all-v4 protocol gate.

The provider name identifies a storage namespace. Using another name selects a separate, initially
empty namespace, so renaming a provider requires an explicit migration. The initial
`PartitionCount`, journal segment capacity, maximum replay entries, and layout/persistence formats
are validated within that namespace and must not change without migration. Index names, kinds, and property types are also
persisted schema: adding an index does not backfill existing records, and changing or renaming one
requires an explicit rewrite or migration. Null property values are not indexed. Indexed `DateTime`
values must use `DateTimeKind.Utc`.

Layout format version 4 stores virtual routing independently from partition persistence format 3.
A valid format-3 layout is upgraded in place with one layout compare-and-swap; the seeded identity
map is mathematically equivalent to the old modulo placement for every supported initial partition
count, including non-powers-of-two. No partition manifest, journal segment, snapshot, record, or index
is rewritten. Partition persistence format 3 continues to use a small manifest, bounded journal ring,
and two snapshot slots. Existing format-1 and format-2 namespaces still require an explicit migration
or complete rewrite and are rejected rather than read as a fresh or partial namespace.

Index declarations and value accessors are resolved through a cached [PolyType](https://github.com/eiriktsarpalis/PolyType) runtime type model. Complete index scopes are cached per state name, so steady-state writes do not rebuild persisted type identities through reflection. Collection, scalar, and other non-object state shapes remain valid storage values and simply contribute no index entries. Applications only use `SearchableIndexAttribute`; no PolyType attributes or generated witness types are required. This project uses PolyType's reflection provider and does not support Native AOT or trimming.

## Run the API sample

The runnable sample co-hosts an ASP.NET Core minimal API and an Orleans silo:

```bash
dotnet run --project samples/Orleans.SearchableStorage.ApiSample
```

Use [`requests.http`](samples/Orleans.SearchableStorage.ApiSample/requests.http) to write vacancies,
read one by id, execute the `IQueryable` layer, inspect the persisted virtual routing summary, and
remove a record. The [sample walkthrough](samples/Orleans.SearchableStorage.ApiSample/README.md)
follows each request from HTTP through the application grain, query plan, searchable provider,
storage-partition grain, and physical provider.

The one-process topology and in-memory physical storage keep the sample easy to run; neither is a library requirement.

## Backend validation

The test suite defines one reusable storage contract. It runs through a two-silo `TestCluster`,
forces storage-partition reactivation, and injects failures before commit and after commit but before
acknowledgement across journal, manifest, snapshot publication, and cleanup transitions. Recovery is
checked immediately without manually deactivating the failed grain first. The API sample is also
exercised through an in-process HTTP server.

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
