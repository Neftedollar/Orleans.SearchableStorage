# Orleans.SearchableStorage

Orleans-native persistent grain storage with secondary indexes.

The project is an early vertical slice. It implements an `IGrainStorage` provider whose records and local index entries are owned by Orleans grains and persisted through another Orleans storage provider. Applications continue to use `IPersistentState<T>` and add searchable semantics by marking state properties.

## Current semantics

- Hash indexes support exact-value lookup.
- Range indexes support exact-value and bounded range lookup.
- A record and all of its local index entries are committed by one physical `IPersistentState` write.
- Mutations within a partition are serialized by one Orleans grain activation.
- Persisted layout metadata rejects a mismatched storage-format version or partition count within one provider namespace before incomplete results can be returned.
- Queries fan out over a fixed number of partitions and return matching `GrainId` values.
- A focused `IQueryable<TState>` layer supports indexed comparisons combined with boolean AND and OR.
- One complete boolean query plan is evaluated in one non-reentrant call per partition.
- The physical persistence provider remains replaceable through Orleans configuration.

The current implementation persists one snapshot per partition. This keeps the consistency boundary explicit and testable, but it is not yet suitable for large production datasets because each mutation rewrites that partition. Range queries use binary search to seek to a lower bound when one is present and then enumerate only the requested ordered window. Queries do not provide a snapshot across partitions. Text search, composite indexes, arbitrary LINQ, online repartitioning, and backend integration suites are not implemented yet.

## Example

Register one physical provider and the searchable provider in the silo:

```csharp
siloBuilder.AddMemoryGrainStorage(
    SearchableStorageConstants.PhysicalStorageProviderName);

siloBuilder.AddSearchableGrainStorage(
    "Searchable",
    options => options.PartitionCount = 32);
```

The memory provider is only an example. PostgreSQL, Redis, or another Orleans persistence provider can be registered under `SearchableStorageConstants.PhysicalStorageProviderName` without changing application grains.

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

The focused query layer accepts direct indexed-property comparisons using `==`, `<`, `<=`, `>`,
and `>=`. Comparisons can be combined with `&&` and `||`, and additional `Where` calls are treated
as `&&`. The other side of a comparison must be a constant or captured value; method calls and
calculations inside the expression are rejected. Relational operators require a range index.
`ToGrainIdsAsync` is the only execution operation: synchronous enumeration, projections, ordering,
grouping, joins, and other general LINQ operators throw `NotSupportedException` with a diagnostic.
Execution sends the complete translated predicate to each partition once. AND and OR are evaluated
against one serially consistent view inside that partition, then the client merges the partition-local
results into a sorted, distinct list. The merge is not a snapshot across partitions.

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

`SearchableStorageClient` can also be constructed from an `IGrainFactory`, provider name, and partition count. Its partition count and storage-format version are validated against the persisted layout; a mismatch throws instead of returning partial results.

Before using the `IQueryable` surface during an upgrade, deploy this package version to every silo
and Orleans client which can execute searches. The existing bounded `RangeAsync` wire message keeps
its required lower and upper fields, while the new nullable open-bound plan is a separate,
non-persisted protocol message. Existing direct-query consumers can continue resolving
`ISearchableStorageClient` without implementing or depending on the new query surface.

The provider name identifies a storage namespace. Using another name selects a separate, initially empty namespace, so renaming a provider requires an explicit migration. `PartitionCount` and storage-format version are validated within that namespace and must not change without migration. Index names, kinds, and property types are also persisted schema: adding an index does not backfill existing records, and changing or renaming one requires an explicit rewrite or migration. Null property values are not indexed. Indexed `DateTime` values must use `DateTimeKind.Utc`.

Storage format version 2 identifies state types recursively from generic type definitions and their arguments. Assembly simple names, cultures, and public-key tokens participate in the identity, but assembly versions do not, so routine application or dependency version changes cannot hide otherwise compatible generic-state indexes. Existing version 1 namespaces require migration or a complete record rewrite; the runtime rejects their persisted layout instead of returning incomplete results.

Index declarations and value accessors are resolved through a cached [PolyType](https://github.com/eiriktsarpalis/PolyType) runtime type model. Complete index scopes are cached per state name, so steady-state writes do not rebuild persisted type identities through reflection. Collection, scalar, and other non-object state shapes remain valid storage values and simply contribute no index entries. Applications only use `SearchableIndexAttribute`; no PolyType attributes or generated witness types are required. This project uses PolyType's reflection provider and does not support Native AOT or trimming.

## Run the API sample

The runnable sample co-hosts an ASP.NET Core minimal API and an Orleans silo:

```bash
dotnet run --project samples/Orleans.SearchableStorage.ApiSample
```

Use [`requests.http`](samples/Orleans.SearchableStorage.ApiSample/requests.http) to write vacancies, read one by id, execute the `IQueryable` layer over the city and salary indexes, and remove a record. The [sample walkthrough](samples/Orleans.SearchableStorage.ApiSample/README.md) follows each request from HTTP through the application grain, query plan, searchable provider, storage-partition grain, and physical provider.

The one-process topology and in-memory physical storage keep the sample easy to run; neither is a library requirement.

## Backend validation

The test suite defines one reusable storage contract. It currently runs against Orleans in-memory persistence configured with `JsonGrainStorageSerializer` through a two-silo `TestCluster`, forces storage-partition reactivation, and injects failures before commit and after commit but before acknowledgement. The API sample is also exercised through an in-process HTTP server.

- In-memory: implemented in the regular test suite.
- PostgreSQL: required integration target.
- Redis: required integration target.
- Azure Blob Storage or an S3-compatible backend: planned for a separately configured integration environment.

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
