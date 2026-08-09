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
- The physical persistence provider remains replaceable through Orleans configuration.

The current implementation persists one snapshot per partition. This keeps the consistency boundary explicit and testable, but it is not yet suitable for large production datasets because each mutation rewrites that partition. Queries also do not provide a snapshot across partitions. Text search, composite indexes, a query language, online repartitioning, and backend integration suites are not implemented yet.

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

Resolve the named query client inside the silo so it shares the provider configuration:

```csharp
var search = services.GetRequiredKeyedService<ISearchableStorageClient>("Searchable");

var inHelsinki = await search.FindAsync<VacancyState, string>(
    "vacancy",
    state => state.City,
    "Helsinki");

var salaryRange = await search.RangeAsync<VacancyState, int>(
    "vacancy",
    state => state.Salary,
    lowerBound: 5,
    upperBound: 8,
    includeLowerBound: false);
```

`SearchableStorageClient` can also be constructed from an `IGrainFactory`, provider name, and partition count. Its partition count and storage-format version are validated against the persisted layout; a mismatch throws instead of returning partial results.

The provider name identifies a storage namespace. Using another name selects a separate, initially empty namespace, so renaming a provider requires an explicit migration. `PartitionCount` and storage-format version are validated within that namespace and must not change without migration. Index names, kinds, and property types are also persisted schema: adding an index does not backfill existing records, and changing or renaming one requires an explicit rewrite or migration. Null property values are not indexed. Indexed `DateTime` values must use `DateTimeKind.Utc`.

## Run the API sample

The runnable sample co-hosts an ASP.NET Core minimal API and an Orleans silo:

```bash
dotnet run --project samples/Orleans.SearchableStorage.ApiSample
```

Use [`requests.http`](samples/Orleans.SearchableStorage.ApiSample/requests.http) to write vacancies, read one by id, search the hash index by city, search the range index by salary, and remove a record. The [sample walkthrough](samples/Orleans.SearchableStorage.ApiSample/README.md) follows each request from HTTP through the application grain, searchable provider, storage-partition grain, and physical provider.

The one-process topology and in-memory physical storage keep the sample easy to run; neither is a library requirement.

## Backend validation

The test suite defines one reusable storage contract. It currently runs against Orleans in-memory persistence configured with `JsonGrainStorageSerializer` through a two-silo `TestCluster`, forces storage-partition reactivation, and injects failures before commit and after commit but before acknowledgement.

- In-memory: implemented in the regular test suite.
- PostgreSQL: required integration target.
- Redis: required integration target.
- Azure Blob Storage or an S3-compatible backend: planned for a separately configured integration environment.

## Build

The repository pins .NET 10 in `global.json` and Orleans dependencies centrally.

```bash
dotnet restore Orleans.SearchableStorage.slnx
dotnet build Orleans.SearchableStorage.slnx --no-restore
dotnet test Orleans.SearchableStorage.slnx --no-build
```

## Prior art

This project revisits ideas from the archived [Orleans.Indexing 1.5 implementation](https://github.com/OrleansContrib/Orleans.Indexing-1.5) and the paper [Indexing in an Actor-Oriented Database](https://www.cidrdb.org/cidr2017/papers/p29-bernstein-cidr17.pdf). The current design is a new storage-provider implementation for modern Orleans rather than a port of the archived API.

## Contributing and license

See [CONTRIBUTING.md](CONTRIBUTING.md) for engineering and review rules and [docs/architecture.md](docs/architecture.md) for the internal design and consistency boundaries. The project is licensed under the [MIT License](LICENSE).
