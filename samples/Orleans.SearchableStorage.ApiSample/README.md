# API sample

This sample co-hosts an ASP.NET Core minimal API and one Orleans silo. It is intentionally a single process so the storage and query flow can be inspected without deploying a cluster.

Run it from the repository root:

```bash
dotnet run --project samples/Orleans.SearchableStorage.ApiSample
```

The API listens on `http://localhost:5000`. Open [`requests.http`](requests.http) in Visual Studio, Rider, or an editor with HTTP-client support and execute the requests from top to bottom.

## Endpoints

| Method | Path | Purpose |
| --- | --- | --- |
| `PUT` | `/vacancies/{id}` | Write a vacancy and update both indexes. |
| `GET` | `/vacancies/{id}` | Read one vacancy by grain key. |
| `DELETE` | `/vacancies/{id}` | Remove the vacancy and both index entries. |
| `GET` | `/vacancies/search/by-city?city=Helsinki` | Use the hash index for exact lookup. |
| `GET` | `/vacancies/search/by-salary?lower=5&upper=8&includeLower=false&includeUpper=false` | Use the range index with explicit bounds. |

## What happens on a write

1. The HTTP endpoint calls `IVacancyGrain` using the route id as its grain key.
2. `VacancyGrain` writes normal `IPersistentState<VacancyState>` state through the `Searchable` provider.
3. The provider serializes the state, uses its cached PolyType model to read the `[SearchableIndex]` values, and routes the record to one storage-partition grain.
4. The partition persists the record and its index entries together through the physical in-memory provider.
5. Search endpoints build a focused `IQueryable<VacancyState>` predicate and execute it with `ToGrainIdsAsync`.
6. The named `ISearchableStorageQueryClient` sends one complete boolean plan to every storage partition.
7. Each non-reentrant partition evaluates that plan in one turn; the client merges the final local results.

The city endpoint demonstrates an exact hash-index comparison. The salary endpoint builds two
`Where` clauses dynamically so all four inclusive/exclusive bound combinations use the same public
query surface. This `IQueryable` is a deliberately focused, partial query provider which will expand
in later releases: it returns grain ids, does not load state objects, and does not currently support
synchronous enumeration, projections, ordering, pagination, or result limits. Every execution fans
out to all storage partitions, so the sample endpoints are suitable for learning and bounded test
data rather than as an unmodified production search API.
ASP.NET Core passes request cancellation to both search handlers, and each handler forwards it to
`ToGrainIdsAsync`.

The sample's physical provider is in memory, so its data disappears when the process stops. A production host can register PostgreSQL, Redis, or another Orleans storage provider under `SearchableStorageConstants.PhysicalStorageProviderName`. Co-hosting HTTP and Orleans is only a sample convenience; the query client can also use an Orleans client from another process when configured with the same provider name and partition count.

The sample state needs no PolyType-specific annotations. Orleans.SearchableStorage uses PolyType's runtime reflection provider internally; Native AOT and trimming are outside the supported deployment model.
