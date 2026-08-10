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
| `GET` | `/vacancies/search/by-city/page?city=Helsinki&pageSize=1&continuation=...` | Traverse bounded exact-query pages. |
| `GET` | `/vacancies/search/by-salary?lower=5&upper=8&includeLower=false&includeUpper=false` | Use the range index with explicit bounds. |
| `GET` | `/storage/layout` | Read the persisted virtual-routing summary. |

## What happens on a write

1. The HTTP endpoint calls `IVacancyGrain` using the route id as its grain key.
2. `VacancyGrain` writes normal `IPersistentState<VacancyState>` state through the `Searchable` provider.
3. The provider serializes the state and uses its cached PolyType model to read the
   `[SearchableIndex]` values.
4. The provider initializes or loads layout format 4, derives the record's virtual slot from its
   Orleans grain hash, and sends the slot and routing epoch to the assigned physical partition.
5. The partition validates the grain, slot, epoch, and current owner before it reads record state or
   applies ETag logic. It then writes one bounded journal segment and advances a small manifest. The manifest
   write is the commit point for the record and every derived index entry.
6. The activation updates only the affected in-memory hash and range buckets. Automatic compaction
   periodically publishes a whole-partition snapshot through one of two fenced snapshot slots.
7. Search endpoints build a focused `IQueryable<VacancyState>` predicate. The paged city endpoint
   executes it with `ToGrainIdPageAsync`; the two compatibility endpoints use bounded
   all-results-or-exception collection.
8. The named `ISearchableStorageQueryClient` sends one bounded page request to every distinct owner
   in one layout snapshot. Each partition evaluates at most its configured logical-work, item, and
   byte limits in one non-reentrant turn.
9. The client merges a globally safe canonical prefix and protects the next boundary in an
   AES-256-GCM continuation. A routing mismatch discards the complete first-page attempt and retries
   once; a resumed page is tied to its original epoch and becomes stale instead of being upgraded.

The sample uses deliberately visible persistence settings in `Program.cs`: journal segments contain
at most 16 mutations, activation replay is capped at 256 committed mutations, and compaction is
requested after 64. Segment capacity and the replay limit are durable layout settings; changing
either requires migration. The compaction threshold is operational and can be tuned between runs.

The sample starts with eight physical partitions and a virtual-slot target of 64. Initialization
persists exactly 64 slots because the target is already a multiple of eight. Layout format 4 assigns
those slots with the zero-movement identity rule `slot % 8`, so every initial owner has eight slots
at epoch 1. `VirtualSlotTargetCount` is only a seed for a new or version-3 layout: the exact `V` is
persisted per provider namespace and is not recomputed from a later default. Partition manifests,
journals, and snapshots remain persistence format 3.

`GET /storage/layout` uses the keyed `ISearchableStorageAdminClient`. It is read-only and returns
`404 Not Found` before a storage operation initializes the namespace. After the first vacancy write,
it returns the epoch, initial partition count, exact virtual-slot count, and the slot count for each
current owner. The public response deliberately does not expose the mutable assignment array.

The city endpoint demonstrates an exact hash-index comparison. Its `/page` variant returns ids plus
an opaque continuation and deliberately uses a page size of one in `requests.http`. Follow the token
until it is null: a non-terminal page is allowed to be short or empty. The salary endpoint builds two
`Where` clauses dynamically so all four inclusive/exclusive bound combinations use the same public
query surface. This `IQueryable` is deliberately focused: it returns grain ids and does not load
state objects or support synchronous enumeration, projections, grouping, joins, or caller-defined
ordering. Every page fans out to all distinct current owners. The current identity map has every
initial partition as an owner; a future moved layout would still receive only one query call per
distinct owner.

The [bounded query and paging contract](../../docs/bounded-query-contract.md) defines the implemented
logical-work accounting, global frontier, continuation, and weak-consistency semantics. With no
writes and an unchanged layout, concatenating every page is exactly the same sorted, distinct result
as full evaluation. Concurrent writes can be observed on later pages and do not create a distributed
snapshot.

ASP.NET Core passes request cancellation to every search handler, and each handler forwards it to
its asynchronous terminal.

The sample's physical provider is in memory, so its journal, manifest, snapshots, and application data
all disappear when the process stops. A production host can register PostgreSQL, Redis, Azure Blob
Storage, or another Orleans storage provider under
`SearchableStorageConstants.PhysicalStorageProviderName`; the [backend guide](../../docs/backends.md)
contains complete examples. The sample deliberately keeps backend infrastructure out of its API
walkthrough; the same journal, recovery, compaction, and query behavior is exercised separately by
the shared provider contract. Co-hosting HTTP and Orleans is only a sample convenience; the query
client can also use an Orleans client from another process when configured with the same provider
name, initial partition count, bounded-query limits, and continuation key ring.

`Program.cs` uses an all-zero development-only key so the walkthrough runs without secret setup.
Never copy that material to a deployment. Production processes must load the same 32-byte
provider-scoped secret from protected configuration. Rotation distributes the new key as decrypt-only
first, switches every participant to it as current, and removes the old key only after outstanding
tokens may be invalidated safely.

This release does not expose `MoveSlot` or change physical ownership. The v3-to-v4 transition is not
an online mixed-version rollout: pause searchable storage and query traffic, update every silo and
Orleans client, verify that no version-3 process remains, and keep traffic paused while one normal
grain-state storage operation adopts each provider namespace. Verify that `GET /storage/layout`
succeeds and reports epoch 1, then resume traffic; the endpoint returns a layout only for format 4.
Query and admin reads do not perform adoption. Updated legacy calls remain placement-compatible with
the epoch-1 identity map, but any future movement protocol requires a separate coordinated all-v4
gate.

The sample state needs no PolyType-specific annotations. Orleans.SearchableStorage uses PolyType's runtime reflection provider internally; Native AOT and trimming are outside the supported deployment model.
