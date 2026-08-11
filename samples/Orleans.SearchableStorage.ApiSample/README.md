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
| `GET` | `/vacancies/facets/cities?pageSize=1&continuation=...` | Traverse distinct indexed cities in canonical value order. |
| `GET` | `/vacancies/facets/cities/top?topN=2&accuracy=Exact&minimumSalary=7` | Compute filtered exact top-N city counts. |
| `GET` | `/vacancies/facets/cities/top?topN=1&accuracy=Approximate&minimumSalary=7` | Stop after one bounded candidate turn and expose its omitted-count certificate. |
| `GET` | `/vacancies/facets/salaries/min-max?city=Helsinki` | Compute exact filtered salary extrema. |
| `GET` | `/storage/layout` | Read the persisted virtual-routing summary. |
| `GET` | `/storage/index-schemas/vacancies` | Read the durable vacancy index generation and rebuild status. |
| `POST` | `/storage/index-schemas/vacancies/rebuild` | Idempotently run or resume the page-limited vacancy index rebuild. |
| `POST` | `/storage/movement/enable` | Explicitly run/resume the quiesced movement-capability gate. |
| `GET` | `/storage/moves/active` | Read the sole active move, or `204` when none exists. |
| `POST` | `/storage/moves/plan` | Persist one explicit `{ slot, targetPartitionIndex }` move. |
| `POST` | `/storage/moves/{moveId}/advance` | Commit exactly one phase transition or transfer page. |
| `POST` | `/storage/moves/{moveId}/execute` | Resume the same move through completion. |
| `POST` | `/storage/moves/{moveId}/abort` | Roll back an active move before ownership commits. |
| `GET` | `/storage/rebalance/plan?targetPartitionCount=9` | Compute one deterministic minimal-churn next move. |
| `POST` | `/storage/rebalance/execute` | Execute/resume explicit single moves until balanced. |

## What happens on a write

1. The HTTP endpoint calls `IVacancyGrain` using the route id as its grain key.
2. `VacancyGrain` writes normal `IPersistentState<VacancyState>` state through the `Searchable` provider.
3. The provider serializes the state and uses its registered, generation-bound PolyType model to
   read the `[SearchableIndex]` values.
4. The provider loads the routing-compatible layout format 5 created by the startup schema gate,
   derives the record's virtual slot from its
   Orleans grain hash, and sends the slot and routing epoch to the assigned physical partition.
5. The format-5 partition validates the schema generation, grain, slot, epoch, and current owner
   before it reads record state or
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
10. Facet terminals nominate only indexed values. Candidate pages carry bucket metadata, a checked
    page raw-count sum, and the pinned owner scope total; the coordinator derives the remaining bound.
    Exact-count probes then evaluate the complete predicate in bounded, resumable `GrainId` slices.
    Each owner is pinned to one activation data version for the complete attempt, so an intervening
    mutation restarts the attempt once or fails without a partial aggregate.

The sample uses deliberately visible persistence settings in `Program.cs`: journal segments contain
at most 16 mutations, activation replay is capped at 256 committed mutations, and compaction is
requested after 64. Segment capacity and the replay limit are durable layout settings; changing
either requires migration. The compaction threshold is operational and can be tuned between runs.

The sample starts with eight physical partitions and a virtual-slot target of 64. Initialization
persists exactly 64 slots because the target is already a multiple of eight. The routing-compatible
format-5 layout retains the identity rule `slot % 8`, so every initial owner has eight slots at epoch
1. `VirtualSlotTargetCount` is only a seed for a new or version-3 layout: the exact `V` is persisted
per provider namespace and is not recomputed from a later default. Schema adoption upgrades every
partition manifest to persistence format 5 while retaining the bounded journal ring and two
whole-partition snapshot slots. Later movement enablement preserves format 5 rather than downgrading
it to movement-only format 4.

## Managed schema bootstrap

`Program.cs` explicitly registers the `VacancyState` type and application schema version. A small
hosted service completes `RebuildIndexSchemaAsync<VacancyState>` before ASP.NET Core accepts traffic.
That startup shortcut is safe here only because the sample always starts as one fresh process over
empty in-memory persistence. It makes every subsequent write and query carry the same durable schema
fingerprint and demonstrates the normal steady-state API without a manual race at startup.

The GET schema endpoint exposes the active fingerprint and the last completed record count. During
an interrupted rebuild it also reports the durable operation and enabled/scanned owner counts, so
empty-state progress remains visible without exposing record cursors. The POST endpoint is an
idempotent operator surface: it returns the same active generation when nothing changed and resumes
the durable cursor after interruption. Each scan page covers at most 64 catalog records, but a page
can trigger whole-partition compaction and is not a strict work or wall-time bound. A production
deployment must not copy the automatic startup shortcut onto existing data. Quiesce the complete
provider before any participant is
updated, back up the control/data state after traffic is paused, then deploy and restart every silo
and Orleans client with identical registrations. Verify that no old participant remains, then
complete every registered rebuild and verification step in the
[managed index-schema runbook](../../docs/index-schema-lifecycle.md). Format 5 is a one-way
capability gate for the current protocol, and updated schema-unbound calls fail closed. Older
binaries are excluded by the mandatory homogeneous restart: a binary which answers a contradictory
query entirely in-process issues no RPC which a server-side fence could reject.

`GET /storage/layout` uses the keyed `ISearchableStorageAdminClient`. The startup schema bootstrap
initializes the namespace before HTTP traffic, so this sample returns the epoch, initial partition
count, exact virtual-slot count, schema protocol, and slot count for each current owner immediately.
It also reports movement protocol/state and an active move summary. The public response deliberately
does not expose the mutable assignment array or record-key cursors.

## Manual movement walkthrough

The sample exposes every admin primitive directly so its resumable state machine is observable. These
routes are intentionally unauthenticated only because the sample binds a local development host.
Production applications must put equivalent endpoints behind strong authorization, auditing, rate
limits, and provider-scoped serialization—or avoid exposing them over HTTP at all.

Movement enablement is not a normal online request. Before calling it in production, quiesce every
searchable read/write/query, deploy and restart all silos and Orleans clients on the same
movement-capable package, and verify that no old process remains. The one-process sample satisfies
that topology precondition after a fresh start; the hosted schema bootstrap initializes the
namespace, while `requests.http` seeds sample vacancies and invokes enablement explicitly.

`POST /storage/moves/plan` persists the provider's sole intent. Save its `moveId`. The `advance`
route performs exactly one phase or page, making it useful for an operator-controlled loop. The
`execute` route is a client-side convenience loop over the same resumable turns. Request cancellation
is observed between turns and leaves durable progress resumable through the same move id. Page
payloads are bounded, but a protocol mutation can still trigger retained whole-partition compaction,
so an advance is not a strict work or wall-time budget. `abort` is valid only before the ownership
commit and uses bounded cleanup-page payloads; after commit, finish the move and plan a reverse move
if needed.

While phase is `Planned`, an `advance` can first reconcile a participant's movement capability and
routing-epoch floor. A previously unused target and a source whose durable floor lags after an
unrelated move can each consume one participant-only call, so repeat `advance` until the source is
frozen. Persistent `Planned` during those reconciliation calls is not a stalled move.

The rebalance plan never materializes or persists every move. For a requested contiguous owner
count it reports the current epoch, minimum remaining ownership commits, an active move if present,
and at most one deterministic minimal-churn next move. `ExecuteRebalanceAsync` repeatedly recomputes
that summary and executes one explicit slot move. Core storage does not choose a target count or run
a background policy; a production policy belongs in a host-owned service with operator controls.

The sample config uses 128 records and 256 KiB as each transfer page's count ceiling and canonical
movement-encoding target. One accepted record larger than the target is sent alone, yielding the
documented `O(target + largest accepted record)` in-memory page/transfer shape. This deterministic
measure is used for replay accounting; it is not actual Orleans wire, network, or physical-provider
bytes. Movement therefore scales with the selected slot's records and canonical encoded size,
including skew. Compaction and activation recovery still operate on a whole physical partition. See
the complete
[live-movement runbook](../../docs/live-movement.md) before using the admin API outside this sample.

The city endpoint demonstrates an exact hash-index comparison. Its `/page` variant returns ids plus
an opaque continuation and deliberately uses a page size of one in `requests.http`. Follow the token
until it is null: a non-terminal page is allowed to be short or empty. The salary endpoint builds two
`Where` clauses dynamically so all four inclusive/exclusive bound combinations use the same public
query surface. This `IQueryable` is deliberately focused: it returns grain ids and does not load
state objects or support synchronous enumeration, projections, grouping, joins, or caller-defined
ordering. Every page fans out to all distinct current owners. The current identity map has every
initial partition as an owner; a moved layout still receives only one query call per distinct owner.

The [bounded query and paging contract](../../docs/bounded-query-contract.md) defines the implemented
logical-work accounting, global frontier, continuation, and weak-consistency semantics. With no
writes and an unchanged layout, concatenating every page is exactly the same sorted, distinct result
as full evaluation. Concurrent writes can be observed on later pages and do not create a distributed
snapshot.

The three facet endpoints deliberately cover distinct values, value counts, and extrema without
loading vacancy state into the API process. Facet selectors must name one indexed property; nulls are
not indexed. Distinct cities use canonical indexed-value order and an opaque token bound to the
predicate, selected index, layout, response family, and effective limits. As with id paging, a
non-terminal distinct page can be short or empty and is weakly consistent across calls rather than a
cross-partition snapshot.

The top-cities endpoint accepts `accuracy=Exact` or `accuracy=Approximate` and always returns exact
counts for the values it includes. Exact mode keeps requesting value-ordered candidate pages and
bounded count probes until every owner is exhausted or the Nth count is strictly greater than the
sum of the owners' conservative unseen bounds. Approximate mode stops after the first candidate
turn. It can omit a true winner, so consumers must inspect both `isExact` and
`maximumOmittedCount`; the latter is an inclusive upper bound for every omitted value's count. The
optional `minimumSalary` is compiled into the normal indexed predicate before city counts are
computed. The min/max endpoint similarly filters by city and returns exact salary extrema; an empty
match set is represented by both values being null. Terminal exact work is all-or-throw under the
configured aggregate work, item, byte, and round ceilings.

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
name, initial partition count, bounded-query limits, continuation key ring, and an equivalent
`SearchableStorageSchemaRegistry` declaration.

`Program.cs` generates a fresh cryptographic 32-byte development key when the process starts, so the
walkthrough runs without secret setup and tokens intentionally become invalid after a restart.
Production processes must instead load the same stable 32-byte provider-scoped secret from protected
configuration. Rotation distributes the new key as decrypt-only first, switches every participant
to it as current, and removes the old key only after outstanding tokens may be invalidated safely.

Neither layout adoption nor managed-schema adoption is an online mixed-version rollout. For existing
production data, pause the complete provider, update and restart every silo and Orleans client, and
verify that no old process remains. If the persisted layout is version 3, the first schema rebuild
adopts it to routing format 4 as its initial idempotent step. Its owner sweep then upgrades supported
partition-persistence format 3 or 4 directly to format 5; a separate format-4 partition sweep is not
required. Run every registered
state's schema rebuild: the first establishes the provider-wide format-5 gate, while later states
remain unavailable until their own generation is active.
Keep traffic paused until every registered state reports `Active`. If movement is also being enabled,
keep it paused through that gate as well and require both schema protocol 1 and movement protocol 1
before resuming. Format-5 partitions reject schema-unbound or old-binary RPC traffic instead of
mixing index generations.

The sample state needs no PolyType-specific annotations. Orleans.SearchableStorage uses PolyType's runtime reflection provider internally; Native AOT and trimming are outside the supported deployment model.
