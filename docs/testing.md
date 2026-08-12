# Testing strategy

This document defines the maintainer-facing testing policy for Orleans.SearchableStorage. Every pull request must receive a test-sufficiency review in addition to the general engineering review and any domain-specific review.

## Required review lens

The reviewer must map changed behavior to tests instead of approving a raw test count. The review considers:

- successful public API behavior;
- validation and unsupported inputs;
- storage concurrency and ETag conflicts;
- failures before persistence and ambiguous failures after commit;
- activation loss, rehydration, and serializer compatibility;
- layout-format migration independently from partition-persistence compatibility;
- provider-wide managed-schema adoption, registration completeness, generation replacement,
  continuation invalidation, durable cursor recovery, and control-document lost acknowledgements;
- virtual-slot derivation, ownership, epoch mismatches, and whole-attempt retry behavior;
- quiesced movement enablement, version high-watermarks, source visibility, bounded page replay,
  abort/forward recovery, and single authority after reactivation;
- deterministic execution across more than one storage partition;
- facet value ordering, exactness/approximation certificates, owner-version pinning, and aggregate
  limit behavior;
- exact collection-membership declaration/extraction, supported `Contains` shapes, scalar
  `WhereIn` bounds/snapshotting, and rejection of every collection selector surface;
- cancellation and retry behavior where applicable;
- user-facing samples at their executable boundary;
- every physical backend claimed as supported.

Missing coverage must be called out explicitly in the pull request with a reason and a follow-up scope. Line and branch coverage reports are diagnostic evidence and do not replace behavioral review.

## Test layers

### Value and metadata tests

Fast unit tests protect index-value normalization, comparison/hash equivalence, ordering, supported CLR types, PolyType model construction and caching, inherited and nullable property shapes, attribute metadata, selector validation, recursive version-independent and cached scope identities, comparer-based range-bucket canonicalization, open and bounded range traversal, query expression translation, deferred captured values, reversed operands, compiler-generated integral, decimal, enum, and BCL-operator promotions, rejected semantic-changing conversions and custom comparison methods, fractional and adjacent floating-point bounds, NaN and infinity, out-of-domain numeric bounds, all equal-bound inclusivity combinations, bound combination, unsupported syntax, and query-plan simplification. Membership metadata tests cover exact SZ `T[]` and exact `List<T>`, the supported scalar/nullable element domain, Hash-only enforcement, null collection/element omission, empty strings, canonical sort/deduplication, fingerprint formats, and the rejected collection-shape matrix. Translation tests admit only exact array/list `Contains`; they reject reverse, interface, comparer, indirect, nested, direct-comparison, direct Find/Range, and facet-selector shapes. `WhereIn` tests cover its public marker contract, immediate snapshot, raw 64/65 boundary, null rejection, empty/exact/balanced-OR lowering, canonical deduplication/order, scalar Hash/Range selectors, collection-selector rejection, and cross-resume continuation binding. Boundary tests use the production plan constants to cover accepted and rejected depth and node counts, conversion-chain limits, balanced and chained predicates, order-preserving AND/OR rebalancing, semantic and wire cycles or shared subtrees, hidden child graphs, and payload on the wrong wire node kind.

Managed-schema tests freeze deterministic fingerprints, the application-owned version, codec
identities, generation-bound scopes, duplicate registration rejection, direct-client registry
snapshots, appended wire IDs, and `Reindex` replay without ETag or allocator movement. Real two-silo
Memory tests seed 70 legacy records on one owner, stop after the true 64-record page, deactivate both
partition and control, verify the persisted cursor/count, and resume through the public admin loop.
Separate scenarios prove fresh-namespace bootstrap without a dummy read; provider-wide rejection of
unregistered write, clear, one-shot query, page, facet, and schema-unaware direct-client paths;
replacement of a physically active application-version-1 control and its old generation-bound
scopes by registered version 2; automatic restart of that version-2 rebuild intent after a
completed movement-enablement layout change; and invalidation of a
pre-adoption continuation followed by successful use of an explicit external registry.
Before-commit and committed-lost-ack faults target the control document at `Begin`, a
non-final page cursor, the checkpoint after layout publication, and final activation. Each case
deactivates the affected grains and converges through the public admin loop. This is protocol
evidence, not permission to run the documented quiesced migration under live traffic.
A real Orleans-proxy regression uses an indexed getter with a deterministic failure gate. It proves
that the remote error retains provider/state/`GrainId`/owner and the underlying exception type while
omitting the application-controlled message and raw indexed value, the durable rebuild id, cursor,
and count do not advance, and removing the fault resumes that same rebuild through active query
results.

Client execution tests use a narrow internal constructor with controlled `IStoragePartitionGrain`
implementations. This seam deterministically proves one complete request per distinct layout owner, sorted and
deduplicated merge behavior, empty-plan short-circuiting after layout validation, rejection before
fan-out when expression limits are exceeded, no fail-fast partial result, observation of immediate
and late partition failures, and cancellation while calls are blocked. It is intentionally limited to the
fan-out boundary; real `TestCluster` tests cover Orleans dispatch, generated serialization, and
partition-grain execution.

### Virtual routing tests

Focused layout tests freeze the separation between routing-equivalent layout formats 4/5 and
partition persistence formats 3/4/5. They cover checked derivation of the per-layout virtual-slot
count, the 262,144-slot cap, exact
identity-placement equivalence for power-of-two and non-power-of-two initial partition counts, and
defensive copying of persisted assignments. Fresh initialization and an exact version-3 adoption
must each use one layout compare-and-swap. Migration rejects provider, initial partition count,
journal-setting, or partially populated routing-field mismatches, and an ambiguous layout write
poisons that activation. Existing version-4 and version-5 layouts are validated from their persisted
exact slot count rather than recomputed from a later target seed.

Layout-cache and routed-client tests cover shared concurrent loads, caller-local cancellation,
faulted and absent-layout reloads, conditional invalidation, and one shared refresh for concurrent
stale callers. Query execution fans out once per distinct owner. A route mismatch discards the
entire attempt and retries once with one refreshed snapshot; a non-routing failure remains
authoritative, and a second mismatch is surfaced. Partition tests validate grain id, derived slot,
epoch, and current owner before point-state or ETag behavior and prove that routed queries exclude
legacy records which no longer belong to the addressed owner.

Admin-client tests cover an uninitialized namespace, immutable public summaries, sorted per-owner
slot counts, keyed provider identity, movement-limit capture, validation, and caller-local
cancellation. Movement cases cover resumable enablement, sole-intent conflicts, one durable
transition or bounded page payload per advance, execute/abort loops, abort rejection after ownership
commit, stable progress projection, and deterministic minimal-churn rebalance recomputation without
a bulk persisted plan. The executable
sample bootstraps an empty in-memory managed generation before traffic, exposes status/rebuild HTTP
endpoints, and verifies both active status and the published layout capability. It also exercises the
movement surface through explicit HTTP endpoints; core storage has no automatic rebalance policy.

Movement protocol tests inject deterministic failures before commit and after commit but before
acknowledgement at enablement, freeze, target WAL high-watermark, every export/import page, ownership
CAS, durable source visibility, target enable, every cleanup page, participant retirement, abort,
and intent clear. They require idempotent exact-page replay, reject conflicting ordinal/digest/cursor
reuse, preserve target `NextVersion` after reactivation before the first import, and prove a
reactivated hidden source cannot serve an old-epoch fan-out. Count limits, byte targets, empty pages,
oversize singletons, skew, move chains, cancellation, and cleanup resumption remain structural test
oracles rather than timing assumptions.

### Bounded query protocol gate

The [bounded query and paging contract](bounded-query-contract.md) is enforced by production-path
tests. The matrix covers
exact work-vector totals, every work/result/byte stop boundary, selective and broad boolean plans,
duplicate-heavy union, ordered partition frontiers, multi-owner merge, short and empty non-terminal
pages, and concatenation equivalence when no writes occur. It must also cover activation rebuild and
mutation equivalence for the ordered state catalog/postings, ordered exact drivers,
candidate-tested exact-and-range intersection, bounded range k-way merge, complete candidate-group
frontiers, canonical sorted/distinct `OR` union, selected-window range admission, deterministic
charged `AND` selection, every permutation and grouping of three associative operands, composed
operands at different prepared heights, maximum-depth linear predicate work, bounded large-value
canonical-preparation allocation, duplicate-heavy eight-bucket range and eight-input union
source-admission sweeps across their catalog transitions, odd-budget half-turn rounding, small-turn
fallback-minimum precedence, fixed-share permutation invariance, and ordered-catalog fallback. The
PR13 materializing evaluator remains the benchmark baseline; the ordered implementation matrix adds
activation-build, mutation, retained-memory, latency, allocation, paging-progress, and work-vector
evidence.

Facet tests separately cover indexed-selector validation, null exclusion, and materialization of
every supported CLR shape; canonical distinct-value pages and family-bound tokens; exact filtered
counts and count/value tie order; exact and approximate top-N cutoff proofs and inclusive omitted-
count bounds; empty and non-empty extrema; per-owner data-version pinning, one restart, and a second-
change failure; every partition and aggregate work/item/byte/round boundary; deterministic fan-out,
late failures, cancellation, and no partial result. Candidate-page work vectors must prove zero
posting/group/record/predicate work, while count slices prove progressing canonical `GrainId`
frontiers and charge the complete filtered predicate.

Scheduled concurrency cases must prove the documented weak behavior when a record begins or stops
matching on either side of the global frontier; they must not assert snapshot isolation. Token tests
must cover missing/inconsistent key rings, unknown or duplicate key ids, rotation, nonce uniqueness,
oversized envelopes, altered headers/nonces/ciphertext/tags, plaintext-frontier leakage,
malformed authenticated plaintext, cross-provider associated data, cross-query,
cross-response-family, wrong-policy, and stale-epoch cases. Failure tests must prove that route
refresh is allowed only for a first page, resumed pages reject an epoch change, caller cancellation
observes late calls, and no failure returns a partial page or advanced token. Legacy tests must prove
complete small `ToGrainIdsAsync`, `FindAsync`, and `RangeAsync` results plus all-or-nothing limit
failure without silent truncation or an old unbounded RPC fallback. Serializer and shared
provider-contract coverage must run the new messages and behavior through Memory, PostgreSQL, Redis,
and Azure Blob before the implementation is called complete.

### Benchmark infrastructure tests

Benchmark tests are correctness tests, not timing gates. They reject unknown schema fields and
versions, altered spec digests, unsupported path/workload combinations, and unsafe broad-result
profiles. Golden vectors freeze deterministic record and operation generation. Scheduler tests cover
closed-loop in-flight bounds, open-loop overload accounting, cancellation, timeouts, and late-call
drain. Histogram tests cover compatible union, incompatible metadata, clamping, and the rule that
percentiles are computed only after union. Result tests round-trip required provenance, canonical
effective configuration hashes, p95 summaries, unknown dirty-state handling, secret redaction,
cleanup state, completed measurement on teardown failure, and raw artifact references. Tamper tests
also reject unknown result fields, broken source-digest graphs, mismatched embedded source/effective
JSON, missing enabled-phase evidence, impossible percentile summaries, non-HDR payloads with updated
self-checksums, and unsafe histogram paths. Secret tests cover connection strings, signed URLs, URI
userinfo, JSON credentials, and HTTP bearer authorization values.

The pull-request smoke reflects the built microbenchmark assembly and requires exactly the reviewed
22 `[Benchmark]` identities and every exact `[Params]` vector. It validates the actual
BenchmarkDotNet job, GC, diagnoser, p95 column, exporters, and artifact-retention config rather than
trusting duplicated provenance text, then invokes every production-backed fixture with semantic
oracles for query-plan construction/evaluation, wire and journal serialization, journal append and
replay, and snapshot detachment. It executes small Memory scenarios through searchable closed-loop,
searchable open-loop, and plain closed-loop point-operation paths and asserts each resulting
effective mode. It also emits and validates the 62-entry quick ordered-work matrix, four retained-
managed-memory cells, all 15 grain-page work counters, explicit access-path evidence, range/union/
catalog strategies, and clean
`DeterministicEvidence` provenance; these JSON files are correctness evidence, not a timing gate.
The production facet evaluator adds two BenchmarkDotNet identities across 4,096/65,536 records,
8/1,024 distinct values, uniform/skewed distributions, and all/selective predicates. Setup uses an
independent value/count oracle and freezes the exact candidate and resumable-count work vectors; a
focused CI vector requires `(seek=1, visit=8, materialize=8)` for metadata nomination and zero hidden
posting scans, then 32 filtered slices with the exact probe vector.
Four movement identities call the production slot-catalog and transfer helpers for rebuild, export,
import, and delete. Their uniform, skewed, and oversize-singleton fixtures freeze exact slot
membership, cursor, count/byte target, digest, apply, and idempotence outcomes. They explicitly keep
whole-partition snapshot construction and activation rebuild outside the per-page claim.
A pinned Crank Controller `--debug` expansion gate checks both distributed-client
coordinates, exact source revision/environment/arguments, and artifact download paths without
executing an agent. These are correctness gates with no wall-clock threshold. Dedicated nightly and
capacity workflows retain raw artifacts;
see [benchmarks.md](benchmarks.md) for environment and comparison requirements.
Focused guardrail tests separately pin the fixed logical capacity limits, representative inclusive
and next-byte/next-element boundaries, membership's exactly-64/65-unique boundary, duplicate-heavy
raw extraction, fail-before-partition-mutation/WAL-authority ordering, healthy retry, and fail-closed
durable recovery described in [storage-capacity-limits.md](storage-capacity-limits.md). They also pin
the managed gate precedence: schema/layout authority may be consulted before indexed extraction,
while an unmanaged zero-registration first write rejects capacity before layout initialization.
Those are safety and contract tests, not throughput or provider-capacity evidence.

### Storage contract tests

The reusable contract exercises normal `IGrainStorage` behavior for indexed object state and
non-object state without indexes, exact and range primitives, nested `IQueryable` intersection and
union, empty plans, nullable indexed values, compiler-promoted byte and enum queries through real
Orleans serialization, inclusive and exclusive one-sided and equal bounds, deterministic sorted
deduplication, a resumed bounded hash-index page, cancellation, updates, clears, layout validation, ETags, deterministic
multi-partition fan-out, activation rehydration, malformed wire-plan rejection followed by a
healthy call, protection against boolean mutation of live index buckets, and physical-write failure
boundaries. Its version-3 adoption case seeds a real record plus hash and range entries, performs the
single layout CAS, and verifies unchanged physical write counters for every partition manifest,
journal slot, and snapshot slot before exercising point, direct-index, and `IQueryable` operations.
Its nested-plan case resolves the keyed `ISearchableStorageQueryClient` and dispatches the recursive
`PartitionQueryPlan` through real Orleans grains. Every supported physical provider must run the
same contract.

The inherited journal contract also seeds a real format-3 record, performs the provider-wide schema
upgrade to format 5, verifies the preserved ETag and hash/range results, compacts and reactivates the
partition and schema control, then exercises a managed update and clear. Memory, PostgreSQL, Redis,
and Azure Blob/Azurite run that same state/serializer path; it demonstrates backend portability of
the implemented protocol, not equivalent performance or disaster-recovery behavior.

That inherited contract also registers an isolated one-partition membership schema. It writes exact
array/list entries, verifies both public `Contains` predicates, compacts and reactivates WAL/snapshot
state, and moves the live slot before reactivating both participants and rechecking point and
membership results. Focused Memory acceptance adds live write/update/remove/clear, null and duplicate
canonical extraction, scalar-facet filtering, `WhereIn` paging/cancellation, resumable schema rebuild,
and rejection of collection facets/direct Find/Range. The same inherited fact runs through Memory,
PostgreSQL, Redis, and Azure Blob rather than substituting an in-memory membership engine for Orleans
storage.

The shared contract also executes distinct, exact/approximate top-N, and min/max terminals through
real generated Orleans dispatch before and after activation rehydration. It verifies that persisted
records rebuild the activation-derived ordered hash-value projection without a persistence-format
migration. The existing layout-adoption case remains the explicit physical-write-counter oracle.

The same inherited contract creates an isolated provider namespace, enables movement, moves a slot
with multiple transfer pages while routed writers exercise both the frozen slot and another slot,
and continuously completes exact-index and exact-facet reads while ownership changes. The
successful path also advances one durable turn at a time and anchors point, exact-index, exact-facet,
and min/max checks in every observed phase from `SourceFrozen` through `Completed` before
reactivating source and target. Every completed read must contain the frozen membership/count
exactly, never a duplicate or partial epoch result. Post-commit writes succeed only at the target;
after both participants reactivate, the old source rejects a current-epoch write and clear plus an
E−1 write without changing its physical record count, while the target ETag/payload and
public indexes/facets remain unchanged. Source routed reads report the new owner; final
point/index/facet membership contains every moved record exactly once; exported/deleted counts
match; and no active intent remains. Memory, PostgreSQL, Redis, and Azure Blob execute this
identical acceptance case.

Before the successful move, that inherited case also aborts at `Planned`, `SourceFrozen`,
`TargetVersionFenced`, after a 2-of-5-record partial import, and after all 5 records reach
`CopyComplete`. Every rollback must leave the target slot physically empty, preserve source
records/indexes/facets and ETags, clear the active intent, and retain the target version floor at or
above the captured source high-water mark. At the two staged-copy checkpoints, direct physical
target counts are 2 and 5 while public exact top-N still reports the authoritative count of 5.
Approximate top-N returns only ownership-filtered exact counts: depending on its first canonical
candidate turn it returns either that 5-record value or a known singleton with count 1, and its
conservative omitted-count certificate covers the possibly omitted count of 5. A focused evaluator
test separately proves that staged target raw candidate metadata can be positive while
ownership-filtered exact count contribution is zero.

The generic write-ahead log (WAL) contract is inherited by the memory, PostgreSQL, Redis, and Azure Blob fixtures. It verifies committed replay after reactivation, bounded segment rollover, the steady-state journal-plus-manifest write shape, snapshot publication and two-slot reuse, retirement fencing, hard replay-limit backpressure, and recovery at each injected before-commit or lost-acknowledgement boundary. The same cases also prove that records and exact/range indexes are immediately usable after recovery without a test-only deactivation step.

Lower-level tests isolate the durable protocol from provider setup. They cover journal and snapshot
idempotency, writer-epoch and generation fencing, ring reuse, immutable-state copying and equality,
slot arithmetic and addressability limits, slot-catalog rebuild/mutation/order, movement page
count/byte/digest rules, high-watermark/import/delete replay, manifest capability and minimum-epoch
fences, layout initialization after an ambiguous write, malformed manifest/snapshot/journal
rejection, and coordinator poisoning after an ambiguous manifest write. These tests complement the
provider matrix; they do not replace it.

Serializer and API contract tests freeze the required non-null fields and IDs of the existing
bounded range message, the IDs and nullable bounds of the new non-persisted query plan, and a nested
plan round trip through the configured Orleans serializer. They freeze the routed page request,
partition page result, the preserved work-vector IDs 0 through 8 plus appended IDs 9 through 15,
stop reason, and budget exception; real Orleans
round trips cover non-terminal responses and exceptions with non-zero work components. They also freeze every virtual-routing
envelope, mismatch exception, layout descriptor, identity, snapshot, and durable layout-state field
ID, including the original `PartitionCount` property identity. Compile-time test implementations
keep the old direct-client interface independent from the opt-in query and paging interfaces and
exercise external public async, paging, facet terminal, and `WhereIn` marker providers. The API
sample compatibility test compiles the documented exact array/list `Contains` forms and inspects the
deferred `WhereIn` marker to prove immediate input snapshotting and the public raw-value ceiling
without changing the sample's persisted `VacancyState` schema. Facet serializer coverage
freezes all four response-family values, the distinct/candidate/count request and result IDs, their
data-version fields, candidate page/total raw counts, the nine-component facet work vector, and the
concurrent-change exception.
Movement serializer coverage freezes appended layout/manifest/journal fields and enum values, every
partition RPC request/result ID, stable move identity and page digest inputs, public progress/layout/
rebalance shapes, and real Orleans dispatch of pages, exceptions, and reactivation state.

The memory, PostgreSQL, Redis, and Azure Blob fixtures inherit this same contract class; backend tests do not copy or weaken its assertions.

### Backend-specific tests

Provider fixtures add environment setup, cleanup, serializer selection, and registration assertions which cannot be expressed in the shared contract. All four fixtures use `JsonGrainStorageSerializer`, two in-process silos, and the same fault-injecting decorator around the physical provider. The default shared contract uses eight storage partitions; focused WAL scenarios create isolated one-partition provider namespaces so each physical transition has one unambiguous target.

- PostgreSQL uses `Microsoft.Orleans.Persistence.AdoNet` with Npgsql. The fixture creates an isolated schema and applies operational SQL copied from the Orleans 10.2.2 source tag under the full upstream MIT notice retained inline; the SQL body is unchanged apart from its source and license header. `eng/orleans-sql.sha256` pins the complete vendored files, and CI verifies the manifest before restore.
- Redis uses `Microsoft.Orleans.Persistence.Redis` with a unique Orleans service id.
- Azure Blob uses `Microsoft.Orleans.Persistence.AzureStorage` with a unique container and runs against Azurite in CI. Azure.Storage.Blobs 12.27.0 defaults to API `2026-02-06`, while pinned Azurite 3.36.0 implements `2025-11-05`; native support is tracked in [Azure/Azurite#2623](https://github.com/Azure/Azurite/issues/2623). The emulator therefore starts with `--skipApiVersionCheck`. The complete contract still validates the implemented Blob operations; this flag skips only the emulator's request-version allow-list check and must be removed after the pinned emulator supports `2026-02-06`.

Each external fixture also runs one resource-isolation cleanup case without starting another cluster. PostgreSQL removes a populated owned schema while a populated foreign schema survives, Redis removes only the selected service id's state keys, and Azure Blob removes only the selected container. The Redis test obtains two key suffixes from states written by the official provider before cloning owned and unrelated sentinels, so it fails if provider and fixture namespace formats diverge. The cleanup helper deduplicates endpoint scans and dispatches only single-key deletes in bounded batches, avoiding cross-slot multi-key commands on Redis Cluster. `try`/`finally` cleanup removes every sentinel and both provider-created probes even when an assertion fails.

External fixture lifecycle tests use a recording cluster abstraction to verify one-call teardown ordering and exception precedence without live infrastructure. A release attempts graceful stop, full `TestCluster` disposal, and backend cleanup in that order even when an earlier stage fails. Successful full disposal makes cleanup safe after a graceful-stop failure; combined failures retain the first exception and attach later failures under stable diagnostic keys. The memory fixture follows the same full-disposal requirement.

The package versions and conditional-test pattern follow the Orleans 10.2.2 repository: `Xunit.SkippableFact` marks the reusable external contract, and fixture preconditions skip it unless `ORLEANS_SEARCHABLE_STORAGE_RUN_BACKEND_TESTS` is explicitly enabled. Npgsql, Azure.Storage.Blobs, and StackExchange.Redis are direct test dependencies because the fixtures prepare and remove backend resources in addition to configuring the Orleans providers.

CI writes four independently filtered TRX files using the `Backend` trait. The workflow stores the
100-case shared contract count once, then derives the exact profile totals: memory has 101 cases
(shared plus one provider assertion), while PostgreSQL, Redis, and Azure Blob each have 102 (shared
plus provider and cleanup assertions). The small `eng/validate-trx.sh` gate requires one `Counters`
element, the exact total, executed and passed counts, zero failed and not-executed summary counts,
no `NotExecuted` result, and no non-passed result element. Missing files, empty filters, partial
discovery, per-provider omissions, failures, and skips therefore fail independently. The external
job starts the pinned images from `tests/backends.compose.yml`, uploads its TRX and coverage
artifacts, and removes the volumes even after failure. When the shared contract changes, update its
single workflow count; when a profile gains a provider-specific case, update the corresponding
derivation. For local execution and connection-string overrides, see
[physical storage backends](backends.md).

### Executable sample tests

The API sample is tested through HTTP using ASP.NET Core `WebApplicationFactory`. These tests ensure
the documented host starts, keyed Orleans services resolve, writes reach the searchable provider,
the focused `IQueryable` API returns both compatibility results and resumable pages, concatenated
pages preserve canonical order, facet endpoints expose distinct continuation, `IsExact`,
`MaximumOmittedCount`, filtered exact counts and extrema, the layout endpoint reports the persisted
owner summary, and deletes remove both state and index entries. The movement test explicitly enables
the protocol, plans/advances/aborts and then executes one move, completes a deterministic rebalance,
verifies moved point/index state, reverses the rebalance to the sample's initial owner count, and
then re-verifies the same record through point, index, and facet reads. This supplies an executable
same-record move-chain oracle rather than treating reverse cleanup as best effort. Invalid admin
inputs return HTTP 400, while durable state conflicts return 409. A keyed blocking query client
verifies that HTTP request cancellation reaches every search and facet endpoint and its async
terminal while it is in flight. A separate compile-time compatibility case keeps the runnable
membership/`WhereIn` README snippet aligned with the public expression surface while deliberately
leaving the hosted sample's managed schema unchanged.

## Coverage artifacts

CI collects Coverlet line and branch coverage together with test results. The regular job covers unit, memory-contract, and sample tests; the backend job records the external contract separately so a skipped local run cannot be mistaken for backend validation. Coverage changes guide review toward untested branches, but no pull request can satisfy the test-sufficiency requirement by meeting a percentage alone.
