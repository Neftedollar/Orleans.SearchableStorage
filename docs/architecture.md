# Architecture

This document describes the maintainer-facing design of the first Orleans.SearchableStorage vertical slice. It records the boundaries which tests and future implementations must preserve.

## Components

`SearchableGrainStorage` implements Orleans `IGrainStorage`. Application grains use it through normal `IPersistentState<T>` injection and do not coordinate index updates themselves.

Each searchable provider owns one `StorageLayoutGrain` and a set of
`StoragePartitionGrain` instances addressed by physical owner index. Layout formats 4 and 5 hold the
same immutable virtual-slot space, movement-capable assignment map, and routing epoch; format 5 adds
the managed-schema capability and maintenance intent. A partition keeps its records and derived hash and range buckets
in the activation. Its durable representation is split into one constant-size manifest, a fixed ring
of bounded journal-segment grains, and two reusable snapshot-slot grains. All four grain kinds write
through the physical Orleans storage provider registered as `Orleans.SearchableStorage.Physical`.
Application grains never address those implementation grains directly.

`SearchableStorageClient` supports direct exact/range primitives and a focused `IQueryable`
boundary. `QueryTranslator` produces an explicit Empty/Exact/Range/And/Or plan. The paged terminal
sends one bounded request to every distinct owner in one immutable layout snapshot. Each partition
validates the route and fingerprints, evaluates only a configured logical-work slice against one
serially consistent activation view, and returns a sorted local prefix plus a safe frontier. The
coordinator merges the global safe prefix under item and byte caps and protects the next boundary in
a stateless authenticated-encrypted continuation.

The same query provider exposes a separate `ISearchableStorageFacetQueryProvider`. It compiles the
focused predicate once, resolves one indexed-property selector, and uses typed partition families
for distinct values, candidate metadata, and resumable exact counts. Exact/approximate top-N and
min/max are coordinator reductions over those bounded primitives; they never deserialize state into
the client or introduce untyped aggregate payloads.

## Sample topology

`Orleans.SearchableStorage.ApiSample` co-hosts an ASP.NET Core minimal API and one Orleans silo only
to provide a zero-infrastructure executable walkthrough. HTTP writes call an application grain, its
normal `IPersistentState<T>` uses `SearchableGrainStorage`, and the provider routes the serialized
record plus extracted index entries to one storage-partition grain. HTTP search endpoints use
`Query<TState>` and `Where` on the named `ISearchableStorageQueryClient`; one endpoint demonstrates
`ToGrainIdPageAsync`, compatibility endpoints demonstrate bounded all-results collection, and three
facet endpoints demonstrate distinct values, explicit exact/approximate top-N counts, and filtered
extrema. Explicit admin endpoints demonstrate enablement, planning, one-step advancement, execution,
pre-commit abort, and deterministic rebalance planning; there is no automatic core policy. ASP.NET
Core supplies `HttpContext.RequestAborted` as the endpoint cancellation token.

This co-hosting is not an architectural constraint. An API can run in another process as an Orleans client and construct a `SearchableStorageClient` with the same provider namespace and partition count. The sample deliberately uses in-memory physical persistence, so process termination removes its data; backend durability is covered by the reusable provider contract rather than by the sample.

## Identity and partitioning

A record key contains the state name and the raw Orleans grain type and key bytes encoded as hexadecimal. Partition placement uses `GrainId.GetUniformHashCode()`, which Orleans defines as uniform and stable.

The searchable provider name is the key of its layout grain and part of every partition grain key. This isolates logical providers even though their grains use the same physical storage-provider registration. A different or renamed provider name selects a separate, initially uninitialized namespace; it cannot be detected as a mismatch against the old namespace and therefore requires an explicit migration when existing data must move with it.

The layout grain durably records provider identity, layout format, initial partition count, journal
segment capacity, maximum replay entries, exact virtual-slot count, assignments, and routing epoch.
The persisted C# property remains named `PartitionCount` because JSON property names are durable
provider data; its format-4 meaning is the initial owner count. The compaction threshold and
virtual-slot target are absent from persisted identity: the former is operational, while the latter
is only a seed used to derive the exact slot count once.

For initial count `P0`, the provider derives `V` as the smallest checked multiple of `P0` at or above
the configured target, capped at 262,144. It seeds `assignment[s] = s % P0`. Since `P0` divides `V`,
`assignment[hash % V]` is exactly `hash % P0`; migration does not move records even for non-power-of-two
counts. A valid version-3 layout becomes version 4 through one layout CAS. Partition manifests,
journal segments, snapshots, records, and index buckets remain untouched by layout adoption.
The explicit quiesced movement-enablement owner sweep upgrades partition manifests to persistence
format 4; merely activating or mutating through a movement-capable binary leaves format 3 intact.
Format 4 retains those physical structures and appends durable capability, minimum-epoch,
source/target-control, and movement-WAL fields.

`StorageLayoutCache` shares one immutable, defensively copied snapshot between concurrent callers.
All partition activations on one silo obtain their provider's cache from a singleton registry, so a
full `V`-entry assignment map is retained once per provider and silo rather than once per partition.
The keyed storage provider and query/admin clients retain a bounded constant number of additional
process-local snapshots. Caller cancellation does not cancel a shared grain call. Point requests
carry `GrainId`, slot, and epoch; partitions validate the derived slot and current owner before
record lookup or ETag logic.
Page requests carry the epoch and layout fingerprint and fan out once per distinct owner. On a first
page, a routing-only mismatch makes the client observe and discard the complete attempt,
conditionally invalidate only the rejected snapshot, and retry once. Conditional invalidation
prevents concurrent stale callers from replacing each other's refresh. A continuation pins its
layout and becomes stale instead of refreshing across an ownership change.

A query against an uninitialized namespace returns empty and does not initialize the layout. A
read-only admin client likewise returns no layout. A persisted version-3 namespace instead requires
adoption through `SearchableGrainStorage`, which has the full provider and journal descriptor; query
and admin clients never invent or migrate a virtual map.

Before movement is enabled, assignments remain the epoch-1 identity map. Enablement requires a
quiesced, coordinated restart because an already-running older provider can retain successful layout
validation. It durably upgrades/fences each current owner, then publishes movement protocol version 1
and a new routing epoch. After publication, legacy placement-only calls are rejected.

## Live movement protocol

`ISearchableStorageAdminClient` is the only public ownership-mutation surface. It persists at most one
move intent per provider and can advance exactly one phase or transfer-page payload, loop over those
turns to completion, or request rollback before ownership commit. Cancellation stops the client loop
between turns without canceling or reversing durable progress. A stable move id makes a later call
resumable. A protocol mutation can still cross the retained whole-partition compaction/recovery
boundary, so this is a payload/state-machine guarantee rather than a strict per-call work or time
bound.

While the public phase is `Planned`, one advance may reconcile the target's movement capability and
routing-epoch floor and another may reconcile a lagging source floor. A newly introduced target plus
a source last used at an earlier epoch can therefore leave `Planned` visible for up to two
participant-only calls before the next call freezes the source.

Each partition derives an activation-local map from virtual slot to ordinal record-key set while
rebuilding its normal indexes. Source export and source deletion use that stable ordinal set rather
than scanning every partition record on every page. Import updates records, lookup/ordered indexes,
and the slot map together only after the corresponding journal entry commits.

The state machine preserves these load-bearing fences:

1. The layout persists the sole move identity, source epoch/owner, target owner, phase, and scalar
   progress.
2. The source manifest freezes mutations for the slot and captures `NextVersion`.
3. The target commits a replayable `AdvanceVersion` WAL operation which raises `NextVersion` to at
   least the frozen source value, including for an empty slot.
4. Source export and target import proceed in record-count-bounded, byte-targeted pages. The target
   remains mutation-inactive and non-authoritative for the source epoch.
5. One layout CAS changes the assignment and increments the routing epoch.
6. The source durably raises its minimum routing epoch and becomes query-invisible for the slot. Only
   after that acknowledgement may the target accept mutations.
7. Obsolete source records are removed by record-count-bounded, byte-targeted `MoveDelete` WAL page
   payloads, both controls retire, and the layout clears the intent.

Target enablement after the durable source-visibility acknowledgement is essential: a caller with a
stale pre-commit layout can otherwise query the source while target writes change predicate
membership. `.Distinct()` cannot repair that ghost membership. The minimum routing epoch survives
source reactivation, and the target's version high-water mark survives snapshot+journal recovery.

Move pages use a stable ordinal record-key cursor and a SHA-256 digest over the complete canonical
identity, limits, progress, and payload. A lost acknowledgement can repeat the exact page
idempotently; another payload at the same ordinal is rejected. The page record limit is absolute.
The byte target is evaluated with the deterministic canonical movement encoding used for replay
accounting, not an Orleans serializer or a physical provider. Movement-only wire records carry text
as explicit big-endian UTF-16 code units, preserving the full persisted string domain—including
unpaired surrogates—which Orleans string transport would otherwise normalize. An accepted record
larger than the target is emitted alone, so the in-memory page/transfer shape is
`O(byte target + largest accepted record)`; actual wire and provider byte counts are not bounded by
or reported as this value.

Pre-commit abort changes the target to an abort-delete phase, removes imported records in bounded
pages, retires target control, and finally unfreezes the still-authoritative source. Once assignment
and epoch commit, abort is illegal and recovery converges forward. Deterministic rebalance planning
computes a minimal-churn quota and returns one next move; it never stores an unbounded bulk plan and
does not implement a load policy. See the operator [live-movement runbook](live-movement.md).

## Journal commit protocol

For one record, the serialized state and every local secondary-index entry are one logical mutation.
The manifest is the only authoritative commit point:

1. The storage provider serializes the application state and extracts index entries.
2. The owning partition grain checks the expected record ETag.
3. Before the first mutation of an activation, the partition advances a writer epoch with a manifest
   compare-and-swap. This fences journal writes from an older activation.
4. It creates one journal entry containing the absolute sequence, writer epoch, unique operation id,
   previous committed operation id, expected record ETag, replacement record or delete marker, and
   the next record-version counter.
5. A journal-slot grain conditionally stores that entry in the bounded segment selected by the
   absolute sequence. The entry is still non-authoritative.
6. The partition advances `CommittedSequence`, `CommittedOperationId`, and `NextVersion` in one
   small manifest compare-and-swap. This makes the journal entry and all of its index entries visible.
7. The activation applies only that record's hash and range bucket delta. Only then does a successful
   write return its record ETag.

Steady mutations therefore perform one bounded journal-segment write and one constant-size manifest
write. The first mutation after activation also acquires its writer epoch. A clear is a journaled
delete and does not allocate a new record ETag. The non-reentrant partition activation serializes
these transitions, while physical provider ETags protect against duplicate or stale activations.

Journal entries have unique operation ids and an exact predecessor chain. An exact retry is
idempotent. At most one entry can exist immediately after the manifest commit point; a new activation
may replace that orphan only with a higher writer epoch. Entries from lower epochs, conflicting reuse
of an operation id, and stale absolute segments are rejected. The manifest CAS still decides whether
the logical mutation committed: a durable journal write without the matching manifest advance is not
visible during recovery.

Every failed physical write is potentially a lost acknowledgement. The affected manifest, journal,
snapshot, or layout activation is poisoned and cannot use a restored value with a provider-updated
ETag. It requests deactivation; a fresh activation rereads authoritative physical state. If the
manifest commit was durable, an unchanged caller retry has a stale record ETag and conflicts without
minting another version. If only the journal entry was durable, recovery ignores it and a higher
writer epoch may replace it.

Automatic compaction runs after the mutation manifest commits. A maintenance failure must not turn
that already committed mutation into a reported write failure, so the current mutation returns its
success while the activation is poisoned and retired. Compaction required by the hard replay limit
runs before allocating the next journal entry; failure there backpressures the new mutation.

## Recovery and bounded compaction

Activation validates the manifest before serving a request. It loads the active snapshot, replays
the exact journal operation-id chain through `CommittedSequence`, validates the recovered
`NextVersion`, and rebuilds the in-memory indexes. A pending snapshot reservation is completed before
the activation becomes usable. Retirement cleanup is also attempted before serving; a cleanup
failure fails activation instead of exposing an activation which might reuse ambiguous state.

For capacity `C` and maximum replay entries `R`, a partition addresses
`ceil(R / C) + 2` journal slots. Absolute segment index, not the reusable slot number, is persisted in
each slot. Reuse requires a durable tombstone and a newer absolute index; delayed stores and retires
cannot resurrect an older segment. `CommittedSequence - SnapshotSequence` never exceeds `R`. Segment
capacity is at most 64 operations; the same segment is also capped at 320 MiB of deterministic
canonical journal bytes, with each entry capped at 5 MiB. These logical limits are independent of
Orleans serialization and provider-physical size. The complete fixed envelope is defined in
[storage-capacity-limits.md](storage-capacity-limits.md).

Compaction is a four-stage protocol:

1. Reserve the complete pending descriptor in the manifest: generation, random snapshot id, target
   slot, committed sequence and operation id, and next record version.
2. Store an immutable whole-partition snapshot in the inactive slot. Exact retries are idempotent;
   the slot rejects lower generations or same-generation mismatches.
3. Publish that exact descriptor as active in the manifest and mark the previous active descriptor
   as retiring.
4. Tombstone the retired snapshot payload and every fully covered journal segment, then advance the
   manifest prune boundary.

Generations alternate deterministically between two stable slots. Snapshot and journal payloads are
retired by writing tombstones rather than physically clearing their grain state, because the retained
generation or absolute-segment fence rejects delayed stale RPCs. Repeated ambiguous snapshot writes
therefore reuse one reserved generation instead of leaking sequence-named snapshot objects.

## Read and query semantics

A point read is routed directly to the record's owning partition. Missing records receive a new state instance from Orleans' configured activator and have no ETag.

One query page fans out once to each distinct owner in the current routing layout. The partition
grain is non-reentrant and page evaluation contains no await, so every predicate probe in that turn
observes one serially consistent partition-local state. Owners can observe different instants and a
later continuation observes later turns, so the merged traversal is deliberately not a
cross-partition snapshot.

Each in-memory range index stores its distinct values in a `SortedSet` of mutable buckets. During
construction, comparer-equal buckets are canonicalized and merged. A mutation adds or removes only
the affected bucket in O(log d), where d is the number of distinct indexed values, and empty buckets
and scopes are removed. `GetViewBetween` seeks into the tree and visits only the requested ordered
window, giving O(log d + k) traversal for k visited buckets. Hash indexes likewise update only the
affected record buckets. Hash scopes now use the same canonical balanced value projection rather
than an unordered bucket dictionary, so value seeks and affected bucket updates are also O(log d).
Each bucket stores its posting cardinality as scalar metadata; facet candidate nomination does not
enumerate posting members. These structures remain activation-derived and add no durable index
representation.

Once a successful write returns, that partition's activation already contains the committed state
and corresponding index entries. No asynchronous index-maintenance pipeline exists in this version;
recovery deterministically derives the same indexes from snapshot records plus committed journal
entries.

## Query translation and execution

`ISearchableStorageQueryClient.Query<TState>(stateName)` returns an `IQueryable<TState>` only as a familiar expression-building surface. It is not an in-memory state collection and cannot enumerate `TState` values. `ToGrainIdPageAsync` opts into the public `ISearchableStoragePagedQueryProvider` contract. `ToGrainIdsAsync` remains an all-results terminal through `ISearchableStorageAsyncQueryProvider`; external providers own and must document their own bounding semantics.

Facet terminals opt in independently through `ISearchableStorageFacetQueryProvider`, preserving the
existing async and paging provider contracts for external implementations. Their selector must map
the query element directly to one declared index. `IndexValueMaterializer` uses that selected
index's converter to reconstruct the exact public CLR type from the canonical wire value; the
internal value kind alone is insufficient for shared representations such as enum/integer,
nullable/non-nullable, `char`/string, or `DateTime`/`DateTimeOffset`.

Translation accepts one or more `Queryable.Where` calls. Predicate leaves must compare one direct indexed property with a constant or captured value using equality or an ordered comparison. Boolean `AndAlso` and `OrElse` become plan intersection and union. Reversed operands are normalized. Intersected bounds on the same index are combined before execution; contradictory bounds become an empty plan. Equality can use either index kind, while ordered comparisons require a range index. Null comparison is rejected because null index values are deliberately omitted. An empty plan still validates persisted layout compatibility and cancellation, but skips partition fan-out.

The closed value side is intentionally narrow: constants, captured fields or properties, and built-in conversion nodes are evaluated using the expression interpreter. Method calls, calculations, user-defined value conversions, state-to-state comparisons, nested state member access, and arbitrary LINQ operators are rejected with `NotSupportedException`. Compiler-generated integral and enum promotions are normalized through the indexed property's PolyType-derived value domain. A conversion of the indexed property is accepted only when it represents that complete domain exactly and preserves equality and ordering. Out-of-domain integral equality becomes an empty plan, and ordered bounds are saturated to a correct full or empty domain window. Boxing, reference, narrowing, user-defined, and lossy floating-point conversions of the indexed property are rejected rather than silently changing C# semantics. Custom binary comparison methods are also rejected; only the corresponding BCL operators for supported built-in value types are accepted.

Supported translation traversal and both semantic and wire plans share fixed limits of 64 levels and 256 visited nodes. Indexed-property and state-parameter conversion chains are independently capped at 64. Recursive conversion and evaluation occur only after those bounds have been checked. Both plan validators require trees rather than cyclic or shared graphs; wire validation also rejects hidden child graphs, payload on the wrong node kind, missing boolean children, malformed leaves, and unknown operations. Associative boolean plans are rebuilt as balanced trees without changing leaf order before serialization.

After translation, `PartitionQueryPlanFactory` creates a recursive Orleans wire message for Empty,
Exact, Range, And, and Or nodes. `StoragePartitionGrain.QueryPageRoutedAsync` validates the complete
plan, protocol versions, response family, route, query fingerprint, layout fingerprint, and hard
limits before evaluation. Activation-local tree-backed state catalogs and postings use canonical
`GrainId` order and are rebuilt from durable records. Writes and clears update them synchronously
with the existing hash/range indexes. Exact and selective exact-AND plans use ordered exact drivers;
range leaves merge bounded ordered bucket postings; general boolean plans use a bounded ordered
catalog scan. Every data-dependent step is charged before it runs, and a candidate group must finish
before its `GrainId` can become a returned item or frontier.

The client starts every owner call before awaiting the aggregate, so a partition failure fails the
entire page and no partial items or token escape. Non-canceled attempts classify simultaneous
failures deterministically in sorted-owner order. Cancellation interrupts the local wait, not the
Orleans RPCs already in flight; a detached observer still observes their aggregate completion.

`ISearchableStorageClient` intentionally retains only the existing direct `FindAsync` and `RangeAsync` surface. `ISearchableStorageQueryClient` derives from it and adds `Query<TState>`. The keyed registrations for both interfaces point to the same `SearchableStorageClient` instance, which keeps existing direct-client implementations source compatible while making the new expression surface opt-in.

### Bounded paging protocol

The [bounded query and paging contract](bounded-query-contract.md) freezes the implemented
logical-work accounting, canonical `GrainId` order, partition-local safe frontier, bounded
coordinator merge, AES-256-GCM stateless continuation, and weak concurrent-write semantics. The
continuation binds provider, query, response family, ordering/work versions, layout epoch and
fingerprint, global exclusive frontier, page size, and effective limits. It contains no per-owner
cursor or buffered result. A non-terminal page may be short or empty; only a null token is terminal.

`ToGrainIdsAsync`, `FindAsync`, and `RangeAsync` call the same page RPC and collect internally under
aggregate work, item, byte, and round ceilings. They return a complete result or throw
`SearchableStorageQueryLimitExceededException`; no old unbounded RPC fallback exists.

### Bounded facet protocol

Distinct-value requests merge one safe canonical `IndexValue` prefix and protect one exclusive
value boundary in a response-family-bound continuation. Candidate requests return value/raw
posting-count metadata plus checked page and pinned owner-total raw counts. The coordinator
accumulates page totals locally and derives the conservative remaining bound. Exact-count requests seek one
nominated posting and evaluate the complete predicate in resumable canonical `GrainId` slices.
Candidate, probe, and filtered-predicate work all have explicit checked counters.

One multi-turn attempt pins each owner to the partition data version observed on its first response.
A change discards and restarts the whole attempt once; a second change fails without a partial
aggregate. Owners still have no common observation instant, and distinct continuations across public
calls are weakly consistent. Exact top-N deepens value-order pages until owners exhaust or the Nth
count strictly exceeds the summed unseen bound. Approximate top-N stops after one candidate turn and
reports both `IsExact` and an inclusive `MaximumOmittedCount`. Min/max exhausts candidates. Aggregate
work, turns, candidate items, and encoded bytes make every terminal all-or-throw. The normative proof
and response-family values are in the
[bounded query contract](bounded-query-contract.md#indexed-facet-protocol).

## Index metadata

Public readable state properties marked with `SearchableIndexAttribute` are indexed. Index scope combines a length-prefixed persisted state-type identity, Orleans state name, and stable index name. A named type identity contains its assembly simple name, culture, public-key token, and full type name. Constructed generic identities contain the generic definition followed by recursively encoded argument identities; arrays encode their shape and element identity. Assembly versions are deliberately excluded. Length prefixes make every boundary unambiguous and prevent unrelated states from sharing buckets accidentally.

`IndexMetadataProvider` builds one `SearchableTypeModel<TState>` through PolyType's reflection provider and caches each valid model for the process lifetime. A non-object PolyType shape, such as a collection or scalar state type, produces an empty model and remains writable through the provider. An object model contains the indexed member identity, index kind, normalized index name, value converter, stable persisted type identity, and a strongly typed PolyType getter delegate. Each indexed property caches its complete scope per Orleans state name. Steady-state writes therefore neither call `PropertyInfo.GetValue` nor repeat assembly identity reflection; they invoke the cached getter, converter, and scope. Failed model construction is not cached so an invalid state declaration continues to produce its direct validation exception.

PolyType usage is contained behind the indexing model. Storage grains and persisted messages do not depend on type-shape objects. Query selectors and predicates are expression trees: the expression boundary reads the selected `PropertyInfo` only to match it to the member identity supplied by PolyType, while persisted value discovery and access remain type-shape operations. The same model is used by writes and queries, so index kind, converter, and scope have one definition.

The runtime reflection provider is intentional. Orleans `IGrainStorage` accepts unconstrained application state types, and this project does not require consumers to annotate those types for PolyType source generation. Native AOT and trimming are not supported.

Null values are omitted. String ordering is ordinal. The canonical facet wire representation accepts
at most 16,384 bytes of valid strict UTF-8 for string/`char` values. Writes retain their older CLR
domain and do not enforce this wire limit; a facet traversal which reaches an overlong value or
unpaired surrogate fails atomically with `SearchableStorageQueryLimitExceededException`, so
applications must validate facet text as a schema rule.
`DateTime` values must be UTC so index ordering cannot depend on a silo's local time zone.
Floating-point NaN values are rejected because they do not provide a useful total ordering for these
indexes.

Index names, index kinds, indexed property types, codec versions, and a positive application-owned
version are persisted schema. Applications register exactly one CLR type/version for every
provider/state-name pair. The deterministic fingerprint binds those inputs to physical scopes and
is stored on each rebuilt record. A separate per-state control grain stores the active fingerprint,
last completed count, and one resumable rebuild cursor. Partitions commit page-limited `Reindex` WAL
entries which preserve payload, `GrainId`, ETag, and the object-version allocator. A page covers at
most 64 catalog records but can trigger retained whole-partition compaction, so it is not a strict
work, memory, or wall-clock bound.

The first rebuild is also a provider-wide, one-way capability transition. It upgrades every current
owner to persistence format 5 and rebuilds every record for its one state name before publishing
schema protocol version 1 in layout format 5. One additional control commit then activates only
that state fingerprint. Other registered states retain independent controls and remain unavailable
until their own rebuilds reach `Active`; first adoption therefore completes all registered states in
one quiesced provider window before traffic or movement resumes.
After publication, every state using that provider must be registered on every silo, and every
direct query client must declare every state it queries. Schema-unbound writes, clears, queries,
pages, and facets are rejected; current routed point reads remain generation-independent. Managed
operations reject an absent, rebuilding, or different generation. The control automatically resets
its owner/frontier scan when a different completed routing layout appears before the maintenance
intent is acquired. Once that intent exists, movement cannot change the layout until schema work
publishes or confirms the provider capability and clears the intent. Old and new physical scopes
remain disjoint, and continuations authenticated against the old plan cannot resume under the new
generation. The protocol is deliberately quiesced rather than dual-write; see
[index-schema-lifecycle.md](index-schema-lifecycle.md).

## Persisted compatibility rules

The layout format version, provider name, initial partition count, journal capacity, replay limit,
layout-grain type and key, partition-grain type, partition-key format, journal-ring and snapshot-slot
key formats, record-key format, and index-scope format determine how durable data is located and
interpreted. Changes to any of them require a format-version increment and a migration path. In
particular, renaming the layout grain interface or changing its key can silently select a fresh
namespace unless the old identity is migrated.

Layout format and partition persistence format are independent compatibility axes. Layout format 4
adds the exact virtual-slot count, assignment array, and routing epoch to the existing layout state.
Layout format 5 preserves that routing identity and appends the index-schema protocol plus
per-rebuild maintenance intent; adopting it does not advance the routing epoch. The intent durably
excludes movement throughout the owner sweep, record pages, and final capability confirmation. It is
used for later generation changes as well as first adoption. Partition format 4
appends movement protocol/minimum-epoch/control fields and WAL operations for version advance,
import, and movement delete. Partition format 5 appends the schema capability, per-record
fingerprints, and the replayable `Reindex` operation. Existing values and every prior field ID remain
unchanged. The layout state's existing C# property names and Orleans field IDs remain durable,
including the `PartitionCount` JSON property at field ID 3. New fields use appended IDs and absent
fields are interpreted only through explicit compatibility paths.

Persistence formats 4 and 5 retain the recursive, assembly-version-independent type identity
introduced in format 2 and can read every format-3 record, journal, and snapshot representation.
Format 4 appends a snapshot record-encoding marker at field ID 9 and a lossless record list at field
ID 10; newly published format-4 snapshots leave the legacy string dictionary at field ID 8 empty and
encode persisted text as explicit UTF-16 code units. A format-3 active snapshot remains readable
after explicit enablement. The next format-4 or format-5 compaction writes the lossless
representation to the inactive snapshot slot and publishes it through the existing manifest fence,
without rewriting the active child in place. A movement-only owner sweep upgrades a supported
format-3 manifest to format 4. A schema-adoption owner sweep upgrades a supported format-3 or
format-4 manifest directly to format 5.
In either protocol, every current owner is durably fenced before the corresponding layout capability
is published. Ordinary activation and mutation do not opt into a newer capability. Version 1 and
version 2 partitions are intentionally not interpreted as formats 3, 4, or 5 and still require an
explicit migration or complete rewrite.

The per-state `index-schema` control document is persisted through the same physical provider but is
not embedded in a partition manifest or layout. It must be retained and backed up with both. Losing
only that control leaves a schema-enabled provider fail-closed. A quiesced rebuild can recreate the
active generation when the layout has no surviving schema-maintenance intent and every application
payload is still decodable. If an intent survives a mid-rebuild control loss, the public protocol
cannot reconstruct its missing rebuild/schema identity; recovery requires a consistent backup or a
reviewed physical repair. Reindexing uses the configured application-state serializer. A serializer-
or CLR-state-breaking deployment can therefore stop a
page after earlier records in that page have committed; its durable control cursor remains safe to
retry after payload compatibility is restored. The remote failure identifies the provider, state,
record, owner, and exception type, but deliberately omits the application-controlled exception
message and all raw index or payload values.

Orleans serializer `[Id]` values on persisted layout, manifest, journal, snapshot, record,
index-entry, and `IndexValue` types are field identities. Existing IDs must never be reused or
renumbered after release. New fields must use new IDs and remain readable when absent from older data.

The existing `RangeIndexQuery` protocol remains a strictly bounded message: both lower and upper
bounds are required and retain field IDs 1 and 2. Open bounds exist only on the new recursive
`PartitionQueryPlan`, which is transmitted between participants but is never persisted. Its field
IDs and operation values are still wire compatibility and are frozen by contract tests.

The query-plan and routed RPCs change the internal grain contract; mixed-version execution is not
supported. The bounded `ExactIndexQuery` and `RangeIndexQuery` messages and legacy partition methods
remain frozen for compatibility on updated processes, while the current client carries them inside
additive routed envelopes.

Facet request/result messages are likewise non-persisted but wire-frozen. Adding them and changing
the activation-derived hash value projection did not change layout format 4, the then-current
partition persistence format 3, record/index entries, snapshots, journals, manifests, or the write protocol. Mixed-version
facet traffic is unsupported: query traffic remains quiesced until every partition host and built-in
query client understands all typed response families.

Virtual routing adds server methods without removing the legacy partition and layout methods, but
method addition alone does not make a mixed-version cluster safe. A new `SearchableGrainStorage`
uses routed layout and partition methods on its first operation; Orleans may place the receiving
grain activation on an old silo which does not implement them, and an old activation cannot read a
format-4 layout. Operators must quiesce searchable storage and query traffic, update every silo and
Orleans client, and verify that no version-3 process remains. While traffic is still paused, one
normal grain-state storage operation must adopt each provider namespace as the epoch-1 identity map;
operators should verify that the admin read succeeds and reports epoch 1.
Starting a managed-schema rebuild is an alternative quiesced adoption path: its first step invokes
the same idempotent routing initializer before it creates the schema-rebuild intent.
The admin path returns a snapshot only for routing-capable format 4 or 5. Version-3 adoption performs one layout CAS and no
partition-persistence write. Query and admin reads do not perform adoption. Legacy calls remain
available on updated processes, and the identity map preserves their modulo placement, but this is
not an online rolling upgrade guarantee. Operators may then resume traffic with movement disabled,
or keep it paused if they are enabling movement in the same maintenance window. Live movement has a
second explicit gate: quiesce traffic again if it was resumed, restart every participant on the
movement-capable package, call `EnableMovementAsync`, and
require the enabled protocol from the admin read. The owner-by-owner enablement intent is resumable,
but quiescence remains an operator precondition until its final capability/epoch CAS commits.

Managed schemas add another mixed-version boundary. Before first adoption, operators quiesce the
whole provider before deploying the first schema-capable binary or changed registration. While it
remains paused, they deploy the complete registration set to every silo and external query client
and verify that no older participant remains. The owner sweep may make an individual partition
reject unbound legacy calls before the final layout capability write, so starting the sweep is
already an irreversible rollout decision. Once the layout publishes schema protocol version 1, old
binaries, clients without a complete registry, and persistence-v3/v4 rollback are unsupported. That
publication does not activate a state generation; the following state-control commit activates only
the rebuild's target fingerprint. Finish and verify every registered state before ending the
first-adoption provider pause. Later schema generation changes remain quiesced for each
affected state and invalidate existing page/distinct-facet continuations; a layout change between
rebuild turns restarts the durable scan automatically.

Physical providers can use serializers other than Orleans' binary serializer. With JSON persistence, CLR type and property names plus configured JSON converters are part of the compatibility surface; Orleans `[Id]` values do not rename JSON members. The memory contract suite explicitly uses `JsonGrainStorageSerializer` so partition and layout reactivation exercise that representation.

## Physical backend contract

Backend tests must exercise the same public storage contract instead of backend-specific expected
behavior. The contract covers partition rehydration, exact and range lookup, incremental index
replacement and removal, optimistic-concurrency rejection, persisted-layout mismatch, bounded
journal replay, snapshot compaction, and before/after-commit failure boundaries for mutation and
maintenance transitions. It also covers one live move under routed writes, point/index/facet
membership during and after the epoch change, source/target reactivation, and single authority. A
shared schema case upgrades a real format-3 record to persistence format 5, preserves its ETag,
compacts/reactivates partition and control state, and verifies managed update and clear behavior.

The contract runs unchanged against Orleans memory, ADO.NET/PostgreSQL, Redis, and Azure Blob
providers. A compatible physical provider must offer atomic whole-state writes, authoritative point
reads, and ETag-based conditional replacement; those guarantees fence manifest and reusable child
slots. Each external fixture registers the official Orleans 10.2.2 provider under an internal name
and places the same failure-injecting decorator under `Orleans.SearchableStorage.Physical`. The
decorator is test infrastructure only. All fixtures select `JsonGrainStorageSerializer`, so
reactivation crosses a physical serialization boundary instead of retaining object references.

External resources are isolated per fixture. PostgreSQL receives a unique schema and an Npgsql connection string with that schema as its search path. Redis receives a unique Orleans service id, which is part of every provider key. Azure Blob receives a unique container. The fixtures stop their two-silo clusters before deleting those resources. CI provides PostgreSQL, Redis, and Azurite through pinned containers, while connection-string overrides allow the same tests to target a separately managed environment.

External fixture initialization treats backend preparation, cluster construction, and deployment as one guarded lifecycle. Release is completed in one fixture call: it attempts a graceful cluster stop, always performs full `TestCluster` disposal, and only then removes the backend namespace. Full disposal releases the port allocator and any handles left by a partial stop failure. The first failure remains primary, while later stop, disposal, and cleanup failures are attached under stable diagnostic keys. Cleanup is marked final only after the cluster stopped or was fully disposed, or when cluster construction never completed. Deterministic lifecycle tests cover preparation, construction, deployment, ordered one-call release, and combined failure diagnostics without environment-variable mutation or live infrastructure.

Provider cleanup is deliberately namespace-scoped and tested against unrelated sentinels. PostgreSQL drops only its quoted schema, Redis deletes only keys under the selected Orleans service id's `state` prefix, and Azure Blob deletes only its generated container. Redis keys discovered through multiple endpoints are deduplicated and removed with bounded batches of single-key commands, so cleanup remains valid when keys occupy different Redis Cluster hash slots. Its live case first writes two states through the official Orleans provider and derives owned and unrelated sentinels from both physical keys; this guards the cleanup contract against provider key-format drift. These checks run against the real external service already used by the fixture and do not deploy a second Orleans cluster.

The vendored PostgreSQL bootstrap scripts retain the complete upstream MIT notice and their Orleans 10.2.2 source URLs. Their complete SHA-256 values live in `eng/orleans-sql.sha256`; the regular CI job verifies that manifest before any build or backend job can pass.

Azurite validates the Azure Blob provider protocol and storage semantics used by the searchable layer, but it is not a substitute for Azure service-level performance, identity, network, redundancy, or disaster-recovery testing. The pinned emulator uses `--skipApiVersionCheck` so a newer Azure SDK service-version header reaches the implemented operations; this does not enable unsupported service behavior or weaken the storage assertions. Likewise, passing the common contract does not equate the operational characteristics of PostgreSQL, Redis, and Blob Storage.

Every change is also evaluated using the mandatory test-sufficiency review described in [testing.md](testing.md). That review verifies behavioral, failure, durability, distributed, serialization, and sample coverage rather than relying on a raw test count.

An S3-compatible provider is not part of the current supported matrix. Adding one requires an Orleans `IGrainStorage` implementation and the complete contract; Azure Blob compatibility does not imply S3 compatibility.

## Measurement boundary

The benchmark projects are outside the shipping package. Process-local cases call extracted internal
helpers which are also called by `StoragePartitionGrain` and `StoragePartitionPersistence`; benchmark
copies of query evaluation, replay, or snapshot construction are forbidden because they can drift
from the durability and query contracts. The journal-append microcase substitutes only the physical
`IPersistentState` boundary and is labelled as a state-machine measurement. Movement microcases call
the production slot-index rebuild and export/import/delete helpers with uniform, skewed, and
oversize-singleton fixtures. They report page work honestly and do not erase retained
whole-partition snapshot/recovery boundaries.

Distributed measurements keep scenario, dataset, and workload inputs independently versioned and
content-addressed. Each client owns raw compatible HDR histograms; aggregation unions recordings
before computing percentiles. Searchable and plain Orleans baselines run identical deterministic
point histories in isolated namespaces. Secondary-index queries have no plain-storage analogue.

Open-loop latency begins at the scheduled offer time, not worker dequeue time, so queueing and
coordinated omission remain visible. Offered work that cannot enter the bounded queue, or that was
accepted but cannot start after a fatal/canceled phase shutdown, is recorded as dropped. A timeout
ends the caller's wait but not an Orleans RPC, so the driver tracks and drains the
underlying operation before another phase; an incomplete drain invalidates the run. Source artifacts,
the post-override canonical effective configuration, commit/dirty state, runtime/GC, topology, and
machine identity travel with every result. Provider credentials never do.

Distributed phase barriers report each client ordinal's success or failure and release every client
with the same aggregate outcome. The barrier RPC explicitly outlives the maximum configured barrier
plus late-drain windows; the driver's shorter scenario-specific deadlines remain authoritative rather
than Orleans' default response timeout.

The execution tiers and capacity tuple are defined in [benchmarks.md](benchmarks.md). CI proves that
the harness and oracles work; it does not certify performance. Provider-scale conclusions require a
trusted dedicated runner, provider-native telemetry, raw artifacts, cleanup evidence, and repeated
same-hardware baselines.

## Current scaling limit

Normal mutation I/O is bounded by one configured journal segment plus one small manifest, and index
maintenance touches only the changed buckets. The remaining partition-size costs are activation and
compaction: an activation materializes the whole active snapshot, and compaction copies and
serializes the whole partition while holding its non-reentrant turn. A snapshot is capped at
1,000,000 records and 512 MiB of deterministic canonical record bytes; journal entries and segments
have the dual count/byte ceilings described above. Those ceilings are safety bounds, not provider-
physical sizes or evidence that a near-limit partition has acceptable latency or memory use. The
activation also retains both the existing lookup indexes and the ordered catalog/postings used by
paging; benchmark capacity tuples therefore report the additive retained-memory cost rather than
treating per-operation allocation as activation size.

Queries fan out to every distinct current owner in the layout snapshot. The epoch-1 identity map has
one owner for each initial partition, so increasing the initial `PartitionCount` reduces records and
snapshot size per partition but increases query RPC fan-out. Paging bounds each partition turn,
owner response, coordinator buffer, public page, and continuation. It does not reduce fan-out or the
activation's retained whole-partition state, and following a broad traversal still consumes work
across many pages. The benchmark and capacity plan is tracked in repository
[issue #8](https://github.com/Neftedollar/Orleans.SearchableStorage/issues/8); results must report
both total records and records per physical partition instead of extrapolating from one aggregate
count. Live movement can redistribute fixed virtual slots across a changed owner set, but move cost
and write unavailability remain proportional to the selected slot's records and bytes, including
skew. It does not change the whole-partition activation/compaction model or justify a capacity claim
without representative end-to-end evidence.
