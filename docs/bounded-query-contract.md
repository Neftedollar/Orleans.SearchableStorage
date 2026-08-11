# Bounded query and paging contract

This document freezes the implemented bounded-query delivery contract tracked by
[issue #5](https://github.com/Neftedollar/Orleans.SearchableStorage/issues/5). PR13 introduced the
materializing-evaluator work baseline and protocol design; PR14 adds the ordered partition engine,
public paging API, bounded compatibility terminals, authenticated-encrypted continuations, and
implementation-specific benchmarks. PR15 adds typed indexed-facet response families, bounded
distinct-value paging, exact or certified-approximate top-N counts, and exact extrema tracked by
[issue #9](https://github.com/Neftedollar/Orleans.SearchableStorage/issues/9).

The keywords **must**, **must not**, **should**, and **may** are normative for protocol version 1.

## Required properties

The bounded protocol must provide all of these properties together:

- one partition turn has a deterministic logical-work ceiling;
- every partition response and every public page has count and encoded-byte ceilings;
- results are sorted and distinct in one versioned, stable `GrainId` order;
- a continuation resumes an ordered prefix without retaining activation-local cursor state;
- a page never combines partition results from different routing epochs;
- failure, cancellation, or a stale route returns no partial page and no advanced continuation;
- concurrent writes have explicit weak-consistency semantics rather than an implied snapshot;
- internal posting-list representation and evaluator choice do not leak into tokens;
- distinct-value and count-probe payloads cannot be confused with a `GrainId` page or each other.

This protocol does not create a distributed snapshot, add text search, or define live slot movement.
It also does not change persistence format 3 or layout format 4. A later slot-movement protocol must
increment the layout epoch and therefore invalidate continuations created under the previous map.

## Logical work accounting

The retained PR13 materializing baseline measures this checked component vector:

| Component | Charge rule |
| --- | --- |
| `EmptyNodeCount` | One for each evaluated empty plan node. |
| `ExactNodeCount` | One for each evaluated exact plan node. |
| `RangeNodeCount` | One for each evaluated range plan node. |
| `AndNodeCount` | One for each evaluated intersection node. |
| `OrNodeCount` | One for each evaluated union node. |
| `ExactCandidateCount` | One for each record-key occurrence copied from an exact bucket, including an occurrence later removed by a boolean node. |
| `RangeBucketVisitCount` | One for each range bucket visited by the bounded range view, including an equal endpoint bucket excluded by an open bound. |
| `RangeCandidateCount` | One for each record-key occurrence offered by an included range bucket, whether or not insertion changes the destination set. |
| `AndCandidateCheckCount` | One for each left-side record key checked by the current intersection operation, whether or not it survives. |
| `OrCandidateMergeCount` | One for each right-side record key offered to the current union operation, including duplicates. |

The derived values are normative measurement vocabulary:

```text
NodeCount =
    EmptyNodeCount + ExactNodeCount + RangeNodeCount + AndNodeCount + OrNodeCount

CandidateOperationCount =
    ExactCandidateCount + RangeCandidateCount
    + AndCandidateCheckCount + OrCandidateMergeCount

TotalOperationCount =
    NodeCount + RangeBucketVisitCount + CandidateOperationCount
```

All additions use checked arithmetic. The component vector remains part of diagnostics and tests even
when `TotalOperationCount` is used for comparisons. A strategy must not make expensive work disappear
from the total merely by moving it from a hash-set operation into a bitmap, an ordered driver, or a
new helper. This is a deterministic logical-work measure, not elapsed time, CPU instructions, or an
assertion that every component has equal physical cost; latency and allocation remain benchmark
outputs alongside it.

That baseline observes work after whole-plan evaluation; it remains comparative instrumentation, not
the bounded page budget. Work-policy version 1 charges this production page vector:

| Component | Charge rule |
| --- | --- |
| `OrderedCandidateVisitCount` | One before visiting a complete canonical candidate group. |
| `RecordProbeCount` | One before inspecting each live record occurrence in that group. |
| `PredicateNodeProbeCount` | One before evaluating each plan-node occurrence against a record. |
| `IndexEntryProbeCount` | One before inspecting each record index-entry occurrence. |
| `OwnershipProbeCount` | One before the routing-ownership lookup for a candidate group. |
| `PostingSeekCount` | One before each ordered catalog, exact-posting, range-bucket, or posting-boundary seek. |
| `RangeBucketVisitCount` | One before accessing each selected ordered range bucket. |
| `ResultMaterializationCount` | One before adding a matching `GrainId` to a response. |
| `RangeMergeOperationCount` | One before loading each range-posting candidate occurrence and before each canonical comparison used to merge or group occurrences. |

Its checked `TotalOperationCount` is the sum of all nine fields. The partition charges before each
data-dependent step and stops at a safe cursor boundary before the effective budget would be
exceeded. Bulk collection and vectorized operations charge their logical contents, not one operation
for an arbitrarily large input. A future representation which introduces another kind of work must
map it to an exact documented rule or increment the work-policy version with a new non-zero field.

Facet work-policy version 1 separately charges this checked vector:

| Component | Charge rule |
| --- | --- |
| `ValueSeekCount` | One before seeking an ordered facet-value catalog or one exact-value posting. |
| `ValueVisitCount` | One before visiting one distinct indexed-value bucket. |
| `GrainGroupVisitCount` | One before visiting one canonical `GrainId` group in an exact-count posting. |
| `OwnershipProbeCount` | One before checking the group against the current virtual-slot owner. |
| `RecordProbeCount` | One before reading one live record occurrence in the group. |
| `PredicateNodeProbeCount` | One before evaluating one query-plan node occurrence against that record. |
| `IndexEntryProbeCount` | One before inspecting one record index-entry occurrence. |
| `CountIncrementCount` | One before increasing an exact matching count. |
| `ResultMaterializationCount` | One before adding one distinct value or candidate metadata item to a response. |

Its `TotalOperationCount` is the checked sum of all nine components. Candidate nomination reads the
ordered bucket value and stored posting cardinality; every candidate page reports zero group,
ownership, record, predicate, index-entry, and count-increment work. Exact filtered counts are a
separate resumable operation and charge every posting group and predicate probe there. This split
prevents an implementation from hiding an unbounded posting scan behind one candidate item.

The existing plan limits of 64 levels and 256 nodes remain independent admission limits. Request
validation, token decoding, and plan validation are bounded by their own size limits; they must happen
before partition fan-out where possible.

The PR13 matrix remains a baseline for the materializing evaluator only. The checked-in matrix also
covers the ordered, resumable, and candidate-driven implementation described below, including its
activation build, mutation, retained memory, query latency, allocation, progress, and logical-work
costs. Defaults are conservative operating choices within hard structural safety caps, not latency
SLOs or capacity claims. The named policy constants are:

- `DefaultPartitionWorkBudget` and `MaximumPartitionWorkBudget`;
- `DefaultPageSize` and `MaximumPageSize`;
- `MaximumPartitionResponseItems` and `MaximumPartitionResponseBytes`;
- `MaximumCoordinatorBufferedItems`, `MaximumCoordinatorBufferedBytes`, and `MaximumPageBytes`;
- `MaximumContinuationTokenBytes`;
- the legacy aggregate work and result ceilings described below;
- `MaximumFacetTopN`, `MaximumFacetAggregateWork`, `MaximumFacetRounds`,
  `MaximumFacetAggregateItems`, and `MaximumFacetAggregateBytes`.

The repository retains both the PR13 materializing baseline and every implementation-specific
comparison, plus dataset distribution, records-per-partition value, runtime configuration, and
decision rationale. A client request cannot raise an effective value above a server-side maximum.

## Canonical ordering

The version-1 page order is the existing ascending `GrainId` order: compare the grain type bytes
lexicographically as unsigned bytes, then compare the grain key bytes the same way. Equality uses the
complete type and key byte sequences. Pages and partition responses are sorted and distinct under
this comparator.

A canonical version-1 `GrainId` contains 1–1,024 type bytes and 1–4,096 key bytes. The same bounds
are enforced before sizing, encoding, protecting, and decoding a frontier. A canonical facet text
value contains at most 16,384 bytes of valid strict UTF-8. The 32 KiB hard token cap covers either
the maximum canonical `GrainId` or text-value frontier plus the versioned AEAD envelope; a smaller
configured token limit can still reject an unusually large frontier without emitting an unusable
continuation.

The storage write path predates and does not enforce the facet text ceiling. An indexed string over
16,384 UTF-8 bytes, or a string/`char` containing an unpaired UTF-16 surrogate, can therefore exist
durably but cannot be represented by the canonical facet wire format. A distinct, top-N, or min/max
traversal which reaches that value fails the complete operation without a partial page, aggregate, or
advanced continuation; it never truncates, replaces, or skips the value. Applications which use text
facets must enforce this schema constraint before writes and rewrite any pre-existing incompatible
records. The complete operation deterministically throws
`SearchableStorageQueryLimitExceededException`; this existing query/facet wire-domain limit is not a
write rejection. Raising or changing it requires a new wire/order version rather than a silent
reinterpretation.

The ordering algorithm has an explicit version in requests, responses, fingerprints, and
continuations. It must not depend on:

- `GetHashCode`, virtual slot, or physical owner;
- locale, process, runtime hash randomization, or collection enumeration order;
- activation-local dense ids, bitmap ordinals, selectivity estimates, or evaluator choice;
- assembly version or JSON formatting.

The existing record-key string encodes state name plus hexadecimal grain type and key bytes. An
implementation may seek an ordered record-key structure only after golden and property tests prove
that, for one state name, its ordinal order is exactly equivalent to the version-1 `GrainId` order.
The public frontier and token still carry a canonical `GrainId` encoding, not the internal record
key and never an activation-local id.

## Derived ordered-access implementation

Version 1 implements and measures this resumable baseline:

- an activation-local ordered catalog for every state name, keyed by canonical version-1 `GrainId`
  order and pointing to the corresponding live record;
- activation-local ordered postings for every exact and range bucket, using the same `GrainId`
  comparator;
- synchronous catalog and posting updates in the same non-reentrant turn which applies a committed
  record/index mutation.

These structures are derived from durable records whenever a partition activates. They are not
persisted, do not change snapshot or journal formats, and do not make activation-local dense ids part
of ordering or continuation. Rebuild must reject the same malformed records and indexes as the
existing derived-index build.

Both hash and range scopes expose a canonical balanced-tree projection by `IndexValue`. Before
facets, the hash lookup projection used an unordered dictionary of buckets; PR15 replaces only that
activation-derived projection with the same ordered bucket structure used by ranges. Durable index
entries, physical writes, persistence format, commit atomicity, and the write protocol do not change.
The derived in-memory mutation path does change: hash value lookup/mutation moves from average
`O(1)` dictionary access to `O(log D)` balanced-tree access for `D` distinct values, and every live
add/remove maintains the scope's checked total-record counter. Activation rebuild and each committed
mutation update the lookup and ordered projections together. Candidate nomination reads bucket and
scope scalar metadata and never walks a posting; filtered count probes walk only an explicitly
nominated exact posting in bounded canonical `GrainId` slices. The activation-build and steady
mutation benchmarks retain the materializing representation as a comparison and gate the total
counter under both uniform/high-cardinality and hot/low-cardinality distributions.

The baseline query access paths are:

- an exact leaf streams its ordered exact posting;
- a selective exact `AND` range query uses the ordered exact posting as its driver and tests the
  remaining range predicate for each candidate instead of materializing the broad range side;
- a range leaf performs a bounded k-way merge of the ordered postings in its selected buckets;
  bucket enumeration, posting seeks, heap initialization, comparisons, and duplicate candidates all
  consume logical work, and a query which cannot initialize that merge within its budget falls back
  to the ordered state catalog. Version 1 admits the merge with a conservative whole-scope bucket
  bound because the underlying balanced tree does not expose an O(log N) selected-range rank; a
  narrow range over a high-cardinality scope can therefore choose the catalog fallback even when
  only a small bucket window would match;
- `OR` and a general plan without a proven selective driver fall back to a bounded scan of the
  ordered state catalog and test the complete predicate for each candidate.

An implementation may select another driver only when it proves that the driver is a superset of the
remaining matches and preserves canonical order. Every occurrence for one candidate `GrainId`,
including duplicates from several range buckets or boolean branches, forms one candidate group. The
group and every required predicate probe must complete before the result is emitted or the frontier
advances. If the remaining work cannot cover the complete group, execution stops at the preceding
frontier; it must not serialize an internal heap, bucket cursor, or partial predicate state into the
public token.

Benchmarks report ordered-catalog and posting rebuild time, steady mutation latency and
allocation, retained activation memory, page latency/allocation, and the complete work vector for
exact, range, selective exact-and-range, broad `AND`, broad `OR`, duplicate-heavy `OR`, and catalog
fallback cases. The current hash-set/materializing results remain the baseline; they do not by
themselves justify the ordered strategy or its limits.

## Partition prefix protocol

A partition page request conceptually contains:

- a paging protocol version and response-family discriminator;
- the complete validated query plan and its fingerprint;
- layout format, routing epoch, and layout fingerprint;
- the canonical ordering version;
- one exclusive global `after` boundary, absent on the first page;
- the effective work, item, and encoded-byte limits.

The partition remains non-reentrant. It validates the request, evaluates only one bounded slice, and
returns a response containing:

- sorted, distinct matching `GrainId` values after the input boundary;
- a finite inclusive frontier or an exhausted marker equivalent to positive infinity;
- the complete logical-work vector and `TotalOperationCount`;
- an explicit stop reason such as exhausted, work budget, item limit, or byte limit;
- the protocol, response family, fingerprint, order, layout, and epoch values needed for the
  coordinator to reject a mismatched response.

For input boundary `B` and returned frontier `F`, the central invariant is:

> Relative to the one partition-local state observed during that non-reentrant turn, the response
> contains every matching `GrainId` in `(B, F]` and contains no item outside that interval.

The evaluator may drive an `AND` from a selective child because the final result is a subset of that
child. An `OR` driver must be a proven superset of every branch, or it must fall back to a bounded
ordered record scan. Whatever strategy is used, advancing `F` asserts that no matching key at or
before `F` was omitted. A partially evaluated candidate cannot advance the frontier. Work needed for
a non-resumable internal operation must be conservatively precharged, or that operation must itself
be made resumable.

For any non-empty remaining candidate stream, a conforming request must either advance the frontier,
mark the partition exhausted, or fail with a deterministic budget-too-small error. Repeated success
responses with `F == B` are forbidden because they would create a non-progressing continuation.

Each partition response is independently bounded by both item count and encoded bytes. A single
`GrainId` which cannot fit the maximum encoded response fails explicitly; it is not silently skipped.

## Coordinator merge

The coordinator obtains one immutable layout snapshot and contacts every distinct owner in sorted
owner order. It supplies the same `B`, plan, fingerprint, epoch, ordering, response family, and
effective policy to every owner. Local response limits are derived so the worst-case aggregate is no
greater than `MaximumCoordinatorBufferedItems` or `MaximumCoordinatorBufferedBytes`; an impossible
owner-count/page-policy combination is rejected before fan-out.

A first-page query against an uninitialized namespace returns one final empty page without creating
the layout. A translated empty plan still performs the current layout and cancellation validation,
then returns one final empty page without partition fan-out. No valid continuation can exist for
either final result; supplying one is invalid rather than a request to restart silently.

Let `F[p]` be each partition's returned frontier and let an exhausted frontier compare as positive
infinity. The global safe frontier is:

```text
Fglobal = min(F[p] for every distinct current owner p)
```

Only returned items at or before `Fglobal` are eligible for this public page. Items beyond it came
from partitions which advanced farther than the slowest partition; the coordinator discards them and
may scan them again from the next global boundary. They are never hidden in the continuation token.

The coordinator performs a bounded k-way merge, deduplicates defensively, and applies count and byte
limits:

1. If more eligible items exist than fit, return the first fitting prefix and set the next exclusive
   boundary to its last item.
2. If every eligible item fits, set the next boundary to `Fglobal`. This may advance across an
   interval containing no matches.
3. Return no continuation only when every partition is exhausted and every eligible item fits.
4. Otherwise return an authenticated-encrypted continuation for the next boundary.

A non-terminal page may therefore contain fewer than the requested page size, including zero items.
Callers must follow the continuation rather than treating a short or empty page as end-of-results.
This is required to bound sparse or expensive scans without inventing results or holding a grain turn
until a match appears.

This algorithm keeps the continuation constant-size with respect to partition count. It deliberately
trades some repeated partition work for avoiding per-owner cursors or buffered result lists in the
token.

## Continuation token

The public continuation is opaque, stateless, and authenticated-encrypted. Its plaintext binds at
least:

- token and paging protocol versions;
- provider namespace identity;
- response family and response-specific parameters;
- canonical query fingerprint;
- ordering version;
- layout format, routing epoch, and layout fingerprint;
- response-specific exclusive global boundary: canonical `GrainId` for family 1 or canonical
  `IndexValue` for family 2;
- effective page size, work-policy version, and effective limits.

The query fingerprint is SHA-256 over a canonical, length-delimited binary encoding of the normalized
wire plan and state/index identities. It includes operation kinds, child order, scopes, index kinds,
canonical `IndexValue` payloads, bounds, and inclusivity. It does not use object hashes, JSON, or
activation-local representation. Semantically equivalent expressions are not required to share a
fingerprint; resumption requires the same normalized plan and captured values.

The application must configure one provider-scoped AEAD key ring consistently in every silo and
external Orleans client which can issue or resume a page. The version-1 protection baseline is
AES-256-GCM with a 256-bit key, a fresh 96-bit nonce for every token under that key, and a 128-bit
authentication tag. The clear envelope contains only the token-envelope version, algorithm id, key
id, nonce, ciphertext, and tag. Canonical provider identity, envelope version, algorithm id, and key
id are authenticated as associated data. The response-specific frontier, query/layout fingerprints,
and all other cursor fields remain inside the ciphertext because a nonmatching frontier can otherwise
reveal catalog or index existence. Family-1 version-1 tokens issued by the PR14 codec remain decodable
under the same key and binding; a checked-in captured-token oracle freezes that compatibility.

Every nonce is generated by a cryptographic random-number generator; nonce reuse under one key is
forbidden. Keys come only from application secret configuration. They are not persisted in layout or
partition state, embedded in tokens, logged, or copied into benchmark/test artifacts. Key ids are
operational identifiers and are not secret.

The configured key ring has exactly one current encryption key and may have explicit decrypt-only
keys with distinct stable ids. New tokens use the current key. Resumption selects a configured key by
id and accepts an older token only while that decrypt key remains configured; removing it invalidates
the token. `SearchableStorageQueryOptions.ContinuationProtection` is the concrete configuration
surface. It validates the current/decrypt-only key ring and snapshots every id and 32-byte key before
use; the public `SearchableStorageClient` overload supplies the same snapshot to external clients.
Paging must fail closed when the provider has no valid current key. There is no process-random key,
plaintext, MAC-only, checksum-only, or other insecure fallback.

A safe rotation first distributes the new key id and material to every participant as decrypt-only,
then switches every participant to that id as the current encryption key, and only later removes the
old decrypt key. Removing a decrypt key deliberately invalidates any outstanding token which names
it; the protocol does not pretend to resume such a token with another key.

Validation rejects an overlong encoded token before base64 decoding, rejects an oversized decoded
envelope before AEAD processing, authenticates the complete envelope before parsing plaintext, and
then applies strict length and field validation before partition fan-out. Authentication failure,
unknown key id, malformed plaintext, another provider, query, response family, ordering version, page
policy, or work-policy version is invalid. A valid token whose layout epoch or fingerprint no longer
matches is stale. Neither category is silently treated as a first page, and diagnostics must not form
an authentication oracle.

Encryption and integrity do not turn the token into application authorization. Normal application
authorization still decides whether a caller may issue the query, while the server independently
re-derives every hard work and response cap. Tokens must not contain record payloads, per-owner
buffered results, mutable enumerators, posting-list offsets, activation-local dense ids, bitmap
ordinals, or implementation-specific selectivity hints. Replaying the same token is allowed: tokens
are not consumed and the service retains no cursor session.

## Consistency under concurrent writes

Continuation is **weakly consistent**, not snapshot-like. One partition response observes one
serially consistent activation state, but owners may observe different instants during a page and a
later page observes later partition turns.

The exclusive global frontier gives these precise consequences:

- with no writes and one unchanged layout, concatenating pages yields exactly the same sorted,
  distinct set as full evaluation;
- a record which remains matching and keeps the same `GrainId` cannot appear on both sides of an
  advancing frontier;
- a record inserted or changed to match at or before an already returned frontier may be missed;
- a record returned on an earlier page remains in that page even if it is later deleted or stops
  matching;
- a record changed to match after the frontier may appear on a later page;
- replaying an input token can repeat its page, and concurrent writes can make the replay differ.

No continuation claims repeatable read, read-your-writes across owners, a total count, or a global
time at which all returned records matched. Consumers which require a snapshot must provide an
application-level immutable generation in their indexed predicate or use another storage system with
that isolation guarantee.

## Failure, cancellation, and routing changes

A public page is atomic at the coordinator boundary: all partition replies must complete and pass
their invariants before any page or next token is returned. A partition or transport failure discards
the complete attempt. No partial item list and no advanced continuation may escape.

Caller cancellation ends the local wait promptly but does not cancel Orleans calls already in
flight. Their aggregate completion must still be observed. Cancellation returns neither a partial
page nor a new token. Retrying the original request or token starts another weakly consistent attempt.

For a first page without a continuation, a route mismatch discards the complete attempt, refreshes
the shared layout snapshot, and may retry once, matching the current whole-query rule. For a resumed
page, the token pins its epoch: a mismatch or changed layout fingerprint makes the continuation stale
and must not be upgraded automatically. The caller must restart from the first page, because carrying
the old frontier into a moved layout could skip or duplicate records.

The first-page route retry is allowed only when the completed attempt contains routing mismatches and
no non-routing failure. Any validation, evaluator, serialization, transport, timeout, or
budget-too-small failure is authoritative and prevents a routing retry. All started calls are still
observed before a non-canceled attempt is classified; simultaneous failures use a deterministic
precedence rather than whichever task happens to notify first. After caller cancellation they may be
observed by a detached aggregate, as described above, without delaying the cancellation response.

A work, item, or byte limit is normal pagination control only when the partition returns a valid safe
frontier. Arithmetic overflow, an item too large for the hard byte cap, inability to make progress,
malformed responses, or a policy mismatch are failures and cannot be represented as an empty final
page.

## Legacy `ToGrainIdsAsync` policy

The built-in `SearchableStorageClient` preserves `ToGrainIdsAsync`'s all-results-or-exception shape
while bounding its work and memory:

- it executes through the bounded engine under hard aggregate work, item, byte, and round ceilings;
- it returns the complete sorted, distinct result only if every owner is exhausted within all of
  those ceilings;
- reaching any ceiling before exhaustion throws a dedicated limit exception and returns no partial
  list;
- it never silently truncates and never exposes an internal continuation;
- cancellation and failure retain the all-or-nothing rules above.

This terminal remains suitable for known-small result sets and compatibility. New code which can
consume multiple pages should use the queryable paged terminal.

An external `IQueryable` provider already controls `ExecuteToGrainIdsAsync` through
`ISearchableStorageAsyncQueryProvider`; an extension method cannot retrofit partition budgets into
that implementation. The public provider contract therefore requires external providers to own and
document their bounding semantics; it does not claim they use the built-in distributed protocol.

The built-in lower-level `FindAsync` and `RangeAsync` implementations route exact and range
evaluation through the same bounded partition paging RPC
and aggregate internally under the same hard legacy work, item, byte, and round ceilings. They return
the complete sorted, distinct result only when every owner is exhausted; otherwise they throw the
same dedicated limit exception without a partial list. A bounded value interval does not imply a
bounded match count. These compatibility methods do not expose continuation, so callers which need
to traverse a larger exact or range result must express it through the queryable paged terminal.
Neither the client nor a partition falls back to the old unbounded array-returning RPC.

## Indexed facet protocol

Facet terminals are opt-in through `ISearchableStorageFacetQueryProvider` and accept a selector for
one declared hash or range index on the query element type. The normal focused predicate is compiled
first, then evaluated for every nominated exact value. An unindexed, mismatched, nested, computed, or
otherwise unsupported selector is rejected before fan-out. Null property values have no index entry
and therefore never appear as a distinct value, count item, minimum, or maximum.

Canonical facet-value order is `IndexValue.CompareTo`: first the closed internal kind order, then the
kind's normalized ordinal/numeric order. It is independent of locale, owner, bucket insertion order,
runtime hashing, and CLR collection enumeration. The selected property's cached converter
materializes the canonical value back to its exact public CLR type; enum/underlying, nullable,
`char`/string, floating-point, timestamp, GUID, and Boolean shapes are not guessed from the internal
kind alone.

`ToDistinctFacetValuePageAsync` performs one bounded value-order turn on every distinct current
owner, using one exclusive canonical value boundary. The coordinator takes the least finite owner
frontier, deduplicates values at or before it, and exact-probes those values against the complete
predicate. Only positive-count values enter the public page. If the page fills, its last value is the
next boundary; otherwise the global safe value frontier is the boundary. No continuation is returned
only after every owner is exhausted and every eligible value fits. A non-terminal page may be short
or empty, and callers must continue until its token is null.

The distinct continuation uses the same AEAD envelope as id paging but binds response family 2,
facet query/index fingerprint, canonical facet-value ordering version, layout snapshot, provider,
page size, and effective limits. It contains one value boundary, never per-owner cursors, candidate
lists, counts, or activation state. It cannot resume an id page or another predicate/index. Across
public pages it is weakly consistent: unchanged data and layout produce the exact sorted distinct
sequence, while a concurrent insert at or before an emitted boundary may be missed and a later value
may appear. It is not a distributed or multi-page snapshot.

`ToFacetValueCountsAsync` requires an explicit `Exact` or `Approximate` accuracy and a positive
`TopN`. Owners nominate bounded canonical value pages containing `(value, raw local posting
cardinality)` metadata, the checked sum of those page counts, and the selected scope's total raw
posting occurrences. The total is pinned by the expected owner data version and must remain identical
on every response. The coordinator checked-accumulates page sums locally and derives that owner's
remaining bound as `TotalRawCount - cumulativePageRawCount`; exhaustion requires equality. No
accumulator is sent back to or trusted by the partition.

Every newly nominated value is exact-probed across every owner in bounded canonical `GrainId`
slices. The returned count for every included value is therefore exact under the attempt's pinned
owner versions. Ranking is count descending and then canonical value ascending. Every unseen value's
filtered count is at most the sum of its owners' remaining raw occurrences. Let `U` be that checked
sum. Exact top N is proven when all owners are exhausted
or when at least N positive candidates exist and the Nth count is strictly greater than `U`. Strict
comparison is required because an unseen value tied at the cutoff could win by canonical value order.
Exact mode keeps deepening candidate pages until that proof holds or all owners exhaust.

One exact-count slice advances only after a complete canonical `GrainId` group has been evaluated.
Work spent on a partially evaluated group is still charged, but neither its count delta nor its
frontier is committed; the next slice therefore replays that group from its preceding safe frontier.
If the first atomic group cannot fit the effective work budget, the partition reports a deterministic
budget-too-small failure and the public terminal remains all-or-throw. The durable storage invariant
is one record key per `(stateName, index scope, GrainId)`, while the grouping rule also remains safe
if a malformed or copied posting contains more than one record occurrence.

Approximate mode stops after the first bounded candidate turn. It can omit a true winner. Its
`MaximumOmittedCount` is the maximum of `U` and the largest exact nominated count excluded by `TopN`,
so it is an inclusive upper bound on every omitted value's count. `IsExact` is true only when the
same exhaustion/cutoff proof already holds. Consumers must inspect both fields; exact counts for the
returned candidates do not by themselves make the ranking exact. Exact mode can also return a
positive `MaximumOmittedCount` for values correctly ranked below N.

`ToFacetMinMaxAsync` exhausts bounded candidate pages and exact-probes each distinct value. It returns
the least and greatest positive-count canonical values or null for an empty filtered set. It is an
exact all-or-throw terminal; it never returns one-sided or partial extrema.

Every multi-turn facet attempt pins each owner to the data version returned by its first value or
candidate turn. All later value, candidate, and exact-count slices carry that expected version. If an owner changes,
the coordinator discards the complete attempt and restarts once from the same layout; a second change
throws `SearchableStorageFacetConcurrentChangeException`. This prevents one result from mixing two
versions of the same owner, but owners are still observed at different instants and no common
cross-partition snapshot is claimed. A first-attempt route mismatch may refresh the complete layout
once; a resumed distinct token is layout-pinned and becomes stale.

Facet aggregate work, candidate/probe turns, items, and encoded bytes are checked against
`FacetAggregateWorkLimit`, `FacetRoundLimit`, `FacetAggregateItemLimit`, and
`FacetAggregateByteLimit`. Partition work/item/byte limits still apply to every individual turn,
and `FacetTopNLimit` applies before fan-out. Candidate pages, every per-owner exact-count slice, and
filtered predicate work are all charged. Exhausting any terminal ceiling throws
`SearchableStorageQueryLimitExceededException` with no partial ranking or extrema. Failure,
cancellation, malformed responses, arithmetic overflow, a non-progressing slice, and concurrent-data
failure likewise return no partial aggregate or advanced continuation.

## Typed response families

The paging header and request fingerprint use this closed response-family discriminator:

| Value | Family | Payload |
| --- | --- | --- |
| 1 | `GrainIdPage` | Sorted matching `GrainId` prefix and safe key frontier. |
| 2 | `DistinctFacetValuePage` | Sorted indexed values and safe value frontier. |
| 3 | `FacetValueCountCandidates` | Value-ordered `(value, raw count)` metadata plus page and pinned total raw counts. |
| 4 | `FacetValueCountProbe` | One resumable exact-count delta and canonical `GrainId` frontier. |

Top-N and min/max are coordinator reductions over families 3 and 4 rather than ambiguous new wire
payloads. Unknown values are rejected before evaluation. Each family has separate request/result
messages with frozen Orleans field IDs; none uses `object`, an untyped dictionary, or optional fields
whose meaning depends on guesswork. A family-2 token cannot resume family 1, 3, or 4.

## Compatibility and rollout

The paging messages and token format are wire protocol even though they are not persisted storage
state. Unknown protocol, ordering, work-policy, response-family, or token versions fail closed.
Changes which alter cursor interpretation or work accounting require a new version and invalidate old
continuations rather than guessing.

The implementation adds no persistence or layout migration, but routes `ToGrainIdsAsync`,
`FindAsync`, `RangeAsync`, and facets through typed bounded RPCs. Operators must therefore quiesce
all searchable
query traffic before the first silo or query client is upgraded and keep it quiesced until every silo
which can activate a storage partition and every built-in query client has been upgraded and shares
the provider's AEAD key ring. Point storage reads, writes, and clears may continue during this
server-first rollout because their existing routed protocol and persistence format do not change.

After all participants are verified, bounded and legacy query traffic may resume together. Sending a
new paging/facet RPC to an old activation, or allowing an old query client to use an unbounded legacy
RPC,
is unsupported and must never trigger a compatibility fallback. A continuation from another paging
protocol or response family cannot cross the upgrade. The earlier version-3 to version-4 layout
adoption rules
remain a separate concern: PR15 does not repeat that migration for an already valid format-4
namespace.

## Acceptance evidence

The test and benchmark suites prove:

- exact work-vector and `TotalOperationCount` values for exact, range, selective `AND`, broad
  `AND`/`OR`, and duplicate-heavy `OR` fixtures;
- stopping immediately before and after every work, item, and byte boundary without an omitted key;
- ordered partition-prefix invariants and global frontier merge over multiple owners;
- activation rebuild and mutation equivalence for the ordered state catalog and ordered postings;
- ordered exact drivers, candidate-tested exact-and-range intersection, bounded range k-way merge,
  duplicate candidate groups, and ordered-catalog fallbacks;
- non-terminal short and empty pages, final-page detection, and no-write concatenation equivalence;
- the documented insert, update, delete, replay, and concurrent-turn weak-consistency cases;
- missing or inconsistent key-ring configuration, duplicate/unknown key ids, key rotation, nonce
  uniqueness, oversized envelopes, altered headers/nonces/ciphertext/tags, malformed authenticated
  plaintext, plaintext-leak checks, cross-provider associated data, cross-query, cross-family,
  wrong-policy, and stale tokens;
- first-page route refresh versus resumed-page stale-epoch rejection;
- immediate and late partition failures, cancellation, and no partial page or token;
- complete `ToGrainIdsAsync`, `FindAsync`, and `RangeAsync` results within limits and all-or-nothing
  failure at every legacy ceiling, with no unbounded-RPC fallback;
- canonical distinct-value pagination, null exclusion, family-bound continuations, and no-write
  concatenation equivalence for every supported indexed CLR value shape;
- exact filtered counts, canonical top-N tie order, conservative unseen bounds, strict cutoff proof,
  one-turn approximate certificates, exact extrema, and all-or-nothing aggregate ceilings;
- per-owner data-version pinning, one complete restart, deterministic concurrent-change failure, and
  no cross-partition snapshot claim;
- exact facet work vectors proving metadata-only candidate nomination and bounded resumable filtered
  count slices across low/high cardinality and uniform/skewed distributions;
- serializer round trips and frozen IDs for every new internal message;
- the same shared contract through Memory, PostgreSQL, Redis, and Azure Blob providers.

The selected evaluator, numeric defaults, and maxima additionally use the implementation matrix
under the evidence rules in [benchmarks.md](benchmarks.md), including activation build,
mutation, memory, and paged-query costs. Faster representation is useful only inside this boundary;
it does not replace the boundary.
