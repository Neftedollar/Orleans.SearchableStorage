# 1.0 product and query contract

This document is the human-readable contract candidate for Orleans.SearchableStorage 1.0. It has
two deliberately separate purposes:

- **Implemented now** records behavior provided and tested by the current codebase.
- **Freeze for 1.0** records the implemented scalar and bounded-membership boundary proposed for the
  release contract.

This is not a claim that version 1.0 has been released, and it does not create compatibility
promises before that release. A discrepancy between this matrix, the public API, and an executable
contract test is a bug to resolve explicitly; it is not permission to broaden the product by
accident. The protocol and operations documents linked below remain authoritative for their deeper
wire-format and runbook details.

## Product boundary

**Implemented now, and the intended 1.0 boundary:** Orleans.SearchableStorage provides an integrated
Orleans `IGrainStorage` mode and a payload-free index-only mode, both with derived secondary indexes
and bounded discovery of matching `GrainId` values.

In integrated mode, applications persist state through `IPersistentState<T>` and the provider stores
a record and its local index changes through the same journaled commit path. In index-only mode, the
application owns payload persistence and explicitly submits index replacements or removals. A query
returns identifiers in either mode; the application decides how to hydrate state or perform work.

It is intentionally not any of the following:

- a relational, document, or general-purpose database API;
- an object materializer, repository, or unit-of-work abstraction;
- arbitrary LINQ, client-side expression fallback, or a silent full-data scan API;
- SQL, text search, `StartsWith`, substring search, composite indexes, joins, projections, grouping,
  or ordering;
- a cross-partition transaction or distributed snapshot;
- a transaction, outbox, ordering, deduplication, or stale-event layer between an external payload
  store and an index-only namespace;
- a promise that query cost is independent of partition size or owner count.

Each non-empty query page contacts every distinct current owner; a predicate which translates to an
empty plan can stop after validating the layout and schema. A partition activation retains derived
indexes and an active snapshot in memory, and compaction has a whole-partition boundary. These are
important sizing properties even though mutation journals, page work, response sizes, and movement
pages are bounded separately.

### Implemented bounded membership slice

The 1.0 candidate includes one deliberately narrow extension to the scalar surface: bounded Hash
membership for exact SZ `T[]` and exact `List<T>` properties, plus the canonical bounded scalar
`WhereIn` operator. Both lower to existing exact/Boolean query plans and share the normal bounded
page engine; there is no unbounded scan, new wire opcode, or client-side fallback.

The supported shapes, null/duplicate behavior, admission limits, and API spelling are frozen below.
`StartsWith`, text search, arbitrary collection interfaces, and other LINQ membership shapes remain
explicitly deferred.

## Public surface

| Need | Public surface | Contract |
| --- | --- | --- |
| Register the provider | `AddSearchableGrainStorage` | Registers the named `IGrainStorage`, query client, and admin client. |
| Register a payload-free index | `AddSearchableIndex` | Registers the named index writer, query client, and admin client, but no `IGrainStorage`. |
| Declare a state schema | `AddSearchableStorageState<TState>` | Binds one provider/state-name pair to one CLR type and positive application schema version. |
| Replace indexed values | `ISearchableStorageIndexWriter.UpsertAsync<TState>` | Extracts marked properties from the supplied state and unconditionally replaces the key; it never serializes or retains the payload. |
| Remove indexed values | `ISearchableStorageIndexWriter.RemoveAsync<TState>` | Unconditionally removes the key; an absent key is a successful no-op. |
| Declare an index | `[SearchableIndex(Hash\|Range)]` | Marks one readable public instance property; `Name` supplies its stable effective name. |
| Direct exact lookup | `ISearchableStorageClient.FindAsync` | Complete, sorted, distinct `GrainId` result or an exception; no truncation. |
| Direct bounded-range lookup | `ISearchableStorageClient.RangeAsync` | Same all-or-throw result contract; the selected property must use a range index. |
| Deferred query | `ISearchableStorageQueryClient.Query<TState>` | Creates an expression root for the focused predicate subset below. |
| Bounded scalar membership | `WhereIn` | Snapshots at most 64 raw non-null values for one scalar Hash or Range index and creates a deferred canonical exact/OR predicate. |
| Bounded identifier page | `ToGrainIdPageAsync` | Preferred terminal for a result which can be large. |
| Compatibility identifier result | `ToGrainIdsAsync` | Collects bounded pages and returns all results or throws at an aggregate ceiling. |
| Distinct facet page | `ToDistinctFacetValuePageAsync` | Returns non-null indexed values in canonical value order. |
| Top-N value counts | `ToFacetValueCountsAsync` | Returns exact counts with an explicit exact or bounded-approximate ranking contract. |
| Extrema | `ToFacetMinMaxAsync` | Returns an exact pair, or `null` when no indexed value matches. |
| Schema lifecycle | `GetIndexSchemaAsync`, `RebuildIndexSchemaAsync` | Reports or drives the quiesced, durable, resumable managed-schema lifecycle. |
| Layout and movement | `GetLayoutAsync` and the movement/rebalance methods | Exposes explicit operator-driven routing changes; core storage does not auto-rebalance. |

The three external-query provider interfaces are independent opt-ins:
`ISearchableStorageAsyncQueryProvider`, `ISearchableStoragePagedQueryProvider`, and
`ISearchableStorageFacetQueryProvider`. An external provider owns its execution and bounding
semantics; the built-in guarantees in this document do not silently attach to another provider.

## Index declaration contract

An indexed member must be a readable public instance property. Inherited properties are supported.
Fields, write-only properties, nested selector paths, and duplicate effective index names are
rejected. A scalar, collection, or other non-object state remains a valid Orleans storage value but
cannot declare indexed properties.

Null is not an index key. A null scalar, null collection, or null collection element contributes no
index entry, never matches a predicate, and never appears in a facet.

### CLR type matrix

This is the closed built-in **scalar** index-value set implemented now and proposed as the scalar
part of the 1.0 freeze. A scalar `Hash` index supports equality and all facet terminals. `Range`
adds relational query operators. A collection `Hash` index supports only the exact membership
predicate described below.

| CLR property type | Hash | Range | Canonical behavior |
| --- | :---: | :---: | --- |
| `string` | yes | yes | Ordinal text equality and order. |
| `char` | yes | yes | One-character ordinal text. |
| `sbyte`, `short`, `int`, `long` | yes | yes | Signed integral value and numeric order. |
| `byte`, `ushort`, `uint`, `ulong` | yes | yes | Unsigned integral value and numeric order. |
| `decimal` | yes | yes | Decimal value and order. |
| `float`, `double` | yes | yes | Floating-point value and order; NaN is not indexable. |
| `DateTime` | yes | yes | UTC ticks; indexed values must have `DateTimeKind.Utc`. |
| `DateTimeOffset` | yes | yes | Normalized UTC ticks. |
| `Guid` | yes | no | Exact lookup and canonically ordered facets only. |
| `bool` | yes | no | Exact lookup and canonically ordered facets only. |
| Any enum with a CLR integral underlying type | yes | yes | Uses the underlying integral codec and order. |
| `Nullable<T>` for a supported value type | same as `T` | same as `T` | Non-null values use `T`; null is omitted. |

Representative scalar types outside this closed set include `TimeSpan`, `DateOnly`, `TimeOnly`,
`Half`, `Int128`, `UInt128`, `nint`, and `nuint`. Arrays and collections are unsupported except for
the exact membership shapes below. Adding another scalar, collection shape, or element domain is a
deliberate contract, codec, schema-fingerprint, documentation, and test change, not an incidental
converter refactor.

Additional value rules:

- indexed `float` or `double` NaN fails the write; a query comparison against NaN has an empty
  result. Infinities are supported;
- indexed `DateTime` values which are Local or Unspecified are rejected rather than normalized;
- compiler numeric and enum promotions are accepted only when translation preserves CLR equality
  and ordering over the complete indexed domain;
- canonical query and facet text fields use strict UTF-8 with a 16 KiB encoded limit. The storage
  write path does not share that wire limit and preserves invalid-surrogate strings, but the fixed
  storage envelope still rejects an index entry above 64 KiB of canonical data or more than
  512 KiB of aggregate canonical index-entry data for one record.
  A query value cannot be encoded beyond the limit, and a facet which reaches an incompatible
  stored value fails without a partial result. See the bounded protocol for exact failure details.

### Collection membership declaration

Only these property shapes can declare a collection membership index:

- an exact one-dimensional, zero-based SZ array `T[]`;
- an exact `System.Collections.Generic.List<T>`.

`T` must be one scalar type in the matrix above, including `Nullable<TValue>` when `TValue` is a
supported value type. The property must use `SearchableIndexKind.Hash`. Range membership, jagged or
multidimensional/non-SZ arrays, nested collections, collection interfaces, other generic collection
types, and derived/custom list shapes are rejected.

Extraction reads the collection once. A null collection contributes zero entries. Null elements are
omitted; an empty string is indexed. Remaining values are converted through the scalar codec,
canonically sorted, and deduplicated. One record may contribute at most 64 unique entries to one
membership scope. More than 64 raw elements are valid when canonical deduplication leaves at most
64. A managed write performs the active-schema gate before invoking indexed getters, so it may
consult schema/layout authority first. After that gate succeeds, the 65th unique value throws
`SearchableStorageCapacityExceededException` before any partition mutation or WAL authority. For an
unmanaged namespace with no registrations, a first over-capacity write fails before layout
initialization or routing. The record-wide entry and canonical-byte limits still apply
independently.

Collection indexes are predicate-only. They cannot be selected by direct `FindAsync` or
`RangeAsync`, by `WhereIn`, or by any facet terminal.

## Focused query expression matrix

Identifier queries must contain at least one `Queryable.Where` predicate or `WhereIn` marker.
Additional `Where`/`WhereIn` calls are combined as logical AND. Facet terminals may execute directly
on the query root to aggregate every scalar indexed value, or use the same filtered predicate
subset.

| Expression shape | Supported | Notes |
| --- | :---: | --- |
| `state.Indexed == value` | yes | Hash or range index. |
| `<`, `<=`, `>`, `>=` | yes | Range index only. Reversed operands are normalized. |
| `predicate && predicate` | yes | Positive indexed predicates only. |
| `predicate \|\| predicate` | yes | Positive indexed predicates only. |
| Repeated `.Where(...)` | yes | Equivalent to AND. |
| Constant value | yes | Evaluated during translation. |
| Captured field/property or static field/property | yes | The member chain must not depend on the state parameter. |
| Built-in conversion which preserves the indexed domain | yes | Includes safe compiler integral and enum promotions. |
| `Enumerable.Contains(state.Array, value)` | yes | Exact two-argument generic `Enumerable.Contains<T>` over a direct exact `T[]` Hash membership property. |
| `state.List.Contains(value)` | yes | Exact instance `List<T>.Contains(T)` over a direct exact `List<T>` Hash membership property. |
| `query.WhereIn(state => state.Scalar, values)` | yes | Direct scalar Hash or Range property; at most 64 raw non-null values, snapshotted immediately and canonically deduplicated/sorted during translation. Empty input is an empty plan. |
| `values.Contains(state.Scalar)` | no | Use the bounded `WhereIn` operator; arbitrary collection-driven LINQ is not translated. |
| Other collection `Contains` shapes | no | No interface, nested, custom method, comparer overload, indirect property, or collection-to-collection form. |
| `!=`, unary `!`, or Boolean shorthand | no | Complement would require a partition-wide set complement. |
| Text methods such as `StartsWith` | no | Explicitly deferred beyond the 1.0 candidate. |
| Arithmetic or another calculation in the expression | no | Precompute the value outside the expression and capture it. |
| Nested or unindexed state property | no | The state side must be one directly declared indexed property. |
| State-property-to-state-property comparison | no | Exactly one side must be the indexed property. |
| Comparison with `null` | no | Nulls are not indexed. |
| Boxing, narrowing, lossy, or user-defined conversion | no | Translation never changes CLR comparison semantics silently. |
| `Select`, `OrderBy`, `Skip`, `Take`, `GroupBy`, joins, or other LINQ operators | no | No general LINQ surface or fallback. |
| Synchronous enumeration or `IQueryProvider.Execute` | no | Use one of the asynchronous identifier or facet terminals. |

The translator and both semantic and wire plans admit at most 64 levels and 256 visited nodes.
Indexed-property, state-parameter, and closed-value conversion chains are independently capped at
64. Unsupported or over-complex expressions fail before query fan-out.

## Terminal and consistency matrix

| Terminal | Result and ordering | Completion and consistency |
| --- | --- | --- |
| `ToGrainIdPageAsync` | At most the requested number of sorted, distinct `GrainId` values in canonical order. | Bounded weak continuation. A non-terminal page may be short or empty; only a null token means complete. No distributed snapshot. |
| `FindAsync`, `RangeAsync`, `ToGrainIdsAsync` on the built-in client | Complete sorted, distinct list. | Internally collects bounded pages. Returns the whole result or `SearchableStorageQueryLimitExceededException`; never truncates. |
| `ToDistinctFacetValuePageAsync` | Non-null distinct values in canonical indexed-value order. | Bounded weak continuation with the same short/empty-page rule. No distributed snapshot. |
| Exact `ToFacetValueCountsAsync` | Exact positive counts, ordered by count descending then canonical value. | Proves the global top N or throws at an aggregate ceiling. |
| Approximate `ToFacetValueCountsAsync` | Every returned count is exact, but a winner may be omitted. | `MaximumOmittedCount` is an inclusive certified upper bound for every omitted count; inspect it with `IsExact`. |
| `ToFacetMinMaxAsync` | Exact canonical minimum and maximum, or `null`. | All-or-throw aggregate operation over non-null indexed values. |

Pages and facets do not return state objects. They do not promise repeatable reads across
continuations. With concurrent writes, an item may be missed or observed according to its position
relative to the advancing canonical frontier; an item is not duplicated within the concatenated
no-write traversal. Facet aggregate attempts pin owner data versions and use the bounded retry rule
defined in [the paging contract](bounded-query-contract.md).

Cancellation cancels the caller's wait. It cannot transport-cancel an Orleans call already in
flight. Failure or cancellation returns no partial page, compatibility list, facet result, or
advanced continuation.

Index-only mutations are independent of the external payload write. The owning partition applies
them in arrival order and the last arrival wins; the writer has no source version, mutation-id
deduplication, tombstone ordering, or cross-store transaction. An exact retry converges to the same
indexed state but may append another durable journal entry. The caller owns per-key serialization,
outbox/reconciliation policy, and tolerance of projection lag during hydration. See the
[index-only mode contract](index-only-mode.md).

## Bounded execution defaults and hard maxima

These are the current built-in `SearchableStorageQueryOptions` defaults and compile-time accepted
maxima. They are safety ceilings and deterministic work units, not latency, throughput, or capacity
claims. A deployment may configure a smaller effective limit.

| Limit | Default | Hard maximum |
| --- | ---: | ---: |
| Compatibility traversal page size | 128 | Internal choice; public requests use the next row. |
| Raw values accepted by one `WhereIn` call | 64 | 64 |
| Accepted requested page size (`PageSizeLimit`) | 1,024 | 1,024 |
| Logical work per partition turn | 65,536 | 1,048,576 |
| Items per partition response | 1,024 | 4,096 |
| Encoded bytes per partition response | 256 KiB | 1 MiB |
| Coordinator buffered items | 8,192 | 65,536 |
| Coordinator buffered bytes | 2 MiB | 16 MiB |
| Encoded bytes per public page | 1 MiB | 4 MiB |
| Encoded continuation token bytes | 2,048 | 32 KiB |
| Compatibility aggregate work | 4,194,304 | 67,108,864 |
| Compatibility result items | 8,192 | 100,000 |
| Compatibility result bytes | 8 MiB | 64 MiB |
| Compatibility rounds | 64 | 1,024 |
| Facet top N | 128 | 1,024 |
| Facet aggregate work | 4,194,304 | 67,108,864 |
| Facet candidate/probe rounds | 2,048 | 32,768 |
| Facet aggregate candidate items | 8,192 | 65,536 |
| Facet aggregate candidate bytes | 8 MiB | 64 MiB |

Work accounting, encoded-size definitions, owner apportionment, and progress rules are specified in
the [bounded query and paging contract](bounded-query-contract.md).

Persisted records, index entries, snapshots, journal objects, and replay configuration have a
separate fixed, non-configurable [storage capacity envelope](storage-capacity-limits.md). Its public
constants, canonical-byte definition, fail-closed recovery behavior, and pre-1.0 rollout procedure
are part of this contract candidate; they are not provider-byte or performance claims.

## Continuations and failures

Public continuations are opaque authenticated-encrypted strings. The current protection key is
exactly 32 bytes and is an application secret shared by every silo or external client which creates
or resumes pages. Tokens are bound to the provider, response family, query or facet, execution
policy, routing layout/epoch, and active managed-schema generation. They contain no activation-local
cursor or buffered result set.

| Failure | Meaning and caller action |
| --- | --- |
| `NotSupportedException` | Expression, selector, terminal provider, type, or protocol surface is outside the supported contract. Change the request; there is no fallback. |
| `ArgumentException` / `ArgumentOutOfRangeException` | A selector, value, request, schema declaration, or option is invalid. |
| `SearchableStorageCapacityExceededException` | A live request or maintenance object exceeded the fixed logical envelope; inspect `Boundary`, `Actual`, and `Limit`, then reduce the state, index contribution, or page. If durable recovery reports it, keep traffic quiesced and follow the capacity runbook; never continue with a partial partition. |
| `SearchableStorageQueryConfigurationException` | Bounded-query options or continuation protection are unusable. Correct provider/client configuration. |
| `SearchableStorageInvalidContinuationTokenException` | Token is malformed, unauthenticated, cross-provider/family/query/policy/schema, or otherwise inapplicable. Restart only after checking the request and key ring. |
| `SearchableStorageStaleContinuationTokenException` | An authenticated token names an obsolete routing layout. Restart from the first page. |
| `SearchableStorageQueryLimitExceededException` | The built-in operation cannot progress or finish under a work/item/byte/round ceiling. Page differently, reduce the query/result, or deliberately revise configuration within hard maxima. |
| `SearchableStorageFacetConcurrentChangeException` | Owner data changed again after the bounded facet retry. Retry the complete facet later. |
| `SearchableStorageIndexSchemaException` | The registered schema is missing, rebuilding, incompatible, or not active. Follow the schema status and runbook; do not bypass the fence. |
| `OperationCanceledException` | The caller stopped waiting. No partial result is returned; durable admin work may still have committed and is resumable. |

Underlying Orleans, serialization, and physical-provider failures remain observable. The library
does not turn them into a partial success.

## Managed schemas and operator boundary

The active generation binds index scopes, queries, facets, pages, snapshots, records, and movement
payloads to a deterministic schema fingerprint. The fingerprint includes the state identity,
positive application-owned version, effective index names and kinds, CLR value domains, and built-in
codec identities. A schema containing membership indexes also binds property multiplicity and the
membership extractor version. Schema keys remain version 1 for all schemas; scalar-only fingerprints
remain byte-for-byte v1, while membership fingerprints use v2.

Once the provider-wide managed-schema capability is enabled:

- every state name using that provider must be registered on every silo, including states with no
  indexed properties;
- directly constructed clients must declare every queried state in a
  `SearchableStorageSchemaRegistry`;
- integrated writes/clears, index-only mutations, queries, pages, and facets fail closed until the
  declared generation is active;
- a changed integrated declaration requires the documented quiesced rebuild; an index-only change
  requires a new namespace and authoritative replay. Neither is online DDL or a state payload
  migration;
- rebuild scan requests cover at most 64 catalog records and persist resumable progress, but the
  complete rebuild and retained compactions are not strict time, memory, or work bounds;
- continuations from another generation cannot be resumed.

For integrated storage, a changed declaration uses the quiesced in-place rebuild because retained
payloads can be deserialized and reindexed. An index-only namespace can activate its first schema
while empty and can confirm the same fingerprint idempotently, but it rejects an incompatible
rebuild after activation. Create a new provider namespace and replay the authoritative external
corpus instead.

Schema adoption, binary rollout, and movement enablement require homogeneous, quiesced procedures.
The durable controls support safe resume after interruption; they do not make mixed-version rollout
or arbitrary simultaneous maintenance safe. Follow the
[managed-schema runbook](index-schema-lifecycle.md) and
[movement runbook](live-movement.md) rather than inferring an online migration from individual
idempotent steps.

## Runtime and physical-provider validation

This table states current repository evidence, not an invented multi-framework promise.

| Dimension | Implemented and validated now | 1.0 freeze decision |
| --- | --- | --- |
| Target framework | `net10.0`, C# 14; the repository declares SDK 10.0.302 with its checked-in roll-forward policy | Do not claim other target frameworks without building and running the full contract suite. |
| Orleans packages | Repository currently pins Orleans 10.2.2 | Compatibility beyond the tested package line requires explicit evidence. |
| Type model | PolyType runtime reflection provider | Native AOT and trimming are outside the contract. Consumers need no PolyType annotations or generated witnesses. |
| In-memory backend | Official Orleans Memory provider | Contract-validated. It is not a production durability recommendation. |
| PostgreSQL backend | Official Orleans ADO.NET provider with Npgsql and pinned Orleans SQL | Contract-validated with the documented operational prerequisites. |
| Redis backend | Official Orleans Redis provider | Contract-validated with the documented durability prerequisites. |
| Azure Blob backend | Official Orleans Azure Blob provider, exercised against Azurite | Storage protocol is validated; emulator runs do not establish Azure service performance, identity, availability, backup, or disaster-recovery properties. |
| Another `IGrainStorage` backend | Architectural extension point | Not contract-validated until it passes the reusable backend suite; no S3 adapter is shipped or claimed. |

Backend independence means the searchable protocol is layered over `IGrainStorage`; it does not
erase a physical provider's ETag, retry, size, latency, backup, retention, or availability behavior.
See [physical backend configuration](backends.md) and the
[testing strategy](testing.md) for the evidence boundary.

## Stable 1.x compatibility and SemVer contract

Stable packages use SemVer 2.0.0. Package, protocol, persistence, schema, codec, token, and work-policy
versions are independent: a package version never authorizes changing another version or reusing an
old identity. `1.0.0` establishes the first stable compatibility promise. A prerelease may still
change the candidate only through an explicit review of every affected baseline and release note;
there is no implicit waiver for a change made shortly before the stable tag.

The checked-in C# source, compiled API, and compatibility manifests are normative review gates:

- the standard Roslyn public-API baseline freezes public/protected declarations, nullability,
  overloads, defaults, and constants;
- the compiler-symbol constraint baseline freezes every generic parameter, including an explicit
  unconstrained state, so source-only constraint changes cannot disappear from review;
- the compiled reflection manifest independently freezes runtime/binary metadata;
- `eng/compatibility-manifest.json` and executable wire/golden tests freeze protocol numbers,
  numeric identities, and canonical bytes.

The following rules apply after stable 1.0:

| Contract | Patch release | Minor release | Major or new-identity requirement |
| --- | --- | --- | --- |
| Public C# API and exceptions | Implementation fixes with identical shipped source/compiled baselines and the same documented failure classification. Exception messages are diagnostic text, not a parsing contract. | Additive, opt-in API and exception types only. New declarations enter the unshipped baselines and must not change existing overload resolution. | Removing, renaming, or changing an existing declaration, constraint, nullability contract, default, public constant, return/result type, documented exception type/properties, or catch behavior is breaking. So is an overload which makes valid source ambiguous or selects another member. |
| Orleans messages and wire values | No field-ID, numeric-value, or semantic-version change. | An optional append-only field or capability may be added only with a new explicit version/capability gate, old/new executable tests, fail-closed behavior, and documented homogeneous or mixed-version rollout. | Field IDs and enum/opcode numeric values are never reordered or reused. A change that an existing peer can misread requires a new protocol identity and, when same-major interoperation cannot be preserved, a major package version. |
| Durable formats | Read and recover every acknowledged object written by an earlier 1.x patch/minor. | A new format requires a versioned reader, deterministic adoption/migration, crash/lost-ack coverage, and explicit rollback boundary while every supported prior 1.x object remains readable. | Removing a same-major reader, silently reinterpreting stored bytes, or requiring destructive out-of-band conversion is breaking. A package bump alone is not a storage migration. |
| Schema fingerprints and codecs | Existing identity and canonical meaning are byte-for-byte unchanged. | A new built-in meaning receives a new schema/fingerprint/codec identity and the normal generation rebuild/adoption path. | An existing identity is never assigned new bytes or semantics, even in a major release; migration uses a new identity. A release which cannot preserve old-state access requires an explicit major migration boundary. |
| Continuation tokens | Existing tokens for the same layout, generation, query, and work policy remain accepted. A security or correctness emergency may invalidate them only with explicit release notes and the documented restart behavior. | A versioned token/policy family may explicitly reject an older token with the public token exception and restart guidance; it must never parse the old token as new meaning. Tokens are resumability artifacts, not durable state. | Silent reinterpretation or resumption under different query/layout/schema/work semantics is forbidden. Removing promised same-policy resumption outside the stated invalidation boundary is breaking. |
| Work policy, limits, and accounting | Existing defaults, hard limits, charged units, admission decisions, stop reasons, and continuation bindings are unchanged. | New opt-in policy versions or additive bounded controls are allowed when old behavior remains selectable and protocol/capability tests cover both. | Lowering an accepted public bound, changing a public constant/default, or changing charged meaning under an existing policy version is breaking. A new accounting meaning requires a new policy identity and token binding. |

Dropping a supported target framework or Orleans line is a major change. Adding a separately tested
provider or supported dependency line is minor and does not imply that every `IGrainStorage`
implementation is qualified. A patch may narrow undocumented implementation behavior only when the
documented closed contract, accepted inputs, durable recovery, and error taxonomy remain unchanged.

## 1.0 change filter

Before the 1.0 release, a change to this candidate contract should answer all of these questions:

1. Does it preserve the explicit integrated-storage or index-only ownership model plus bounded
   `GrainId` discovery?
2. Is it implemented end to end without a client-side or unbounded fallback?
3. Are CLR, expression, terminal, consistency, failure, and provider semantics stated explicitly?
4. Are work, memory, response, token, and durable-state consequences bounded or honestly exposed?
5. Does it retain generation, layout, replay, cancellation, and no-partial-result safety?
6. Do focused contract tests, backend tests where relevant, samples, XML docs, and runbooks agree?

For the bounded membership slice, any future expansion must re-answer the exact collection shapes,
public query form, element/null/duplicate semantics, deterministic schema identity, and every
admission, wire, work, and result bound rather than treating another LINQ shape as equivalent.

Until 1.0 is actually released, this matrix and the frozen baselines are the review target. After
the stable tag, the SemVer rules above are the compatibility policy; a green baseline update is
evidence of an intentional change, not proof that its version classification is compatible.
