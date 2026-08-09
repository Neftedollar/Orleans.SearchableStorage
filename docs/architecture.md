# Architecture

This document describes the maintainer-facing design of the first Orleans.SearchableStorage vertical slice. It records the boundaries which tests and future implementations must preserve.

## Components

`SearchableGrainStorage` implements Orleans `IGrainStorage`. Application grains use it through normal `IPersistentState<T>` injection and do not coordinate index updates themselves.

Each searchable provider owns one `StorageLayoutGrain` and a fixed set of `StoragePartitionGrain` instances. A partition persists records together with each record's index entries in one `StoragePartitionState`. Hash and range bucket maps are derived in memory from those durable entries on activation and after each successful mutation. The durable state is written through the physical Orleans storage provider registered as `Orleans.SearchableStorage.Physical`.

`SearchableStorageClient` supports direct exact/range primitives and a focused `IQueryable` boundary. The query provider records expression trees without offering synchronous execution. `QueryTranslator` resolves indexed member identity through the same cached PolyType-backed metadata as writes and produces a small explicit query-plan algebra: exact leaf, one- or two-sided range leaf, intersection, union, and empty result. The client converts that semantic plan to one serializable, non-persisted wire plan and sends it once to every partition. Each partition evaluates the complete boolean plan synchronously during one non-reentrant grain turn. The client only unions the final partition-local results and returns a sorted, distinct set of `GrainId` values.

## Sample topology

`Orleans.SearchableStorage.ApiSample` co-hosts an ASP.NET Core minimal API and one Orleans silo only to provide a zero-infrastructure executable walkthrough. HTTP writes call an application grain, its normal `IPersistentState<T>` uses `SearchableGrainStorage`, and the provider routes the serialized record plus extracted index entries to one storage-partition grain. HTTP search endpoints use `Query<TState>`, `Where`, and `ToGrainIdsAsync` on the named `ISearchableStorageQueryClient` and fan out over the same partition set. ASP.NET Core supplies `HttpContext.RequestAborted` as the endpoint cancellation token, which is passed through the terminal operation.

This co-hosting is not an architectural constraint. An API can run in another process as an Orleans client and construct a `SearchableStorageClient` with the same provider namespace and partition count. The sample deliberately uses in-memory physical persistence, so process termination removes its data; backend durability is covered by the reusable provider contract rather than by the sample.

## Identity and partitioning

A record key contains the state name and the raw Orleans grain type and key bytes encoded as hexadecimal. Partition placement uses `GrainId.GetUniformHashCode()`, which Orleans defines as uniform and stable.

The searchable provider name is the key of its layout grain and part of every partition grain key. This isolates logical providers even though their grains use the same physical storage-provider registration. A different or renamed provider name selects a separate, initially uninitialized namespace; it cannot be detected as a mismatch against the old namespace and therefore requires an explicit migration when existing data must move with it.

The layout grain durably records the provider name, storage-format version, and partition count. Storage operations initialize that record on first use. Query clients validate the storage-format version and partition count within that provider namespace before fan-out and fail instead of silently returning an incomplete result when either value differs. A query made before any storage operation returns an empty result and does not initialize the layout.

`PartitionCount` is part of the persisted layout. Changing it after data has been written makes existing records unreachable without an explicit migration. Online repartitioning is outside the current scope.

## Write invariant

For one record, the serialized state and every local secondary-index entry are one logical mutation:

1. The storage provider serializes the application state and extracts index entries.
2. The owning partition grain checks the expected record ETag.
3. It creates a copy of the current partition state, replaces the record and its index entries, and builds candidate in-memory bucket maps.
4. It persists the candidate partition state with one physical `IPersistentState.WriteStateAsync()` call.
5. Only after that call succeeds does the new record ETag return to the application grain.

The active partition state is replaced before the asynchronous physical write because `IPersistentState` writes its current value. If that write fails, the grain restores the previous in-memory value and requests deactivation. This prevents the failed candidate from being served by that activation.

The physical provider still has the normal distributed-systems ambiguity where a write may have committed even if its acknowledgement was lost. Record ETags detect a subsequent stale retry; this version does not provide idempotency tokens for resolving that ambiguity.

The contract suite injects both sides of this ambiguity. A failure before the physical commit must rehydrate the previous record and indexes. A failure after the physical commit but before acknowledgement must rehydrate the committed candidate, even though the caller observed an exception.

## Read and query semantics

A point read is routed directly to the record's owning partition. Missing records receive a new state instance from Orleans' configured activator and have no ETag.

Direct exact and bounded-range operations fan out to all configured partitions. A focused `IQueryable` execution instead sends its complete boolean plan once to each partition. The partition grain is non-reentrant and plan evaluation contains no await, so all leaves of that plan observe one serially consistent partition-local activation state. The merged result is not a cross-partition snapshot: different partitions can still observe concurrent writes at different points during one distributed query.

Each in-memory range index stores its distinct values in a `SortedList`. During construction, input buckets are first canonicalized by the index ordering comparer and comparer-equal buckets are merged. Partition activation therefore does not depend on hash equality and ordering equality remaining identical as persisted value kinds evolve. A range with a lower bound performs a binary lower-bound lookup through the indexed key view and then visits only buckets inside the requested window; a range without a lower bound starts at the first bucket. Its local traversal is O(log d + k) with a lower bound and O(k) without one, where d is the number of distinct indexed values and k is the number of visited buckets.

Once a successful write returns, that partition's activation already contains the committed state and corresponding index entries. No asynchronous index-maintenance pipeline exists in this version.

## Query translation and execution

`ISearchableStorageQueryClient.Query<TState>(stateName)` returns an `IQueryable<TState>` only as a familiar expression-building surface. It is not an in-memory state collection and cannot enumerate `TState` values. `ToGrainIdsAsync` is the sole terminal operation. It accepts any query whose provider implements the public `ISearchableStorageAsyncQueryProvider` contract, so external query-client implementations do not need access to library internals.

Translation accepts one or more `Queryable.Where` calls. Predicate leaves must compare one direct indexed property with a constant or captured value using equality or an ordered comparison. Boolean `AndAlso` and `OrElse` become plan intersection and union. Reversed operands are normalized. Intersected bounds on the same index are combined before execution; contradictory bounds become an empty plan. Equality can use either index kind, while ordered comparisons require a range index. Null comparison is rejected because null index values are deliberately omitted. An empty plan still validates persisted layout compatibility and cancellation, but skips partition fan-out.

The closed value side is intentionally narrow: constants, captured fields or properties, and built-in conversion nodes are evaluated using the expression interpreter. Method calls, calculations, user-defined value conversions, state-to-state comparisons, nested state member access, and arbitrary LINQ operators are rejected with `NotSupportedException`. Compiler-generated integral and enum promotions are normalized through the indexed property's PolyType-derived value domain. A conversion of the indexed property is accepted only when it represents that complete domain exactly and preserves equality and ordering. Out-of-domain integral equality becomes an empty plan, and ordered bounds are saturated to a correct full or empty domain window. Boxing, reference, narrowing, user-defined, and lossy floating-point conversions of the indexed property are rejected rather than silently changing C# semantics. Custom binary comparison methods are also rejected; only the corresponding BCL operators for supported built-in value types are accepted.

Supported translation traversal and both semantic and wire plans share fixed limits of 64 levels and 256 visited nodes. Indexed-property and state-parameter conversion chains are independently capped at 64. Recursive conversion and evaluation occur only after those bounds have been checked. Both plan validators require trees rather than cyclic or shared graphs; wire validation also rejects hidden child graphs, payload on the wrong node kind, missing boolean children, malformed leaves, and unknown operations. Associative boolean plans are rebuilt as balanced trees without changing leaf order before serialization.

After translation, `PartitionQueryPlanFactory` creates a recursive Orleans wire message for Empty, Exact, Range, And, and Or nodes. Unlike the existing bounded `RangeIndexQuery`, its lower and upper range bounds are nullable so one-sided predicates do not need sentinel values. `StoragePartitionGrain.QueryAsync` validates and evaluates this complete plan synchronously in one non-reentrant turn. AND intersects and OR unions record keys inside the partition; only that final local result crosses the grain boundary. Exact leaves copy the live index bucket before boolean operations mutate their working set. A compound predicate therefore makes one request per partition rather than one request per leaf.

The client starts all partition calls before awaiting `Task.WhenAll`, so a partition failure fails the entire query and no partial result is returned. Cancellation interrupts the local wait, not the Orleans RPCs already in flight. A detached observer awaits the aggregate after cancellation so later transport or partition failures are still observed.

`ISearchableStorageClient` intentionally retains only the existing direct `FindAsync` and `RangeAsync` surface. `ISearchableStorageQueryClient` derives from it and adds `Query<TState>`. The keyed registrations for both interfaces point to the same `SearchableStorageClient` instance, which keeps existing direct-client implementations source compatible while making the new expression surface opt-in.

## Index metadata

Public readable state properties marked with `SearchableIndexAttribute` are indexed. Index scope combines a length-prefixed persisted state-type identity, Orleans state name, and stable index name. A named type identity contains its assembly simple name, culture, public-key token, and full type name. Constructed generic identities contain the generic definition followed by recursively encoded argument identities; arrays encode their shape and element identity. Assembly versions are deliberately excluded. Length prefixes make every boundary unambiguous and prevent unrelated states from sharing buckets accidentally.

`IndexMetadataProvider` builds one `SearchableTypeModel<TState>` through PolyType's reflection provider and caches each valid model for the process lifetime. A non-object PolyType shape, such as a collection or scalar state type, produces an empty model and remains writable through the provider. An object model contains the indexed member identity, index kind, normalized index name, value converter, stable persisted type identity, and a strongly typed PolyType getter delegate. Each indexed property caches its complete scope per Orleans state name. Steady-state writes therefore neither call `PropertyInfo.GetValue` nor repeat assembly identity reflection; they invoke the cached getter, converter, and scope. Failed model construction is not cached so an invalid state declaration continues to produce its direct validation exception.

PolyType usage is contained behind the indexing model. Storage grains and persisted messages do not depend on type-shape objects. Query selectors and predicates are expression trees: the expression boundary reads the selected `PropertyInfo` only to match it to the member identity supplied by PolyType, while persisted value discovery and access remain type-shape operations. The same model is used by writes and queries, so index kind, converter, and scope have one definition.

The runtime reflection provider is intentional. Orleans `IGrainStorage` accepts unconstrained application state types, and this project does not require consumers to annotate those types for PolyType source generation. Native AOT and trimming are not supported.

Null values are omitted. String ordering is ordinal. `DateTime` values must be UTC so index ordering cannot depend on a silo's local time zone. Floating-point NaN values are rejected because they do not provide a useful total ordering for these indexes.

Index names, index kinds, and indexed property types are persisted schema. Adding an attribute does not backfill records which were written before that index existed. Renaming an index, changing its kind or property type, or changing the index-scope format requires a migration or complete record rewrite.

## Persisted compatibility rules

The layout format version, provider name, partition count, layout-grain type and key, partition-grain type, partition-key format, record-key format, and index-scope format determine how durable data is located and interpreted. Changes to any of them require a format-version increment and a migration path. In particular, renaming the layout grain interface or changing its key can silently select a fresh namespace unless the old identity is migrated.

Format version 2 introduces the recursive, assembly-version-independent type identity described above. Version 1 index scopes are intentionally not read as version 2 scopes. A provider namespace whose persisted layout still reports version 1 is rejected and must be migrated or completely rewritten before it can be opened by this version.

Orleans serializer `[Id]` values on persisted layout, partition, record, index-entry, and `IndexValue` types are field identities. Existing IDs must never be reused or renumbered after release. New fields must use new IDs and remain readable when absent from older data.

The existing `RangeIndexQuery` protocol remains a strictly bounded message: both lower and upper
bounds are required and retain field IDs 1 and 2. Open bounds exist only on the new recursive
`PartitionQueryPlan`, which is transmitted between participants but is never persisted. Its field
IDs and operation values are still wire compatibility and are frozen by contract tests.

The query-plan RPC changes the internal grain contract. A rollout must update all silos and Orleans
clients before the `IQueryable` surface is used; mixed-version query execution is not supported.
Direct `FindAsync` and bounded `RangeAsync` retain their previous messages during that rollout.

Physical providers can use serializers other than Orleans' binary serializer. With JSON persistence, CLR type and property names plus configured JSON converters are part of the compatibility surface; Orleans `[Id]` values do not rename JSON members. The memory contract suite explicitly uses `JsonGrainStorageSerializer` so partition and layout reactivation exercise that representation.

## Physical backend contract

Backend tests must exercise the same public storage contract instead of backend-specific expected behavior. The contract covers physical partition rehydration, exact lookup, bounded range lookup, index replacement on update, index removal on clear, optimistic-concurrency rejection, persisted-layout mismatch, and physical-write failure boundaries.

The contract runs unchanged against Orleans memory, ADO.NET/PostgreSQL, Redis, and Azure Blob providers. Each external fixture registers the official Orleans 10.2.2 provider under an internal name and places the same failure-injecting decorator under `Orleans.SearchableStorage.Physical`. The decorator is test infrastructure only; production hosts register their selected provider directly under the physical name. All fixtures select `JsonGrainStorageSerializer`, so reactivation crosses a physical serialization boundary instead of retaining object references.

External resources are isolated per fixture. PostgreSQL receives a unique schema and an Npgsql connection string with that schema as its search path. Redis receives a unique Orleans service id, which is part of every provider key. Azure Blob receives a unique container. The fixtures stop their two-silo clusters before deleting those resources. CI provides PostgreSQL, Redis, and Azurite through pinned containers, while connection-string overrides allow the same tests to target a separately managed environment.

External fixture initialization treats backend preparation, cluster construction, and deployment as one guarded lifecycle. Any failure attempts to stop a constructed cluster and clean the backend while preserving the original exception; secondary stop and cleanup failures are attached to that exception under stable diagnostic keys. Stop and cleanup completion flags are set only after success, so a later disposal retries transient failures without repeating successful cleanup. Deterministic lifecycle tests cover preparation, construction, deployment, stop, cleanup, and retry paths without environment-variable mutation or live infrastructure.

Provider cleanup is deliberately namespace-scoped and tested against unrelated sentinels. PostgreSQL drops only its quoted schema, Redis deletes only keys under the selected Orleans service id's `state` prefix, and Azure Blob deletes only its generated container. These checks run against the real external service already used by the fixture and do not deploy a second Orleans cluster.

Azurite validates the Azure Blob provider protocol and storage semantics used by the searchable layer, but it is not a substitute for Azure service-level performance, identity, network, redundancy, or disaster-recovery testing. Likewise, passing the common contract does not equate the operational characteristics of PostgreSQL, Redis, and Blob Storage.

Every change is also evaluated using the mandatory test-sufficiency review described in [testing.md](testing.md). That review verifies behavioral, failure, durability, distributed, serialization, and sample coverage rather than relying on a raw test count.

An S3-compatible provider is not part of the current supported matrix. Adding one requires an Orleans `IGrainStorage` implementation and the complete contract; Azure Blob compatibility does not imply S3 compatibility.

## Current scaling limit

One physical write serializes the entire owning partition snapshot, and the activation rebuilds its bucket maps from durable record entries after each successful mutation. This deliberately makes the initial consistency boundary small and observable, but produces work and write amplification proportional to partition size. Range reads now seek to their lower bound, but queries still fan out to every partition. Increasing `PartitionCount` distributes ownership and write work but does not route a query to fewer partitions. `ToGrainIdsAsync` has no `Take`, pagination, or result-size limit, so a broad result can allocate a large working set and hold a non-reentrant partition turn while matching record ids are collected.

A production-scale layout will need smaller durable units and bounded query-result handling while preserving the record-and-index atomicity described above.
