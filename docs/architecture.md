# Architecture

This document describes the maintainer-facing design of the first Orleans.SearchableStorage vertical slice. It records the boundaries which tests and future implementations must preserve.

## Components

`SearchableGrainStorage` implements Orleans `IGrainStorage`. Application grains use it through normal `IPersistentState<T>` injection and do not coordinate index updates themselves.

Each searchable provider owns one `StorageLayoutGrain` and a fixed set of `StoragePartitionGrain` instances. A partition persists records together with each record's index entries in one `StoragePartitionState`. Hash and range bucket maps are derived in memory from those durable entries on activation and after each successful mutation. The durable state is written through the physical Orleans storage provider registered as `Orleans.SearchableStorage.Physical`.

`SearchableStorageClient` derives index metadata from a typed property selector, sends the query to every partition, and returns a sorted, distinct set of `GrainId` values.

## Sample topology

`Orleans.SearchableStorage.ApiSample` co-hosts an ASP.NET Core minimal API and one Orleans silo only to provide a zero-infrastructure executable walkthrough. HTTP writes call an application grain, its normal `IPersistentState<T>` uses `SearchableGrainStorage`, and the provider routes the serialized record plus extracted index entries to one storage-partition grain. HTTP search endpoints use the named `ISearchableStorageClient` and fan out over the same partition set.

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

Exact and range queries fan out to all configured partitions. Each individual partition answers from a serially consistent activation state. The merged result is not a cross-partition snapshot: concurrent writes can become visible at different points during one query.

Once a successful write returns, that partition's activation already contains the committed state and corresponding index entries. No asynchronous index-maintenance pipeline exists in this version.

## Index metadata

Public readable state properties marked with `SearchableIndexAttribute` are indexed. Index scope combines length-prefixed state assembly, full state type name, Orleans state name, and stable index name components. The length prefixes prevent user-controlled names from creating ambiguous boundaries and keep unrelated states from sharing buckets accidentally.

Null values are omitted. String ordering is ordinal. `DateTime` values must be UTC so index ordering cannot depend on a silo's local time zone. Floating-point NaN values are rejected because they do not provide a useful total ordering for these indexes.

Index names, index kinds, and indexed property types are persisted schema. Adding an attribute does not backfill records which were written before that index existed. Renaming an index, changing its kind or property type, or changing the index-scope format requires a migration or complete record rewrite.

## Persisted compatibility rules

The layout format version, provider name, partition count, layout-grain type and key, partition-grain type, partition-key format, record-key format, and index-scope format determine how durable data is located and interpreted. Changes to any of them require a format-version increment and a migration path. In particular, renaming the layout grain interface or changing its key can silently select a fresh namespace unless the old identity is migrated.

Orleans serializer `[Id]` values on persisted layout, partition, record, index-entry, and `IndexValue` types are field identities. Existing IDs must never be reused or renumbered after release. New fields must use new IDs and remain readable when absent from older data.

Physical providers can use serializers other than Orleans' binary serializer. With JSON persistence, CLR type and property names plus configured JSON converters are part of the compatibility surface; Orleans `[Id]` values do not rename JSON members. The memory contract suite explicitly uses `JsonGrainStorageSerializer` so partition and layout reactivation exercise that representation.

## Physical backend contract

Backend tests must exercise the same public storage contract instead of backend-specific expected behavior. The contract covers physical partition rehydration, exact lookup, bounded range lookup, index replacement on update, index removal on clear, optimistic-concurrency rejection, persisted-layout mismatch, and physical-write failure boundaries.

Every change is also evaluated using the mandatory test-sufficiency review described in [testing.md](testing.md). That review verifies behavioral, failure, durability, distributed, serialization, and sample coverage rather than relying on a raw test count.

The regular suite currently uses Orleans in-memory persistence with `JsonGrainStorageSerializer` in a two-silo `TestCluster`. PostgreSQL and Redis are required integration targets. Azure Blob Storage or an S3-compatible provider belongs to a separately configured integration environment because it needs external infrastructure and credentials.

## Current scaling limit

One physical write serializes the entire owning partition snapshot, and the activation rebuilds its bucket maps from durable record entries after each successful mutation. This deliberately makes the initial consistency boundary small and observable, but produces work and write amplification proportional to partition size. A production-scale layout will need smaller durable units while preserving the record-and-index atomicity described above.
