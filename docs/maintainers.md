# Maintainer guide

This guide is a map from observable Orleans storage behavior to the implementation which owns it.
It is not a second specification. The normative durability, query, capacity, movement, and schema
rules remain in the linked design documents.

## Code map

| Flow | Entry point | Durable or protocol owners | In-memory owner |
| --- | --- | --- | --- |
| Point read/write/clear | `SearchableGrainStorage` | `StorageLayoutGrain`, `StoragePartitionGrain`, `StoragePartitionPersistence`, `StorageJournalSegmentGrain`, `StorageSnapshotGrain` | `StoragePartitionView`, `StoragePartitionRecordRefs`, and `StoragePartitionOrderedIndexes` |
| Query translation | `SearchableStorageClient` and `SearchableStorageQueryableExtensions` | `QueryTranslator`, `QueryPlanBuilder`, `PartitionQueryPlanFactory`, `QueryProtocol` | `ScalarQueryAccessPathPlanner` and `StoragePartitionQueryPageEvaluator` |
| Facets | `SearchableStorageClient.Facets` | the same plan and continuation contracts as paging | `StoragePartitionFacetEvaluator` |
| Activation recovery | `StoragePartitionGrain.OnActivateAsync` | `StoragePartitionPersistence`, `StorageJournalReplay`, `StorageSnapshotFactory` | a freshly rebuilt `StoragePartitionView` |
| Managed schema rebuild | `SearchableStorageAdminClient.RebuildIndexSchemaAsync` | `StorageIndexSchemaGrain`, `StorageLayoutGrain`, each owner `StoragePartitionGrain` | `IndexMetadataProvider` rematerializes entries |
| Virtual-slot movement | `SearchableStorageAdminClient` | the move state machine in `StorageLayoutGrain`; participant methods in `StoragePartitionGrain` | `StorageMovePageOperations` and the partition view |

## Trace a point mutation

1. `SearchableGrainStorage.WriteStateAsync` validates the `GrainId`, serializes the authoritative
   grain state, verifies a managed schema when configured, and asks `IndexMetadataProvider` for
   derived index entries. Capacity admission finishes before a legacy namespace is initialized.
2. `StorageLayoutCache` supplies a persisted layout. `SearchableGrainStorage.ExecuteRoutedAsync`
   hashes the `GrainId` to a virtual slot and calls the current owner with both slot and epoch. It
   invalidates and retries once after `StorageRouteMismatchException`.
3. `StoragePartitionGrain.WriteRoutedAsync` validates routing and delegates the state transition to
   `StoragePartitionView`, with persistence coordinated by `StoragePartitionPersistence`.
4. `StoragePartitionPersistence.PrepareForMutationAsync` first makes journal space available and
   acquires a writer epoch. `CommitAsync` persists the bounded journal slot and then advances the
   manifest. The manifest advance is the visibility and acknowledgement commit point for the record
   and every derived index entry.
5. Only after the durable commit does the activation publish its new `StoragePartitionView`.
   `CompactIfRequiredAsync` is maintenance: a failure cannot turn that committed operation into a
   reported mutation failure, but the poisoned activation retires and the next activation recovers.

`ClearStateAsync` takes the same route and journal protocol. A read follows the route but does not
mutate index or persistence state. ETag rejection happens on the owner; it is not resolved by a scan.

## Trace a query page

1. `SearchableStorageClient.Query<TState>` returns a focused expression builder. Execution starts
   only at a supported terminal such as `ToGrainIdPageAsync`.
2. `QueryTranslator` validates the exact supported expression forms and resolves them against the
   same `IndexSchemaDefinition` used by writes. `QueryPlanBuilder` creates a bounded semantic plan,
   then `PartitionQueryPlanFactory` creates the Orleans wire plan.
3. The client reads one layout snapshot and concurrently calls each distinct current owner.
   `StoragePartitionQueryPageEvaluator` validates the entire plan before touching indexes;
   `ScalarQueryAccessPathPlanner` chooses an indexed candidate path, and the evaluator spends the
   explicit work/item/byte budget.
4. The coordinator merges only a globally safe canonical `GrainId` prefix. `ContinuationTokenCodec`
   authenticates and encrypts the remaining frontier and binds it to query, response family,
   limits, schema, layout, and key id.

A first-page routing change discards the complete attempt and retries once. A resumed token is pinned
and becomes stale. A query returns identities, never materialized application state; hydrate a bounded
page through application grains as the API sample demonstrates.

## Trace recovery and compaction

`StoragePartitionGrain.OnActivateAsync` creates `StoragePartitionPersistence` and calls
`ActivateAsync`. `RecoverAsync` validates the manifest, reads the active immutable snapshot, replays
the exact committed journal chain through `StorageJournalReplay`, and validates
capacity/version/schema invariants. The activation then finishes any pending snapshot publication
and cleanup before `StoragePartitionGrain` rebuilds `StoragePartitionView`. Any inconsistency fails
the aggregate activation/recovery outcome; there is no empty-state fallback.

Compaction is a fenced four-stage publication: reserve a pending descriptor in the manifest, write
the inactive `StorageSnapshotGrain`, publish that descriptor through the manifest, then tombstone
retired snapshot/journal payloads. Retries reuse the reservation. The activation and compaction
boundary is still the accepted whole partition; see [storage capacity limits](storage-capacity-limits.md).

## Trace a schema rebuild

`SearchableStorageAdminClient.RebuildIndexSchemaAsync<TState>` starts or resumes one durable
`StorageIndexSchemaGrain` intent. `AdvanceRebuildAsync` pins the layout, enables protocol and format
5 on every current owner, asks each `StoragePartitionGrain.RebuildIndexSchemaPageAsync` to rematerialize
at most 64 catalog records per page, publishes the provider-wide layout gate, then activates the new
fingerprint. The original serialized state and ETag do not change. Keep the entire provider quiesced
until every registered state is `Active`; follow the [managed schema runbook](index-schema-lifecycle.md).

## Trace a slot move

`SearchableStorageAdminClient` is a convenience loop; `StorageLayoutGrain` owns the only durable move
intent and advances exactly one reconciliation, phase, or transfer page per call. The source freezes
the slot, the target fences its record-version sequence, bounded pages copy records, ownership commits
by advancing the layout epoch, and cleanup deletes source copies. Before ownership commits,
`AbortMoveAsync` runs a bounded reverse cleanup. After commit, recovery converges forward and rollback
means finishing the move and planning a reverse move. See the [movement runbook](live-movement.md).

## Diagnose fail-closed and poisoned activations

Start from the first exception in the application or physical provider. Later deactivation, retry,
or cleanup errors are consequences until proven otherwise.

| Symptom | Meaning | Safe response |
| --- | --- | --- |
| `SearchableStorageIndexSchemaException` names an unregistered state, inactive fingerprint, or rebuild id | A provider/state declaration does not match durable managed-schema authority | Keep traffic quiesced; make registrations identical on every silo/client and resume the exact rebuild. Do not bypass the gate. |
| `StorageRouteMismatchException` escapes after retry or a stale-continuation exception is returned | Layout ownership changed during the operation | Inspect `ISearchableStorageAdminClient.GetLayoutAsync` and the active move. Restart a fresh page; never rewrite an old continuation. |
| “ambiguous persistence write/outcome” from manifest, layout, journal, or snapshot activation | The physical provider may have committed even though the acknowledgement failed | Do not retry within the same activation or edit ETags. Allow deactivation/recovery to reread durable authority, then repeat the idempotent high-level operation. |
| Recovery reports an invalid chain, unsupported format, incompatible snapshot, or missing schema control | Durable objects are inconsistent or from an unsupported version | Stop traffic. Preserve all objects for diagnosis and restore one consistent namespace backup or run the documented migration. Never delete one control object to make activation look fresh. |
| Writes are backpressured near the replay limit, or mutation latency follows snapshot size | Required compaction cannot reclaim the bounded journal ring cheaply enough | Reduce partition density through planned slot movement, reduce write rate, and inspect physical-provider latency/errors. Raising limits is a migration/capacity decision, not an incident toggle. |

The physical provider may expose its own request id, ETag, or conditional-write diagnostics. Preserve
those with the provider name, state name, partition key, operation id, move/rebuild id, layout epoch,
and first exception. Never log continuation key bytes, payloads, or indexed values by default.

## Change recipes

### Add a supported indexed CLR type

Update `IndexValueKind`, `IndexValueConverter`, canonical encoding/ordering, and the PolyType-driven
metadata path together. Decide equality, range ordering, null, invalid-value, culture/timezone, byte
accounting, and movement/snapshot behavior before coding. Bind any persisted identity or codec change
in `eng/compatibility-manifest.json`; add converter/order boundary tests, schema goldens, write/recovery,
query/facet, movement, and every-provider contract evidence. Document rollout/migration implications.

### Add a query operator

Keep the surface focused. Extend `QueryTranslator`, plan validation/fingerprinting, wire lowering only
when a new opcode is unavoidable, work accounting, access-path planning, and continuation binding.
Reject unsupported expression shapes before RPC. Test semantic edges, malicious/deep plans, bounded
work, paging frontiers, layout changes, facets if relevant, and external-provider marker behavior.
Do not add a scan fallback or return `TState`.

### Add or change an index declaration

`SearchableIndexAttribute` and `IndexMetadataProvider` define one model for writes and queries. An
application adds or changes an index by incrementing its application schema version, deploying
identical registrations everywhere, and completing a quiesced rebuild. Library changes to schema-key
or fingerprint encoding require a compatibility-manifest update and frozen old/new goldens. Never
mutate existing derived entries in place without the rebuild protocol.

Before review, run the test-sufficiency matrix in [testing.md](testing.md), the release dry run in
[release.md](release.md), and verify that the product still behaves as Orleans `IGrainStorage` with
authoritative grain state plus rebuildable indexes. Before stable 1.0, an unfamiliar maintainer must
also complete the checked-in [clean-room worksheet](qualification-handoff.md#worksheet). The worksheet
being present is not completion evidence; retain the participant's commands, observations, and issue
dispositions in an immutable public artifact.
