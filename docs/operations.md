# Operations index

This page is the operator entry point. It links to the detailed runbooks instead of restating their
invariants. Orleans grain state remains authoritative; the searchable provider's records and indexes
must be backed up, restored, rolled out, and moved as one provider namespace.

## Runbook map

| Task | Primary runbook | Mandatory companion |
| --- | --- | --- |
| Choose/configure a physical Orleans provider | [Backends](backends.md) | [Testing strategy](testing.md) for provider qualification |
| Size partitions, journal, replay, and compaction | [Storage capacity limits](storage-capacity-limits.md) | [Architecture](architecture.md#recovery-and-bounded-compaction) |
| Roll out paging/query changes or rotate continuation keys | [Bounded query contract](bounded-query-contract.md#compatibility-and-rollout) | [1.0 contract](one-zero-contract.md) |
| Adopt/change a managed index schema | [Managed schema lifecycle](index-schema-lifecycle.md) | [Maintainer trace](maintainers.md#trace-a-schema-rebuild) |
| Enable movement, move a slot, or rebalance | [Live movement](live-movement.md) | [Maintainer trace](maintainers.md#trace-a-slot-move) |
| Investigate a failed activation or write | [Maintainer diagnostics](maintainers.md#diagnose-fail-closed-and-poisoned-activations) | Physical-provider request/error telemetry |
| Qualify a release | [Release process](release.md) | [Testing strategy](testing.md) and [compatibility manifest](../eng/compatibility-manifest.json) |

## Rollout and rollback

Classify the release before deployment:

- Application-only code with unchanged registrations/formats follows the application's ordinary
  Orleans rollout policy.
- Query-protocol changes require quiescing query traffic and a homogeneous silo/client rollout.
- Layout adoption, movement enablement, and managed-schema adoption require the complete provider to
  be quiesced and every silo/client restarted on the same package before the gate is advanced.
- A schema declaration change additionally requires every registered state to complete its rebuild
  and report `Active` before traffic resumes.

Take the namespace backup after traffic is paused and before advancing a one-way gate. A package
rollback is safe only while its older binary understands every already-persisted format/protocol and
all registrations still match. After schema protocol 1 or movement protocol 1 is published, do not
start a binary which lacks that capability. Finish/resume the durable operation or restore the entire
pre-gate backup; do not roll back selected grains.

## Compaction and capacity pressure

Alert on mutation latency/error rate and physical-provider latency first, then correlate with accepted
record count/bytes, committed journal tail, compaction attempts, and partition ownership skew. The
journal ring deliberately backpressures rather than overwriting unrecovered history. Safe relief is
to repair the physical provider, lower ingress, or explicitly move virtual slots to reduce a dense
owner. Changing durable segment/replay settings or raising hard capacity limits requires a migration
and capacity proof. Restarting repeatedly or deleting journal/snapshot objects is not relief.

## Ambiguous physical-provider failures

A timeout or exception around a conditional physical write is a potentially lost acknowledgement.
Capture the first error and provider request id. The affected storage/layout/journal/snapshot
activation poisons itself so the next activation rereads durable state and converges by operation id,
generation, epoch, and ETag. Let that recovery happen before repeating the same high-level idempotent
admin operation. Do not force a new ETag, reuse a stale activation, or infer “not committed” from the
exception alone.

## Backup and restore

Back up one consistent physical-provider namespace while searchable traffic and admin protocols are
quiesced. Include all of these object families and their physical-provider metadata/ETags:

- the provider layout control and any movement/schema-enablement or move intent it contains;
- every partition manifest;
- every journal-ring slot and both snapshot slots for every partition owner, including owners with no
  current virtual slots while retention still includes their state;
- every per-state `index-schema` control document;
- the continuation-key configuration needed only when outstanding tokens must remain valid.

Restore the complete set to an empty namespace under the same provider name and durable layout
settings. Start one homogeneous package/configuration, keep traffic paused, verify layout and every
schema status, allow activation recovery to finish pending publication/cleanup, and then resume. A
data-only restore without layout or schema controls is invalid. Losing continuation keys invalidates
tokens but does not lose grain state; issue fresh queries instead of restoring key bytes from logs.

## Minimum alarms and dashboard cuts

Use stable event names and metric dimensions documented by the runtime diagnostics surface. At
minimum, page or alert on:

- sustained point mutation/read/query failure or cancellation rate, split by provider and operation;
- any ambiguous physical persistence outcome or repeated activation/recovery failure;
- capacity/backpressure exceptions and sustained compaction failure or duration growth;
- schema rebuild not advancing, a state not `Active`, or mismatched registered/durable fingerprint;
- movement enablement or a move phase not advancing, abort failure, or repeated route mismatch;
- query/facet limit exhaustion, stale/invalid continuation spikes, and owner fan-out growth;
- physical-provider throttling, conditional-write conflict, latency, availability, and storage growth.

Treat high-cardinality record keys, grain ids, indexed values, payloads, continuations, and key bytes
as diagnostic evidence sampled on demand—not normal metric labels or routine logs. For incident
handoff record provider/state, deployment revision, layout epoch/format, partition owner, schema
fingerprint/rebuild id, move id/phase, first exception, and physical-provider request id.

## Runtime observability contract

The built-in runtime emits metrics from meter `Orleans.SearchableStorage`. The current instruments are
exactly:

| Instrument | Kind | Unit | Meaning |
| --- | --- | --- | --- |
| `orleans.searchable_storage.operation.count` | `Counter<long>` | `{operation}` | One completed observed operation, including failure or cancellation. |
| `orleans.searchable_storage.operation.duration` | `Histogram<double>` | `s` | Elapsed seconds for the same completed operation. |
| `orleans.searchable_storage.operation.work` | `Histogram<long>` | `{item}` | Non-negative bounded result/work count on successful operations which define one. |

Every measurement has exactly four metric tags: `provider`, `operation`, `phase`, and `outcome`.
The current outcome values are `success`, `failure`, and `cancelled`. State name, owner/partition,
grain id, virtual slot, epoch, move/rebuild id, schema fingerprint, exception type, and physical
request id are deliberately not metric tags: they are unbounded or topology-dependent and would turn
normal dashboards into a high-cardinality incident store. Use explicit admin/status calls and the
physical provider's diagnostics for owner/state/epoch/request detail.

The currently emitted operation/phase pairs are:

| Operation | Phase(s) | Successful work value, when emitted |
| --- | --- | --- |
| `storage.read`, `storage.write`, `storage.clear` | `execute` | none |
| `query.page`, `query.legacy` | `execute` | returned `GrainId` count |
| `query.facet.distinct`, `query.facet.count` | `execute` | returned facet item count |
| `query.facet.min_max` | `execute` | `0` for no extrema, otherwise `2` |
| `persistence.recovery` | `activation` | recovered record count after recovery, pending publication, and cleanup complete as one aggregate activation outcome |
| `persistence.compaction` | `replay-boundary`, `automatic`, `manual` | accepted snapshot record count |
| `schema.rebuild` | `orchestrate` | processed record count |
| `movement.enable` | `orchestrate` | none |
| `movement.execute`, `movement.abort` | `orchestrate` | maximum of exported and deleted record counts |

Structured logs use the following exact EventIds and levels:

| EventId | Level | Event | Safe structured fields |
| --- | --- | --- | --- |
| 1000 | Debug | request failure | `Provider`, `Operation`, `Phase`, `ErrorType` |
| 1001 | Warning | lifecycle failure | `Provider`, `Operation`, `Phase`, `ErrorType` |
| 1002 | Information | lifecycle cancellation | `Provider`, `Operation`, `Phase`, `ErrorType` |
| 1003 | Information | lifecycle completion | `Provider`, `Operation`, `Phase`, `WorkCount` |
| 1004 | Debug | request cancellation | `Provider`, `Operation`, `Phase`, `ErrorType` |

Routine diagnostics never attach the exception object or exception message and never include state
payloads, record/grain keys, indexed values, continuations, or continuation-key material. Request
success is metric-only; lifecycle success is logged except automatic compaction success, which stays
metric-only. Replay-boundary and manual compaction success are lifecycle logs. Logging and
metric-listener failures are swallowed so diagnostics cannot alter storage semantics.
