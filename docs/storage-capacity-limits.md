# Storage capacity envelope

This document defines the fixed logical capacity envelope enforced by
`Orleans.SearchableStorage`. The limits bound objects accepted by this Orleans `IGrainStorage`
implementation; they do not turn it into a database, a provider-independent quota service, or an
estimate of backend storage consumption.

The public values live in `SearchableStorageCapacityLimits`. They are deliberately not
configurable. Every silo, maintenance caller, persistence child grain, and recovery path must make
the same admission decision for the same durable object. Changing a limit therefore requires a
library compatibility decision and a homogeneous rollout, not an options change on one silo.

## Fixed limits

| Boundary | Maximum | `SearchableStorageCapacityExceededException.Boundary` |
| --- | ---: | --- |
| Raw Orleans grain-type component | 1,024 bytes | `grain-id-type-bytes` |
| Raw Orleans grain-key component | 4,096 bytes | `grain-id-key-bytes` |
| Canonical record key | 16 KiB | `record-key-canonical-bytes` |
| Serialized state payload in one record | 4 MiB | `record-payload-bytes` |
| Index entries for one record | 256 | `record-index-entries` |
| Index entries for one scope in one record | 64 | `record-scope-index-entries` |
| Canonical bytes in one index entry | 64 KiB | `index-entry-canonical-bytes` |
| Aggregate canonical index-entry bytes in one record | 512 KiB | `record-index-canonical-bytes` |
| Complete canonical record, including its record key | 4.75 MiB | `record-canonical-bytes` |
| Records in one partition snapshot | 1,000,000 | `snapshot-records` |
| Aggregate canonical record bytes in one partition snapshot | 512 MiB | `snapshot-canonical-bytes` |
| Canonical bytes in one journal entry | 5 MiB | `journal-entry-canonical-bytes` |
| Entries in one physical journal segment | 64 | `journal-segment-entries` |
| Aggregate canonical entry bytes in one physical journal segment | 320 MiB | `journal-segment-canonical-bytes` |
| Committed journal entries recoverable after one snapshot | 65,536 | Configuration error; no capacity boundary name. |

Every maximum is inclusive; the next element or canonical byte is rejected.

`JournalSegmentCapacity` and `MaximumJournalReplayEntries` may be configured below their fixed
maxima. Values above them fail option, layout, protocol, and recovered-manifest validation with an
`ArgumentOutOfRangeException` or startup options failure; they do not invent another capacity
boundary name. The remaining boundaries are library constants rather than deployment knobs. The
14 names in the table are the exact stable public diagnostic values; adding or changing one is a
compatibility decision even though their implementation constants are internal.

The 4.75 MiB record ceiling is exactly the 4 MiB payload ceiling plus the 512 KiB aggregate-index
ceiling plus 256 KiB for the record key, `GrainId`, ETag, schema fingerprint, and canonical framing.
The 320 MiB segment ceiling is exactly 64 times the 5 MiB entry ceiling.

## Meaning of canonical bytes

Canonical bytes are deterministic logical accounting based on the existing lossless movement and
snapshot representation:

- text contributes a four-byte length prefix plus two bytes for every UTF-16 code unit, including
  unpaired surrogates;
- raw byte sequences and `GrainId` components contribute their fixed framing and exact raw length;
- an index entry includes its scope, kind, value kind, optional text, and every persisted primitive
  field, including fields inactive for the current value kind;
- a record includes its record key, `GrainId`, payload, ETag, index entries, and optional managed-
  schema fingerprint;
- a journal entry includes its fixed control fields and its complete record or movement payload.

The measure is not the Orleans serializer output size, an RPC payload size, a compressed size, a
network byte count, or the physical bytes written by Memory, PostgreSQL, Redis, Azure Blob, or
another provider. Provider framing, encoding, retries, replication, and write amplification are
measured separately. In particular, a canonical object at its limit can occupy more physical or
transient memory than the number in this table.

## Enforcement and failure behavior

Current point reads, writes, and clears reject an oversized `GrainId` before record-key expansion
or application serialization. They reject the completed record key before schema coordination,
routing, or partition authority. Writes validate the serialized payload and extracted index entries
before the partition RPC. The partition repeats admission before WAL authority and validates the
projected complete snapshot.

Schema rebuild, movement import/export and cleanup, journal publication, snapshot publication, and
their physical child grains repeat the limits at their own trust boundaries. Recovery validates
the manifest settings, snapshot payload, every committed journal segment and entry, and the
projected recovered partition before exposing the activation. A retry cannot bypass a limit, and
no path truncates a record, index set, page, journal, or snapshot to make it fit.

An over-limit live request throws `SearchableStorageCapacityExceededException` before changing
durable authority and leaves a healthy activation reusable. Its `Boundary`, `Actual`, and `Limit`
properties identify the violated dimension. Library-produced diagnostics contain counts and the
stable boundary name only; they do not include record keys, payloads, index values, tokens, or
secrets.

An over-limit or malformed object already present in durable state is different: recovery or a
child-grain read fails closed, requests deactivation, and does not expose partially recovered data.
Restore or repair the authoritative physical state; do not catch the exception and continue with a
partial partition.

## Upgrade from an earlier pre-1.0 build

Earlier builds could persist records or configure journal settings above this envelope. Before the
first rollout which enforces these limits:

1. Quiesce all users of the searchable provider and take a verified provider backup.
2. Verify every provider's journal options against the table.
3. Under the old compatible binary, reduce or rewrite oversized current state and indexes. Complete
   and verify compaction, journal retirement, old-snapshot retirement, movement cleanup, and any
   reusable physical slots which can still retain an older object.
4. Restore a clone of that complete physical namespace and rehearse activation and recovery of
   every partition, followed by representative writes, compaction, schema rebuild, and movement.
5. Deploy the same binary and options to every participant before resuming traffic.

There is no automatic truncation or in-place repair for pre-existing oversized state. If rehearsal
or rollout finds one, keep traffic quiesced. Restore the previous compatible binary or backup,
reduce or migrate the application state/index declaration, and rewrite or repair the affected
physical records with provider-specific tooling before trying the homogeneous rollout again.

Inspecting only current logical grain state is insufficient. Recovery revalidates the active
snapshot, retained WAL entries (including an older value which was later replaced or deleted), and
cleanup/reuse slots. This release has no built-in offline namespace scanner, supported public
operator-wide compaction command, or physical-byte estimator; a full restored-clone rehearsal plus
provider inspection is the available end-to-end preflight. If the old binary cannot prove and
retire every oversized retained object, migrate into a fresh provider namespace instead of
guessing.

Persisted `JournalSegmentCapacity` or `MaximumJournalReplayEntries` values above the new maxima are
immutable layout identity. Changing only application options is not a migration. Use a fresh
provider namespace and application-level state copy, or an audited provider-specific offline
migration which updates every related layout, manifest, journal, and snapshot invariant together.

## Operational interpretation

These are corruption, denial-of-service, and worst-object safety ceilings, not recommended steady-
state sizes and not release-scale evidence. Keep ordinary partitions comfortably below both
snapshot ceilings. Activation still materializes the complete accepted partition and its derived
indexes, while compaction serializes a complete accepted snapshot in one non-reentrant grain turn.
Use more partitions to reduce records and bytes per owner only after measuring the resulting query
fan-out.

The [1.0 contract](one-zero-contract.md), [architecture](architecture.md),
[backend notes](backends.md), [schema lifecycle runbook](index-schema-lifecycle.md), and
[movement runbook](live-movement.md) describe the surrounding product and protocol boundaries.
