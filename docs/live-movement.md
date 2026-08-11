# Live virtual-slot movement

Live movement changes one virtual slot's physical owner without changing the provider namespace or
its persisted virtual-slot count. The core library supplies an explicit, resumable admin protocol;
it does not start moves automatically or choose a background balancing policy.

Enabling movement preserves the routing layout format: namespaces without managed schemas remain
format 4, while schema-enabled namespaces remain routing-equivalent format 5. The sweep upgrades a
supported format-3 partition to persistence format 4 and preserves a format-5 schema participant.
It publishes movement protocol version 1 only after every current owner has accepted the
routed-operation fence. Transfer and cleanup progress is durable, so a process restart or caller
cancellation does not require an operator to reconstruct the move.

## Rollout and enablement

Enabling movement is an operational compatibility boundary, not an online mixed-version upgrade. A
persisted layout cannot prove that an old process has stopped or forgotten a cached layout.

For each provider namespace:

1. Quiesce searchable-storage reads, writes, clears, queries, facets, and admin mutations.
2. Deploy and restart every participating silo and Orleans client on the movement-capable package.
   Verify that no older process remains.
3. Ensure the namespace has a valid routing layout in format 4 or 5. An existing format-3 layout
   must first be adopted using the format-4 rollout described in [backends.md](backends.md).
4. Resolve the keyed `ISearchableStorageAdminClient` and call `EnableMovementAsync` while traffic is
   still quiesced. The method advances one durable owner fence at a time and is safe to call again
   after cancellation or a process failure.
5. Read the layout and require `MovementState == Enabled` and `MovementProtocolVersion == 1` before
   resuming traffic.

Once movement is enabled, legacy placement-only partition calls are rejected. Do not roll an old
binary back into the namespace. Restoring an older application build requires restoring the complete
provider namespace from a compatible backup or performing a separately designed downgrade.

```csharp
var admin = services.GetRequiredKeyedService<ISearchableStorageAdminClient>("Searchable");

var enabled = await admin.EnableMovementAsync(cancellationToken);
if (enabled.MovementState != SearchableStorageMovementState.Enabled)
{
    throw new InvalidOperationException("The movement fence was not published.");
}
```

`EnableMovementAsync` cancellation stops only the client-side loop between owner calls. It
does not undo already persisted upgrades. Call the same method again to resume.

## One explicit move

Only one move intent can exist in a provider namespace. Planning validates the slot against the
current layout and rejects its current owner as the target. `AdvanceMoveAsync` performs exactly one
durable transition or one transfer/delete page payload. `ExecuteMoveAsync` is a convenience loop
over those resumable turns; cancellation is observed between turns and leaves the move available
through `GetMoveAsync`.

While phase is `Planned`, an advance may first reconcile a participant's movement capability and
routing-epoch floor. A newly introduced target and a source whose durable floor lags after an
unrelated move can each consume one such participant-only advance, so `Planned` can remain visible
for up to two advances before the following advance freezes the source.

```csharp
var move = await admin.PlanMoveAsync(
    slot: 17,
    targetPartitionIndex: 9,
    cancellationToken);

// An operator can persist one observable step at a time:
move = await admin.AdvanceMoveAsync(move.MoveId, cancellationToken);

// Or resume the same durable intent through completion:
move = await admin.ExecuteMoveAsync(move.MoveId, cancellationToken);
```

The durable order is:

1. persist the sole layout move intent;
2. freeze source mutations for the slot and capture its `NextVersion` high-water mark;
3. advance the target version to at least that watermark through a replayable WAL entry;
4. export and import stable record-key pages while the target remains mutation-inactive;
5. atomically assign the slot to the target and increment the routing epoch;
6. durably make the source query-invisible for the new epoch and confirm that fence;
7. enable target mutations;
8. delete obsolete source records in pages, retire source and target controls, and clear the intent.

Target writes remain frozen after the ownership commit until the source visibility fence is durable.
This ordering prevents a stale old-epoch query from returning obsolete predicate membership; result
deduplication is not used as a correctness mechanism. Routed point and query calls which observe an
epoch/owner mismatch refresh the layout and retry the complete logical attempt under the existing
routing contract.

`SearchableStorageSlotMoveProgress` exposes the move id, slot, source and target owner, source/current
epochs, phase, transferred/deleted record counts, canonical movement-encoding byte counts, and
whether abort remains legal. Those byte counters are deterministic replay accounting, not observed
Orleans wire, network, or physical-provider bytes. Progress does not expose the durable record-key
cursor.

## Abort and recovery

`AbortMoveAsync` is available only before the ownership commit. It deletes any imported target copy
in bounded pages, retires the target control, and unfreezes the authoritative source. Calling it
again after cancellation resumes the same durable rollback. After ownership commits, rollback would
violate epoch authority and the admin client rejects abort; finish the move and, if necessary, plan a
new move in the opposite direction.

Lost acknowledgements are handled by stable move identity, page ordinal, cursor, and digest. A
repeated page must describe the same contents and is accepted idempotently; a conflicting replay is
rejected. Recovery reconstructs `NextVersion` from snapshot plus journal, including an empty-slot
high-watermark entry, so imported records cannot reuse source ETags. Source visibility and minimum
routing-epoch fences live in the partition manifest and survive activation recovery.

An operator recovering an interrupted move should:

1. call `GetMoveAsync`;
2. inspect the phase and `CanAbort`;
3. call `ExecuteMoveAsync` to converge forward, or `AbortMoveAsync` while `CanAbort` is true;
4. verify that `GetMoveAsync` returns `null` and the layout reports the expected epoch/owner counts.

Do not clear physical provider records or edit layout/manifest documents manually.

## Manual rebalance planning

Rebalancing is also explicit. The core computes a deterministic minimal-churn quota for the requested
physical partition count and returns only the next required single move; it does not persist an
unbounded bulk plan. Recompute the summary after every completed move because the routing epoch and
current assignment are authoritative.

```csharp
var plan = await admin.PlanRebalanceAsync(targetPartitionCount: 12, cancellationToken);
while (plan.RequiredMoveCount != 0 || plan.ActiveMove is not null)
{
    if (plan.ActiveMove is { } active)
    {
        await admin.ExecuteMoveAsync(active.MoveId, cancellationToken);
    }
    else
    {
        var next = plan.NextMove
            ?? throw new InvalidOperationException("A non-terminal plan must nominate one move.");
        var move = await admin.PlanMoveAsync(next.Slot, next.TargetPartitionIndex, cancellationToken);
        await admin.ExecuteMoveAsync(move.MoveId, cancellationToken);
    }

    plan = await admin.PlanRebalanceAsync(12, cancellationToken);
}

// ExecuteRebalanceAsync performs the same resumable sequence as a client-side convenience loop.
```

Applications which want a policy based on CPU, storage bytes, hot keys, or schedules should implement
it in a host-owned `BackgroundService` and call this admin surface. Such a service must serialize its
decisions per provider and retain operator controls; it is not part of searchable storage itself.

## Page bounds and retained whole-partition work

Configure movement pages per provider:

```csharp
options.Movement.TransferPageRecordLimit = 128;       // maximum 1,024
options.Movement.TransferPageByteTarget = 256 * 1024; // maximum 4 MiB
```

The record limit is a hard page cardinality ceiling. The byte value is evaluated with the protocol's
deterministic canonical movement encoding. It is not the size produced by Orleans serialization or
the physical provider. Movement messages and newly published persistence-format-4/5 snapshots encode
persisted text as explicit big-endian UTF-16 code units so even unpaired surrogates survive export,
import WAL, compaction, recovery, and cleanup without Orleans string normalization. An active legacy
snapshot adopted during movement enablement remains readable; the next format-4/5 compaction writes
the lossless payload to the inactive snapshot slot before the manifest publishes it. A single
accepted record larger than the target is returned alone, so the
in-memory page/transfer shape is `O(target + largest accepted record)` in canonical units; it is not
an absolute wire-byte cap. Every export response, import journal payload, and delete journal payload
is record-count bounded and canonical-byte-targeted. Before an import, delete, or version-fence WAL
mutation, the retained persistence engine can trigger whole-partition compaction; activation can
also perform whole-partition recovery. The protocol therefore bounds each page payload, not an
advance call's total work or wall time. End-to-end movement work remains sensitive to the moved
slot, physical-partition size, record sizes, compaction timing, and skew; there is no fixed
freeze-duration promise.

Partition persistence formats 4 and 5 deliberately retain whole-partition snapshots. Compaction can
still serialize every record owned by a physical partition, and activation recovery still
materializes the whole active snapshot and rebuilds the derived slot index in
`O(partition records)`. Slot export, import, and delete are bounded movement paths, but they do not
turn those recovery/compaction boundaries into per-slot physical work. Capacity testing must report
both page-level movement costs
and any concurrent or recovery-time whole-partition work rather than extrapolating from an average
slot.

The process-local movement benchmarks cover slot-index rebuild plus export/import/delete pages for
uniform, skewed, and oversize-singleton fixtures. They validate deterministic membership, cursor,
digest, byte accounting, and idempotence; they do not measure Orleans transport, backend service
latency, source freeze duration under distributed load, or a complete large-namespace rebalance.

## Operational checklist

- Back up the provider namespace using the physical backend's supported consistent procedure.
- Verify all clients and silos use the same provider name and movement-capable package.
- Keep movement disabled until the quiesced enablement gate has completed.
- Monitor phase, epoch, moved/deleted counts, errors, compaction, and time between turns.
- Prefer `AdvanceMoveAsync` when an operator or job needs explicit checkpoints and cancellation
  between resumable turns; do not treat a turn as a strict work or wall-time budget.
- Resume by move id after cancellation or restart; never start a competing move.
- Abort only before ownership commit; otherwise converge forward.
- Measure skew and largest-record size before choosing page limits or an automatic host policy.
- Keep provider-native backup, capacity, request, and storage-write telemetry alongside admin progress.
