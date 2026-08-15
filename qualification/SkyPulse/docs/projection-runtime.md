# Projection runtime boundary

`Orleans.SearchableStorage.Qualification.SkyPulse.Runtime` owns the only supported qualification
topology: one process containing one Memory silo and one projection dispatcher.

## Incarnation and readiness

- The process acquires PostgreSQL session advisory lock
  `skypulse.projection-dispatcher.v1` before touching the index.
- The owning PostgreSQL connection stays open for the entire runtime incarnation.
- Readiness remains false while the complete, publishable `desired_projection` keyspace is scanned
  in bounded keyset pages.
- Complete upserts are rebuilt as PostgreSQL hydration preparation, blind index upsert, then an
  exact rebuild checkpoint.
- Complete removals are rebuilt as blind index removal, durable versioned removal tombstone, then
  an exact rebuild checkpoint.
- The checkpoint completes historical outbox versions through the rebuilt desired version, so an
  old outbox item cannot overwrite the freshly rebuilt current index.
- After the final rebuild page, readiness remains false while every recalculation already due at
  the current UTC minute is committed and every projection it creates is dispatched.
- Only after both the due queue and the ordered outbox produce an empty bounded pass does readiness
  become true.

Incomplete desired upserts are intentionally not externally publishable and are omitted from the
rebuild scan. The transition planner must emit a complete `Remove` whenever a previously published
account must become undiscoverable during reconciliation; an incomplete upsert alone is not a
removal instruction.

## Live dispatch order

The PostgreSQL lease query selects only the earliest unfinished version for each account. The
single dispatcher then applies:

- Upsert: prepare exact published hydration, blind index upsert, exact finalize.
- Remove: blind index remove, atomically retain the exact removal tombstone and finalize.

No newer version for an account is leaseable until the earlier version is finalized. Batch
hydration reads only complete `published_projection` rows whose operation is `Upsert`; retained
removal tombstones can never become UI payloads.

## Rolling-window expiration

Each visible projection stores its next exact UTC-minute expiration. PostgreSQL leases due rows in
bounded batches. For each lease the worker:

1. reads the exact account state and desired projection for the leased source version;
2. returns PostgreSQL's current UTC minute in that same lease statement and chooses a monotonic cut
   at the later of that authoritative minute, leased due minute, and prior projection cut;
3. executes one version- and generation-fenced aggregate over the trailing 30-day bucket range;
4. preserves lifecycle, repository revision, last activity, posts, following, and followers while
   replacing only the 1/7/30-day values;
5. atomically advances account state, desired projection, the next due row, and ordered outbox.

The aggregate result is one fixed-size row; a quiet account does not cause 43,200 minute rows to
cross the application boundary. If the worker is delayed, one pass jumps directly to the current
minute instead of replaying every missed minute. At a boundary minute a bucket is already outside
that window, so the counters decay without requiring another TAP event.

Fixed-size output is not constant database work: PostgreSQL still scans the populated minute
buckets in that account's trailing 30-day range. The exact cost and due-queue throughput remain a
qualification measurement, especially for accounts active in many distinct minutes.

The application host clock is not used for the projection cut. This prevents clock skew between
the application machine and the separate PostgreSQL machine from expiring a value early.

An optimistic conflict means a newer account transition superseded the lease and is safe to
re-read. Any exact live lease that fails before commit is released with a bounded delay and a
sanitized error. The co-located hosted service then fails readiness closed and lets the process
supervisor restart; it does not serve knowingly stale rolling values.

## Ambiguous index outcome

The Memory index writer has no compare-and-set token or transactional result. Once a blind index
call begins, an exception, cancellation, advisory-lock loss, expired outbox lease, false finalize,
or database error is ambiguous. `IFatalProcessTerminator` is invoked immediately. Production uses
`Environment.FailFast`; the same process must never retry. A new process reacquires the advisory
lock and repeats the full rebuild, where blind upsert and missing-remove operations converge.

Before an index call, deterministic hydration preparation errors may safely release the exact
outbox lease for a bounded retry. A false preparation result means the lease is no longer exact and
does not invoke the index.
