# SkyPulse PostgreSQL persistence recovery

This project is a reconstructed persistence foundation, not an operational ingestion runtime and
not production qualification evidence. It persists metadata only. Raw AT Protocol bodies, post
text, handles, media, and arbitrary JSON are outside this database contract.

## Schema compatibility

Schema version 1 is local, unpublished, and unfrozen. This recovery replaces that disposable
schema in place and deliberately provides no migration from an earlier local v1. Drop and recreate
any database made from an earlier draft. The migration SHA produced by this reconstructed source
is not the lost historical SHA and must not be represented as byte-exact recovery evidence.

The runtime manifest is insert-once and binds the exact profile/cap/allowlist, source instance,
index namespace/provider/schema, raw and canonical package digests, repository URL and commit, and
build SDK. Delivery reservation fails closed until that manifest exists and matches the source.

Every source delivery is independently reserved as `Pending` before transition planning. The first
observed UTC minute survives redelivery. Commit, validated stale no-op, and quarantine lock and
recheck the exact delivery ID, digest, and original minute in their transaction. A completed exact
duplicate remains acknowledgement-safe; a conflicting digest or forged reservation fails closed.

`account_state.last_applied_revision` is the repository-wide ordering high-water for ordinary live
record transitions. `repo_sync` establishes it; historical replay does not advance it; later live
events advance it monotonically (several rkeys from one commit may share one revision). A strictly
older cross-rkey event is acknowledgement-safe only through the dedicated validated-no-op proof,
which is rechecked in the commit transaction. The current record revision independently fences the
same rkey.

Ordinary visible-mutation planning uses
`PostgreSqlPlanningStore.ReadActivityWindowAggregateAsync`. One PostgreSQL statement fences the
exact account state version and repository generation, scans only the trailing thirty-day range,
and returns a fixed-size row with the 1/7/30-day counters plus the next expiry. The planner does
not need to transfer or materialize every UTC-minute bucket.

Long lifecycle transitions use `lifecycle_transition_work`. The row contains only digests,
account key, generation, lifecycle/repository revision, observation minute, and phase. Inactive
cleanup and `repo_sync` dependency drain advance in deterministic pages of at most 1,000 rows.
Their TAP delivery remains Pending until the final transaction inserts the semantic event and
completes the delivery. This makes a process restart resumable without treating partially purged
state as acknowledgement-safe. A unique account key permits only one pending lifecycle transition
per account. Ordinary commits and lifecycle pages share sorted transaction-scoped account locks,
including indirect relationship/dependency targets; ordinary mutations return an unacknowledgeable
optimistic conflict while any affected account has pending lifecycle work.

## Bounded operational retention

`PostgreSqlRetentionStore` requires an explicit `PostgreSqlRetentionPolicy`; it has no implicit
retention periods. A qualification profile must freeze all five ages before ingestion. Activity
buckets cannot be configured below thirty days because the longest indexed window is thirty days.

Age is never sufficient proof for deleting replay-sensitive state. Operators must monotonically
advance source-delivery, semantic-event, and activity watermarks with a non-empty public evidence
reference. Every delete selects at most 1,000 rows with `FOR UPDATE SKIP LOCKED`. Cleanup never
deletes Pending deliveries, unfinished outbox rows, current relationship state, desired or
published projections, or a current-generation record row, including a deletion tombstone.

Schema validation constructs the reviewed migration in PostgreSQL's temporary schema and compares
the complete catalog contract with the installed schema: column order/types/nullability/defaults,
generated and identity attributes, constraints, indexes, and sequences. Missing, extra, or
structurally different objects fail closed.

## Runtime boundary and remaining blockers

A projection lease is **not** a fencing token. The first runtime therefore deliberately supports
only one co-located dispatcher and Memory-index incarnation. It takes a PostgreSQL session
advisory lock, rebuilds the empty index before readiness, and terminates the whole process after
any ambiguous blind index call. Startup replay, ordered Upsert/Remove dispatch, versioned removal
tombstones, PostgreSQL hydration, and Web readiness wiring are implemented for that topology.
They are not a general multi-process fencing protocol.

The bounded rolling-window worker leases exact due rows, reads one fixed-size aggregate, and
atomically publishes a new desired version and outbox row. Durable Web startup keeps readiness
closed until pre-existing due work and its outbox are empty, so 1/7/30-day fields decrease even
when no new source event arrives.

The commit-before-ACK TAP adapter is connected to the durable Web mode. The application remains
non-qualifying until the real PostgreSQL integration and full source-to-query recovery suites run
against PostgreSQL; locally skipped integration tests are not evidence. A future multi-silo
topology still requires target-side version fencing or index-namespace rotation.
