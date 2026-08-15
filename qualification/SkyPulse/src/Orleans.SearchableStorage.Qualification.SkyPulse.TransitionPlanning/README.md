# SkyPulse transition planning and durable lifecycle orchestration

This project is the pure, metadata-only planner for sanitized `RecordMutation` deliveries. It does
not read PostgreSQL, acknowledge TAP, publish to the searchable index, or retain DIDs, handles,
record bodies, text, media, or arbitrary JSON.

The runtime must reserve a delivery first, then supply the exact account, record, follow-pair,
current desired-projection, and fixed-size activity-aggregate reads represented by
`RecordMutationPlanningInput`. A retry result is not acknowledgement-safe. A validated no-op must
be committed through `PostgreSqlIngestionStore.CommitValidatedNoOpAsync`, which rechecks its proof
inside the serializable commit transaction. A quarantine must likewise be committed before ACK.

## Record and relationship semantics

- Post stock counts the current `app.bsky.feed.post` records, including replies and quotes.
- Follow records are retained by raw rkey and reduced to source-target multiplicity. Following and
  follower stocks change only when multiplicity crosses zero.
- A live like, repost, or direct-reply create increments the admitted target's received-engagement
  minute bucket. A quote that is not a direct reply remains only a post.
- Historical records update current stock and add admitted, changed targets as reconciliation
  dependencies. They do not update rolling activity, publish partial upserts, advance last
  activity, or advance the repository-wide live revision high-water. A newly blocked target whose
  desired projection is currently an upsert receives a complete removal at the next account-state
  version, so stale visible data does not masquerade as a reconciled projection.
- `repo_sync` establishes both the completed-sync barrier and the initial
  `last_applied_revision`. Live events may share one revision across different rkeys; a strictly
  older revision is a commit-time-validated no-op. The same rkey is additionally fenced by its
  canonical record revision.

## Projection fencing

Visible account mutations emit a complete 17-field projection at
`max(reserved observation minute, current desired projection cut)`. Counts use the exact
half-open 1/7/30-day UTC-minute windows `(cut - duration, cut]`. A delayed retry therefore still
updates stock and repository revision, while its activity delta is added only to the windows that
still contain its originally reserved minute. Neither the desired projection version nor its cut
can move backwards.

`PostgreSqlPlanningStore.ReadActivityWindowAggregateAsync` returns one row containing exact
create/update/delete/post totals for 1/7/30 days, received engagement for 30 days, and the next
real expiry. The SQL may scan the bounded thirty-day range, but the runtime never materializes or
transfers up to 43,200 minute buckets. Account state version and repository generation are fenced
inside that same aggregate statement.

## Ordinary-record adapter recipe

1. Reserve the TAP delivery and retain its first observed minute.
2. Read the owner account, current raw record, required follow pairs, and corpus-admission/account
   snapshots for at most the owner, old target, and new target. Read each visible account's current
   desired projection.
3. For each visible affected account, calculate
   `cut = max(reservation.FirstObservedAtMinuteUtc, desired.ProjectionCutMinuteUtc)` and call
   `ReadActivityWindowAggregateAsync(accountKey, state.StateVersion,
   state.RepositoryGeneration, cut)`.
4. If an aggregate read throws `PlanningStateChangedException`, discard the whole evidence set and
   reread it. Otherwise construct `AccountPlanningSnapshot` values and call `Plan`.
5. Commit the returned decision. An optimistic account-version conflict also restarts from the
   still-pending durable reservation; it must not invent a new observation minute.

## Lifecycle and repository-sync barrier

`LifecycleTransitionPlanner` is the pure start planner. An active lifecycle observation replaces
the account with `synchronization_complete = false`, adds the generation-scoped self dependency,
and emits a complete removal when the authoritative desired projection was visible. An inactive
lifecycle observation starts restartable cleanup; `repo_sync` starts restartable dependency
drain. Neither paged start permits ACK.

`PostgreSqlLifecycleOrchestrator` stores only bounded event metadata and the current phase in
`lifecycle_transition_work`. Each call deletes at most 1,000 outgoing follow pairs, owned records,
owned activity buckets, or reconciliation dependencies. Outgoing distinct relationships repair
the admitted target's follower stock exactly once. A target is republished only after no remaining
reconciliation dependency blocks it. Inactive completion zeroes the owner's post/following stocks;
repository-sync completion sets both `completed_sync_revision` and `last_applied_revision` and
rebuilds the complete 17-field projection.

Each page preflights its bounded target set, then acquires the owner and every affected target in
one canonical account-lock order before it rechecks the work row, exact owner generation/lifecycle
fence, and current page coverage. If the page changed while locks were acquired, it returns Retry
without changing a table or permitting ACK.

The source delivery remains `Pending` across every intermediate commit. Only the final locked
transaction inserts the generation-scoped semantic event, removes the work row, and completes the
delivery; that result alone permits ACK. Redelivery resumes the same durable work. Real PostgreSQL
scenarios are environment-gated and are not qualification evidence when reported as skipped.

Expiry-only recalculations remain outside this project.
