# SkyPulse capacity plan

The frozen account cap bounds searchable **account entities**. It does not bound the number of AT
Protocol records which TAP and PostgreSQL must examine or retain in order to calculate current
post/follow stocks, handle deletes, and reconcile a repository. One admitted account can own many
posts, likes, reposts, and follow records. The source-state databases may therefore dominate both
disk and write load even when the Memory index fits comfortably.

## Memory-index planning estimate

The nearest existing compact-index probe retained about 2,113 bytes per entity at eight scalar
index entries. Until a real SkyPulse probe replaces it, planning adds a deliberately conservative
128 bytes for each of the other nine scalar entries in the fixed 17-field schema:

```text
2,113 + (9 * 128) = 3,265 bytes per admitted account
```

That is roughly 3.27 GB for one million accounts and 32.65 GB for ten million accounts in the
central retained graph, before native allocations, fragmentation, transient query/build memory,
Orleans runtime state, or a safety margin. A 25% planning margin makes those figures about 4.1 GB
and 40.8 GB. These are extrapolations, not qualification evidence or a machine-size promise.

The first two 32-GB machines therefore remain a useful fixed calibration topology: one application
machine for the co-located Memory silo, dispatcher, and TAP process, and one PostgreSQL machine.
We do not change that topology merely because the corpus profile changes. We first measure how far
it goes, then vertically resize the single application/index machine if the next measured profile
fits. Ten million accounts with failure headroom is expected to exceed this two-machine envelope.
Adding Memory-index machines is not supported by the current correctness boundary; it first needs
target-side version fencing or index-namespace/cluster rotation and a separately reviewed topology.

## State which is not in the Orleans index

Capacity reports must measure these separately:

- TAP's current repository catalog, with one row per selected current AT Protocol record;
- PostgreSQL current record/tombstone state used for idempotence and delete handling;
- distinct follow-pair multiplicity state;
- sparse activity-minute buckets which have not passed the frozen retention watermark;
- desired/published projections and ordered outbox rows;
- unfinished TAP outbox/resynchronization rows and PostgreSQL WAL, indexes, vacuum headroom, and
  backups.

No estimate derived only from 17 index fields is allowed to size those stores. In particular,
`CorpusCap = 1,000,000` means at most one million searchable accounts; it does not mean one million
PostgreSQL or TAP rows.

## Fixed-machine calibration ladder

The same code, schema, corpus parent, and two-machine topology is exercised with exact prefix
profiles in this order:

1. 10,000 accounts: validate ingestion/recovery and obtain bytes-per-source-record baselines.
2. 100,000 accounts: measure repository-record distribution, WAL/vacuum behavior, rolling-bucket
   expiry, full Memory rebuild time, and peak RSS.
3. 1,000,000 accounts: run only if the measured disk, RAM, and rebuild envelopes fit the frozen
   limits with explicit headroom.
4. 10,000,000 and larger: select the next prefix without changing entity semantics only after
   measured evidence identifies a valid single-process envelope, or after the future multi-silo
   fencing/rotation design is implemented and reviewed. Index-only linear arithmetic is not enough.

Moving between reviewed steps can use the monotonic online protocol described in
[runtime corpus growth](runtime-corpus-growth.md). It does not rebuild the older prefix, but it also
does not make an unmeasured target safe: the operator must provision memory and PostgreSQL/TAP disk
headroom before submitting the administrative request.

Each step records at least: admitted/visible account counts; TAP repository rows and bytes;
PostgreSQL rows, table/index bytes, WAL rate, and vacuum lag by table; index retained bytes and
process RSS by silo; event/ACK lag; rebuild duration; query latency; and disk/RAM high-water marks.
A failed or resource-exhausted step is retained as evidence and is not re-labelled as a successful
smaller run.
