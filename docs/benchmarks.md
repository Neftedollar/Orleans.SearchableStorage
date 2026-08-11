# Benchmarking

The benchmark suite is a reproducible measurement foundation, not a published scalability claim.
It separates process-local costs from end-to-end Orleans workloads and preserves the raw inputs and
histograms needed to compare runs later. Issue #8 remains open until dedicated environments have
produced reviewed provider, 10-million-record, and capacity artifacts.

## What is measured

`Orleans.SearchableStorage.Benchmarks` uses BenchmarkDotNet for isolated CPU, allocation, and
serialization costs. Its cases invoke the same internal production helpers as the storage grains:

- incremental hash/range index mutation and bounded range lookup;
- activation rebuild of the materializing indexes and the production additive ordered view;
- steady indexed mutation for production-shaped `state/type-hex/key-hex` record keys across unique
  and low-cardinality index distributions;
- expression translation, wire-plan construction, and partition boolean-plan evaluation;
- facet candidate metadata pages and resumable filtered exact-count slices;
- Orleans serialization of query plans and journal segments;
- bounded journal-segment append using an in-memory `IPersistentState` test double;
- validated journal replay and compaction snapshot construction;
- activation-local virtual-slot catalog rebuild and bounded movement export/import/delete helpers.

The journal-append case measures the state machine and allocations. It does not measure a physical
provider, network, or durable write. End-to-end provider latency belongs to the load driver.

BenchmarkDotNet's memory diagnoser reports bytes allocated by the timed operation. It does not report
the managed objects which remain live in an activation after temporary rebuild structures are
collected. The separate retained-memory evidence command below measures that delta in isolated worker
processes. It is deliberately labelled retained *managed* memory, not working set or native memory.

`Orleans.SearchableStorage.LoadDriver` is a native Orleans workload driver. It can run an embedded
single-machine smoke topology or split `serve` and `run` across independently scalable Crank jobs.
The searchable path and a plain Orleans `IPersistentState<T>` path consume the same deterministic
point-operation sequence. Plain storage is not a baseline for secondary-index queries, so a plain
workload containing query operations is rejected.

## Reproducible specifications

Scenario, dataset, and workload documents are independently versioned JSON artifacts. A scenario
pins the exact SHA-256 of its dataset and workload files. Parsing rejects unknown members, unsupported
versions, invalid capability combinations, and broad query profiles whose expected result exceeds
the declared safety bound. Validation of a billion-record description is arithmetic only; it never
materializes the records.

Every result must retain both the source artifacts and the canonical effective configuration after
command-line overrides. The result provenance includes their hashes, the commit and dirty state,
runtime and GC settings, package versions, machine/cgroup information, serializer, implementation
path, provider, topology, seed, and artifact references. Connection strings and other secret values
must never enter a result artifact.

The effective configuration records the declared total silo count and, for searchable storage, the
actual derived virtual-slot count in addition to its requested target. Searchable-only partition,
virtual-slot, and journal settings are `null` for the plain Orleans path rather than implying that
the baseline used those controls. Artifact validation decodes every embedded source/effective JSON
document, recomputes its SHA-256, and checks that the decoded effective document is structurally
identical to the top-level effective configuration. The effective document records the normalized,
non-secret command-line overrides that were actually applied. The validator rebuilds the complete
configuration from the embedded scenario, dataset, and workload, those declared overrides, and the
run identity, then requires exact structural equality. It also applies the strict result schema and
binds the scenario's dataset/workload digests to the embedded artifacts.

The v1 generator is stateless and uses a frozen integer-mixing algorithm rather than `System.Random`.
The same seed, dataset shape, workload operation mix, client ordinal, and global operation sequence
therefore reproduce the same key selection and operation choice; seed, record ordinal, and revision
reproduce the payload and indexed values. Any future generator change requires a new schema or
algorithm version and new golden vectors.

## Histograms and counters

Each load-generator instance writes one raw HDR histogram per operation and outcome. Histograms may
be combined only when unit, trackable range, significant digits, operation, and outcome match.
Percentiles are calculated after histogram union; client percentiles are never averaged.
Result summaries include p50, p90, p95, p99, and p99.9, while the raw HDR logs remain authoritative.
Post-run validation opens every referenced log, requires exactly one real HDR histogram with the
declared settings and count, recomputes those summary percentiles, and rejects unreferenced HLOGs.

Closed-loop workloads hold fixed concurrency. Open-loop workloads schedule a fixed offered rate and
measure scheduled-to-completion latency, including queueing. Results distinguish offered, started,
completed, succeeded, failed, timed out, and dropped operations, plus scheduled duration and final
drain duration. A timed-out Orleans call is not transport-cancelled; the driver observes and drains
late calls before another phase can begin, or fails the run as incomplete.
If teardown or cleanup fails after measurement completed, the failure result still retains that
measurement and its raw histograms. Population/restoration failures retain completed-record counts
as explicitly partial evidence.

Physical-call counters can report logical state writes and logical serialized state bytes. Those
values must not be labelled network bytes or backend-written bytes without provider-native telemetry.

## Running locally

Build and run the deterministic microbenchmark fixture check:

```bash
dotnet run --project benchmarks/Orleans.SearchableStorage.Benchmarks \
  --configuration Release -- --self-test
```

Run selected BenchmarkDotNet cases and keep its full JSON artifacts:

```bash
dotnet run --project benchmarks/Orleans.SearchableStorage.Benchmarks \
  --configuration Release -- --filter "*QueryPlan*Benchmarks*"
```

For an engine/export smoke rather than a timing sample, pass `--smoke`. This selects one dry
in-process BenchmarkDotNet iteration while retaining the production runtime/GC declaration and full
exporters:

```bash
dotnet run --project benchmarks/Orleans.SearchableStorage.Benchmarks \
  --configuration Release -- --smoke --filter "*DerivedIndexBuildBenchmarks*" \
  --artifacts artifacts/bdn-smoke
```

Smoke provenance is deliberately stamped
with `ExecutionMode=BenchmarkDotNetInProcessDryRun` and
`net10-server-smoke;...;nonComparableInProcessDryRun=true`, which the baseline artifact gate rejects.
Its JSON proves fixture, benchmark-engine, diagnoser, and exporter wiring only; it is not latency or
allocation evidence and does not validate the out-of-process generated host. Normal comparable BDN
provenance instead requires `ExecutionMode=BenchmarkDotNet`.

Generate the deterministic ordered-work matrix. `--quick` covers every 4K distribution/scenario/
ordered-policy combination plus focused long-`GrainId` cases; omit it for the 64K matrix too:

```bash
dotnet run --project benchmarks/Orleans.SearchableStorage.Benchmarks \
  --configuration Release -- --query-work-matrix artifacts/query-evidence --quick
```

The resulting `query-work-matrix.json` contains the complete first-page work vector, item and byte
counts, stop reason, safe-frontier encoding, sequence digest, selective-exact driver cardinality, and
for traversal variants the round count, aggregate work vector, maximum-page work vector, terminal
status, and complete-prefix digest. It also labels the selected range execution strategy; the raw
`RangeBucketVisitCount` and `RangeMergeOperationCount` distinguish an admitted ordered merge from a
catalog fallback. Setup independently verifies the full ordered result sequence, not only its count.

Measure retained managed memory after a forced full compacting collection. Each data point is the
median of three fresh worker processes; the input records exist before the baseline, so the delta is
the derived representation retained by the activation:

```bash
dotnet run --project benchmarks/Orleans.SearchableStorage.Benchmarks \
  --configuration Release -- --retained-memory artifacts/query-evidence --quick
```

Omit `--quick` to include 65,536 records. `retained-memory.json` states the measurement semantics and
retains minimum, median, maximum, and median bytes per record. GC-based retained deltas are useful for
same-runtime comparisons but do not include allocator fragmentation, native allocations, or process
working set. Both evidence commands write commit/runtime provenance with
`ExecutionMode=DeterministicEvidence` beside their JSON. `JobIdentity` records the declared runtime/GC
configuration for reproducibility; the execution mode makes explicit that these manual evaluators
and isolated child processes did not execute the BenchmarkDotNet job.

Validate or execute a committed scenario:

```bash
dotnet run --project benchmarks/Orleans.SearchableStorage.LoadDriver \
  --configuration Release -- validate --spec <scenario.json>

dotnet run --project benchmarks/Orleans.SearchableStorage.LoadDriver \
  --configuration Release -- run --spec <scenario.json> \
  --output artifacts/benchmarks --instance-id local-1
```

The embedded Memory scenario is a functional smoke: it validates generation, serialization,
routing, storage, workload execution, correctness checks, provenance, and histogram output. Its
latency is not a baseline. Use `serve` plus an external-topology scenario or the checked-in Crank
configuration for separate silo and load-generator processes.

Crank runs are deliberately not allowed to float on a branch name or a seconds-only run id. Restore
the checked-in Controller `0.2.0-alpha.25473.1` manifest, verify every agent reports that same exact
version, and pass both a full commit SHA and a collision-resistant shared id:

```bash
commit=$(git rev-parse HEAD)
run_id="$(date -u +%Y%m%d%H%M%S)-$(openssl rand -hex 8)"
dotnet tool restore --tool-manifest benchmarks/crank/.config/dotnet-tools.json
(
  cd benchmarks/crank
  dotnet tool run crank -- \
    --config searchable-storage.yml \
    --scenario distributed-smoke --profile local-memory \
    --variable revision="$commit" --variable runId="$run_id"
)
```

The config forwards the same SHA and clean-state declaration to every silo/load process. Each result
must still pass provenance validation; a result from a different SHA or unknown/dirty checkout is not
comparable.

## Execution tiers

- Pull requests validate all specifications, run unit/golden tests, execute the microbenchmark
  self-test, and run small deterministic searchable closed-loop, searchable open-loop, and plain
  closed-loop Memory scenarios. The self-test reflects the built benchmark assembly and requires
  the reviewed set of exactly 22 `[Benchmark]` methods, the exact `[Params]` vectors, the semantic
  fixture results, and the real BenchmarkDotNet config (one .NET 10 server/concurrent-GC job,
  memory diagnostics, p95, full JSON plus GitHub Markdown exporters, and retained benchmark files).
  The benchmark-smoke job also generates the 62-entry quick ordered-work matrix and four-cell
  retained-managed-memory document, validates exact counts, work sums and caps, range-strategy
  coverage, positive isolated-worker samples, deterministic-evidence execution mode, and clean commit
  provenance, then secret-scans and uploads those raw JSON artifacts.
  The job also restores the exactly pinned Crank Controller and expands both two-client coordinates
  with `--debug`; it checks the exact source SHA, environment, arguments, and download paths without
  executing an agent. The benchmark job
  checks out the exact pull-request head it records, proves that tracked inputs are clean, and has no
  timing threshold. The quick evidence is a correctness/artifact-contract gate, not a performance
  baseline.

- Nightly runs are permitted only from trusted `main` on a dedicated benchmark runner. The intended
  searchable matrix is one million records on Memory, PostgreSQL, Redis, and a configured Azure
  Blob-compatible endpoint. The workflow does not infer whether that endpoint is Azurite or Azure, so
  its artifacts must be described only as Azure Blob-compatible unless separate environment metadata
  establishes the service.
  The workflow is explicitly skipped unless repository variables `BENCHMARK_NIGHTLY_ENABLED` and
  `BENCHMARK_NIGHTLY_CLEANUP_GUARD_READY` are both `true`. In-process provider cleanup is
  graceful-only: `SIGKILL`, runner loss, or host failure can
  bypass it, so enabling the job requires a tested out-of-process cleanup fallback or provider TTL.
- The protected weekly/capacity workflow is a disabled integration contract in this foundation, not a
  runnable scale claim. It remains explicitly not ready until a reviewed, tracked manifest under
  `benchmarks/readiness/protected/` allow-lists the exact scenario digest, tier, backend, and
  implementation, and `BENCHMARK_PROTECTED_READINESS_MANIFEST` points to that tracked file. No such
  manifest is committed by this pull request.
- Once enabled by a later reviewed topology change, protected runs require a protected environment,
  an isolated namespace, cost and authorized-duration inputs, authorization recorded before
  provisioning, a concurrency lock, and at least 30 minutes reserved for cleanup. Provision and
  cleanup hooks receive the same canonical lowercase run id used by the driver. Their evidence must
  bind that id, the readiness-manifest and scenario digests, backend, implementation, declared total
  silo count, isolation namespace, and concrete resource ids. Provision evidence must list exactly
  that many unique `silo` resources; cleanup must report every provisioned resource as released and
  verified absent. Result validation binds the canonical run id, scenario digest, and silo count back
  to the authorization and provision evidence.
- One billion records is an opt-in capacity qualification, not routine CI. A representative
  partition or extrapolated fan-out is labelled as such; only a complete representative deployment
  may be called a one-billion-record result.

Searchable-provider results report `(N total, P physical owners, N/P records per active owner,
actual V virtual slots, dataset shape, backend)`. Current snapshots keep activation memory
proportional to records per active owner, while queries contact every distinct owner. Plain-Orleans
point baselines mark `P`, `V`, and journal settings as not applicable because that path has no
searchable partition layer. A large `N` alone is therefore not a meaningful scale result.

### Partition-query evaluation matrix

`QueryPlanEvaluationBenchmarks` keeps one production benchmark identity while expanding it to a
reviewed implementation matrix:

- 4,096 and 65,536 short-id records in one active partition, plus a 4,096-record fixture whose
  canonical grain key is at least 1,024 bytes;
- uniform and correlated hot-key/low-cardinality-range distributions;
- exact, bounded range, selective exact-and-broad-range intersection, broad intersection, broad
  union, and duplicate-heavy union plans;
- the unchanged PR13 materializing whole-plan evaluator;
- one default bounded partition page, a deliberately constrained 4,096-work page, and a page using
  the public hard page/work/partition-byte caps;
- complete partition-local traversal under the hard round ceiling and a separately labelled default
  64-round window.

That is 216 BenchmarkDotNet cases: three datasets by two distributions by six plans by six variants.
The separate full work-evidence matrix omits the materializing variant because its PR13 vector is
already validated in setup, leaving 180 ordered entries. The quick evidence command writes the 60
short-id 4K ordered entries plus two focused long-id entries.

The materializing variant calls the unchanged production
`StoragePartitionQueryEvaluator.Evaluate` path. Ordered variants call the production partition-page
evaluator directly. The traversal variants are intentionally named *partition traversal*: they do
not measure the coordinator, Orleans transport/serialization, response validation/merge, AEAD token
protection, or the public paging API. End-to-end public paging requires the load driver and is not
inferred from these process-local numbers.

Setup independently builds the expected key and `GrainId` sequences, freezes distribution and
selectivity, proves every page is a sorted/distinct safe prefix with a progressing frontier, and
compares every captured traversal to the exact ordered oracle. It reproduces a page and requires an
identical complete work vector. For selective exact-and-broad-range plans it records the exact-posting
driver cardinality, requires page candidate visits not to exceed that posting, and requires complete
hard-ceiling traversal to visit exactly that bounded driver.

Range-merge admission deliberately precharges a safe whole-scope upper bound rather than the selected
range-view cardinality: for `D` range buckets it reserves
`D × (3 + ceil(log2 D))` logical operations after the initial seek. `SortedSet` does not promise an
O(log N) rank operation, so using the narrower selected-view count would itself require unbounded
enumeration. Consequently the uniform 65,536-bucket fixture needs 1,245,184 operations and uses
`CatalogFallback` even for a narrow range under the public maximum work cap of 1,048,576. The full
matrix and focused self-test assert that boundary as at least two posting seeks with zero range-bucket
visits and zero range-merge operations. This is intentional conservative admission, not evidence
that the selected range was materialized or merged.

`DerivedIndexBuildBenchmarks` compares the old materializing indexes with the production additive
`StoragePartitionView` (materializing plus ordered indexes). `IndexMutationBenchmarks` makes the same
comparison for replacement and delete/restore work using real stored-key structure. Both cover a
unique range distribution and a hot/low-cardinality distribution. MemoryDiagnoser reports transient
allocation; the isolated retained-memory document reports the live managed delta. This comparison
includes the hash projection's change from average-`O(1)` dictionary access to `O(log D)` canonical
tree mutation and its checked scope-total update. Setup/cleanup invariants require both hash and range
scope totals to remain exactly equal to the live record count after rebuild, replacement, and
delete/restore.

### Partition-facet evaluation matrix

`FacetPartitionBenchmarks` adds two production-evaluator identities: one bounded candidate metadata
page and one complete traversal of resumable filtered exact-count slices. Their reviewed parameters
cover 4,096 and 65,536 records, 8 and 1,024 distinct values, uniform and 50%-hot skewed value
distributions, and `All` or selective range predicates. That is 16 cases per identity and 32 cases
across the two identities.

Setup builds record/value/count expectations independently from the production indexes. It requires
the candidate page to return the exact canonical values and raw bucket counts, and freezes the exact
work vector. A 16-item candidate page is exactly one value seek, 16 value visits, and 16 result
materializations, with zero group, ownership, record, predicate, index-entry, or count-increment
work. Low-cardinality pages exhaust with unseen bound zero; non-exhausted high-cardinality pages use
an exact checked `PageRawCount`, pinned `TotalRawCount`, and the coordinator's documented
`total - cumulative` remaining bound. This proves nomination reads activation-local bucket scalar
metadata rather than hiding a posting scan or using a count-ranked index.

The count case nominates one exact posting and deliberately admits 16 complete canonical `GrainId`
groups per non-terminal slice. The independent oracle verifies the final filtered count, progress,
round count, and every aggregate work component. The focused 4,096-record/8-value/uniform/selective
self-test freezes 32 seeks/slices, 512 group/ownership/record/predicate probes, 1,024 index-entry
probes, and 256 count increments.

These microbenchmarks do not measure owner fan-out, Orleans transport/serialization, coordinator
ranking, data-version restart, AEAD continuation protection, or end-to-end public facet latency.
They also do not introduce or imply a count-ranked tree or additional retained-memory result. Those
behaviors remain correctness tests or future load-driver evidence; shared-runner timing is not a
facet performance claim.

### Slot-movement matrix

`SlotMovementBenchmarks` adds four production-path identities: rebuild the activation-local slot
catalog, export one bounded record page, apply one import page, and apply one movement-delete page.
Each identity covers 4,096 and 65,536 partition records across uniform, 50%-hot skewed-slot, and
oversize-singleton distributions. That is six cases per identity and 24 movement cases across the
four identities.

The page fixtures use a 16-record ceiling and 64-KiB canonical movement-encoding target. Uniform and
skewed pages must contain the exact stable ordinal prefix without exceeding either multi-record
bound. The oversize fixture places one accepted record larger than the byte target at the selected
cursor and requires a one-record page; it proves the documented
`O(target + largest accepted record)` in-memory page/transfer shape rather than claiming an absolute
wire-byte cap. Setup independently computes target-slot membership and expected record-key order,
canonical encoded bytes, cursor, and exhaustion. It recomputes the production digest to prove
deterministic replay; frozen core-test golden vectors separately protect that digest's protocol
identity across compatible binaries.

Rebuild invokes the same slot-catalog constructor used after durable recovery. Export invokes the
same page builder used by the source RPC. Import and delete invoke the same committed-page application
helpers used after the target/source WAL manifest commit, with fresh views outside the measured
iteration; validation requires exact records plus hash/range/ordered/slot indexes and idempotent page
identity. Benchmark copies of page selection or view mutation are forbidden.

These process-local cases measure neither the layout coordinator nor Orleans serialization/RPC,
physical-provider writes, source freeze duration, a complete move, or concurrent query retry. More
importantly, they do not label current persistence as structurally per-slot: activation recovery
still loads the active whole-partition snapshot and rebuilds all derived indexes/catalogs, while
compaction can still serialize the complete physical partition. A movement capacity result must
report those retained boundaries, slot skew, largest accepted record, canonical page counts/bytes,
actual transport/provider telemetry, and their distinct units separately.

### Interpreting query policy constants

The default 128-item page and 64-round compatibility window are internally aligned with the default
8,192-item legacy ceiling (`128 × 64`). The work matrix reports whether each default-round case is
terminal; non-terminal is a bounded outcome, not silent truncation by the production API, whose
all-results compatibility terminal throws instead.

The configured hard maxima are structural safety/admission caps, not throughput thresholds and not a
promise that a maximum-sized turn is operationally desirable. In particular:

- item and byte maxima are enforced and covered by boundary tests; the short and long-id BDN variants
  expose their allocation/latency consequences;
- work and round maxima bound checked counters and loops; the maximum-policy page is measurement
  evidence for the current implementation, not permission to raise a deployment to that value without
  same-hardware testing;
- coordinator-buffer maxima follow owner-count apportionment and are not justified by this
  single-partition microbenchmark;
- the continuation-token cap follows the canonical envelope size proof and hostile-input tests, not
  query throughput;
- legacy aggregate maxima are all-or-throw compatibility ceilings, not recommended query sizes.

No wall-clock result from a shared runner freezes these constants. A performance-based threshold may
be claimed only with the full JSON and provenance from repeated runs on controlled hardware, a
reviewed comparison table, and an explicitly versioned baseline.

## Comparison rules and remaining work

Comparable runs use the same specs, provider class, topology, hardware class, serializer, benchmark
version, and clean/dirty policy. Searchable and plain Orleans paths use isolated namespaces. A prior
searchable protocol must be measured from its real source commit/build, not simulated inside the
current binary.

The first benchmark pull request intentionally does not establish latency regression thresholds or
claim provider-scale results. Post-run validation accepts either a successful result
or a machine-readable failure manifest, checks phase/operation accounting, provenance and cleanup,
and validates the exact 15-file HDR tuple set, metadata, checksums, and manifest counts for completed
measurements. Upload is suppressed if the selected backend secret or an unredacted high-confidence
credential shape appears in the artifact tree. Follow-up work
in issue #8 includes
dedicated nightly artifacts and a same-workload one-million-record plain/searchable provider baseline,
provider-native bytes and resource telemetry, a persistence-safe bulk seeder, previous-protocol
baselines, fault/chaos histories, 10M/100M/1B qualification, a real coordinator/public paged
distributed workload for #5, distributed #7 live-movement workloads beyond the process-local page
matrix, and protected-topology attestation of the
external silo build/configuration plus an out-of-process cleanup fallback or provider TTL. Thresholds
become enforceable only after a stable same-hardware history exists.

Workload names and ratios are compatible with ideas popularized by YCSB; the implementation here is
independent. BenchmarkDotNet is MIT licensed, Microsoft.Crank.EventSources is an exactly pinned
prerelease dependency, and HdrHistogram is used for mergeable latency recordings. These dependencies
remain confined to non-packable benchmark projects and do not enter the shipping library package.
