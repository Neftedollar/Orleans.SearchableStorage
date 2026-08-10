# Benchmarking

The benchmark suite is a reproducible measurement foundation, not a published scalability claim.
It separates process-local costs from end-to-end Orleans workloads and preserves the raw inputs and
histograms needed to compare runs later. Issue #8 remains open until dedicated environments have
produced reviewed provider, 10-million-record, and capacity artifacts.

## What is measured

`Orleans.SearchableStorage.Benchmarks` uses BenchmarkDotNet for isolated CPU, allocation, and
serialization costs. Its cases invoke the same internal production helpers as the storage grains:

- incremental hash/range index mutation and bounded range lookup;
- expression translation, wire-plan construction, and partition boolean-plan evaluation;
- Orleans serialization of query plans and journal segments;
- bounded journal-segment append using an in-memory `IPersistentState` test double;
- validated journal replay and compaction snapshot construction.

The journal-append case measures the state machine and allocations. It does not measure a physical
provider, network, or durable write. End-to-end provider latency belongs to the load driver.

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
  the reviewed set of exactly 15 `[Benchmark]` methods, the exact `[Params]` vectors, the semantic
  fixture results, and the real BenchmarkDotNet config (one .NET 10 server/concurrent-GC job,
  memory diagnostics, p95, full JSON plus GitHub Markdown exporters, and retained benchmark files).
  The job also restores the exactly pinned Crank Controller and expands both two-client coordinates
  with `--debug`; it checks the exact source SHA, environment, arguments, and download paths without
  executing an agent. The benchmark job
  checks out the exact pull-request head it records, proves that tracked inputs are clean, and has no
  timing threshold.
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
baselines, fault/chaos histories, 10M/100M/1B qualification, #5 bounded/paged query work and
representation comparisons, #7 live-movement workloads, and protected-topology attestation of the
external silo build/configuration plus an out-of-process cleanup fallback or provider TTL. Thresholds
become enforceable only after a stable same-hardware history exists.

Workload names and ratios are compatible with ideas popularized by YCSB; the implementation here is
independent. BenchmarkDotNet is MIT licensed, Microsoft.Crank.EventSources is an exactly pinned
prerelease dependency, and HdrHistogram is used for mergeable latency recordings. These dependencies
remain confined to non-packable benchmark projects and do not enter the shipping library package.
