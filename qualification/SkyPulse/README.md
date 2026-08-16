# Orleans.SearchableStorage.Qualification

This repository is the independent qualification application for
`Orleans.SearchableStorage`. Its first workload, **SkyPulse**, builds a searchable metadata-only
projection of public AT Protocol accounts without retaining posts, profile text, handles, media,
or raw record bodies.

The repository is still a local implementation workspace. No corpus, qualification profile,
threshold, verdict, release, or NuGet publication is frozen yet.

## What SkyPulse indexes

One searchable entity represents one admitted AT Protocol account. The public DID is converted to
an exact SHA-256 account key before it reaches the index. A frozen account-key file defines a
maximum corpus; admission never depends on event arrival order. A 1,000,000-account run is an exact
prefix of the same larger frozen parent used for 10,000,000 and later profiles.

The payload-free Orleans index contains exactly 17 scalar `long` Range fields:

| Fields | Meaning |
| --- | --- |
| `LastActivityMinuteUtc` | UTC Unix minute of the latest observed admitted-account mutation. |
| `CreatedRecordCount1/7/30Days` | Creates in the trailing 1, 7, and 30-day windows. |
| `UpdatedRecordCount1/7/30Days` | Updates in the trailing 1, 7, and 30-day windows. |
| `DeletedRecordCount1/7/30Days` | Deletes in the trailing 1, 7, and 30-day windows. |
| `CurrentPostCount` | Current `app.bsky.feed.post` records, including replies and quote posts. |
| `CurrentFollowingCount` | Current distinct outbound follow relationships. |
| `CurrentFollowerCount` | Current distinct inbound follows whose source account is in the selected corpus. |
| `PostCreates1/7/30Days` | Feed-post creates in the trailing 1, 7, and 30-day windows. |
| `ReceivedEngagementCreates30Days` | Likes, reposts, and direct replies created by selected accounts and targeting this account's posts. |

Inbound follower and received-engagement values count only relationships whose source actor is in
the selected K-account corpus. They are **partial** until every selected repository has completed
bootstrap and reconciliation; after that they are exact only within that selected-source scope,
never global Bluesky totals. Partial values must not be presented as exact qualification results.

## Data flow and safety boundary

```text
AT Protocol Relay
       |
       v
patched TAP -- sanitized metadata only --> PostgreSQL transaction
                                           |  reducer state
                                           |  minute buckets
                                           |  desired projection + ordered outbox
                                           v
single co-located dispatcher --> Orleans Memory index --> bounded query UI
                                           |
                                           +--> external hydration + current-page SSE updates
```

The TAP overlay removes record bodies before either of TAP's durable buffers, reconciles deletes,
and emits an explicit `repo_sync` barrier. PostgreSQL is the authoritative reducer, checkpoint,
relationship, rolling-window, desired-projection, hydration, and outbox store. The Orleans index
retains only the 17 values plus each grain identifier.

The initial topology deliberately uses one process containing the Memory silo and the only index
dispatcher. A blind index write has no fencing version. Therefore any ambiguous external write
outcome must terminate that entire process; a replacement process creates an empty Memory-index
incarnation and fully rebuilds it from PostgreSQL before readiness. Ordinary retry inside the same
incarnation is not a correctness boundary. A future multi-silo topology needs a stronger fencing
or namespace-rotation design.

The checked-in configuration explicitly selects `LocalFunctional`; it is not qualification mode.
Selecting `SkyPulse:Mode=Durable` fails closed unless
`ConnectionStrings:SkyPulsePostgreSql` and the following `SkyPulse:Durable` values are supplied:
`ProfileId`, `ProfileVersion`, `CorpusCap`, `ProfilePrefixSha256`, `SourceInstanceId`,
`CorpusManifestPath`, `TapEndpoint`, the secret-backed `TapAdminPassword`, the absolute
`RoutingManifestPath`, and true values for `ExclusiveRepositoryAdministrationConfirmed`,
`FullNetworkModeDisabledConfirmed`, and `AutomaticRepositoryDiscoveryDisabledConfirmed`.
Optional online growth targets are declared in `GrowthProfiles`; configuring any target also
requires the secret-backed `CorpusGrowthAdminToken`.
`ProfilePrefixSha256` is the selected profile's exact `prefixSha256` from
`corpus.manifest.json`, not the hash of the larger parent file. Dispatcher and recalculation batch
sizes, leases, and delays are optional bounded overrides. While PostgreSQL validation, manifest
binding, the complete Memory-index rebuild, or startup rolling-window catch-up is in progress,
`/health` and every `/api` route return HTTP 503. Durable ingestion additionally waits for the
projection runtime, verifies the corpus, inserts the exact selected account baseline into
PostgreSQL, and requires independent proof that the same private repository set was provisioned
in TAP. A configured private provisioner runs only after `/channel` is open and the receive/ACK
loop has started; API readiness remains closed until its exact-set proof completes.

## Current implementation status

Implemented and locally tested:

- exact .NET SDK `10.0.303` with roll-forward disabled;
- package-only consumption of `Orleans.SearchableStorage` `1.0.0-rc.2`;
- fixed 17-field projection and both in-memory and verified file-backed frozen-prefix admission
  primitives, so a parent corpus can grow beyond 10 million keys without a giant managed array;
- deterministic bounded-memory corpus freezing from a strict sanitized lifecycle-observation
  journal, with exact-prefix profiles, canonical hashes, deep verification, and a no-DID public
  artifact gate;
- strict metadata-only TAP parser and reference reducer;
- authenticated bounded TAP WebSocket transport plus a sequential durable ingestion worker which
  reserves the exact received digest, drives bounded ordinary/lifecycle planning, and acknowledges
  only explicit durable success; see [docs/tap-consumer.md](docs/tap-consumer.md);
- exact private route verification and bounded idempotent `/repos/add` provisioning, with
  authenticated cardinality proof and receive/ACK draining active during backfill; see
  [docs/private-tap-repository-provisioning.md](docs/private-tap-repository-provisioning.md);
- resumable privacy-preserving account acquisition from pinned `listRepos` and Jetstream contracts,
  plus exact-prefix private DID route export;
- patched, pinned TAP source overlay;
- real package-backed index-only writer, bounded paging, batch hydration, and simple SSE UI;
- PostgreSQL schema, delivery reservation, durable typed state, reconciliation dependencies,
  ordered projection outbox, runtime identity manifest, and bounded retention contracts;
- fixed-size ordinary-record planning plus restartable, bounded inactive-account and `repo_sync`
  lifecycle orchestration; source acknowledgement remains forbidden until the final durable page;
- explicit local-functional and durable Web modes; durable startup validates and binds PostgreSQL,
  rebuilds the Memory index, catches up due rolling windows before readiness, then advances both
  rolling recalculations and the outbox in bounded batches.
- restartable monotonic corpus-cap growth from one reviewed frozen prefix to another while the
  existing query surface remains available; see
  [docs/runtime-corpus-growth.md](docs/runtime-corpus-growth.md).

Still required before a real run:

- run the real PostgreSQL integration and full source-to-query recovery tests;
- execute the acquisition adapter against the selected deployments, freeze the real corpus, and
  approve its operational privacy/retention policy;
- create the immutable `oss-package-target/v3` release record and freeze thresholds.

## Local package boundary

`NuGet.Config` maps only `Orleans.SearchableStorage` to the ignored
`lock/package-source/` feed. No project reference, extracted DLL, or fallback package source is
allowed for the library under test. See [lock/README.md](lock/README.md) for the currently verified
candidate identity; that note is intentionally not a formal qualification target.

## Initial deployment shape

The first calibration target is two cost-optimized 32-GB machines: one co-located application,
Memory index, dispatcher, and TAP process; one PostgreSQL machine. This is a capacity calibration,
not high availability. Corpus caps stop admission above the frozen prefix, so the same software
and profile format can move between exact prefixes without changing entity semantics. The current
Memory-index implementation scales
only by giving its single co-located application process more memory; adding index machines is a
future topology that first requires the fencing or namespace-rotation work described above. The
account cap does not cap the much larger TAP/PostgreSQL current-record graph; the run therefore
starts at 10K and 100K prefixes before 1M.
Reviewed prefix increases can be requested at runtime without replacing the older prefix; resource
headroom must still be provisioned before the request.
See [docs/capacity-plan.md](docs/capacity-plan.md) for the measurement ladder and the deliberately
non-promissory memory estimate.

## Run the empty local UI

After restoring/building with the exact local package feed, start the explicitly non-durable mode:

```bash
ASPNETCORE_URLS=http://127.0.0.1:5080 \
dotnet run --project src/Orleans.SearchableStorage.Qualification.SkyPulse.Web \
  -c Release --no-build
```

Open `http://127.0.0.1:5080`. This mode starts a real package-backed Memory index and the bounded
query/SSE UI, but deliberately creates no sample records. It is a local functional check, not a
durability or qualification run.

## Verification

Build and unit/contract tests use the exact SDK:

```bash
dotnet restore Orleans.SearchableStorage.Qualification.slnx --locked-mode
dotnet build Orleans.SearchableStorage.Qualification.slnx -c Release --no-restore
dotnet test Orleans.SearchableStorage.Qualification.slnx -c Release --no-build --no-restore
```

The PostgreSQL suite is opt-in locally and must not be counted when skipped. See
[docs/postgresql-integration-tests.md](docs/postgresql-integration-tests.md). The latest local,
non-qualification checkpoint is recorded in
[docs/local-verification-2026-08-15.md](docs/local-verification-2026-08-15.md).

## Deployment handoff

The conditional two-host calibration bundle is in
[deploy/](deploy/README.md). A local operator should start with the Russian
[handoff checklist](deploy/hetzner-cx53/HANDOFF.ru.md) and the detailed
[runbook](deploy/hetzner-cx53/README.md).

It is intentionally fail-closed: the reviewed OpenTofu provider lock, exact
external image digests, encrypted-at-rest storage, encrypted off-host recovery,
real corpus/routes, secrets, target-host checks, PostgreSQL integration suite
and restore drill remain local release gates. It is not a one-command
production deploy and does not claim qualification evidence.
