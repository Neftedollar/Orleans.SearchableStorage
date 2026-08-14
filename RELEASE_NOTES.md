# Release notes

## 1.0.0-rc.2 — qualification candidate

> [!CAUTION]
> **NOT PRODUCTION-QUALIFIED. DO NOT USE THIS PACKAGE IN PRODUCTION.**
> This prerelease exists so the exact package intended for 1.0 qualification can be exercised by
> applications and controlled benchmark infrastructure. The required scale and distributed
> qualification evidence has not been completed.

This is a NuGet release candidate for the 1.0 contract. It packages the integrated Orleans
`IGrainStorage` provider, the separate payload-free index writer, bounded secondary-index query API,
facets, managed schema lifecycle, and live slot-movement controls already described by the
repository documentation. Application state remains authoritative: inside the searchable namespace
for integrated mode and in the caller's external store for index-only mode. Index-only calls are
last-arrival-wins and do not provide cross-store transactions, ordering, outbox, or deduplication.
This package is not a database and does not claim arbitrary LINQ, scan, join, or snapshot-query
semantics.

### Evidence status

- Formal qualification targets the exact unsigned `.nupkg`, canonical-manifest digest, and
  repository commit frozen before publication. Qualification must consume those exact package
  bytes from an isolated package-only source; a project reference or rebuild is invalid.
- The target must be built with the exact .NET 10.0.303 SDK, deterministic build properties, and
  branch-independent repository URL/commit provenance. Patch roll-forward and nuspec branch
  metadata are rejected because either can make package content depend on the build context.
- No version-1 or version-2 target record was admitted to qualification. Any rc.2 package produced
  before this exact build-context pin is not a qualification target, even when its version and
  repository commit match.
- This correction changes package bytes and the qualification target-record schema only. It does
  not change the public API or durable, WAL, snapshot, movement, wire, or continuation semantics.
- The build, unit and integration suites, Memory contract, container-backed PostgreSQL and Redis
  contracts, Azurite-backed Azure Blob contract, package provenance checks, and package-only
  consumer smoke are release gates.
- Azurite is a contract-test dependency, not evidence for the real Azure Blob service.
- Controlled 1,000,000-record provider runs and a full, non-modelled, external/distributed
  10,000,000+ record run have not yet qualified this release. No smoke, modelled, or synthetic
  result substitutes for those runs.
- Performance envelopes, production SLOs, and provider-specific operational limits must not be
  inferred from this prerelease version.

### Compatibility and rollout

- The public API and source-compatibility baseline are frozen for this qualification candidate.
  After the `oss-package-target/v3` record is frozen, any library implementation, dependency, SDK,
  generated binary, public or durable contract, observable behavior, packaged documentation/content,
  or repository provenance change requires
  `1.0.0-rc.3` (or a later candidate) and invalidates qualification evidence collected for this
  package. Stable SemVer compatibility obligations begin with the final `1.0.0` package. There is no
  previously published package or continuation-token compatibility promise to migrate from.
- Package version numbers are independent of persistence, wire, schema, continuation, and movement
  protocol versions. Those versions remain frozen in `eng/compatibility-manifest.json` and are not
  changed merely for this package.
- Managed-schema adoption is provider-wide and one-way. Follow
  `docs/index-schema-lifecycle.md`, quiesce searchable traffic, and use a homogeneous deployment.
- Index-only namespaces use durable layout and persistence format 6 as a downgrade fence. They
  cannot rebuild an incompatible active fingerprint without retained payloads; create a new provider
  name and replay the authoritative external corpus as documented in `docs/index-only-mode.md`.
- Page and distinct-facet continuations are bound to their schema generation and routing epoch.
  Restart traversal after an incompatible schema or layout transition; never treat a prerelease
  continuation as an upgrade-stable application record.
- All participants which create or resume continuations need the same provider-scoped key id and
  32-byte key material. Keep it outside source control and persisted provider state.
- Capacity ceilings are safety boundaries rather than latency or memory guarantees. Review
  `docs/storage-capacity-limits.md`, `docs/bounded-query-contract.md`, and `docs/operations.md`
  before qualification or integration work.

### Produce the reviewed local artifact

From a clean checkout of the reviewed commit with the pinned SDK installed:

```bash
OSS_RELEASE_OUTPUT_DIRECTORY=artifacts/release-candidate \
  bash eng/release-dry-run.sh
```

The command packs twice, compares canonical package contents, validates the exact version,
metadata, warning surfaces, and commit provenance, and builds a standalone package-only consumer.
Only after every gate passes does it retain
`Orleans.SearchableStorage.1.0.0-rc.2.nupkg` and `package.canonical.json` in the requested directory.
Record the SHA-256 of both files and preserve them as immutable qualification-repository assets. It
does not publish anything to NuGet.org. After qualification, the Trusted Publishing workflow must
upload that exact unsigned `.nupkg`, verify the repository-signed download against the qualified
canonical manifest, and record the signed package SHA-256.
