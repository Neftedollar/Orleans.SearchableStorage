# Prerelease handoff and qualification boundary

This document defines the handoff between an unsigned non-production qualification target, an
independent human usability exercise, scale qualification, and later registry publication. These
are separate gates. Freezing a package, completing the worksheet below, producing a large benchmark
artifact, or publishing the prerelease does not by itself qualify the package for production.

The actual exercise by a maintainer who is unfamiliar with the project is intentionally still open.
This checked-in kit makes that exercise reproducible; it is not evidence that a person completed it.

## Gate ownership

| Gate | Target | Required output | What it does not prove |
| --- | --- | --- | --- |
| Qualification package | One repository commit, one exact unsigned `.nupkg`, and its canonical manifest | Version-2 package identity record, package gates, and release notes with a non-production warning | Production readiness, provider scale, repository signing, or independent usability |
| Clean-room human handoff | The same exact package and repository documentation | Completed worksheet, timings, commands, observations, and issue links from an unfamiliar .NET/Orleans maintainer | Throughput, capacity, or provider qualification |
| Scale qualification | The exact unsigned package bytes named in a frozen external qualification release | Raw evidence, producer attestations, independent verification output, and a measured envelope | Database semantics, a later package rebuild, or workloads outside the frozen profile |
| Registry publication | The exact qualified unsigned package and its NuGet.org repository-signed form | Trusted Publishing run, signature verification, canonical-equivalence proof, signed SHA-256, and package-only consumer result | Stable-1.0 readiness or a wider operating envelope |

The shipping package exposes an integrated Orleans `IGrainStorage` mode and a separate payload-free
index-only writer, both with bounded `GrainId` discovery. The qualification system must not broaden
either ownership model into a general database or cross-store transaction claim.

## Package identity record

Create the identity record immediately after the clean release dry run and before any qualification
work. Preserve the exact unsigned `.nupkg` and canonical manifest as immutable, content-addressed
assets in the public qualification boundary. Do not qualify a project reference, extracted DLL, or
later rebuild. Store this record in the clean-room evidence and qualification repository:

```json
{
  "schema": "oss-package-target/v2",
  "packageId": "Orleans.SearchableStorage",
  "packageVersion": "REPLACE_WITH_EXACT_PRERELEASE_VERSION",
  "packageKind": "unsigned-qualification-target",
  "artifactUrl": "REPLACE_WITH_RELEASE_ASSET_URL_BOUND_BY_SHA256",
  "artifactFileName": "REPLACE_WITH_EXACT_FILE_NAME.nupkg",
  "nupkgSha256": "REPLACE_WITH_64_LOWERCASE_HEX_CHARACTERS",
  "canonicalManifestUrl": "REPLACE_WITH_RELEASE_ASSET_URL_BOUND_BY_SHA256",
  "canonicalManifestSha256": "REPLACE_WITH_64_LOWERCASE_HEX_CHARACTERS",
  "repositoryUrl": "https://github.com/Neftedollar/Orleans.SearchableStorage",
  "repositoryCommit": "REPLACE_WITH_40_LOWERCASE_HEX_CHARACTERS",
  "packageValidatorPassed": true,
  "packageOnlyConsumerPassed": true,
  "recordedAtUtc": "REPLACE_WITH_RFC3339_UTC_TIMESTAMP"
}
```

Obtain the values from the artifacts, not from release prose. Hash both files with `sha256sum`, read
repository provenance from the nuspec, and run the ordinary strict package validator and
package-only consumer in [the release process](release.md#local-dry-run). A matching version string
or a canonical-only match is not a substitute for the exact unsigned bytes during qualification.

`oss-package-target/v1` described the former publish-first process and was never used for a
published or qualified release. Version 2 replaces it without a migration target. Any code,
dependency, generated binary, persisted-format, wire-contract, package-content, documentation, or
source-provenance change creates a new qualification target.

The required scale qualification emits this separately hashed verdict only after independent
verification succeeds. This verdict does not claim that the clean-room human handoff has happened;
that remains a separate stable-1.0 gate.

```json
{
  "schema": "oss-qualification-verdict/v1",
  "outcome": "pass",
  "packageId": "Orleans.SearchableStorage",
  "packageVersion": "REPLACE_WITH_EXACT_PRERELEASE_VERSION",
  "qualifiedNupkgSha256": "REPLACE_WITH_64_LOWERCASE_HEX_CHARACTERS",
  "qualifiedCanonicalManifestSha256": "REPLACE_WITH_64_LOWERCASE_HEX_CHARACTERS",
  "libraryRepositoryCommit": "REPLACE_WITH_40_LOWERCASE_HEX_CHARACTERS",
  "qualificationRepository": "https://github.com/Neftedollar/Orleans.SearchableStorage.Qualification",
  "qualificationRepositoryCommit": "REPLACE_WITH_40_LOWERCASE_HEX_CHARACTERS",
  "targetRecordUrl": "REPLACE_WITH_IMMUTABLE_PUBLIC_TARGET_RECORD_URL",
  "targetRecordSha256": "REPLACE_WITH_64_LOWERCASE_HEX_CHARACTERS",
  "evidenceReleaseUrl": "REPLACE_WITH_IMMUTABLE_PUBLIC_EVIDENCE_RELEASE_URL",
  "verifiedEvidenceManifestUrl": "REPLACE_WITH_IMMUTABLE_PUBLIC_VERIFIED_EVIDENCE_MANIFEST_URL",
  "verifiedEvidenceManifestSha256": "REPLACE_WITH_64_LOWERCASE_HEX_CHARACTERS",
  "recordedAtUtc": "REPLACE_WITH_RFC3339_UTC_TIMESTAMP"
}
```

The release tooling validates the exact target, verdict, and publication shapes with
`eng/validate-release-record.py`; its focused tests fail closed on non-passing, malformed, misbound,
or untrusted-repository records. `verifiedEvidenceManifestUrl` names the exact
`verified-evidence.json` asset in `evidenceReleaseUrl`, and its SHA-256 covers the independently
verified evidence manifest rather than an unspecified archive. The qualification repository must
freeze the corresponding producer schemas and independent-verifier binding before the first real
run.

After a passing verdict, Trusted Publishing uploads the exact unsigned target. NuGet.org adds its
repository signature as the only permitted semantic-container difference. Record the delivered
identity separately after the workflow verifies the verdict, signature, and canonical equivalence:

```json
{
  "schema": "oss-package-publication/v1",
  "packageId": "Orleans.SearchableStorage",
  "packageVersion": "REPLACE_WITH_EXACT_PRERELEASE_VERSION",
  "qualifiedNupkgSha256": "REPLACE_WITH_64_LOWERCASE_HEX_CHARACTERS",
  "qualifiedCanonicalManifestSha256": "REPLACE_WITH_64_LOWERCASE_HEX_CHARACTERS",
  "qualificationVerdictUrl": "REPLACE_WITH_RELEASE_ASSET_URL_BOUND_BY_SHA256",
  "qualificationVerdictSha256": "REPLACE_WITH_64_LOWERCASE_HEX_CHARACTERS",
  "qualificationVerdictVerified": true,
  "publishedNupkgSha256": "REPLACE_WITH_64_LOWERCASE_HEX_CHARACTERS",
  "repositoryCommit": "REPLACE_WITH_40_LOWERCASE_HEX_CHARACTERS",
  "repositorySignatureVerified": true,
  "canonicalPayloadEquivalent": true,
  "packageOnlyConsumerPassed": true,
  "recordedAtUtc": "REPLACE_WITH_RFC3339_UTC_TIMESTAMP"
}
```

## Clean-room human protocol

### Participant and environment

Choose a .NET/Orleans maintainer who has not implemented or reviewed the feature stack and has not
read private development chat or agent transcripts. A release coordinator may provide only:

- the public repository URL and commit;
- the immutable qualification-package and canonical-manifest URLs plus their identity record;
- this worksheet;
- ordinary infrastructure credentials through a secret manager.

Do not provide a private walkthrough, unpublished commands, a prepared source patch, or answers which
bypass a documentation problem. A blocked participant should record the block before asking a
question. The coordinator may clarify infrastructure access, but product guidance becomes a finding.

Use a new machine, VM, or container account with an empty NuGet cache and no repository-local build
outputs. Record OS, architecture, SDK version, Orleans version, physical provider and version, and
whether each process is local or remote. Keep connection strings and continuation-key material out
of the evidence.

### Worksheet

Record start/end time, commands, documentation pages consulted, result, and any surprise for every
task. Attach concise logs with secrets redacted. A task is `PASS` only when its observable checks
succeed without an unpublished workaround.

| ID | Task | Observable acceptance |
| --- | --- | --- |
| H1 | Restore a new application against only the exact qualification package. Configure one durable Orleans physical provider and the searchable provider. | Restore/build uses `packages.lock.json`, locked mode, an empty cache, and source mapping to the isolated package-only source containing the verified target bytes; it contains no project reference to this repository and records the exact package identity above. |
| H2 | Declare one state with a Hash index and one Range index, register it on the silo and query client, and complete initial schema adoption. | The schema reports the declared fingerprint as `Active`; no traffic is admitted through an unregistered or inactive state. |
| H3 | Write a deterministic set of at least 100 application grain states, update some records, and clear at least one through normal `IPersistentState<T>` use. | Point reads return the expected state and ETag behavior; the application grain remains authoritative. |
| H4 | Execute exact and range predicates, traverse at least two public pages, and hydrate one bounded page through application grains. | Pages are sorted and distinct, a null token alone marks completion, hydration returns the matching authoritative states, and no API returns `TState` as a search result. |
| H5 | Stop every silo without deleting physical storage, start a fresh process, and repeat point reads and queries. | Recovery returns the same committed records/index results and does not fall back to empty state. Record activation/recovery telemetry. |
| H6 | In integrated mode, change one index declaration and application schema version under the documented quiescence procedure, then run and resume the managed rebuild. | The new fingerprint becomes `Active`, original serialized state/ETags remain unchanged, and an old page token is rejected rather than silently resumed. |
| H7 | Enable movement under homogeneous quiescence, plan one populated virtual-slot move to a different owner, advance or execute it through cleanup, and query before/after. | The layout epoch/owner changes as documented, no active move remains, point/query results are preserved, and progress can be explained from public status APIs. |
| H8 | From the public docs alone, trace the implementation path for write, query page, activation recovery, schema rebuild, and slot movement. | The participant identifies the public entry point, durable/protocol owner, commit or visibility boundary, recovery/resume behavior, and primary tests for all five paths. |
| H9 | Perform backup/restore or write the exact provider-specific procedure when disposable infrastructure cannot safely exercise it. | The procedure includes layout, manifests, journal/snapshot slots, schema controls, physical metadata/ETags, quiescence, homogeneous restart, and post-restore verification. A written-only result is explicitly marked `NOT EXERCISED`. |
| H10 | Attempt one documented unsupported query or unsafe rollout action in a disposable namespace. | The system or procedure fails closed; the participant can locate the documented reason and safe recovery. |
| H11 | Under a different provider key, keep payloads in an ordinary application store and maintain only their projection through `AddSearchableIndex` and `ISearchableStorageIndexWriter`. Exercise replacement, an exact retry, missing removal, query, and external hydration. | No index-only durable record retains the application payload; the final query reflects last arrival, missing removal succeeds, and the participant records that ordering, outbox/reconciliation, cross-store consistency, and hydration are application responsibilities. |

Suggested public starting points are the [API sample](../samples/Orleans.SearchableStorage.ApiSample/README.md),
[operations index](operations.md), [maintainer guide](maintainers.md),
[schema runbook](index-schema-lifecycle.md), [index-only guide](index-only-mode.md), and
[movement runbook](live-movement.md). Listing them is
part of the public handoff and does not replace the participant's own navigation notes.

### Evidence template

Copy this template to an issue or immutable release artifact. Do not edit this source document to
pretend that an exercise has happened.

```text
Exercise id:
Package id/version:
Qualification-target nupkg SHA-256:
Canonical-manifest SHA-256:
Repository commit:
Participant name or stable public identifier:
Participant prior project exposure:
Environment and physical provider:
Start/end UTC:

H1: PASS | FAIL | BLOCKED
  commands/docs/evidence:
  elapsed time:
  observations:
H2: PASS | FAIL | BLOCKED
  commands/docs/evidence:
  elapsed time:
  observations:
H3: PASS | FAIL | BLOCKED
  commands/docs/evidence:
  elapsed time:
  observations:
H4: PASS | FAIL | BLOCKED
  commands/docs/evidence:
  elapsed time:
  observations:
H5: PASS | FAIL | BLOCKED
  commands/docs/evidence:
  elapsed time:
  observations:
H6: PASS | FAIL | BLOCKED
  commands/docs/evidence:
  elapsed time:
  observations:
H7: PASS | FAIL | BLOCKED
  commands/docs/evidence:
  elapsed time:
  observations:
H8: PASS | FAIL | BLOCKED
  commands/docs/evidence:
  elapsed time:
  observations:
H9: PASS | FAIL | BLOCKED | NOT EXERCISED
  commands/docs/evidence:
  elapsed time:
  observations:
H10: PASS | FAIL | BLOCKED
  commands/docs/evidence:
  elapsed time:
  observations:
H11: PASS | FAIL | BLOCKED
  commands/docs/evidence:
  elapsed time:
  observations:

Unpublished help requested:
Documentation defects:
API/usability defects:
Operational safety defects:
Issue links:
Participant conclusion (not a scale or production certification):
Coordinator verification:
```

For the stable 1.0 release, every `FAIL` or `BLOCKED` item needs a linked disposition and rerun.
`H9: NOT EXERCISED` is acceptable only when a reviewed provider-specific procedure and an existing
automated restore test cover the same boundary. A prerelease may publish while this entire human
gate remains open, provided its release notes say that it is not for production use.

## Separate qualification repository

Qualification should be independently consumable and reviewable without granting its runner source
access to this repository checkout. Create the public qualification repository before freezing the
target package so its first immutable release can retain the exact unsigned `.nupkg`, canonical
manifest, and identity record. Its runner may be distributed as source and optionally as a
separately named tool package, but it must never share the `Orleans.SearchableStorage` package id or
imply that the library package contains certification.

The repository should contain these reviewed boundaries:

```text
lock/target-package.json          exact unsigned package and canonical-manifest identity
lock/package-source/              exact target .nupkg, or a verified immutable download snapshot
profiles/frozen/                 thresholds and workload profiles frozen before execution
schemas/                         machine-readable input, raw evidence, and result schemas
src/                             package-only runner and typed evidence producers
provision/                       reviewed isolation/readiness/cleanup contracts
evidence/raw/<run-id>/           immutable producer output and attestations
evidence/verified/<run-id>/      independently recomputed summaries and verdict
SECURITY.md                      redaction, credential, and disclosure procedure
README.md                        reproduction and verifier commands
```

Before its first release, enable
[GitHub Release immutability](https://docs.github.com/en/code-security/concepts/supply-chain-security/immutable-releases)
for the qualification repository. Create each target, evidence, or publication release as a draft,
attach every asset, and only then publish it; immutability applies after publication and protects
both its tag and assets. The independent verifier must validate the generated release attestation
and every local input asset:

```bash
gh release verify RELEASE_TAG --format json \
  --repo Neftedollar/Orleans.SearchableStorage.Qualification
gh release verify-asset RELEASE_TAG PATH_TO_LOCAL_ASSET \
  --format json \
  --repo Neftedollar/Orleans.SearchableStorage.Qualification
```

Record those commands' machine-readable output with the evidence. A mutable draft, ordinary release
without the immutability policy, Actions artifact, or URL without the frozen digest is not the
qualification authority.

Large histograms and provider traces need not become Git objects. A tagged release may keep them as
immutable, content-addressed release assets or in a public object store, while the repository tracks
their sizes, SHA-256 digests, media types, run ids, and redundant download locations in a signed
manifest. A summary without those retrievable raw objects is not independently verifiable evidence.

The qualification runner must download and verify the immutable target artifact before provisioning
anything, copy the exact `.nupkg` into an isolated package-only source, and restore a locked
`Orleans.SearchableStorage` dependency from that source. A project reference, extracted or locally
rebuilt DLL, floating version range, network fallback for the library package, or package with a
different SHA-256 invalidates the run. Runner commit, profile-catalog commit, profile digest,
target-package digest, canonical-manifest digest, and every producer identity must appear in each
attestation.

## External verification flow

1. From one clean immutable library commit, run the release dry run and place the exact unsigned
   `.nupkg`, canonical manifest, and version-2 identity record in an immutable public qualification-
   repository release. Verify both SHA-256 values, package provenance, strict package validation,
   and package-only consumption before accepting the target.
2. In the qualification repository, implement typed raw producers and an independent verifier, then
   freeze the reference profiles and thresholds on a signed/tagged commit **before** running them.
   The current in-repository evidence-v2 fixtures remain deliberately unqualified.
3. Execute controlled one-million-record runs for Memory, PostgreSQL, Redis, and real Azure Blob
   Storage. Memory has no durable-provider byte claim; Azure Blob emulation is contract evidence,
   not a real-Azure qualification. Each durable provider supplies provider-native operations/bytes,
   population audit, lifecycle/resource telemetry, raw HDR histograms, and producer attestations.
4. Execute at least one complete, non-modelled ten-million-or-more-record external topology with
   multiple silos and load clients. Record total records, physical owners, records per owner, actual
   virtual slots, hardware, runtime/GC, provider configuration, seed, run id, package identity, and
   cleanup proof. Exercise public paging/facets/hydration plus recovery, schema rebuild/resume,
   compaction, and movement/fan-out cases selected by the frozen profile.
5. Publish raw artifacts even for a failed or incomplete run when redaction and cleanup succeed.
   Never turn a partial population, model, extrapolation, embedded smoke, or synthetic fixture into a
   scale claim.
6. From a clean checkout, an independent verifier downloads the exact target package and evidence release,
   validates every digest/attestation/schema, recomputes histogram summaries and population totals,
   evaluates only the pre-frozen thresholds, and emits the machine-readable verdict.
7. Publish the verifier output and an archive SHA-256 in a tagged qualification-repository release.
   Do not copy only a summary table into this repository and discard the raw evidence.
8. Only after the target receives the required qualification verdict, configure the protected
   `release` environment and NuGet Trusted Publishing policy. Set its qualification identity
   variables only after the qualification-side verifier or release coordinator has captured the
   immutable-release and asset-attestation JSON above. Tag the same current `main` commit, and let
   the publication workflow revalidate the exact URLs, records, and SHA-256 identities before it
   uploads the exact qualified unsigned bytes; it deliberately does not require a cross-repository
   GitHub token. Download the repository-signed package, verify its signature and canonical
   equivalence, rerun package-only consumption, record the signed SHA-256, and link the qualification
   evidence from the library release.

The workflow retains `package-publication.json` as a 90-day Actions handoff artifact. Before
announcing the prerelease, copy that exact record into a new immutable qualification-repository
publication release, verify its asset attestation, and link it from the library release. The
expiring Actions copy is not the durable publication authority.

The detailed measurement semantics and current deliberately fail-closed validator are documented in
[benchmarking](benchmarks.md). Scale results decide whether storage-path optimization or owner
pruning is needed; those features are not prerequisites unless the frozen measured envelope fails.

## Promotion rule

A qualification verdict applies only to the exact unsigned target package, its canonical manifest,
the frozen profile, provider class, and stated topology/envelope. It transfers to the NuGet.org
repository-signed form only when the exact qualified unsigned bytes were uploaded and the signed-
package validator proves canonical payload equivalence; the publication record must name both
SHA-256 identities. It does not automatically transfer to a later `1.0.0` package whose canonical
payload differs from the qualified prerelease. Promotion must publish a machine-readable
equivalence statement for the stable package, rerun all package/signature/consumer gates, and
explain every byte or semantic difference. Any implementation, dependency, format, protocol, or
runtime-target change requires the affected qualification runs again.

Until the clean-room exercise and qualification verdict are both complete, the prerelease and its
documentation must retain an explicit “not for production use” warning.
