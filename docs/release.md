# Release process

This project is pre-1.0. A package version is not a claim that the 1.0 contract is complete. Releases
must nevertheless be reproducible in meaning, auditable, locally consumable, and explicit about every
persisted/protocol compatibility decision.

## Prepare

1. Ensure the tree is clean and identify the exact commit being released.
2. Review `eng/compatibility-manifest.json`. Every changed value needs a migration or explicit
   rejection plan, updated executable binding/golden evidence, and release-note treatment. The
   manifest holds protocol/format versions, compact codec and wire-enum maps, frozen digests, and
   names the executable Orleans wire-contract tests; the large field-ID tables stay in those
   focused tests instead of being duplicated in JSON, while compact enum maps remain visible here.
3. Review all three API baselines. `PublicAPI.Shipped.txt` and `PublicAPI.Unshipped.txt` are consumed
   by the standard `Microsoft.CodeAnalysis.PublicApiAnalyzers` compiler analyzer. They freeze the C#
   symbol surface, nullability, defaults, overloads, and constants. `SourceConstraints.Shipped.txt`
   and `SourceConstraints.Unshipped.txt` supplement that standard baseline with every public or
   protected generic parameter, including an explicit `<none>` entry; this makes adding, removing,
   or changing `notnull`, `class`, `struct`, `unmanaged`, base/interface, constructor, and
   `allows ref struct` constraints fail closed. `eng/public-api.txt` independently records the
   effective compiled runtime/binary metadata, including tuple/dynamic/required-member attributes;
   it is not the authority for source-only `notnull`. After an intentional API decision, update the
   source baselines from the exact analyzer diagnostics, regenerate the compiled manifest, and run
   both gates:

   ```bash
   dotnet run --project eng/public-api-generator --configuration Release -- eng/public-api.txt
   bash eng/validate-source-compat.sh
   ```

   A compatible additive API belongs in both `PublicAPI.Unshipped.txt` and, when generic,
   `SourceConstraints.Unshipped.txt`; it requires a minor version. Moving or marking a shipped entry
   is a breaking decision, not a way to silence CI. At release, reviewed unshipped entries become the
   next shipped baseline. Never suppress `RS0016`, `RS0017`, `OSSAPI001`, or `OSSAPI002` for shipping
   code.

4. Review user docs, maintainer docs, XML docs, package contents, backend evidence, benchmark
   provenance (when claimed), and intentional test gaps.
5. Set the package version intentionally. Do not edit protocol numbers merely to match a package.

6. Require both jobs in the repository-wide `Security` workflow to pass for the release commit.
   `Source secret scan` runs Gitleaks over complete repository history and tracked source; the CodeQL
   C# analysis job builds the solution, applies the `security-extended` query suite, and fails when
   its SARIF contains a finding. These are release gates, not advisory uploads. The benchmark
   artifact redaction scanner remains a separate gate because source scanning cannot establish that
   runtime artifacts are safe to publish.

## Prerelease and qualification status

A 1.0 prerelease qualification target may be frozen before registry publication only when the
package description and release notes state plainly that it is not for production use. Do not call
it qualified, production-ready, published, or the stable 1.0 release.

Before qualification, produce one exact unsigned package from one immutable `main` commit with the
[local dry run](#local-dry-run). Preserve that `.nupkg` and its canonical manifest in the separate
public qualification boundary, then create the version-2
[package identity record](qualification-handoff.md#package-identity-record). The clean-room
worksheet and qualification runner must consume those exact package bytes through a locked
package-only source. A project reference, extracted DLL, or later local rebuild is not the target.

Only after qualification succeeds may the release environment and Trusted Publishing policy be
configured. Publication must upload the exact qualified unsigned `.nupkg`, not a rebuild. NuGet.org
adds its repository signature; the workflow then proves that every canonical payload entry still
matches the qualified manifest, records the signed package SHA-256, and repeats package-only
consumption. The qualified unsigned SHA-256 and the delivered signed SHA-256 are intentionally
different identities connected by that fail-closed equivalence proof.

The actual unfamiliar-maintainer exercise remains a stable-1.0 gate. Committing its worksheet does
not complete it. A verdict for a prerelease does not silently transfer to a different canonical
payload or a byte-different stable package.

### Post-qualification Trusted Publishing setup

After qualification succeeds and before creating `v1.0.0-rc.2`, configure one NuGet.org Trusted
Publishing policy with this exact tuple:

| Policy field | Required value |
| --- | --- |
| Package/policy owner | `neftedollar` |
| GitHub repository owner | `Neftedollar` |
| GitHub repository | `Orleans.SearchableStorage` |
| Workflow file | `publish-prerelease.yml` |
| GitHub environment | `release` |

The NuGet login user is the policy creator's profile name, `neftedollar`, not an email address. Also
create the GitHub `release` environment, protect it with the intended reviewers, and set these
environment variables from the independently verified qualification target:

| Environment variable | Required value |
| --- | --- |
| `QUALIFICATION_PACKAGE_URL` | Qualification release asset URL for the exact unsigned `.nupkg`, bound by the SHA-256 below |
| `QUALIFICATION_PACKAGE_SHA256` | Exact 64-character lowercase SHA-256 of that `.nupkg` |
| `QUALIFICATION_CANONICAL_SHA256` | Exact 64-character lowercase SHA-256 of its canonical manifest |
| `QUALIFICATION_VERDICT_URL` | Qualification release asset URL for the passing scale verdict, bound by the SHA-256 below |
| `QUALIFICATION_VERDICT_SHA256` | Exact 64-character lowercase SHA-256 of that verdict |
| `QUALIFIED_LIBRARY_COMMIT` | Exact 40-character lowercase library commit qualified by the evidence |

The workflow accepts the package URL only from a release of the public
`Neftedollar/Orleans.SearchableStorage.Qualification` repository and requires a separately hashed
`oss-qualification-verdict/v1` record whose outcome is `pass` and whose package id, version, two
digests, and library commit match the protected target identity. Enable GitHub Release immutability
in that repository before its first release, upload every asset to a draft, publish the complete
release, and have the qualification-side verifier or release coordinator capture the release and
asset-attestation JSON before transferring the URLs and digests into the protected environment, as
documented in the [qualification handoff](qualification-handoff.md#separate-qualification-repository).
The publication workflow then revalidates the exact URL shapes, records, and bytes by SHA-256; it
does not need a cross-repository GitHub token. It requests only `contents: read`, `checks: read`, and
`id-token: write`; it uses the official `NuGet/login` action pinned to the reviewed `v1.2.0` commit
and contains no persistent NuGet API key secret. The OIDC exchange itself is the authoritative
external-policy check: a missing, inactive, or mismatched policy stops the job, and there is no
credential fallback.

Publication accepts only an exact `v1.0.0-rc.2` tag on the current `main` commit whose CI and Security
checks already succeeded and whose identity matches the qualified target. It repeats the source
build/tests and reproducible package dry-run as an independent equivalence check, downloads the
exact qualified unsigned package, verifies its SHA-256, provenance, and canonical-manifest digest,
and publishes those exact bytes without `--skip-duplicate`. After push it downloads the NuGet.org
repository-signed bytes and applies the same canonical package and package-only consumer
verification. Do not create the tag until qualification has passed and the external policy,
protected environment, and qualification variables match the reviewed records above.

## Local dry run

From the repository root, with the pinned SDK restored:

```bash
bash eng/release-dry-run.sh
```

To retain the locally validated prerelease package and canonical manifest without publishing them,
use a new or empty output directory whose final files do not already exist:

```bash
OSS_RELEASE_OUTPUT_DIRECTORY=artifacts/release-candidate \
  bash eng/release-dry-run.sh
```

Before admitting that artifact to qualification, record both content identities:

```bash
sha256sum \
  artifacts/release-candidate/Orleans.SearchableStorage.1.0.0-rc.2.nupkg \
  artifacts/release-candidate/package.canonical.json
```

The script builds the shipping project, packs it twice with the current commit as repository
provenance, compares a canonical sorted list of package entry names and SHA-256 content hashes,
validates the exact package allowlist and nuspec metadata, and restores/builds a standalone consumer
against only the locally produced package. The two source packs are compared canonically because ZIP
timestamps are not semantic content. Once one pack is admitted as the qualification target, its raw
`.nupkg` SHA-256 also freezes the exact container which the later publication workflow must upload.

CI runs the same validators after its ordinary solution build/test and pack. The docs link checker
validates local Markdown paths and GitHub-style anchors. The shipping project treats missing public
XML documentation as an error; tests, samples, benchmarks, generated members, and private/internal
implementation are not forced into that documentation policy. The compiler source baselines and
focused reflection test are deliberately independent. `eng/validate-source-compat.sh` also builds a
canary twice and requires the build which removes `where T : notnull` to fail with the source
constraint diagnostics, so a silently inactive or incomplete constraint gate cannot pass CI.

## Publish and verify

Publish only the exact qualified unsigned `.nupkg` named by the version-2 identity record. Retain the
workflow run, commit, both package identities, canonical manifest, test results, backend contract
evidence, and release notes. After upload, download the registry artifact into a clean directory and
run the same package validator and consumer smoke against it before announcing the release. NuGet.org
adds a repository signature as the sole root
`.signature.p7s` entry. Verify that signature with the SDK trust policy, exclude only that signature
container from the semantic source-package comparison, and consume the exact downloaded artifact:

```bash
OSS_RELEASE_INPUT_PACKAGE=/clean/download/Orleans.SearchableStorage.VERSION.nupkg \
OSS_RELEASE_INPUT_REPOSITORY_SIGNED=true \
bash eng/release-dry-run.sh
```

The signed mode is explicit and fail-closed: it requires exactly the root signature entry and a
successful `dotnet nuget verify --all` under the reviewed `eng/nuget-repository-policy.json`.
That policy pins the NuGet.org service index, the `neftedollar` package owner, the current repository
certificate, trusted-root enforcement, and online revocation. Certificate rotation requires an
explicit reviewed policy change; it is never learned from the downloaded artifact. The validator
copies the input once into a private bounded snapshot, verifies and compares that snapshot, and the
consumer smoke restores the same bytes. It does not permit any other extra entry or content change.
The signature is the only additional root ZIP entry; `[Content_Types].xml`, `_rels/.rels`, and every
payload entry remain part of the canonical comparison even though signing rebuilds ZIP container
metadata.
Do not use it for the unsigned package produced by CI before upload; CI intentionally exercises the
ordinary strict allowlist path. NuGet.org documents that repository signing adds the signature file
without changing other package content:
<https://devblogs.microsoft.com/dotnet/Introducing-Repository-Signatures/>.

Release notes must call out package/API changes, persistence/query/schema/movement compatibility,
required quiescence or homogeneous rollout, continuation-key implications, known capacity boundaries,
and any deferred evidence. A prerelease with open human or scale gates must repeat the explicit
non-production warning in both package metadata and release notes. Never imply database semantics:
results are bounded `GrainId` discovery over derived indexes; application grains remain the state
authority.
