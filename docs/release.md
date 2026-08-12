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

A 1.0 prerelease may be published before the external scale qualification and unfamiliar-maintainer
exercise are complete only when the package description and release notes state plainly that it is
not for production use. Do not call it qualified, production-ready, or the stable 1.0 release.

Immediately after publishing, download the repository-signed package and create the exact package
identity record in the [prerelease handoff kit](qualification-handoff.md#package-identity-record).
The clean-room worksheet and later qualification runner must consume those package bytes from the
public registry, never a project reference or a local rebuild. Qualification code, frozen profiles,
raw evidence, attestations, and independent verification belong in the separate public boundary
defined by that kit.

The actual unfamiliar-maintainer exercise remains a stable-1.0 gate. Committing its worksheet does
not complete it. External qualification must bind the exact `.nupkg` SHA-256 and repository commit,
and a verdict for a prerelease does not silently transfer to a byte-different stable package.

### One-time Trusted Publishing setup

Before creating `v1.0.0-rc.1`, configure one NuGet.org Trusted Publishing policy with this exact
tuple:

| Policy field | Required value |
| --- | --- |
| Package/policy owner | `neftedollar` |
| GitHub repository owner | `Neftedollar` |
| GitHub repository | `Orleans.SearchableStorage` |
| Workflow file | `publish-prerelease.yml` |
| GitHub environment | `release` |

The NuGet login user is the policy creator's profile name, `neftedollar`, not an email address. Also
create the GitHub `release` environment and protect it with the intended reviewers before pushing
the tag. The workflow requests only `contents: read`, `checks: read`, and `id-token: write`; it uses
the official `NuGet/login` action pinned to the reviewed `v1.2.0` commit and contains no persistent
NuGet API key secret. The OIDC exchange itself is the authoritative external-policy check: a missing,
inactive, or mismatched policy stops the job, and there is no credential fallback.

Publication accepts only an exact `v1.0.0-rc.1` tag on the current `main` commit whose CI and Security
checks already succeeded. It repeats the source build/tests, runs the reproducible package dry-run,
freezes and rechecks the exact unsigned package SHA-256 immediately before push, and does not use
`--skip-duplicate`. After push it downloads the NuGet.org repository-signed bytes and applies the
same canonical package and package-only consumer verification. Do not create the tag until the
external policy and protected environment match the tuple above.

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

The script builds the shipping project, packs it twice with the current commit as repository
provenance, compares a canonical sorted list of package entry names and SHA-256 content hashes,
validates the exact package allowlist and nuspec metadata, and restores/builds a standalone consumer
against only the locally produced package. Raw `.nupkg` bytes are deliberately not compared: ZIP
container timestamps are not part of the shipped semantic content proof.

CI runs the same validators after its ordinary solution build/test and pack. The docs link checker
validates local Markdown paths and GitHub-style anchors. The shipping project treats missing public
XML documentation as an error; tests, samples, benchmarks, generated members, and private/internal
implementation are not forced into that documentation policy. The compiler source baselines and
focused reflection test are deliberately independent. `eng/validate-source-compat.sh` also builds a
canary twice and requires the build which removes `where T : notnull` to fail with the source
constraint diagnostics, so a silently inactive or incomplete constraint gate cannot pass CI.

## Publish and verify

Publish only the `.nupkg` produced from the reviewed commit. Retain the workflow run, commit, canonical
package manifest, test results, backend contract evidence, and release notes. After upload, download
the registry artifact into a clean directory and run the same package validator and consumer smoke
against it before announcing the release. NuGet.org adds a repository signature as the sole root
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
