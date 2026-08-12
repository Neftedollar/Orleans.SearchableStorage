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
3. Review `eng/public-api.txt`. It is a reviewed runtime/binary metadata inventory of every effective
   public/protected shipping type and member, including accessibility, emitted generic constraints,
   nullable read/write metadata, compiler-significant tuple/dynamic/required-member attributes,
   parameter modifiers/types/defaults, returns, properties, events, bases, and implemented
   interfaces. C# source-only distinctions which the compiler does not emit (notably `notnull`) are
   outside this reflection gate and still require source review; the final 1.0 API freeze must add a
   proven source-compatibility baseline before claiming complete source compatibility. After an
   intentional API decision,
   regenerate it with:

   ```bash
   dotnet run --project eng/public-api-generator --configuration Release -- eng/public-api.txt
   ```

4. Review user docs, maintainer docs, XML docs, package contents, backend evidence, benchmark
   provenance (when claimed), and intentional test gaps.
5. Set the package version intentionally. Do not edit protocol numbers merely to match a package.

## Local dry run

From the repository root, with the pinned SDK restored:

```bash
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
implementation are not forced into that documentation policy. A focused reflection test separately
compares the built assembly to `eng/public-api.txt`, so documentation coverage cannot mask an
accidental emitted binary API change. It is intentionally not described as a complete C#
source-compatibility checker.

## Publish and verify

Publish only the `.nupkg` produced from the reviewed commit. Retain the workflow run, commit, canonical
package manifest, test results, backend contract evidence, and release notes. After upload, download
the registry artifact into a clean directory and run the same package validator and consumer smoke
against it before announcing the release.

Release notes must call out package/API changes, persistence/query/schema/movement compatibility,
required quiescence or homogeneous rollout, continuation-key implications, known capacity boundaries,
and any deferred evidence. Never imply database semantics: results are bounded `GrainId` discovery
over derived indexes; application grains remain the state authority.
