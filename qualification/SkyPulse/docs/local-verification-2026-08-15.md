# Local verification checkpoint — 2026-08-15

This note records a local implementation checkpoint. It is not a qualification
verdict, an immutable evidence release, or permission to publish the NuGet
package.

## .NET application

- SDK: exact `10.0.303`, with roll-forward disabled by `global.json`.
- Target package: `Orleans.SearchableStorage` `1.0.0-rc.2` from the isolated
  local feed.
- Target package SHA-256:
  `d9c05681a0866f027d394843089d6534d06d151f18f611dce3f1e7b5f1e9331c`.
- Locked restore: passed for all 21 projects.
- Release build: passed with zero warnings and zero errors.
- Unit and contract tests: 384 passed, zero failed.
- PostgreSQL integration tests: 34 skipped because this environment had no
  PostgreSQL server. Skipped tests are not evidence of database correctness.
- Runtime corpus growth: migration 2, monotonic admission, configuration,
  dynamic private-route selection, fixed-time administrative authorization,
  and UI contracts are covered by local unit/contract tests. The migration 2
  SHA-256 is
  `f982657018c290b2944c49a25238432195075b0ec950a9dd30e73ce1acd18395`.

## Patched TAP

- Pinned Indigo revision:
  `52c38ce3daca2e85a9f70cf052b475506463018e`.
- Metadata-only patch SHA-256:
  `17575e48b5762616fe0e7c6fc56ebe23d442df3a4cf60d35d5377193b6a36056`.
- Startup-hardening patch SHA-256:
  `2b8be0ceb8e2a71d15710199e545579a82a70ac2428d89bb88d1e74825c20101`.
- Privacy/logging patch SHA-256:
  `63ff8131d6fe838f92464f4b133b798bbb29e902e954ee42cb1660af5cf9ceb0`.
- Reproducible Linux AMD64 binary SHA-256:
  `0142caff15f321cdabe68761f2cbf5e9f85cfbb8f8eb21787b72987666a368f2`.

Two independent clean worktrees produced byte-identical binaries with exact Go
`1.26.1`. Each worktree passed `go test ./cmd/tap`,
`go test ./atproto/atdata`, and `go vet ./cmd/tap` before the deterministic
build. The Dockerfile was inspected but the Docker image was not built in this
environment.

## Checks still required

- Execute all PostgreSQL integration and full source-to-query recovery tests
  against a real PostgreSQL instance.
- Run live acquisition, freeze and independently verify a real corpus, then
  perform the 10K/100K/1M capacity ladder.
- Create and verify the immutable `oss-package-target/v3` artifacts, freeze the
  qualification profiles and thresholds, and only then evaluate a verdict.

No repository was created or pushed, no release was published, and no NuGet
package was uploaded as part of this checkpoint.
