# Recovery note

The original workspace containing this overlay was reset before it was
committed or published. This directory is a clean reconstruction from the same
pinned Indigo source plus the recorded source-review decisions and P0 fixes.

Pre-reset evidence recorded these identities:

- original patch SHA-256:
  `eba96fe71c09f1b9132c66275c7b465b873d2d079b415581db6d09a0f38e39a3`;
- original Linux AMD64 binary SHA-256:
  `99b6330ae1081f3c1074dcda998be62bf3e613701677ae89be1d98bf40652740`.

Those hashes identify the lost pre-reset bytes only. They are not claims about
the reconstructed files.

The current reviewed reconstruction has these independent identities:

- metadata-only protocol patch SHA-256:
  `17575e48b5762616fe0e7c6fc56ebe23d442df3a4cf60d35d5377193b6a36056`;
- qualification startup-hardening patch SHA-256:
  `2b8be0ceb8e2a71d15710199e545579a82a70ac2428d89bb88d1e74825c20101`;
- qualification privacy/logging patch SHA-256:
  `63ff8131d6fe838f92464f4b133b798bbb29e902e954ee42cb1660af5cf9ceb0`;
- Linux AMD64 binary SHA-256 from the current three-patch source:
  `0142caff15f321cdabe68761f2cbf5e9f85cfbb8f8eb21787b72987666a368f2`.

On 2026-08-15, exact Go `1.26.1` compiled the pinned Indigo revision in two
independent clean source worktrees. The binaries were byte-identical. The full
`go test ./cmd/tap` and `go vet ./cmd/tap` checks, plus the focused
`go test ./atproto/atdata` check for the removed raw-CBOR diagnostic path, also
passed after all three patches were applied. These are local source/build
checks, not deployed qualification evidence and not a claim that the Dockerfile
was executed.

The earlier reconstructed two-patch binary had SHA-256
`a87d0fa176f69bec0e954ee4128bd65aa1598bab360929836e7ec47c92b251dd`.
That identity is retained only as superseded historical evidence; it predates
the version-3 privacy/logging boundary and must not be deployed.

An intermediate version-3 binary using a denylist logger had SHA-256
`9ed501da2ddb98f685b7519d1f5e71ea237adfe081d4e183f14ebe3bd984b1a3`.
It is also superseded and must not be deployed: the current patch uses a closed
allowlist and disables unstructured dependency logging instead.
