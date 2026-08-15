# SkyPulse corpus builder

This command freezes a deterministic parent account corpus from a **pre-sanitized, append-only
NDJSON observation journal**. It does not connect to AT Protocol services, crawl repositories, or
claim that the input is an atomic network snapshot. The separate
`SkyPulse.CorpusAcquisition` adapter now produces this journal as a bounded observed census without
persisting raw frames, handles, profile data, content, media, or record bodies. See
[`docs/corpus-acquisition.md`](../../docs/corpus-acquisition.md).

## Input contract

Each line is one JSON object with exactly four properties:

```json
{"ordinal":1,"did":"did:plc:example","status":"active","sourcePosition":"adapter-cursor-1"}
```

- `ordinal` is a positive local 64-bit integer and must increase strictly in file order. It is the
  only ordering used by the fold.
- `did` is the canonical repository DID. The account key is SHA-256 over its exact decoded UTF-8
  bytes; no normalization is performed.
- `status` is exactly `active` or `inactive`. Unknown states fail closed.
- `sourcePosition` is a non-empty, bounded opaque adapter position. It is validated but is not
  used as an ordering surrogate.

Unknown or duplicate properties, invalid UTF-8, non-monotonic ordinals, oversized lines, malformed
DIDs, and unknown lifecycle states are rejected. For every DID, the fold takes the observation
with the greatest local ordinal and admits the account only when that explicit state is `active`.

The parser retains only this allowlisted metadata. Private spill runs temporarily contain DIDs so
that two different DIDs with the same 32-byte key can be detected as a strict collision. The spill
directory is mode 0700, every spill file is mode 0600, and the directory is removed before
publication. The private journal and spill directory are ignored by Git and must remain under the
deployment retention/access-control policy.

## Freeze

```bash
dotnet run --project src/Orleans.SearchableStorage.Qualification.SkyPulse.CorpusBuilder \
  -c Release -- freeze \
  --journal observations.private.ndjson \
  --output frozen-corpus \
  --memory-mib 64 \
  --merge-fan-in 32 \
  --profile one-million=1000000 \
  --profile ten-million=10000000 \
  --hex
```

The output directory must not exist. Work is created in a sibling staging directory and published
with a same-filesystem directory rename only after all evidence and privacy checks pass. A failed
run never overwrites an existing corpus.

`--memory-bytes` permits small deterministic test/calibration budgets (minimum 4096 bytes).
`--memory-mib` is more convenient for production runs. The budget limits encoded observations in
one sort batch; merge memory and open files are separately bounded by `--merge-fan-in` (2-128).
Spill runs are compacted during ingestion, so their managed bookkeeping does not grow once per
source chunk.

With no `--profile`, a `parent` profile selects the complete parent. Every requested `K` must be at
most the parent count. Counts are 64-bit and can exceed 10 million. Profiles are exact byte prefixes
of one canonical parent, not independent samples.

## Public artifacts

- `accounts.ak32`: exactly `32 * N` bytes containing non-zero account keys in strict unsigned
  lexicographic order, with no header or delimiter.
- `accounts.hex`: optional lowercase hexadecimal rendering, one key per line.
- `corpus.manifest.json`: unique canonical JSON containing the exact source-journal byte length and
  SHA-256, parent count and raw SHA-256, the Core-compatible domain-separated corpus fingerprint,
  and each profile's `K`, byte length, and raw-prefix SHA-256.

Before publication every public file is scanned for the byte sequence `did:`. Unexpected files or
subdirectories also fail closed. Account-key hashes are stable public-DID identifiers, not an
anonymity or secrecy boundary.

## Verify

```bash
dotnet run --project src/Orleans.SearchableStorage.Qualification.SkyPulse.CorpusBuilder \
  -c Release -- verify \
  --manifest frozen-corpus/corpus.manifest.json \
  --deep \
  --journal observations.private.ndjson
```

Deep verification checks canonical manifest encoding, exact file sets and lengths, raw hashes,
profile-prefix hashes, non-zero unique sorted keys, the domain-separated corpus fingerprint, and
the public privacy scan. Supplying `--journal` also checks that its exact bytes match the recorded
source identity. Omitting it is allowed and reported explicitly; the recorded source hash is then
not independently checked.
