# Privacy-preserving AT Protocol corpus acquisition

`SkyPulse.CorpusAcquisition` produces the private lifecycle journal consumed by
`SkyPulse.CorpusBuilder`. Its result is deliberately described as a **bounded observed census**.
It is neither an atomic snapshot nor a global claim about every AT Protocol deployment.

## Pinned upstream contracts

An acquisition manifest binds the configured source origins and operator-assigned deployment IDs
to exact upstream source identities:

- `network.bsky.jetstream.subscribeEvents` at Jetstream commit
  [`9a30defd224e9058814a7d6ce8d9e4fc48d5493c`](https://github.com/bluesky-social/jetstream/blob/9a30defd224e9058814a7d6ce8d9e4fc48d5493c/lexicons/network/bsky/jetstream/subscribeEvents.json),
  lexicon SHA-256 `fcc7532518a896771d69c71462f57e94d454f96bc6d63e951d10285d9f8f37be`;
- `com.atproto.sync.listRepos` at atproto commit
  [`02f6e227bbb35da2596c476fdf2711d14036ef0b`](https://github.com/bluesky-social/atproto/blob/02f6e227bbb35da2596c476fdf2711d14036ef0b/lexicons/com/atproto/sync/listRepos.json),
  lexicon SHA-256 `afb3599e6075c8b413cf3431e3d0ce0e66aa7eff681bcdfd0bf88aefdf0b52d1`;
- the wrapped lifecycle definitions in
  [`com.atproto.sync.subscribeRepos`](https://github.com/bluesky-social/atproto/blob/02f6e227bbb35da2596c476fdf2711d14036ef0b/lexicons/com/atproto/sync/subscribeRepos.json),
  SHA-256 `bfc3e22bfeae701736fbbbd68a56f0b4b8b66ef4e0e10f1c281b2de61c3328ae`.

The v2 Jetstream request is exactly
`/xrpc/network.bsky.jetstream.subscribeEvents?kinds=account&kinds=identity&kinds=sync`
with WebSocket subprotocol `xrpc.v1.json`. Commit events are not requested. Receiving a commit,
an error/info cursor-clamp frame, malformed input, a configured instance identity which disagrees
with the durable run, a regressing
cursor, or a different sanitized event at the same inclusive replay cursor poisons the run.

Jetstream does not expose a cryptographic deployment-instance ID on this wire. The required
`--jetstream-instance` value is therefore an operator-controlled identity bound to the exact
configured origin. The wire cannot detect replacement of a backend at the same origin, so the
operator must rotate this identity whenever that deployment changes. A cursor must never be moved
to another deployment: Jetstream cursors are instance-local. Because the kinds filter omits
commits, delivered sequence numbers are naturally
sparse; continuity is enforced through the v2 no-clamp/error contract, monotonic cursors, and
deterministic inclusive replay, not by requiring `next == previous + 1`.

The default `listRepos` transport also refuses redirects, proxies, cookies, and automatic
decompression. It therefore contacts only the configured Relay origin. Its
`--relay-instance` is the same kind of operator-controlled deployment identity and must likewise
be rotated after a same-origin backend replacement; the public API does not prove that identity.

## Census boundary

The acquisition opens Jetstream before the crawl begins. It then performs at least two complete
`com.atproto.sync.listRepos` sweeps with `limit <= 1000`. Pagination cursors are treated as opaque:
they are never ordered or interpreted. Repeated cursors, page-count overflow, and a configured or
adapter-reported instance identity which differs from the durable run fail closed. This does not
claim automatic detection of a same-origin backend replacement.

After all configured sweeps drain, the runner records the latest received lifecycle cursor while
holding the same serialization gate that commits journal observations, then drains through that
cursor. This closes the observation interval without pretending that either `listRepos` sweep was
an atomic snapshot. A network failure leaves the run resumable but never creates a final journal or
a freeze-eligible manifest.

One positive 64-bit local ordinal is assigned across page items and lifecycle frames. Account
events and `listRepos` entries write `active`/`inactive` observations. Identity and sync events
consume an ordinal and advance the durable Jetstream cursor but do not invent account status, so
gaps in journal ordinals are expected. A missing explicit `active` value is an unknown lifecycle
state and prevents publication.

## Durability and privacy

The only source-derived values retained in the journal are:

```json
{"ordinal":1,"did":"did:plc:example","status":"active","sourcePosition":"listrepos:s1:p1:c...:i1"}
```

Raw WebSocket frames and HTTP bodies exist only in bounded, short-lived managed buffers and are
never persisted. Application-owned pooled read buffers and explicit returned copies are cleared;
buffers internal to the managed HTTP/WebSocket/runtime stack remain subject to its lifetime, so
process-memory capture is outside this boundary. Handles, profiles, content, media, repository heads/revisions, record bodies, and
opaque cursors are never written to the journal or application logs. Opaque list cursors exist only
in the mode-0600 checkpoint required for resume; the source position contains their SHA-256.

For every committed page or lifecycle event the writer:

1. appends validated sanitized output;
2. flushes the journal and cursor-loop ledger to disk;
3. atomically replaces and directory-fsyncs the mode-0600 checkpoint.

On restart, bytes beyond the checkpointed lengths are truncated before inclusive replay. A
same-cursor/same-sanitized-event replay is ignored; same cursor with different sanitized metadata
poisons the run. Until successful drain, the journal is named
`observations.private.ndjson.partial`; CorpusBuilder's final input name and
`acquisition.manifest.json` do not exist. The private workspace is a real, non-link Unix mode-0700
directory; existing private artifacts are rejected unless they are regular, non-link mode-0600
files. Private sort workspaces are mode 0700 and every DID-bearing spill run is mode 0600. The
configured parent path must be owned and controlled by the deployment account; this code validates
the workspace leaf and opened artifacts, not the ownership of every ancestor directory. A hard
process kill can leave a private staging directory behind; `.private-sort-work` is ignored by Git,
but the operator must remove stale staging directories through the same restricted account and
retention process.

The current network adapter intentionally does not retry inside one WebSocket session. A transport
failure exits without finalization; restarting the command resumes from the durable inclusive
cursor. Unit tests use injectable synthetic sources and make no live-network calls. No live
acquisition evidence has been produced yet.

## Capture CLI

Both endpoint arguments are origin URIs (no path, query, credentials, or fragment):

```bash
dotnet run --project src/Orleans.SearchableStorage.Qualification.SkyPulse.CorpusAcquisition \
  -c Release -- capture \
  --output private-census \
  --jetstream wss://JETSTREAM_ORIGIN \
  --jetstream-instance OPERATOR_PINNED_JETSTREAM_ID \
  --relay https://RELAY_ORIGIN \
  --relay-instance OPERATOR_PINNED_RELAY_ID \
  --sweeps 2 \
  --page-limit 1000
```

A successful directory contains the mode-0600 final journal, canonical acquisition manifest,
checkpoint, and bounded cursor-loop ledger. The manifest records source/contract identities,
start/close cursors, sweep page/repository counts, observation counts, and exact journal length and
SHA-256.

## Private exact-prefix routing

After CorpusBuilder freezes and verifies a profile, the route exporter performs another bounded
external sort of the private journal. It merge-joins the latest explicit-active DID mapping against
the selected `accounts.ak32` prefix without a `Dictionary<string,...>` proportional to account
count:

```bash
dotnet run --project src/Orleans.SearchableStorage.Qualification.SkyPulse.CorpusAcquisition \
  -c Release -- route \
  --acquisition-manifest private-census/acquisition.manifest.json \
  --corpus-manifest frozen-corpus/corpus.manifest.json \
  --profile ten-million \
  --output private-routing \
  --batch-records 500
```

`routing.private.ndjson` is mode 0600 and contains exactly K records in unsigned account-key order:

```json
{"ordinal":1,"accountKey":"0123...64-lowercase-hex...","did":"did:plc:example"}
```

Its canonical private manifest binds the acquisition/journal/corpus identities, parent corpus,
selected profile name/K/prefix SHA-256, exact route bytes, SHA-256 of the concatenated raw 32-byte
key projection, and contiguous batches of at most `--batch-records`. The key-projection SHA-256
must equal the frozen profile prefix SHA-256. Each batch records its ordinal range, byte range, and
SHA-256, allowing a later authenticated `/repos/add` provisioner to send bounded, composition-
verified batches without logging DIDs.

Before provisioning, verify against an independently configured profile identity with
`PrivateRoutingExporter.Verify(manifestPath, PrivateRoutingExpectedProfile)`; the CLI's structural
check is:

```bash
dotnet run --project src/Orleans.SearchableStorage.Qualification.SkyPulse.CorpusAcquisition \
  -c Release -- verify-route --manifest private-routing/routing.private.manifest.json
```

The exporter and verifier do not perform HTTP provisioning.
