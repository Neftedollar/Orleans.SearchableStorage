# SkyPulse metadata-only TAP overlay

This directory builds a narrowly scoped metadata-only variant of Bluesky's
TAP service from the immutable upstream Indigo commit
`52c38ce3daca2e85a9f70cf052b475506463018e`.

The overlay exists only as an ingestion boundary for the SkyPulse qualification
application. It is not a general AT Protocol archive, content index, or fork of
Indigo. Read [THREAT_MODEL.md](THREAT_MODEL.md) and
[COMPATIBILITY.md](COMPATIBILITY.md) before operating it.

## What the overlay retains

Only these record collections can cross a durable TAP buffer:

| Collection | Retained record metadata |
| --- | --- |
| `app.bsky.feed.post` | Direct reply parent AT URI, when present |
| `app.bsky.feed.like` | Subject AT URI |
| `app.bsky.feed.repost` | Subject AT URI |
| `app.bsky.graph.follow` | Follow subject DID |
| `app.bsky.actor.profile` | No record-body fields |

AT URIs must contain a DID authority and a complete collection/record-key path.
A handle authority is rejected. The sanitizer never copies post text, embeds,
media, labels, languages, timestamps from a record, profile fields, handles,
referenced CIDs, arbitrary body keys, or source-controlled diagnostics. A
malformed required shape becomes the fixed status `invalid` with no metadata.
Unknown collections/actions cannot mutate `repo_records` and are represented
only by fixed `invalid` sentinels at the final durable boundary.

Sanitization runs before both `OutboxBuffer` and `ResyncBuffer` serialization.
The raw decoded record exists only transiently while the signed commit or
authoritative repository snapshot is being verified.

## Wire contract

A valid create/update record has this closed envelope (optional `metadata` is
absent when there is nothing to retain):

```json
{
  "id": 17,
  "type": "record",
  "record": {
    "live": true,
    "did": "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa",
    "rev": "3jzfcijpj2z2b",
    "collection": "app.bsky.feed.post",
    "rkey": "post-a",
    "action": "create",
    "cid": "bafy-record",
    "metadata_status": "valid",
    "metadata": {
      "reply_parent_uri": "at://did:plc:bbbbbbbbbbbbbbbbbbbbbbbb/app.bsky.feed.post/parent"
    }
  }
}
```

The only valid metadata maps are:

- post: absent/empty, or exactly `reply_parent_uri`;
- like/repost: exactly `subject_uri`;
- follow: exactly `follow_subject_did`;
- profile: absent/empty.

A delete contains `live`, `did`, `rev`, `collection`, `rkey`, and `action` only.
It contains no `cid`, `metadata_status`, or `metadata`.

Account lifecycle messages contain no handle:

```json
{
  "id": 18,
  "type": "identity",
  "identity": {
    "did": "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa",
    "is_active": false,
    "status": "deactivated"
  }
}
```

An authoritative resync ends with an acknowledged ordering barrier:

```json
{
  "id": 19,
  "type": "repo_sync",
  "repo_sync": {
    "did": "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa",
    "rev": "3jzfcijpj2z2b",
    "status": "active"
  }
}
```

The `repo_sync` row is enqueued after every authoritative create/update/delete
row and before buffered live commits resume. It is marked live, so normal TAP
ACK ordering prevents later rows for the same DID from passing it.

## Resync and lifecycle behavior

- Records found in the old `repo_records` catalog but absent from the current
  authoritative snapshot produce deterministic, sorted delete events and are
  removed from the catalog.
- Buffered commits already included in the snapshot are removed as subsumed.
- A newer buffered commit must extend the snapshot/previous commit exactly;
  otherwise completion fails closed and no `repo_sync` is emitted.
- A filtered zero-operation buffered commit still advances repo metadata and is
  removed transactionally.
- Startup converts a repo left `resyncing`, or an `active` repo with a pending
  resync buffer, back to `desynchronized` under a fresh ownership token.
- Missing referenced CAR blocks and invalid authoritative record paths fail the
  resync; they cannot produce a false completion barrier.
- Reactivation emits an active identity barrier, clears the old CID catalog,
  and forces a full authoritative projection before `repo_sync`.
- Inactive transitions invalidate an in-flight resync before any later
  historical batch can be appended.
- A random durable 128-bit sync token owns each resync claim. Every lifecycle
  transition and new claim replaces it, preventing inactive/active and
  delete/recreate ABA races. The token is internal and never appears on wire.

The durable compatibility marker is version `3`. It also stores the monotonic
delivery-ID high-water, reserved in the same transaction as each outbox append.
IDs therefore do not restart at `1` after the outbox becomes empty. Every
pending outbox and resync row is strictly decoded and compared with its
canonical sanitized form on restart. A version-2 database is migrated in one
transaction: arbitrary legacy `repos.error_msg` diagnostics are replaced with a
closed code before the marker advances to version 3.

Operational logging has a separate fail-closed boundary. Slog keeps only closed
source labels, numeric measurements, sanitized endpoint URLs and
syntax-validated structural identifiers; every unknown string/object attribute
is omitted, including at debug level. Error values become the fixed word
`redacted`. Unstructured standard-library and Echo logging is disabled. Access
logs contain only a server-owned route template and numeric status/size/latency
data. Resync failures persist one of four closed codes rather than an upstream
error message. Indigo handle verification is disabled because TAP needs DID
keys, not handles. DIDs, revisions, record paths and CIDs remain permitted
structural identifiers in restricted operational logs only.

## Build

Host requirements are Git, GNU/Linux or macOS checksum tools, a C compiler for
SQLite, and exactly Go `1.26.1`:

```bash
./build.sh
```

The default output is `dist/tap`. A caller-relative or absolute output can be
selected explicitly:

```bash
TAP_OUTPUT=/tmp/skypulse-tap ./build.sh
```

`build.sh` verifies the pinned commit and all three reviewed patch SHA-256
values, applies each patch with `git apply --check`, runs `go test ./cmd/tap`,
`go test ./atproto/atdata`, and `go vet ./cmd/tap`, and creates a trim-path
binary.
`verify.sh` performs two independent clean builds and requires byte identity.

The Docker build has immutable source, patch, builder-image, and runtime-image
pins:

```bash
docker build -t skypulse-tap:metadata-only .
```

No build argument can override these identities. The Docker context excludes
local `dist/` files and every file not explicitly needed by the build.

## Operation

Use WebSocket ACK mode for qualification ingestion. Persist the TAP database,
ack an event only after the downstream durable ingestion transaction succeeds,
and treat `repo_sync` as the authoritative reconciliation-complete barrier.
`TAP_ADMIN_PASSWORD` is mandatory. The qualification hardening patch rejects startup before the
database is opened when the password is empty, acknowledgements are disabled, webhook mode is
selected, full-network or signal-collection discovery is enabled, source replay is disabled, or a
collection subset is configured. The overlay itself already admits exactly the five reviewed
metadata collections. These checks preserve authenticated WebSocket-ack delivery and an explicit
repository set; an unauthenticated client cannot read or acknowledge the durable outbox.

Keep TAP logs in a private, access-controlled sink with bounded retention. Raw
DIDs and deterministic hashes of DIDs must not be copied into public
qualification evidence; publish aggregate counts, timings and artifact hashes
instead. Retire any logs produced by the older two-patch/version-2 binary before
using the version-3 privacy boundary.

The patch and build artifacts are covered by Indigo's upstream dual-license
terms; see [NOTICE.md](NOTICE.md) and the included license texts.
