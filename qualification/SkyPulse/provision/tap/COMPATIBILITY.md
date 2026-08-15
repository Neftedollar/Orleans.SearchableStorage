# Compatibility boundary

## Source identity

The patch applies only to `bluesky-social/indigo` commit
`52c38ce3daca2e85a9f70cf052b475506463018e`. Both build paths verify that exact
commit and all three reviewed patch identities:

- metadata-only protocol patch SHA-256
  `17575e48b5762616fe0e7c6fc56ebe23d442df3a4cf60d35d5377193b6a36056`;
- qualification startup-hardening patch SHA-256
  `2b8be0ceb8e2a71d15710199e545579a82a70ac2428d89bb88d1e74825c20101`;
- qualification privacy/logging patch SHA-256
  `63ff8131d6fe838f92464f4b133b798bbb29e902e954ee42cb1660af5cf9ceb0`.

Moving to another Indigo revision or changing any patch requires new source
tests, a compatibility decision, and a new artifact identity.

## Wire incompatibility

This overlay intentionally changes TAP's public event stream:

- record bodies become `metadata_status` plus a closed optional metadata map;
- identity handles are removed;
- invalid required shapes use a fixed invalid result;
- `repo_sync` is a new acknowledged event type;
- delete events omit all record metadata and CIDs;
- delivery IDs are durable monotonic high-water values.

An unmodified TAP consumer is not assumed compatible. A SkyPulse consumer must
strictly validate exact properties, quarantine fixed invalid results, handle
all lifecycle statuses, acknowledge `repo_sync`, and preserve per-DID ACK
ordering. Unknown event types/properties must fail closed or enter quarantine;
they must not be silently interpreted as a compatible record.

The startup-hardening patch also intentionally narrows the supported runtime
configuration. TAP refuses to open its database without a non-empty bounded
admin password, or when acknowledgements are disabled or webhook delivery is
selected. Qualification therefore uses authenticated WebSocket delivery with
explicit acknowledgements only.

## Durable incompatibility

The singleton `metadata_only_overlay_states` row has format version `3` and a
durable `last_event_id`. On first overlay startup, an absent marker is accepted
only when both durable outbox and resync buffers are empty. A marker with any
other version, or a high-water below an existing outbox ID, is rejected.
Pending rows are revalidated against the exact closed metadata-only schema on
every startup; unknown properties or noncanonical encodings are rejected.

Version 2 is the sole migration exception. Its arbitrary `repos.error_msg`
values are replaced server-side with `resync_failed` in the same transaction
that advances the marker to version 3. Version-3 writes use only the closed
codes `resync_failed`, `canceled`, `deadline_exceeded`, and `claim_lost`. The
privacy patch also replaces raw Echo access logging, permits only closed or
syntax-validated string attributes in slog, silences unstructured standard and
Echo loggers, removes an unused raw-CBOR print path, and skips handle
verification during DID lookup. Logs produced by a version-2 binary are outside
this migration and must be retired separately.

Do not point a stock/older TAP binary at an overlay database. Do not reuse a TAP
database that may contain unsanitized pending rows. Create a new namespace and
replay the authoritative source when crossing this boundary. Database backups
must be labeled with the overlay version and patch identity.

The internal random sync token is intentionally not part of the wire contract.
It may change on every claim/lifecycle transition without consumer impact.

## Ordering contract

For one DID, historical snapshot rows precede a live `repo_sync` barrier;
buffered live rows follow it. In ACK mode, the barrier is delivered only after
all prior rows complete, and following rows remain blocked until the barrier is
acknowledged. Cancellation or transport failure does not revoke a row already
made durable; consumers must use delivery IDs for ACK and their own semantic
deduplication for replay.
