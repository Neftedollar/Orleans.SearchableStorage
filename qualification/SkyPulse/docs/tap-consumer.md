# TAP consumer boundary

SkyPulse connects to the reviewed TAP overlay at `/channel` using a WebSocket and HTTP Basic
authentication (`admin` plus the deployment secret). Plain `ws://` is accepted only on loopback;
remote deployments must use `wss://` or a private authenticated tunnel.

## Startup gates

The Web host does not connect to TAP merely because PostgreSQL is reachable. It waits for the
Memory index rebuild and rolling-window catch-up, verifies the canonical corpus manifest and the
selected prefix hash, then idempotently streams all selected account keys into PostgreSQL in
bounded pages. Missing keys receive an Active, generation-zero, unsynchronized zero-stock
baseline and a self-reconciliation dependency. Existing progressed state is never overwritten.
After the pass, the total `account_state` row count must equal the exact corpus cap; this equality
proves there are neither missing selected keys nor extra account states.

For a runtime increase, that same proof is repeated against the requested larger prefix before the
in-process admission view moves forward. The larger TAP route is then replayed while this consumer
continues draining events. The durable target remains pending until exact TAP repository
cardinality is proven; see [runtime corpus growth](runtime-corpus-growth.md).

A second, separate gate must prove that the exact private DID repository set was provisioned in
TAP. The public `accounts.ak32` artifact contains one-way hashes, so SkyPulse cannot derive the DIDs
needed by TAP's authenticated `/repos/add` API. The implemented private provisioner verifies the
mode-0600 route and exact profile without network access before `/channel` is opened. A repository
count alone is not sufficient proof of membership.

The proof does not require ten million `/info` calls: with full-network/auto-discovery disabled and
an exclusive admin secret, the provisioner replays all exact K DIDs from its
verified route through idempotent `/repos/add`, then requires TAP's repository count to equal K.
Replaying proves every selected repository is present; equal cardinality then excludes extras. The
adapter has two phases: non-network configuration validation occurs before connect; actual
provisioning starts only after `/channel` is open and its receive/ACK loop is running. That ordering
lets the consumer drain backfill events while ten million repositories are being added instead of
allowing TAP's outbox to grow without a reader.

"Repository set provisioned" and "all repositories synchronized" are deliberately different
states. Provisioning proof is required for API readiness, while the consumer is already draining
the provisioning backfill. Full synchronization is a measured dataset-completeness signal (for
example, the share of selected accounts whose `repo_sync` barrier has completed); it may take days
and does not by itself keep the query UI unavailable.

The consumer uses TAP's acknowledgement mode. For every sanitized message it performs this order:

1. Receive at most 16 KiB of valid UTF-8 text and compute SHA-256 over the exact received bytes.
2. Parse the closed metadata-only shape. The message is never written to a log or arbitrary JSON
   column.
3. Reserve `(source instance UUID, TAP delivery ID, exact SHA-256)` in PostgreSQL. The first local
   observation minute is fixed by this reservation and survives redelivery.
4. Read the bounded durable planning state and commit an applied transition, a validated no-op, or
   a bounded quarantine record in PostgreSQL.
5. Send exactly `{"type":"ack","id":N}` only when the committed result permits acknowledgement.

An invalid message from which a positive delivery ID cannot be recovered is not acknowledged. It
is a protocol failure requiring operator attention, because there is no safe TAP outbox row to
name. Quarantine messages contain fixed diagnostic text and never copy arbitrary input strings.

If an acknowledgement is lost after the database commit, TAP redelivers the row. The durable
reservation and semantic identity make that duplicate acknowledgement-safe. If optimistic state
changed before commit, the consumer does not acknowledge; it rereads durable state and replans the
same reserved delivery.

The first worker is deliberately sequential: it does not receive the next WebSocket message until
the current message is durably decided and acknowledged, or the connection is abandoned without
an acknowledgement for retry. This preserves a simple auditable boundary but limits throughput to
roughly one delivery per PostgreSQL planning-and-commit round trip (and one lifecycle page at a
time). No concurrency claim should be inferred from TAP's ability to have multiple events in
flight. A future keyed-concurrency worker must retain the same account locks and ACK proof.

The source-instance UUID is part of durable identity. It must be generated once for one persistent
TAP deployment and remain stable across application restarts. It must change when the TAP database
is intentionally replaced. The reviewed overlay itself preserves its event-ID high-water mark even
after acknowledged outbox rows are deleted.

TAP can internally schedule historical rows concurrently and live rows for different repositories
concurrently. PostgreSQL account advisory locks, optimistic versions, repository generations,
record revisions, and the `repo_sync` barrier remain the authority. The current consumer
intentionally processes the received stream one message at a time.

Required durable settings are `CorpusManifestPath`, `TapEndpoint`, `TapAdminPassword`, the absolute
`RoutingManifestPath`, and explicit true values for
`ExclusiveRepositoryAdministrationConfirmed`, `FullNetworkModeDisabledConfirmed`, and
`AutomaticRepositoryDiscoveryDisabledConfirmed`, in addition to the profile/source identity
already bound by the runtime manifest. Supply the TAP password through an environment variable or
another secret provider; it is not present in `appsettings.json`. The exact private-route and
admin-API contract is documented in
[private TAP repository-set provisioning](private-tap-repository-provisioning.md).

The overlay's operational logs are a restricted, finite-retention metadata sink as specified in
its threat model. They must never be exposed publicly; raw record bodies, post/profile text,
handles, media, or source-derived content-bearing error strings remain outside the logging and
durable-storage contract.
