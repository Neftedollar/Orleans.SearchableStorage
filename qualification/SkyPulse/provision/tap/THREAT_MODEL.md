# Threat and privacy boundary

## Protected data

The primary goal is to prevent user-authored AT Protocol content from becoming
durable qualification data. Protected values include post text, profile text,
embed/media data, labels, languages, record timestamps, handles, arbitrary
record fields, referenced CIDs, and error diagnostics derived from record
bodies.

The allowed durable data is structural metadata: event ID/type, live flag,
account DID, repository revision/data CIDs, collection, record key, action,
record CID, the small allowlisted relationship metadata described in the
README, lifecycle status, and internal resync/high-water state. DIDs and AT URIs
remain personal/public identifiers and still require an appropriate retention
policy.

## Trust assumptions

The relay, signed repositories, PDS responses, and all record bodies are
untrusted inputs. The database and host administrator are trusted. TLS,
credential storage, network policy, database encryption, backups, and
downstream deletion/retention enforcement are deployment responsibilities.

The overlay verifies upstream commits/snapshots using Indigo and validates the
closed durable shape again immediately before both durable buffers. It does not
trust collection filters alone as a privacy boundary.

## Fail-closed controls

- Only five fixed collections and three mutation actions are accepted.
- Required relationship values are type checked.
- Retained AT URIs require a DID authority plus full record path.
- DIDs and repository revisions are syntax checked before persistence.
- Invalid metadata produces only `metadata_status: "invalid"`.
- Unknown action/collection/status values are never copied verbatim into a
  permitted durable slot.
- Identity and `repo_sync` JSON use closed fixed structs; handles are absent.
- Existing nonempty pre-marker outbox/resync buffers prevent first startup.
- Unsupported durable marker versions and inconsistent delivery high-water
  state prevent startup.
- Startup fails before opening the database unless a bounded admin password is
  configured and WebSocket acknowledgement mode is enabled; webhook and
  acknowledgement-disabled modes are rejected.
- Every pending durable row is decoded with an exact closed schema and compared
  with its canonical sanitized encoding on every restart.
- The legacy repo handle column is scrubbed on compatible startup and is never
  repopulated.
- Version-2 `repos.error_msg` values are scrubbed server-side during the
  transactional version-3 migration. New failures store only a closed code.
- The global slog handler admits only closed source labels, numeric values,
  sanitized endpoint URLs and syntax-validated structural identifiers. Unknown
  string/object attributes fail closed and error values become `redacted` at
  every level. Legacy standard-library and Echo logging is disabled. The access
  logger records only route templates and numeric measurements.
- DID lookup skips handle verification; TAP needs DID signing keys and does not
  resolve or log handles.
- Resync completion fails if an authoritative record block/path is missing or
  if buffered live commits do not form the expected chain.

## Residual exposure

The running process transiently receives and decodes signed record content in
memory. The overlay does not claim protection against process-memory capture,
core dumps, a malicious binary/database administrator, compromised upstream
Indigo dependencies, or infrastructure logs outside this source tree.

Operational logs can contain DIDs, record paths, revisions, CIDs and sanitized
endpoint URLs. These structural identifiers are allowed by the overlay but can
still identify an account or source record. Keep them in a restricted sink with
bounded retention. Never publish raw DIDs, record paths, or deterministic DID
hashes as qualification evidence; public evidence should contain aggregates and
artifact hashes only. Upstream error strings, handles, bodies, profile fields,
raw HTTP URIs and queries are redacted or omitted. Disable core dumps for
qualification runs.

The admin `/resolve` response is not part of the outbox contract and can expose
identity-document data. The overlay refuses to start unless admin
authentication is configured. Other TAP admin/stat endpoints expose structural
state and must also remain protected by TLS and deployment network policy.

Deleting a source record generates a metadata delete when observed or during
authoritative reconciliation. This is not a legal-compliance service: the
operator must keep collectors running, retain ACK/retry state, monitor lag, and
perform periodic reconciliation. Backups created before sanitization or handle
scrubbing are outside the automatic cleanup boundary and must be retired by the
operator.
