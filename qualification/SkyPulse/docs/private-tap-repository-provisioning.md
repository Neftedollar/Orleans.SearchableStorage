# Private TAP repository-set provisioning

`PrivateTapRepositoryProvisioner` installs the exact frozen account profile in the reviewed TAP
deployment. It consumes the private `routing.private.manifest.json` and its sibling
`routing.private.ndjson`; the public `accounts.ak32` corpus is intentionally insufficient because
an account key cannot be reversed into the DID required by TAP.

## Readiness order

The host must perform these operations in order:

1. verify and bootstrap the exact public corpus profile in PostgreSQL;
2. validate the private routing configuration without contacting TAP;
3. connect the authenticated `/channel` WebSocket and start its receive/acknowledgement loop;
4. provision repositories while that loop remains open and API readiness remains false;
5. report readiness only after provisioning returns `Provisioned`.

If the WebSocket ends during provisioning, the host cancels provisioning and repeats the complete,
idempotent repository replay on the next session. Repository-sync completion is a separate measure
of historical ingestion progress and is not part of this readiness proof.

The same ordering is retained for an online cap increase. The new PostgreSQL account suffix and
the larger admission prefix are installed before the larger private route is submitted. The
existing receive/acknowledgement loop remains open while TAP backfills the new repositories. See
[runtime corpus growth](runtime-corpus-growth.md).

## Exact-set proof

Before making an HTTP request, the provisioner requires both files to be regular, non-link files
with bounded lengths and Unix mode 0600 where Unix modes are supported. The route verifier also
requires the containing directory to be a real, non-link mode-0700 directory. Its trusted parent
path must be owned by the application account and must not be writable by an untrusted principal.

The corpus exporter verifier checks the canonical manifest and route against the independently
configured profile name, account count K, and prefix SHA-256. The provisioner additionally requires
the independently configured durable profile version. It then checks the opened route handle and
performs a second streaming pass which requires:

- contiguous ordinals 1 through K;
- strictly increasing, unique 32-byte account keys;
- `AccountKey = SHA256(exact canonical DID UTF-8)` for every record;
- exact route byte length and SHA-256;
- exact concatenated-account-key prefix SHA-256; and
- every manifest batch's record count, byte length, and SHA-256 before any DID from that batch is
  submitted.

Verified DIDs are sent in bounded authenticated `POST /repos/add` requests. TAP's reviewed handler
is idempotent, so all K DIDs are replayed on every startup. The provisioner then makes an
authenticated `GET /stats/repo-count` request and accepts only the closed JSON response
`{"repo_count":K}`. Under the required deployment invariants—exclusive repository administration,
full-network mode disabled, and automatic repository discovery disabled—successful insertion of
all K members plus total cardinality K proves set equality, not merely equal counts.

The reviewed TAP startup overlay enforces full-network off, no signal collection, replay enabled,
and no collection filters. These binary/startup controls are deployment evidence; the three
provisioner confirmations deliberately fail closed unless the operator also asserts that the
running instance has those controls and exclusive administration.

## Configuration and bounds

Construct `PrivateTapRepositoryProvisionerOptions` from the same validated deployment profile used
by the durable runtime:

- `RoutingManifestPath`: absolute path ending in `routing.private.manifest.json`;
- `TapWebSocketEndpoint`: the existing absolute `ws://` or `wss://` `/channel` endpoint; plain
  `ws://` is accepted only for loopback;
- `AdminPassword`: the TAP admin secret, never a URI or log field;
- `ExpectedProfileVersion`: the positive durable profile version;
- `ExclusiveRepositoryAdministrationConfirmed`, `FullNetworkModeDisabledConfirmed`, and
  `AutomaticRepositoryDiscoveryDisabledConfirmed`: all must be true.

Defaults are 500 DIDs per request, three attempts, a 30-second per-attempt timeout, a 16-KiB
response limit, a 4-MiB request limit, a 64-MiB manifest limit, a 64-GiB route limit, and a 16-KiB
line limit. Configuration validation caps batches at 1,000 records, attempts at eight, responses at
1 MiB, request bodies at 64 MiB, and route artifacts at 1 TiB.

HTTP redirects, cookies, proxies, and automatic decompression are disabled. Only HTTP 200 is
accepted; `/repos/add` must have an empty body and `/stats/repo-count` must match its one-property
closed schema. Statuses 408, 425, 429, 500, 502, 503, and 504, transport failures, and timeouts use
bounded retries. Authentication failures and other contract violations fail immediately. File and
permission failures surface as operational faults; verified identity/content mismatches return the
closed mismatch status.

No DID, route content, response body, password, or administration URI is logged or included in
exception text. Explicit returned response copies, pooled transport buffers, and temporary
credential byte arrays are cleared after use. Other bounded managed request/parser buffers can
remain until garbage collection, and buffers internal to the HTTP/runtime stack remain outside the
clearing claim; process-memory capture is outside this boundary. The Basic authorization value must
remain in process memory for the provisioner's lifetime so that it can authenticate retries;
dispose the provisioner with the host.
