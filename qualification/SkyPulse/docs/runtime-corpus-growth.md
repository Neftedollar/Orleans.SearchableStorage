# Runtime corpus growth

SkyPulse can increase the admitted account cap without stopping queries or rebuilding the already
active prefix. Runtime growth is deliberately monotonic: shrinking a corpus, replacing keys, or
switching to a different parent artifact is not supported.

## Reviewed profile catalog

Every allowed target must already exist as an exact prefix in the canonical
`corpus.manifest.json` and must have its own private exact-route export. The deployment lists those
targets under `SkyPulse:Durable:GrowthProfiles`:

```json
{
  "SkyPulse": {
    "Durable": {
      "GrowthProfiles": [
        {
          "ProfileId": "accounts-2m",
          "CorpusCap": 2000000,
          "ProfilePrefixSha256": "<64 lowercase hex characters>",
          "RoutingManifestPath": "/var/lib/skypulse/routes/2m/routing.private.manifest.json"
        }
      ]
    }
  }
}
```

Profile IDs and caps must be unique, every target must be larger than the immutable base profile,
and all profiles inherit the base profile version. The public target prefix and private route are
fully reverified when the operation starts. A target which is absent from deployment configuration
cannot be requested even if an untrusted caller knows its name.

Supply `SkyPulse:Durable:CorpusGrowthAdminToken` through a secret provider. It must contain 32-4096
characters and is never checked in or logged.

## Durable transition

PostgreSQL migration 2 adds singleton `skypulse.corpus_capacity`. It retains the immutable base,
the fully provisioned active prefix, an optional requested target, and a compare-and-swap operation
version. One serialized transition performs this order:

1. Persist a target from the reviewed catalog. Concurrent targets, smaller caps, and replacement
   profiles are rejected.
2. Reverify the target against the same parent `accounts.ak32` identity.
3. Idempotently bootstrap only the new suffix in PostgreSQL, then prove total `account_state`
   cardinality equals the requested cap.
4. Atomically move the in-process admission view to the larger prefix. Existing membership checks
   continue using a valid open file handle during the swap.
5. Replay the target's complete exact DID route through TAP while the acknowledgement WebSocket is
   draining backfill events.
6. Require TAP's authenticated repository count to equal the target cap, then promote the target
   to active in PostgreSQL.

If the process or connection stops at any boundary, the target remains durable. Startup resolves
that target from the reviewed catalog, repeats suffix bootstrap and exact route replay
idempotently, and completes the same transition. No account from the new suffix is admitted before
its PostgreSQL baseline exists.

Promotion means the exact repository set is installed. It does not claim that every repository has
already emitted its `repo_sync` barrier. New searchable projections appear as synchronization
finishes; the status endpoint reports both total PostgreSQL accounts and synchronized accounts.

## API

Read the current state without the secret:

```http
GET /api/corpus-capacity
```

Request one configured target:

```http
POST /api/corpus-capacity/accounts-2m
X-SkyPulse-Corpus-Admin: <secret>
```

An accepted or already-pending request returns HTTP 202. An already-active target returns 200, an
unknown profile returns 404, and a non-monotonic or competing transition returns 409. The normal
query API remains ready during growth; the capacity response exposes `activeCorpusCap`,
`requestedCorpusCap`, `admissionCorpusCap`, `postgreSqlAccountCount`, and
`synchronizedAccountCount` so partial progress is explicit.

This endpoint is an administrative control surface. Terminate TLS before remote use, do not place
the token in a URL, and restrict network access in addition to the fixed-time header check.
