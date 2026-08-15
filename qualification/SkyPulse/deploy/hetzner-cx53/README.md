# SkyPulse: two-host CX53 deployment

This bundle is an operator handoff for the first **calibration** deployment of
SkyPulse. It is not a high-availability design and does not claim a successful
qualification run.

## Topology

| Host | Private address | Workload |
| --- | --- | --- |
| `skypulse-app-1` | `10.42.0.10` | exactly one SkyPulse Web/Orleans process, patched TAP, optional Caddy |
| `skypulse-pg-1` | `10.42.0.20` | PostgreSQL 17, separate `skypulse` and `skypulse_tap` databases |

SkyPulse binds `127.0.0.1:5080`; TAP binds `127.0.0.1:2480`. Both use Linux
host networking because the application intentionally rejects plaintext
`ws://` for non-loopback TAP endpoints. PostgreSQL is reachable only through
the Hetzner private network and requires certificate verification.

There must be exactly one SkyPulse application container. The current Memory
index/dispatcher protocol is not fenced for multiple application replicas.
This first bundle requires rootful Docker on Linux without `userns-remap`:
host networking and exact numeric ownership of read-only bind mounts are part
of the reviewed contract.

## What is included

- digest-pinned, fail-closed Compose templates;
- a locked application image build using .NET SDK `10.0.303`;
- the existing pinned three-patch TAP image plus a secret-file entrypoint;
- separate PostgreSQL roles/databases for SkyPulse and TAP;
- TLS/HBA, secret, corpus, route and image preflight checks;
- deterministic startup, smoke, backup and corpus-growth commands;
- optional OpenTofu for two CX53 servers and restrictive firewalls;
- an operator checklist in [`HANDOFF.ru.md`](HANDOFF.ru.md).

No secret or live DID-bearing artifact is included. The operator must supply a
verified public corpus, a private route set, passwords, exact external image
digests, TLS material and infrastructure credentials.

The OpenTofu servers use ordinary Hetzner root disks; this bundle does not
configure or prove block-device encryption. Do not place real private routes,
database data, credentials or backups on these hosts until the local operator
has supplied and restore-tested an encrypted-at-rest storage design (or has a
separately reviewed provider guarantee covering the exact volumes/backups).
Without that proof the topology is limited to synthetic/empty calibration.

This directory is not a standalone build context. It must first be committed
at `qualification/SkyPulse/deploy/hetzner-cx53` in the same repository as the
SkyPulse source. Set `DEPLOYMENT_ID` to the full commit which contains these
files; `build-images.sh` deliberately rebuilds from that exact Git tree and
rejects an old SHA or an uncommitted handoff copy.

## First run

Run every command from this directory unless the command says otherwise.
Run source verification, disposable tests and image builds as the checkout
owner with access to the rootful Docker daemon. On the two deployment hosts,
run `preflight-*`, `up-*`, `stop-*`, status/growth and backup commands as root
(normally via `sudo`): they intentionally inspect process-owned mode-0400
secrets. Keep `.env` and `runtime/*.env` mode `0600`, owned by root or the
checkout owner. Membership in the Docker group is already root-equivalent; do
not weaken secret modes to avoid `sudo`.

1. Read [`HANDOFF.ru.md`](HANDOFF.ru.md), [`SECURITY.md`](SECURITY.md), and
   [`UPGRADE-ROLLBACK.md`](UPGRADE-ROLLBACK.md).
2. Create the ignored runtime directory, copy the three non-secret templates,
   and replace every relevant `REPLACE_...` value:

   ```bash
   install -d -m 0700 runtime
   cp .env.example .env
   cp config/app.env.example runtime/app.env
   cp config/postgres.env.example runtime/postgres.env
   chmod 0600 .env runtime/*.env
   ```
3. Before running the bundle gate, follow `infra/README.md` to generate the
   Linux/AMD64 provider lock on a trusted machine, review it, and commit it.
   This handoff intentionally contains no invented `.terraform.lock.hcl`, so
   the command below must fail until that release-maintainer step is complete.
   Then run the non-deployment checks before using any privileged helper. If
   OpenTofu is installed, validation downloads only lock-authorized provider
   bytes into a script-owned temporary directory; it never plans or applies:

   ```bash
   scripts/verify-bundle.sh
   ```

4. Verify the exact package and build/test the source with SDK `10.0.303`:

   ```bash
   scripts/verify-source.sh
   ```

5. Run the mandatory real-PostgreSQL suite in the script-owned disposable
   loopback container. It never accepts a production connection string and
   destroys the container on exit:

   ```bash
   SKYPULSE_ALLOW_DESTRUCTIVE_POSTGRES_TESTS=I_UNDERSTAND_THIS_USES_A_DISPOSABLE_CONTAINER \
     scripts/run-postgres-integration.sh
   ```

6. Generate secrets once on a trusted workstation:

   ```bash
   scripts/generate-secrets.sh /secure/staging/skypulse-secrets
   ```

   Do not distribute anything until step 8 has provisioned the hosts. Then
   distribute only the documented subset. Never copy the staging directory
   into this checkout.

   On host A, Docker must receive process-owned files through root-only parent
   directories (the numeric UIDs are part of the image contract):

   ```bash
   sudo install -d -m 0700 -o root -g root /srv/skypulse/secrets
   sudo install -d -m 0700 -o 10001 -g 10001 /srv/skypulse/secrets/app
   sudo install -m 0400 -o 10001 -g 10001 /secure/app/* /srv/skypulse/secrets/app/
   sudo install -d -m 0700 -o 65534 -g 65534 /srv/skypulse/secrets/tap
   sudo install -m 0400 -o 65534 -g 65534 /secure/tap/* /srv/skypulse/secrets/tap/
   ```

   On host B, first inspect the UID in the exact PostgreSQL image, then install
   only the three database files with that owner:

   ```bash
   python3 scripts/validate-runtime-env.py deploy .env
   set -a; . ./.env; set +a
   pg_uid=$(docker run --rm --entrypoint id "$POSTGRES_IMAGE" -u postgres)
   pg_gid=$(docker run --rm --entrypoint id "$POSTGRES_IMAGE" -g postgres)
   sudo install -d -m 0700 -o root -g root /srv/skypulse/secrets
   sudo install -d -m 0700 -o "$pg_uid" -g "$pg_gid" /srv/skypulse/secrets/postgres
   sudo install -m 0400 -o "$pg_uid" -g "$pg_gid" /secure/postgres/* /srv/skypulse/secrets/postgres/
   ```
7. Use `scripts/generate-postgres-tls.sh` to create a private CA and server
   certificate. Keep the CA key offline; copy only the server certificate/key
   to the database host and `ca.crt` to the application host.

   Install PostgreSQL data, backup and TLS directories using the UID printed
   above. The server key must be `0600`; certificates are `0644`. On host A,
   install only the CA certificate as `/srv/skypulse/tls/ca.crt` mode `0644`.

   Example on host B, where `/secure/tls` is the generated material:

   ```bash
   sudo install -d -m 0700 -o "$pg_uid" -g "$pg_gid" \
     /srv/skypulse/postgres/data /srv/skypulse/postgres/backups /srv/skypulse/tls/postgres
   sudo install -m 0600 -o "$pg_uid" -g "$pg_gid" \
     /secure/tls/server/server.key /srv/skypulse/tls/postgres/server.key
   sudo install -m 0644 -o "$pg_uid" -g "$pg_gid" \
     /secure/tls/server/server.crt /srv/skypulse/tls/postgres/server.crt
   sudo install -m 0644 -o "$pg_uid" -g "$pg_gid" \
     /secure/tls/ca/ca.crt /srv/skypulse/tls/postgres/ca.crt
   ```
8. Provision the two hosts from `infra/` or create equivalent infrastructure.
   Review `tofu plan`; this bundle never runs `tofu apply` automatically. The
   checked handoff intentionally does not invent `.terraform.lock.hcl` without
   an installed OpenTofu/provider registry. A release maintainer must generate
   it for Linux/AMD64, review its signer/checksums, and commit it before
   `verify-bundle.sh`, `tofu init`, plan or apply can pass. Follow
   [`infra/README.md`](infra/README.md); state and plans stay outside Git.
9. Install `nftables`, `iproute2`, Docker Engine/Buildx/Compose and systemd on
   both Ubuntu hosts. Hetzner Cloud Firewalls do **not** filter the private
   Cloud Network, so the checked guest rules are mandatory. Install the
   finite journald policy on both hosts, then install the role-specific
   firewall. The systemd drop-ins make Docker and classic `ssh.service`
   require that firewall at
   boot. Before copying `.env` to each host, discover the non-loopback
   interface which owns that host's exact Hetzner private address:

   ```bash
   ip -4 -brief address
   ip -4 -o address show
   ```

   Set `APP_PRIVATE_INTERFACE` on host A and
   `POSTGRES_PRIVATE_INTERFACE` on host B to those exact interface names. The
   installer and every boot/reload refuse a missing, down, loopback, or
   wrong-address interface; do not guess `eth0`.

   Then install the policies:

   ```bash
   # Host A only
   sudo env SKYPULSE_JOURNAL_CONFIRMATION=I_ACCEPT_SEVEN_DAY_TWO_GIB_HOST_LOG_RETENTION \
     scripts/install-journal-policy.sh
   sudo env SKYPULSE_FIREWALL_CONFIRMATION=I_AM_ON_THE_APP_HOST \
     SKYPULSE_SSH_MODE_CONFIRMATION=I_HAVE_HETZNER_CONSOLE_AND_ACCEPT_CLASSIC_SSH \
     scripts/install-app-firewall.sh

   # Host B only
   sudo env SKYPULSE_JOURNAL_CONFIRMATION=I_ACCEPT_SEVEN_DAY_TWO_GIB_HOST_LOG_RETENTION \
     scripts/install-journal-policy.sh
   sudo env SKYPULSE_FIREWALL_CONFIRMATION=I_AM_ON_THE_POSTGRES_HOST \
     SKYPULSE_SSH_MODE_CONFIRMATION=I_HAVE_HETZNER_CONSOLE_AND_ACCEPT_CLASSIC_SSH \
     scripts/install-postgres-firewall.sh
   ```

   The installer intentionally disables/masks Ubuntu's socket-activated
   `ssh.socket`; requiring a normal post-network firewall from that early socket
   creates a systemd ordering cycle. Keep Hetzner Console open, confirm a second
   public-interface SSH login through classic `ssh.service`, and only then
   close the original session.

   Reboot each host once before ingesting real data. After reboot, rerun the
   matching `preflight-*` and verify that Docker/SSH did not start without the
   firewall service. Caddy intentionally has no automatic restart; after every
   reboot it stays closed until `wait-ready.sh`, the local smoke, and
   `up-proxy.sh` pass again.

   PostgreSQL is also deliberately `restart: "no"`: after a DB-host reboot run
   `preflight-postgres.sh` and `up-postgres.sh` explicitly. This preserves the
   restore-pending semantic gate across host/Docker failures.

10. On the PostgreSQL host, render and validate the HBA file, then start the
   database. PostgreSQL uses host networking and binds only its exact RFC1918
   address; it has no Docker-published port:

   ```bash
   scripts/render-pg-hba.sh 10.42.0.10 runtime/pg_hba.conf
   scripts/preflight-postgres.sh
   scripts/up-postgres.sh
   scripts/write-postgres-release-record.sh
   ```

11. Acquire/freeze a real corpus and export its matching private route set using
   the SkyPulse CLIs. Before transfer, run the full proof on the trusted
   acquisition/build machine:

   ```bash
   PRIVATE_OBSERVATION_JOURNAL=/secure/acquisition/observations.private.ndjson \
     scripts/preflight-artifacts.sh
   ```

   Startup rechecks the public corpus and private route bytes again; the private
   observation journal itself is not copied to the app host.

   Install the public directory as root-owned mode `0555` and its
   `corpus.manifest.json`/`accounts.ak32` files as `0444`. Install
   `${PRIVATE_ROUTING_ROOT}` itself, `${PRIVATE_ROUTING_ROOT}/<profile-id>`, and
   every configured growth-profile directory with owner/group `10001:10001`,
   directory mode `0700`, and both private files mode `0600`. Never replace an active artifact in place: copy to a new
   directory, verify it, then change configuration during a controlled restart.
12. Build the two local images and record their immutable image IDs. The
    current wrapper intentionally requires the default rootful Buildx `docker`
    driver so its locally verified TAP base cannot be replaced by a registry
    lookup:

   ```bash
   scripts/verify-source.sh
   scripts/build-images.sh
   ```

13. Start the single app stack with `ENABLE_PUBLIC_PROXY=false`, validate the
    private loopback path, and wait for synchronization:

    ```bash
   scripts/preflight-app.sh
   scripts/up-app.sh
   scripts/wait-ready.sh
   scripts/smoke.sh
   scripts/wait-synchronized.sh
   scripts/write-release-record.sh
   ```

   The two hosts deliberately produce separate private
   `release-record.app.json` and `release-record.postgres.json` files. Store
   both beside the encrypted whole-cluster backup; neither is public evidence.

   Before treating the stream as operational or opening Caddy, create the
   first stopped-ingestion recovery point and prove it. On host A stop all
   three services, on host B take the backup while PostgreSQL remains running,
   copy the completed directory through the separately approved encrypted
   off-host channel, and run the disposable drill against the local verified
   copy:

   ```bash
   # Host A
   sudo scripts/stop-app.sh

   # Host B, only after independently confirming host A is stopped
   sudo env \
     SKYPULSE_INGESTION_STOPPED=I_STOPPED_APP_AND_TAP \
     SKYPULSE_APP_HOST_STOPPED=I_VERIFIED_CADDY_APP_AND_TAP_STOPPED_ON_HOST_A \
     scripts/backup-postgres.sh
   sudo env \
     SKYPULSE_RESTORE_DRILL_CONFIRMATION=I_WILL_DESTROY_ONLY_THE_SCRIPT_CREATED_VOLUME \
     scripts/drill-postgres-backup.sh /absolute/verified/backup

   # Host A, after the backup and drill pass
   sudo scripts/up-app.sh
   sudo scripts/wait-ready.sh
   sudo scripts/smoke.sh
   sudo scripts/wait-synchronized.sh
   ```

   If the encrypted off-host recovery-set prerequisite is not yet implemented,
   stop here: keep the endpoint private and use only synthetic/limited
   calibration data.

   The safest default is `ENABLE_PUBLIC_PROXY=false` plus an SSH tunnel. For a
   public endpoint, generate the bcrypt hash interactively so the plaintext is
   never an argument:

   ```bash
   python3 scripts/validate-runtime-env.py deploy .env
   set -a; . ./.env; set +a
   docker run --rm -it --entrypoint caddy "$CADDY_IMAGE" hash-password
   ```

   Generate one separate 64-hex UI password and retain it only in a root-owned
   credential file. Save the resulting bcrypt hash in another mode-0600 file,
   and run:

   ```bash
   scripts/render-caddyfile.sh example.org operator /secure/caddy.hash /srv/skypulse/config/Caddyfile
   ```

   Create the curl credential file used by the public smoke. It must contain
   exactly one line; arbitrary curl options (including `insecure`, proxy or
   alternate URL settings) are rejected:

   ```bash
   ui_password=$(</secure/operator/caddy-ui-password)
   [[ "$ui_password" =~ ^[0-9a-f]{64}$ ]]
   printf 'user = "operator:%s"\n' "$ui_password" \
     | sudo tee /srv/skypulse/secrets/operator.curl.conf >/dev/null
   unset ui_password
   sudo chown root:root /srv/skypulse/secrets/operator.curl.conf
   sudo chmod 0600 /srv/skypulse/secrets/operator.curl.conf
   ```

   Only after creating the Caddyfile, both Caddy state directories, and the
   curl credential file, set `ENABLE_PUBLIC_PROXY=true`,
   `SKYPULSE_PUBLIC_BASE_URL=https://example.org` and
   `SKYPULSE_CURL_CONFIG=/srv/skypulse/secrets/operator.curl.conf` in `.env`
   and rerun `scripts/preflight-app.sh`, then `scripts/up-proxy.sh`. The smoke
   first requires unauthenticated HTTP 401,
   then authenticated TLS/hostname validation, case-insensitive operator-path
   blocking, a bounded query and an SSE heartbeat.

   After `up-proxy.sh` succeeds, rerun `scripts/write-release-record.sh` so the
   private app-host record includes the enabled `.env`, exact Caddyfile and
   operator credential-file hash. Archive this replacement record with the
   matching database-host record and backup.

   Create `${CADDY_DATA_DIR}` and `${CADDY_CONFIG_DIR}` as root-owned mode-0700
   directories before `preflight-app.sh`.

   Stock Caddy automatic HTTPS requires Internet-reachable ACME validation.
   With this template that means allowing 80/443 from `0.0.0.0/0` and `::/0`
   for issuance and renewal; Basic auth still protects queries. If that public
   exposure is unacceptable, keep `ENABLE_PUBLIC_PROXY=false` and use SSH/VPN.
   A CIDR-restricted public endpoint needs a separately reviewed custom
   certificate or DNS-challenge Caddy build and is not silently emulated here.

   Operator health/capacity/growth commands always run through loopback or an
   SSH/VPN tunnel. The public Caddy route deliberately returns 404 for
   `/health` and `/api/corpus-capacity*`, strips authorization headers before
   proxying, and must remain firewall-restricted because the application has no
   general query rate limiter.

`/health` is readiness, not liveness. It normally returns 503 throughout
PostgreSQL validation, Memory-index rebuild, rolling-window catch-up, corpus
verification, TAP connection and exact route replay. At a million accounts this
may take hours. Never configure a supervisor to restart solely because
`/health` returns 503.

Readiness proves the runtime/rebuild/provisioning boundary, not that every
selected repository has finished historical synchronization. Exact
selected-corpus follower/engagement measurements additionally require
`wait-synchronized.sh` to observe `synchronizedAccountCount == activeCorpusCap`.

Before the first production use and before every cap increase, run a disposable
restore drill against the newest stopped-cluster backup:

```bash
sudo env \
  SKYPULSE_RESTORE_DRILL_CONFIRMATION=I_WILL_DESTROY_ONLY_THE_SCRIPT_CREATED_VOLUME \
  scripts/drill-postgres-backup.sh /absolute/backup
```

The drill checks the closed file set and hashes, native `pg_verifybackup`, both
current database credentials, exact SkyPulse runtime identity/migrations and
the TAP v3 marker before deleting only its own temporary container and volume.

## Runtime capacity growth

The capacity is an admission cap, not an instantaneous row count. It can only
move monotonically to a larger, pre-reviewed exact-prefix profile already
listed in `runtime/app.env`, with a matching private route manifest. It cannot
shrink and it cannot accept an arbitrary number.

Request a configured profile with:

```bash
scripts/request-growth.sh accounts-100k
scripts/status.sh
```

The request is durable and restartable. It first inserts the suffix baseline in
PostgreSQL, then advances in-process admission, then provisions the exact TAP
repository set, and finally promotes the target. Existing queries continue
during a healthy transition. Do not remove a persisted active or pending
profile from configuration after a restart.

After `wait-synchronized.sh` confirms the promoted target and `status.sh`
shows no requested profile, close Caddy and stop app/TAP. Update only
`EXPECTED_ACTIVE_PROFILE_ID` and `EXPECTED_ACTIVE_CORPUS_CAP` in the protected
`.env` on **both** release copies/hosts; immutable `EXPECTED_BASE_*` values do
not change. Rerun both preflights, write both release records, take a new
whole-cluster backup, and pass its restore drill before reopening traffic.
Keep the old release `.env` beside its old backup: expectations are part of
selecting the correct restore point, not a value to infer during recovery.

## Honest limits

- The checked package is `Orleans.SearchableStorage` `1.0.0-rc.2`, not a frozen
  stable qualification target.
- Previous cloud verification skipped the real PostgreSQL suite. The local
  operator must run it against a disposable database.
- There is no complete checked-in source-to-HTTP recovery harness. These
  deployment smokes are operational checks, not that missing evidence.
- PostgreSQL retention primitives exist, but no maintenance worker/CLI is wired
  into the Web host. Do not run indefinitely without an approved retention
  operator; do not replace it with ad-hoc SQL deletion.
- The account cap does not cap record/follow/activity rows. Disk sufficiency
  must be measured at 10K and 100K before requesting 1M.
- Memory index state is intentionally ephemeral and rebuilt from PostgreSQL.
- `backup-postgres.sh` creates only the database component of a recovery set.
  This calibration bundle does not choose an off-host storage/encryption
  vendor. Long-running production is blocked until the local operator supplies
  and restore-tests an encrypted off-host procedure that preserves the paired
  app/PG release records, matching database secret files, public corpus,
  private routes, TLS material, and the physical backup without exposing them
  in Git or command arguments.
- The checked infrastructure does not implement block-device encryption.
  Real private data is blocked until the local operator supplies and verifies
  encrypted-at-rest storage for database data, routes, secrets and backups.
