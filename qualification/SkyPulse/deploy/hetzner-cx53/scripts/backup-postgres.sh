#!/usr/bin/env bash
set -euo pipefail
umask 077

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
DEPLOY_DIR=$(cd -- "$SCRIPT_DIR/.." && pwd)
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

[[ ${SKYPULSE_INGESTION_STOPPED:-} == I_STOPPED_APP_AND_TAP ]] \
    || die 'stop app then TAP and set SKYPULSE_INGESTION_STOPPED=I_STOPPED_APP_AND_TAP'
[[ ${SKYPULSE_APP_HOST_STOPPED:-} == I_VERIFIED_CADDY_APP_AND_TAP_STOPPED_ON_HOST_A ]] \
    || die 'set SKYPULSE_APP_HOST_STOPPED only after verifying all three host-A containers are stopped'
"$SCRIPT_DIR/preflight-postgres.sh"
load_deploy_env
require_command docker
require_postgres_17_11_image
require_expected_runtime_identity
validate_control_env postgres "$DEPLOY_DIR/runtime/postgres.env"
postgres_image_id=$(docker image inspect --format '{{.Id}}' "$POSTGRES_IMAGE")
require_running_release_container skypulse-postgres postgres "$postgres_image_id" skypulse-postgres
require_container_restart_policy skypulse-postgres no
require_container_bind_source skypulse-postgres /var/lib/postgresql/data "$POSTGRES_DATA_DIR"
require_container_bind_source skypulse-postgres /var/lib/postgresql/backups "$POSTGRES_BACKUP_DIR"
stamp=$(date -u '+%Y%m%dT%H%M%SZ')

assert_no_runtime_sessions() {
    local count
    count=$(compose_postgres exec -T postgres psql -U skypulse_admin -d postgres \
        --no-psqlrc --tuples-only --no-align \
        --command "SELECT count(*) FROM pg_stat_activity WHERE usename IN ('skypulse_app','skypulse_tap')")
    [[ "$count" == 0 ]] \
        || die "host A is not fully stopped: PostgreSQL still has $count SkyPulse/TAP session(s)"
}

assert_no_runtime_sessions

capacity=$(compose_postgres exec -T postgres psql -U skypulse_admin -d skypulse \
    --no-psqlrc --tuples-only --no-align --field-separator='|' --command '
SELECT runtime.source_instance_id::text,
       capacity.base_profile_id,
       capacity.base_corpus_cap,
       capacity.active_profile_id,
       capacity.active_corpus_cap,
       capacity.target_profile_id,
       capacity.target_corpus_cap
FROM skypulse.runtime_manifest AS runtime
JOIN skypulse.corpus_capacity AS capacity ON capacity.capacity_id = runtime.manifest_id
WHERE runtime.manifest_id = 1;')
IFS='|' read -r source_id base_profile base_cap active_profile active_cap target_profile target_cap <<<"$capacity"
[[ -z "$target_profile" && -z "$target_cap" ]] \
    || die 'refusing to back up while corpus growth is pending'
[[ "$source_id" == "$EXPECTED_SOURCE_INSTANCE_ID" \
    && "$base_profile" == "$EXPECTED_BASE_PROFILE_ID" \
    && "$base_cap" == "$EXPECTED_BASE_CORPUS_CAP" \
    && "$active_profile" == "$EXPECTED_ACTIVE_PROFILE_ID" \
    && "$active_cap" == "$EXPECTED_ACTIVE_CORPUS_CAP" ]] \
    || die 'live capacity identity differs from the reviewed .env expectations'
migrations=$(compose_postgres exec -T postgres psql -U skypulse_admin -d skypulse \
    --no-psqlrc --tuples-only --no-align \
    --command "SELECT string_agg(version::text, ',' ORDER BY version) FROM skypulse.schema_migration")
[[ "$migrations" == 1,2 ]] || die "live migration set is not 1,2: $migrations"
tap_marker=$(compose_postgres exec -T postgres psql -U skypulse_admin -d skypulse_tap \
    --no-psqlrc --tuples-only --no-align \
    --command 'SELECT version FROM metadata_only_overlay_states WHERE id=1')
[[ "$tap_marker" == 3 ]] || die "live TAP overlay marker is not version 3: $tap_marker"
identity=$(python3 - "$source_id" "$base_profile" "$base_cap" "$active_profile" "$active_cap" <<'PY'
import json, sys
source, base_profile, base_cap, active_profile, active_cap = sys.argv[1:]
print(json.dumps({
    "sourceInstanceId": source,
    "baseProfileId": base_profile,
    "baseCorpusCap": int(base_cap),
    "activeProfileId": active_profile,
    "activeCorpusCap": int(active_cap),
    "targetProfileId": None,
    "targetCorpusCap": None,
    "migrationVersions": [1, 2],
    "tapOverlayVersion": 3,
}, sort_keys=True, separators=(",", ":")))
PY
)

# shellcheck disable=SC2016 # This program is evaluated by the container shell.
compose_postgres exec -T postgres bash -o pipefail -ceu '
stamp=$1
identity=$2
partial="/var/lib/postgresql/backups/${stamp}.partial"
final="/var/lib/postgresql/backups/${stamp}"
test ! -e "$partial" && test ! -e "$final"
install -d -m 0700 "$partial"
pg_basebackup \
  --host=/var/run/postgresql \
  --username="$POSTGRES_USER" \
  --pgdata="$partial" \
  --format=plain --wal-method=stream --checkpoint=fast
pg_verifybackup "$partial"
printf "%s\n" "$identity" > "$partial/SKYPULSE_BACKUP_IDENTITY.json"
chmod 0600 "$partial/SKYPULSE_BACKUP_IDENTITY.json"
if find "$partial" -mindepth 1 ! -type d ! -type f -print -quit | grep -q .; then
  echo "backup contains an unsupported symlink or special file" >&2
  exit 1
fi
(cd "$partial" && find . -type f ! -name SHA256SUMS -print0 | sort -z | xargs -0 sha256sum) > "$partial/SHA256SUMS"
' -- "$stamp" "$identity"

assert_no_runtime_sessions
capacity_after=$(compose_postgres exec -T postgres psql -U skypulse_admin -d skypulse \
    --no-psqlrc --tuples-only --no-align --field-separator='|' --command '
SELECT runtime.source_instance_id::text,
       capacity.base_profile_id,
       capacity.base_corpus_cap,
       capacity.active_profile_id,
       capacity.active_corpus_cap,
       capacity.target_profile_id,
       capacity.target_corpus_cap
FROM skypulse.runtime_manifest AS runtime
JOIN skypulse.corpus_capacity AS capacity ON capacity.capacity_id = runtime.manifest_id
WHERE runtime.manifest_id = 1;')
[[ "$capacity_after" == "$capacity" ]] \
    || die "runtime capacity changed during backup; leaving ${stamp}.partial unfinalized"
migrations_after=$(compose_postgres exec -T postgres psql -U skypulse_admin -d skypulse \
    --no-psqlrc --tuples-only --no-align \
    --command "SELECT string_agg(version::text, ',' ORDER BY version) FROM skypulse.schema_migration")
tap_marker_after=$(compose_postgres exec -T postgres psql -U skypulse_admin -d skypulse_tap \
    --no-psqlrc --tuples-only --no-align \
    --command 'SELECT version FROM metadata_only_overlay_states WHERE id=1')
[[ "$migrations_after" == "$migrations" && "$tap_marker_after" == "$tap_marker" ]] \
    || die "schema or TAP identity changed during backup; leaving ${stamp}.partial unfinalized"
# shellcheck disable=SC2016 # This program is evaluated by the container shell.
compose_postgres exec -T postgres bash -o pipefail -ceu '
stamp=$1
partial="/var/lib/postgresql/backups/${stamp}.partial"
final="/var/lib/postgresql/backups/${stamp}"
test -d "$partial" && test ! -e "$final"
mv "$partial" "$final"
' -- "$stamp"

note "Whole-cluster physical backup completed: $POSTGRES_BACKUP_DIR/$stamp"
note 'It contains both SkyPulse and TAP databases. Copy it encrypted off-host and perform a restore drill.'
