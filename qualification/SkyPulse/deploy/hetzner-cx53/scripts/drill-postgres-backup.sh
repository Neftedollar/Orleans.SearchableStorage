#!/usr/bin/env bash
set -euo pipefail
umask 077

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
DEPLOY_DIR=$(cd -- "$SCRIPT_DIR/.." && pwd)
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

[[ $# -eq 1 ]] || die 'usage: drill-postgres-backup.sh /absolute/backup'
[[ ${SKYPULSE_RESTORE_DRILL_CONFIRMATION:-} == I_WILL_DESTROY_ONLY_THE_SCRIPT_CREATED_VOLUME ]] \
    || die 'set the exact restore-drill confirmation'
require_command docker
require_command python3
require_rootful_docker
load_deploy_env
require_postgres_17_11_image
require_expected_runtime_identity
for file in app-password tap-password; do
    require_hex_secret "$POSTGRES_SECRET_DIR/$file"
done

backup=$1
[[ "$backup" == /* ]] || die 'backup path must be absolute'
verify_postgres_backup_tree "$backup"
verify_postgres_backup_identity "$backup"
require_file "$backup/backup_manifest"
require_file "$backup/PG_VERSION"
require_directory "$backup/pg_wal"
[[ $(<"$backup/PG_VERSION") == 17 ]] || die 'backup PG_VERSION must be exactly 17'
docker run --rm --network none \
    --volume "$backup:/backup:ro" \
    --entrypoint pg_verifybackup "$POSTGRES_IMAGE" \
    --ignore=SHA256SUMS --ignore=SKYPULSE_BACKUP_IDENTITY.json /backup

suffix="$(date -u +%Y%m%d%H%M%S)-$$-$RANDOM"
container="skypulse-restore-drill-$suffix"
volume="skypulse-restore-drill-$suffix"
docker container inspect "$container" >/dev/null 2>&1 \
    && die "refusing to reuse existing container $container"
docker volume inspect "$volume" >/dev/null 2>&1 \
    && die "refusing to reuse existing volume $volume"
created_container=false
created_volume=false
cleanup() {
    if [[ "$created_container" == true ]]; then
        docker rm -f "$container" >/dev/null 2>&1 || true
    fi
    if [[ "$created_volume" == true ]]; then
        docker volume rm -f "$volume" >/dev/null 2>&1 || true
    fi
}
trap cleanup EXIT
docker volume create --label skypulse.restore-drill=true "$volume" >/dev/null
created_volume=true

pg_uid=$(docker run --rm --network none --entrypoint id "$POSTGRES_IMAGE" -u postgres)
pg_gid=$(docker run --rm --network none --entrypoint id "$POSTGRES_IMAGE" -g postgres)
[[ "$pg_uid" =~ ^[0-9]+$ && "$pg_gid" =~ ^[0-9]+$ ]] \
    || die 'could not resolve PostgreSQL UID/GID'

docker run --rm --network none --user 0:0 \
    --volume "$backup:/backup:ro" \
    --volume "$volume:/restore" \
    --entrypoint bash "$POSTGRES_IMAGE" -o pipefail -ceu '
uid=$1
gid=$2
install -d -m 0700 -o "$uid" -g "$gid" /restore/pgdata
(cd /backup && tar --exclude=./SHA256SUMS --exclude=./SKYPULSE_BACKUP_IDENTITY.json -cf - .) \
  | (cd /restore/pgdata && tar -xf -)
chown -R "$uid:$gid" /restore/pgdata
chmod 0700 /restore/pgdata /restore/pgdata/pg_wal
printf "%s\n" \
  "local all skypulse_admin trust" \
  "local all all reject" \
  "host skypulse skypulse_app 127.0.0.1/32 scram-sha-256" \
  "host skypulse_tap skypulse_tap 127.0.0.1/32 scram-sha-256" \
  "host all all 127.0.0.1/32 reject" \
  "host all all 0.0.0.0/0 reject" \
  "host all all ::/0 reject" > /restore/drill-pg_hba.conf
chown "$uid:$gid" /restore/drill-pg_hba.conf
chmod 0600 /restore/drill-pg_hba.conf
' -- "$pg_uid" "$pg_gid"

docker create --name "$container" --network none --user "$pg_uid:$pg_gid" \
    --env PGDATA=/var/lib/postgresql/data/pgdata \
    --tmpfs "/var/run/postgresql:rw,nosuid,nodev,uid=$pg_uid,gid=$pg_gid,mode=0770" \
    --tmpfs /tmp:rw,noexec,nosuid,nodev,size=64m \
    --volume "$volume:/var/lib/postgresql/data" \
    --volume "$POSTGRES_SECRET_DIR/app-password:/run/secrets/app-password:ro" \
    --volume "$POSTGRES_SECRET_DIR/tap-password:/run/secrets/tap-password:ro" \
    --entrypoint postgres "$POSTGRES_IMAGE" \
    -c listen_addresses=127.0.0.1 -c ssl=off -c max_connections=20 \
    -c hba_file=/var/lib/postgresql/data/drill-pg_hba.conf >/dev/null
created_container=true
docker start "$container" >/dev/null

ready=false
for _ in $(seq 1 60); do
    if docker exec "$container" pg_isready -h /var/run/postgresql \
        -U skypulse_admin -d postgres >/dev/null 2>&1; then
        ready=true
        break
    fi
    sleep 1
done
[[ "$ready" == true ]] || {
    docker logs "$container" >&2 || true
    die 'restored disposable PostgreSQL did not become ready'
}

databases=$(docker exec "$container" psql -h /var/run/postgresql \
    -U skypulse_admin -d postgres --no-psqlrc --tuples-only --no-align \
    --command "SELECT datname FROM pg_database WHERE datname IN ('skypulse','skypulse_tap') ORDER BY datname")
[[ "$databases" == $'skypulse\nskypulse_tap' ]] \
    || die "restored cluster is missing an application database: $databases"

docker exec "$container" sh -ceu '
umask 077
printf "127.0.0.1:5432:skypulse:skypulse_app:%s\n" "$(cat /run/secrets/app-password)" > /tmp/app.pgpass
printf "127.0.0.1:5432:skypulse_tap:skypulse_tap:%s\n" "$(cat /run/secrets/tap-password)" > /tmp/tap.pgpass
'

manifest=$(docker exec "$container" sh -ceu '
exec env PGPASSFILE=/tmp/app.pgpass psql -h 127.0.0.1 -U skypulse_app -d skypulse --no-psqlrc --tuples-only --no-align \
  --command "SELECT source_instance_id::text || '\''|'\'' || profile_id || '\''|'\'' || corpus_cap::text FROM skypulse.runtime_manifest WHERE manifest_id=1"
')
[[ "$manifest" == "$EXPECTED_SOURCE_INSTANCE_ID|$EXPECTED_BASE_PROFILE_ID|$EXPECTED_BASE_CORPUS_CAP" ]] \
    || die "restored SkyPulse runtime identity is not the selected release: $manifest"
migrations=$(docker exec "$container" sh -ceu '
exec env PGPASSFILE=/tmp/app.pgpass psql -h 127.0.0.1 -U skypulse_app -d skypulse --no-psqlrc --tuples-only --no-align \
  --command "SELECT string_agg(version::text, '\'','\'' ORDER BY version) FROM skypulse.schema_migration"
')
[[ "$migrations" == 1,2 ]] || die "restored SkyPulse migration set is not 1,2: $migrations"
capacity=$(docker exec "$container" sh -ceu '
exec env PGPASSFILE=/tmp/app.pgpass psql -h 127.0.0.1 -U skypulse_app -d skypulse --no-psqlrc --tuples-only --no-align \
  --field-separator="|" \
  --command "SELECT active_profile_id, active_corpus_cap, target_profile_id IS NULL AND target_corpus_cap IS NULL FROM skypulse.corpus_capacity WHERE capacity_id=1"
')
[[ "$capacity" == "$EXPECTED_ACTIVE_PROFILE_ID|$EXPECTED_ACTIVE_CORPUS_CAP|t" ]] \
    || die "restored active/pending corpus capacity is not the selected stable release: $capacity"
tap_marker=$(docker exec "$container" sh -ceu '
exec env PGPASSFILE=/tmp/tap.pgpass psql -h 127.0.0.1 -U skypulse_tap -d skypulse_tap --no-psqlrc --tuples-only --no-align \
  --command "SELECT version FROM metadata_only_overlay_states WHERE id=1"
')
[[ "$tap_marker" == 3 ]] || die "restored TAP overlay marker is not version 3: $tap_marker"
note 'Disposable restore drill passed: hashes, native verification, credentials, runtime identity, migrations and TAP v3 marker.'
