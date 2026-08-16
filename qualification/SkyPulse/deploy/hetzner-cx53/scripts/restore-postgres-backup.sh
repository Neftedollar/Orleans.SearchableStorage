#!/usr/bin/env bash
set -euo pipefail
umask 077

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
DEPLOY_DIR=$(cd -- "$SCRIPT_DIR/.." && pwd)
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

[[ $# -eq 1 ]] || die 'usage: restore-postgres-backup.sh /absolute/verified/backup'
[[ ${SKYPULSE_RESTORE_CONFIRMATION:-} == I_STOPPED_POSTGRES_AND_SELECTED_AN_EMPTY_DATA_DIRECTORY ]] \
    || die 'set the exact restore confirmation after stopping PostgreSQL'
[[ ${SKYPULSE_APP_HOST_STOPPED:-} == I_VERIFIED_CADDY_APP_AND_TAP_STOPPED_ON_HOST_A ]] \
    || die 'set SKYPULSE_APP_HOST_STOPPED only after verifying all three host-A containers are stopped'
require_command docker
require_command python3
require_command realpath
require_rootful_docker
load_deploy_env
require_postgres_17_11_image
require_expected_runtime_identity
for container in skypulse-caddy skypulse-app skypulse-tap skypulse-postgres; do
    [[ $(docker container inspect --format '{{.State.Running}}' "$container" 2>/dev/null || true) != true ]] \
        || die "$container is still running; a whole-cluster restore requires all SkyPulse services stopped"
done

backup=$1
[[ "$backup" == /* ]] || die 'backup path must be absolute'
require_directory "$backup"
verify_postgres_backup_tree "$backup"
verify_postgres_backup_identity "$backup"
require_file "$backup/backup_manifest"
require_file "$backup/PG_VERSION"
require_directory "$backup/pg_wal"
[[ $(<"$backup/PG_VERSION") == 17 ]] || die 'backup PG_VERSION must be exactly 17'
require_directory "$POSTGRES_DATA_DIR"
backup_real=$(realpath -e -- "$backup")
restore_real=$(realpath -e -- "$POSTGRES_DATA_DIR")
[[ "$backup_real" != "$restore_real" \
    && "$backup_real/" != "$restore_real/"* \
    && "$restore_real/" != "$backup_real/"* ]] \
    || die 'backup source and restore target must be disjoint, non-nested directories'

if find "$POSTGRES_DATA_DIR" -mindepth 1 -print -quit | grep -q .; then
    die "restore target is not empty: $POSTGRES_DATA_DIR"
fi

docker run --rm --network none \
    --volume "$backup:/backup:ro" \
    --entrypoint pg_verifybackup "$POSTGRES_IMAGE" \
    --ignore=SHA256SUMS --ignore=SKYPULSE_BACKUP_IDENTITY.json /backup

pg_uid=$(docker run --rm --entrypoint id "$POSTGRES_IMAGE" -u postgres)
pg_gid=$(docker run --rm --entrypoint id "$POSTGRES_IMAGE" -g postgres)
[[ "$pg_uid" =~ ^[0-9]+$ && "$pg_gid" =~ ^[0-9]+$ ]] \
    || die 'could not resolve postgres UID/GID from the pinned image'

# Arm the fail-closed startup guard before writing any restored cluster bytes.
# If extraction, ownership repair, or the host itself fails afterward, the
# marker remains and up-postgres.sh still requires an explicit host-A stop
# confirmation before PostgreSQL can start.
printf '%s\n' 'restore pending: keep host A stopped until up-postgres semantic validation passes' \
    > "$POSTGRES_DATA_DIR/.skypulse-restore-pending"
chmod 0600 "$POSTGRES_DATA_DIR/.skypulse-restore-pending"

docker run --rm --network none --user 0:0 \
    --volume "$backup:/backup:ro" \
    --volume "$POSTGRES_DATA_DIR:/restore" \
    --entrypoint bash "$POSTGRES_IMAGE" -o pipefail -ceu '
uid=$1
gid=$2
chown "$uid:$gid" /restore
chmod 0700 /restore
install -d -m 0700 -o "$uid" -g "$gid" /restore/pgdata
(cd /backup && tar --exclude=./SHA256SUMS --exclude=./SKYPULSE_BACKUP_IDENTITY.json -cf - .) \
  | (cd /restore/pgdata && tar -xf -)
chown -R "$uid:$gid" /restore/pgdata
chmod 0700 /restore/pgdata /restore/pgdata/pg_wal
' -- "$pg_uid" "$pg_gid"

note "Restored the verified whole cluster into $POSTGRES_DATA_DIR/pgdata."
note 'Do not delete the backup. Run preflight-postgres.sh, start PostgreSQL, then verify both databases before opening app traffic.'
