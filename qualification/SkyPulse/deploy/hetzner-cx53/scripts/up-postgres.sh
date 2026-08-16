#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
DEPLOY_DIR=$(cd -- "$SCRIPT_DIR/.." && pwd)
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

"$SCRIPT_DIR/preflight-postgres.sh"
load_deploy_env
restore_pending=false
if [[ -f "$POSTGRES_DATA_DIR/.skypulse-restore-pending" ]]; then
    restore_pending=true
    [[ ${SKYPULSE_APP_HOST_STOPPED:-} == I_VERIFIED_CADDY_APP_AND_TAP_STOPPED_ON_HOST_A ]] \
        || die 'restored cluster may start only after verifying Caddy/app/TAP remain stopped on host A'
fi
postgres_validated=false
stop_unvalidated_postgres() {
    if [[ "$postgres_validated" != true ]]; then
        stop_exact_compose_container skypulse-postgres postgres 120 skypulse-postgres
    fi
}
trap stop_unvalidated_postgres EXIT
compose_postgres up -d --remove-orphans
healthy=false
for _ in $(seq 1 90); do
    running=$(docker container inspect --format '{{.State.Running}}' skypulse-postgres 2>/dev/null || true)
    health=$(docker container inspect --format '{{if .State.Health}}{{.State.Health.Status}}{{end}}' \
        skypulse-postgres 2>/dev/null || true)
    if [[ "$running" == true && "$health" == healthy ]]; then
        healthy=true
        break
    fi
    [[ "$running" != false ]] || break
    sleep 2
done
if [[ "$healthy" != true ]]; then
    compose_postgres logs --tail 100 postgres >&2 || true
    die 'PostgreSQL did not reach its checked healthy state'
fi
postgres_image_id=$(docker image inspect --format '{{.Id}}' "$POSTGRES_IMAGE")
require_running_release_container skypulse-postgres postgres "$postgres_image_id" skypulse-postgres
require_container_restart_policy skypulse-postgres no
require_container_bind_source skypulse-postgres /var/lib/postgresql/data "$POSTGRES_DATA_DIR"
require_container_bind_source skypulse-postgres /var/lib/postgresql/backups "$POSTGRES_BACKUP_DIR"
semantic_state=$(compose_postgres exec -T postgres psql \
    -U skypulse_admin -d postgres --no-psqlrc --tuples-only --no-align \
    --command "
SELECT CASE WHEN
    (SELECT count(*) FROM pg_roles WHERE rolname IN ('skypulse_app','skypulse_tap')) = 2
    AND NOT EXISTS (
        SELECT 1 FROM pg_roles
        WHERE rolname IN ('skypulse_app','skypulse_tap')
          AND (NOT rolcanlogin OR rolsuper OR rolcreatedb OR rolcreaterole OR rolinherit OR rolreplication)
    )
    AND EXISTS (
        SELECT 1 FROM pg_database
        WHERE datname='skypulse' AND pg_get_userbyid(datdba)='skypulse_app' AND datallowconn
    )
    AND EXISTS (
        SELECT 1 FROM pg_database
        WHERE datname='skypulse_tap' AND pg_get_userbyid(datdba)='skypulse_tap' AND datallowconn
    )
THEN 'ready' ELSE 'invalid' END;")
if [[ "$semantic_state" != ready ]]; then
    compose_postgres logs --tail 100 postgres >&2 || true
    compose_postgres stop --timeout 120 postgres || true
    die 'PostgreSQL health passed but required SkyPulse/TAP roles or databases are invalid'
fi
if [[ "$restore_pending" == true ]]; then
    unlink "$POSTGRES_DATA_DIR/.skypulse-restore-pending"
fi
postgres_validated=true
trap - EXIT
compose_postgres ps postgres
note 'PostgreSQL is running, healthy, and has the exact application/TAP database-role boundary.'
