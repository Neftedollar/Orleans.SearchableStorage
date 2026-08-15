#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
DEPLOY_DIR=$(cd -- "$SCRIPT_DIR/.." && pwd)
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

require_command docker
# Close any previously validated public proxy before a fallible preflight,
# migration, rebuild, or image replacement begins.
stop_caddy_if_present
"$SCRIPT_DIR/preflight-app.sh"
load_deploy_env
# Keep public traffic closed through migrations, rebuild, corpus bootstrap and
# local smoke. up-proxy.sh is the only script that opens Caddy.
stack_started=false
stop_partial_stack() {
    if [[ "$stack_started" != true ]]; then
        stop_exact_compose_container skypulse-app app 90
        stop_exact_compose_container skypulse-tap tap 60
    fi
}
trap stop_partial_stack EXIT
compose_app up -d --remove-orphans tap app
for container in skypulse-tap skypulse-app; do
    [[ $(docker container inspect --format '{{.State.Running}}' "$container" 2>/dev/null || true) == true ]] \
        || die "$container is not running after Compose startup"
done
require_container_restart_policy skypulse-tap unless-stopped
require_container_restart_policy skypulse-app unless-stopped
stack_started=true
trap - EXIT
compose_app ps tap app
if [[ ${ENABLE_PUBLIC_PROXY:-false} == true ]]; then
    note 'Public proxy remains stopped. Run wait-ready.sh, smoke.sh, then up-proxy.sh.'
fi
