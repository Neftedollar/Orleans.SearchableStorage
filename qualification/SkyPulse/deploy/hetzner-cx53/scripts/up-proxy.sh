#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
DEPLOY_DIR=$(cd -- "$SCRIPT_DIR/.." && pwd)
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

require_command docker
proxy_validated=false
close_unvalidated_proxy() {
    if [[ "$proxy_validated" != true ]]; then
        stop_caddy_if_present
    fi
}
trap close_unvalidated_proxy EXIT
# A stale public proxy must not survive a failed config/image/readiness check.
stop_caddy_if_present
load_deploy_env
[[ ${ENABLE_PUBLIC_PROXY:-false} == true ]] \
    || die 'ENABLE_PUBLIC_PROXY must be true before opening Caddy'
"$SCRIPT_DIR/preflight-app.sh"
validate_control_env images "$DEPLOY_DIR/runtime/images.env"
set -a
# shellcheck disable=SC1091
source "$DEPLOY_DIR/runtime/images.env"
set +a
require_running_release_container skypulse-app app "$SKYPULSE_APP_IMAGE_ID"
require_running_release_container skypulse-tap tap "$SKYPULSE_TAP_IMAGE_ID"
SKYPULSE_BASE_URL=http://127.0.0.1:5080 "$SCRIPT_DIR/wait-ready.sh"
SKYPULSE_BASE_URL=http://127.0.0.1:5080 "$SCRIPT_DIR/smoke.sh"
compose_app --profile public up -d --no-deps --force-recreate caddy
[[ $(docker container inspect --format '{{.State.Running}}' skypulse-caddy) == true ]] \
    || die 'Caddy is not running after recreate'
require_container_restart_policy skypulse-caddy no
"$SCRIPT_DIR/smoke-proxy.sh"
proxy_validated=true
trap - EXIT
compose_app --profile public ps caddy
note 'Caddy is open only after local and public TLS/query/SSE smokes passed.'
