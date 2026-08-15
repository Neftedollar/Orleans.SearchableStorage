#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
DEPLOY_DIR=$(cd -- "$SCRIPT_DIR/.." && pwd)
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

require_command docker
stop_exact_compose_container skypulse-caddy caddy 30
stop_exact_compose_container skypulse-app app 90
stop_exact_compose_container skypulse-tap tap 60
note 'SkyPulse ingestion is stopped. Do not remove containers or volumes before backup/rollback decisions.'
