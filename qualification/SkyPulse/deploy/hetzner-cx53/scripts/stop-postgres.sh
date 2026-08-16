#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
DEPLOY_DIR=$(cd -- "$SCRIPT_DIR/.." && pwd)
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

require_command docker
stop_exact_compose_container skypulse-postgres postgres 120 skypulse-postgres
note 'PostgreSQL is stopped. Keep host-A Caddy/app/TAP stopped until the selected recovery action completes.'
