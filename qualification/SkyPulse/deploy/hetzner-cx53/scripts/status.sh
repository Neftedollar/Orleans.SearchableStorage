#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
DEPLOY_DIR=$(cd -- "$SCRIPT_DIR/.." && pwd)
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

require_command curl
base_url=${SKYPULSE_BASE_URL:-http://127.0.0.1:5080}
[[ "$base_url" == http://127.0.0.1:5080 ]] || die 'status must use the loopback operator origin'
curl_args=(--disable --silent --show-error --fail-with-body --noproxy '*' --proto '=http'
    --connect-timeout 5 --max-time 30)

printf '%s\n' 'SkyPulse health:'
curl "${curl_args[@]}" "$base_url/health"
printf '\n%s\n' 'Corpus capacity:'
curl "${curl_args[@]}" "$base_url/api/corpus-capacity"
printf '\n%s\n' 'TAP repository count:'

load_deploy_env
require_hex_secret "$TAP_SECRET_DIR/tap-admin-password"
tap_password=$(<"$TAP_SECRET_DIR/tap-admin-password")
printf 'user = "admin:%s"\n' "$tap_password" \
    | curl --disable --config - --silent --show-error --fail-with-body \
      --noproxy '*' --proto '=http' --connect-timeout 5 --max-time 30 \
      http://127.0.0.1:2480/stats/repo-count
printf '\n'
unset tap_password
