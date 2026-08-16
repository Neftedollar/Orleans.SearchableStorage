#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
DEPLOY_DIR=$(cd -- "$SCRIPT_DIR/.." && pwd)
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

[[ $# -eq 1 ]] || die 'usage: request-growth.sh PROFILE_ID'
profile_id=$1
[[ "$profile_id" =~ ^[a-z0-9]([a-z0-9._-]{0,78}[a-z0-9])?$ ]] \
    || die 'invalid profile id'
require_command curl
"$SCRIPT_DIR/preflight-app.sh"
load_deploy_env
require_hex_secret "$APP_SECRET_DIR/corpus-growth-admin-token"
token=$(<"$APP_SECRET_DIR/corpus-growth-admin-token")
base_url=${SKYPULSE_BASE_URL:-http://127.0.0.1:5080}
[[ "$base_url" == http://127.0.0.1:5080 ]] \
    || die 'growth token may be sent only to the loopback SkyPulse origin'

curl_args=(--disable --silent --show-error --fail-with-body --noproxy '*' --proto '=http'
    --connect-timeout 5 --max-time 60)

printf 'header = "X-SkyPulse-Corpus-Admin: %s"\n' "$token" \
    | curl "${curl_args[@]}" --config - --request POST \
      "$base_url/api/corpus-capacity/$profile_id"
printf '\n'
unset token
