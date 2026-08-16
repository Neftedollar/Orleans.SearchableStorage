#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

require_command curl
require_command python3
base_url=${SKYPULSE_BASE_URL:-http://127.0.0.1:5080}
[[ "$base_url" == http://127.0.0.1:5080 ]] || die 'readiness must use the loopback operator origin'
timeout_seconds=${SKYPULSE_READY_TIMEOUT_SECONDS:-21600}
[[ "$timeout_seconds" =~ ^[0-9]+$ && "$timeout_seconds" -ge 60 ]] \
    || die 'SKYPULSE_READY_TIMEOUT_SECONDS must be an integer >= 60'

curl_args=(--disable --silent --show-error --noproxy '*' --proto '=http'
    --connect-timeout 5 --max-time 30)

deadline=$((SECONDS + timeout_seconds))
last_status=none
while (( SECONDS < deadline )); do
    body=$(mktemp)
    code=$(curl "${curl_args[@]}" --output "$body" --write-out '%{http_code}' "$base_url/health" || true)
    if [[ "$code" == 200 ]] && python3 - "$body" <<'PY'
import json, sys
value = json.load(open(sys.argv[1], encoding='utf-8'))
raise SystemExit(0 if value.get('status') == 'ready' and value.get('mode') == 'Durable' and value.get('projection') == 'durable-ready' else 1)
PY
    then
        rm -f "$body"
        note 'SkyPulse is ready.'
        exit 0
    fi
    if [[ "$code" != "$last_status" ]]; then
        note "readiness HTTP $code; waiting without restarting"
        last_status=$code
    fi
    rm -f "$body"
    sleep 10
done

die "readiness did not open within $timeout_seconds seconds; inspect logs, do not restart-loop on 503"
