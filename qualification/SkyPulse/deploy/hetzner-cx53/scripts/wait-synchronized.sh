#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

require_command curl
require_command python3
base_url=${SKYPULSE_BASE_URL:-http://127.0.0.1:5080}
[[ "$base_url" == http://127.0.0.1:5080 ]] || die 'sync status must use the loopback operator origin'
timeout_seconds=${SKYPULSE_SYNC_TIMEOUT_SECONDS:-86400}
[[ "$timeout_seconds" =~ ^[0-9]+$ && "$timeout_seconds" -ge 60 ]] \
    || die 'SKYPULSE_SYNC_TIMEOUT_SECONDS must be an integer >= 60'

curl_args=(--disable --silent --show-error --noproxy '*' --proto '=http'
    --connect-timeout 5 --max-time 30)

deadline=$((SECONDS + timeout_seconds))
while (( SECONDS < deadline )); do
    body=$(mktemp)
    code=$(curl "${curl_args[@]}" --output "$body" --write-out '%{http_code}' \
        "$base_url/api/corpus-capacity" || true)
    if [[ "$code" == 200 ]] && python3 - "$body" <<'PY'
import json, sys
v = json.load(open(sys.argv[1], encoding='utf-8'))
active = v.get('activeCorpusCap')
synced = v.get('synchronizedAccountCount')
requested = v.get('requestedProfileId')
raise SystemExit(0 if isinstance(active, int) and synced == active and requested is None else 1)
PY
    then
        rm -f "$body"
        note 'Every account in the active selected corpus has completed synchronization.'
        exit 0
    fi
    rm -f "$body"
    sleep 30
done

die "selected-corpus synchronization did not complete within $timeout_seconds seconds"
