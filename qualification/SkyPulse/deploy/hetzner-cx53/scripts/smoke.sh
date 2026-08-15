#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

require_command curl
require_command python3
base_url=${SKYPULSE_BASE_URL:-http://127.0.0.1:5080}
[[ "$base_url" == http://127.0.0.1:5080 ]] || die 'local smoke must use the loopback operator origin'
curl_args=(--disable --silent --show-error --fail-with-body --noproxy '*' --proto '=http'
    --connect-timeout 5)

health=$(mktemp)
session_response=$(mktemp)
events=$(mktemp)
trap 'rm -f "$health" "$session_response" "$events"' EXIT

curl "${curl_args[@]}" --max-time 30 "$base_url/health" > "$health"
python3 - "$health" <<'PY'
import json, sys
v = json.load(open(sys.argv[1], encoding='utf-8'))
if v.get('status') != 'ready' or v.get('mode') != 'Durable' or v.get('projection') != 'durable-ready':
    raise SystemExit(f'unexpected health payload: {v}')
PY

curl "${curl_args[@]}" --max-time 30 \
    --header 'Content-Type: application/json' \
    --data '{"pageSize":1,"currentPostCount":{"minimum":0}}' \
    "$base_url/api/query-sessions" > "$session_response"
session_id=$(python3 - "$session_response" <<'PY'
import json, sys
v = json.load(open(sys.argv[1], encoding='utf-8'))
sid = v.get('sessionId')
if not isinstance(sid, str) or not sid:
    raise SystemExit(f'query session id missing: {v}')
print(sid)
PY
)

cleanup_session() {
    curl "${curl_args[@]}" --max-time 30 --request DELETE \
        "$base_url/api/query-sessions/$session_id" >/dev/null 2>&1 || true
}
trap 'cleanup_session; rm -f "$health" "$session_response" "$events"' EXIT

set +e
curl "${curl_args[@]}" --no-buffer --max-time 20 \
    "$base_url/api/query-sessions/$session_id/events" > "$events"
sse_status=$?
set -e
[[ "$sse_status" -eq 0 || "$sse_status" -eq 28 ]] || die "SSE request failed with curl status $sse_status"
grep -Fq 'event: heartbeat' "$events" || die 'SSE heartbeat was not observed through the selected endpoint'

cleanup_session
trap 'rm -f "$health" "$session_response" "$events"' EXIT
note 'Durable health, bounded query session and SSE heartbeat smoke passed.'
