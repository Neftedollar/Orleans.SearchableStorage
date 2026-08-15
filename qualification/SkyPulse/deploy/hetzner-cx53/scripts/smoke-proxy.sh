#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

require_command curl
require_command python3
base_url=${SKYPULSE_PUBLIC_BASE_URL:-}
[[ "$base_url" =~ ^https://[A-Za-z0-9.-]+$ ]] \
    || die 'SKYPULSE_PUBLIC_BASE_URL must be an origin such as https://skypulse.example.org'
[[ -n ${SKYPULSE_CURL_CONFIG:-} ]] \
    || die 'SKYPULSE_CURL_CONFIG with Caddy Basic credentials is required'
require_curl_basic_config "$SKYPULSE_CURL_CONFIG"

curl_args=(--disable --silent --show-error --noproxy '*' --proto '=https' --tlsv1.2 \
    --no-insecure --config "$SKYPULSE_CURL_CONFIG" --connect-timeout 5)
deadline=$((SECONDS + 300))
while (( SECONDS < deadline )); do
    if curl "${curl_args[@]}" --fail --max-time 20 "$base_url/" >/dev/null 2>&1; then
        break
    fi
    sleep 5
done
(( SECONDS < deadline )) || die 'public HTTPS origin did not become reachable within 300 seconds'

unauthenticated_code=$(curl --disable --silent --show-error --noproxy '*' --proto '=https' \
    --tlsv1.2 --no-insecure --output /dev/null --write-out '%{http_code}' \
    --connect-timeout 5 --max-time 20 "$base_url/")
[[ "$unauthenticated_code" == 401 ]] \
    || die "public origin must require Basic authentication (found HTTP $unauthenticated_code)"

for private_path in /health /health/ /HeAlTh /api/corpus-capacity /API/Corpus-Capacity; do
    code=$(curl "${curl_args[@]}" --output /dev/null --write-out '%{http_code}' \
        --max-time 20 "$base_url$private_path")
    [[ "$code" == 404 ]] || die "$private_path must be hidden by the public proxy (found HTTP $code)"
done

session_response=$(mktemp)
events=$(mktemp)
trap 'rm -f "$session_response" "$events"' EXIT
curl "${curl_args[@]}" --fail-with-body --max-time 30 \
    --header 'Content-Type: application/json' \
    --data '{"pageSize":1,"currentPostCount":{"minimum":0}}' \
    "$base_url/api/query-sessions" > "$session_response"
session_id=$(python3 - "$session_response" <<'PY'
import json, sys
value = json.load(open(sys.argv[1], encoding='utf-8'))
session_id = value.get('sessionId')
if not isinstance(session_id, str) or not session_id:
    raise SystemExit('public query session id is missing')
print(session_id)
PY
)

cleanup_session() {
    curl "${curl_args[@]}" --max-time 20 --request DELETE \
        "$base_url/api/query-sessions/$session_id" >/dev/null 2>&1 || true
}
trap 'cleanup_session; rm -f "$session_response" "$events"' EXIT

set +e
curl "${curl_args[@]}" --fail --no-buffer --max-time 20 \
    "$base_url/api/query-sessions/$session_id/events" > "$events"
sse_status=$?
set -e
[[ "$sse_status" -eq 0 || "$sse_status" -eq 28 ]] \
    || die "public SSE request failed with curl status $sse_status"
grep -Fq 'event: heartbeat' "$events" || die 'public SSE heartbeat was not observed'

cleanup_session
trap 'rm -f "$session_response" "$events"' EXIT
note 'Public TLS, private-route blocking, bounded query and SSE smoke passed.'
