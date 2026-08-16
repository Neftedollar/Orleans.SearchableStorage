#!/usr/bin/env bash
set -euo pipefail
umask 077

die() {
    printf 'ERROR: %s\n' "$*" >&2
    exit 1
}

[[ $# -eq 1 ]] || die 'usage: generate-secrets.sh /absolute/secure/output'
out=$1
[[ "$out" = /* ]] || die 'output path must be absolute'
command -v openssl >/dev/null 2>&1 || die 'openssl is required'

if [[ -e "$out" ]]; then
    die "refusing to overwrite existing path: $out"
fi

install -d -m 0700 "$out" "$out/app" "$out/tap" "$out/postgres" "$out/operator"

app_db=$(openssl rand -hex 32)
tap_db=$(openssl rand -hex 32)
tap_admin=$(openssl rand -hex 32)
growth_admin=$(openssl rand -hex 32)
postgres_admin=$(openssl rand -hex 32)
caddy_ui=$(openssl rand -hex 32)

printf '%s' "$app_db" > "$out/app/postgres-password"
printf '%s' "$app_db" > "$out/postgres/app-password"
printf '%s' "$tap_db" > "$out/tap/postgres-password"
printf '%s' "$tap_db" > "$out/postgres/tap-password"
printf '%s' "$tap_admin" > "$out/app/tap-admin-password"
printf '%s' "$tap_admin" > "$out/tap/tap-admin-password"
printf '%s' "$growth_admin" > "$out/app/corpus-growth-admin-token"
printf '%s' "$postgres_admin" > "$out/postgres/admin-password"
printf '%s' "$caddy_ui" > "$out/operator/caddy-ui-password"

uuid=$(python3 - <<'PY'
import uuid
print(uuid.uuid4())
PY
)
printf '%s\n' "$uuid" > "$out/source-instance-id"

find "$out" -type d -exec chmod 0700 {} +
find "$out" -type f -exec chmod 0600 {} +

unset app_db tap_db tap_admin growth_admin postgres_admin caddy_ui uuid
printf 'Secrets created once in %s\n' "$out"
printf '%s\n' 'Distribute only app/ and tap/ to host A, postgres/ to host B.'
printf '%s\n' 'Copy source-instance-id into runtime/app.env and never rotate it on restart.'

