#!/bin/sh
set -eu
umask 077

die() {
    printf '%s\n' "TAP startup refused: $*" >&2
    exit 64
}

read_hex_secret() {
    file=$1
    if [ ! -f "$file" ] || [ -L "$file" ]; then
        die "$file must be a regular non-symlink file"
    fi
    mode=$(stat -c '%a' "$file")
    case "$mode" in 400|600) ;; *) die "$file must have mode 0400 or 0600 (found $mode)" ;; esac
    value=$(tr -d '\r\n' < "$file")
    [ "$(wc -c < "$file")" -eq 64 ] || die "$file must contain exactly 64 bytes and no newline"
    printf '%s' "$value" | grep -Eq '^[0-9a-f]{64}$' || die "$file must be lowercase 64-hex"
    printf '%s' "$value"
}

if [ "$#" -ne 1 ] || [ "$1" != run ]; then
    die 'only the run command is permitted'
fi
[ "${TAP_BIND:-}" = '127.0.0.1:2480' ] || die 'TAP_BIND must be 127.0.0.1:2480'
[ "${TAP_ENV:-}" = production ] || die 'TAP_ENV must be production'

for name in \
    TAP_FULL_NETWORK TAP_SIGNAL_COLLECTION TAP_DISABLE_ACKS TAP_WEBHOOK_URL \
    TAP_NO_REPLAY TAP_COLLECTION_FILTERS TAP_OUTBOX_ONLY
do
    eval "present=\${$name+x}"
    [ -z "$present" ] || die "$name must be absent from the production environment"
done

for name in TAP_PG_HOST TAP_PG_PORT TAP_PG_DATABASE TAP_PG_USERNAME; do
    eval "value=\${$name:-}"
    [ -n "$value" ] || die "$name is required"
done
printf '%s' "$TAP_PG_HOST" \
    | grep -Eq '^[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?(\.[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?)+$' \
    || die 'TAP_PG_HOST must be a canonical lowercase dotted DNS hostname'

pg_password=$(read_hex_secret "${TAP_PG_PASSWORD_FILE:-/run/secrets/postgres-password}")
admin_password=$(read_hex_secret "${TAP_ADMIN_PASSWORD_FILE:-/run/secrets/tap-admin-password}")

export TAP_DATABASE_URL="postgres://${TAP_PG_USERNAME}:${pg_password}@${TAP_PG_HOST}:${TAP_PG_PORT}/${TAP_PG_DATABASE}?sslmode=verify-full&sslrootcert=/run/tls/pg-ca.crt"
export TAP_ADMIN_PASSWORD="$admin_password"
unset pg_password admin_password

exec /usr/local/bin/tap run
