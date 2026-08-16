#!/bin/sh
set -eu
umask 077

die() {
    printf '%s\n' "PostgreSQL initialization refused: $*" >&2
    exit 64
}

read_secret() {
    file=$1
    if [ ! -f "$file" ] || [ -L "$file" ]; then
        die "$file must be a regular non-symlink file"
    fi
    value=$(tr -d '\r\n' < "$file")
    [ "$(wc -c < "$file")" -eq 64 ] || die "$file must contain exactly 64 bytes and no newline"
    printf '%s' "$value" | grep -Eq '^[0-9a-f]{64}$' || die "$file must contain lowercase 64-hex"
    printf '%s' "$value"
}

app_password=$(read_secret /run/secrets/app-password)
tap_password=$(read_secret /run/secrets/tap-password)

{
printf "\\set app_password '%s'\n" "$app_password"
printf "\\set tap_password '%s'\n" "$tap_password"
cat <<'SQL'
SELECT format(
    'CREATE ROLE skypulse_app LOGIN PASSWORD %L NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOREPLICATION',
    :'app_password')
WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'skypulse_app') \gexec
ALTER ROLE skypulse_app PASSWORD :'app_password';

SELECT format(
    'CREATE ROLE skypulse_tap LOGIN PASSWORD %L NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOREPLICATION',
    :'tap_password')
WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'skypulse_tap') \gexec
ALTER ROLE skypulse_tap PASSWORD :'tap_password';

SELECT 'CREATE DATABASE skypulse OWNER skypulse_app'
WHERE NOT EXISTS (SELECT 1 FROM pg_database WHERE datname = 'skypulse') \gexec
SELECT 'CREATE DATABASE skypulse_tap OWNER skypulse_tap'
WHERE NOT EXISTS (SELECT 1 FROM pg_database WHERE datname = 'skypulse_tap') \gexec

REVOKE CONNECT ON DATABASE skypulse FROM PUBLIC;
GRANT CONNECT ON DATABASE skypulse TO skypulse_app;
REVOKE CONNECT ON DATABASE skypulse_tap FROM PUBLIC;
GRANT CONNECT ON DATABASE skypulse_tap TO skypulse_tap;
SQL
} | psql --set=ON_ERROR_STOP=1 \
    --username "$POSTGRES_USER" --dbname postgres

unset app_password tap_password
