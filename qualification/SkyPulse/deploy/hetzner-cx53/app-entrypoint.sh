#!/bin/sh
# The referenced names are injected environment variables validated below.
# shellcheck disable=SC2154
set -eu
umask 077

die() {
    printf '%s\n' "SkyPulse startup refused: $*" >&2
    exit 64
}

require_value() {
    name=$1
    eval "value=\${$name:-}"
    [ -n "$value" ] || die "$name is required"
    case "$value" in
        *REPLACE_*|*PLACEHOLDER*) die "$name still contains a placeholder" ;;
    esac
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
    printf '%s' "$value" | grep -Eq '^[0-9a-f]{64}$' || die "$file must contain 32 random bytes as lowercase hex"
    printf '%s' "$value"
}

for name in \
    SkyPulse__Durable__ProfileId \
    SkyPulse__Durable__ProfileVersion \
    SkyPulse__Durable__CorpusCap \
    SkyPulse__Durable__ProfilePrefixSha256 \
    SkyPulse__Durable__SourceInstanceId \
    SkyPulse__Durable__CorpusManifestPath \
    SkyPulse__Durable__TapEndpoint \
    SkyPulse__Durable__RoutingManifestPath \
    SKYPULSE_PG_HOST SKYPULSE_PG_PORT SKYPULSE_PG_DATABASE SKYPULSE_PG_USERNAME
do
    require_value "$name"
done

[ "${SkyPulse__Mode:-}" = Durable ] || die 'SkyPulse__Mode must be Durable'
[ "${ASPNETCORE_ENVIRONMENT:-}" = Production ] || die 'ASPNETCORE_ENVIRONMENT must be Production'
[ "${ASPNETCORE_URLS:-}" = 'http://127.0.0.1:5080' ] || die 'ASPNETCORE_URLS must bind loopback port 5080'
[ "${SkyPulse__Durable__TapEndpoint}" = 'ws://127.0.0.1:2480/channel' ] || die 'TapEndpoint must be the reviewed loopback endpoint'
[ "${SkyPulse__Durable__CorpusManifestPath}" = '/var/lib/skypulse/corpus/corpus.manifest.json' ] \
    || die 'CorpusManifestPath must use the reviewed read-only mount'
expected_route="/var/lib/skypulse/routes-root/${SkyPulse__Durable__ProfileId}/routing.private.manifest.json"
[ "$SkyPulse__Durable__RoutingManifestPath" = "$expected_route" ] \
    || die 'RoutingManifestPath must use the exact configured profile below the read-only route mount'
printf '%s' "$SKYPULSE_PG_HOST" \
    | grep -Eq '^[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?(\.[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?)+$' \
    || die 'SKYPULSE_PG_HOST must be a canonical lowercase dotted DNS hostname'

for name in \
    SkyPulse__Durable__ExclusiveRepositoryAdministrationConfirmed \
    SkyPulse__Durable__FullNetworkModeDisabledConfirmed \
    SkyPulse__Durable__AutomaticRepositoryDiscoveryDisabledConfirmed
do
    eval "value=\${$name:-}"
    [ "$value" = true ] || die "$name must be true"
done

printf '%s' "$SkyPulse__Durable__ProfilePrefixSha256" | grep -Eq '^[0-9a-f]{64}$' \
    || die 'ProfilePrefixSha256 must be lowercase 64-hex'
printf '%s' "$SkyPulse__Durable__ProfileId" \
    | grep -Eq '^[a-z0-9]([a-z0-9._-]{0,78}[a-z0-9])?$' \
    || die 'ProfileId is not canonical'
printf '%s' "$SkyPulse__Durable__ProfileVersion" | grep -Eq '^[1-9][0-9]*$' \
    || die 'ProfileVersion must be positive'
printf '%s' "$SkyPulse__Durable__CorpusCap" | grep -Eq '^[1-9][0-9]*$' \
    || die 'CorpusCap must be positive'
printf '%s' "$SkyPulse__Durable__SourceInstanceId" \
    | grep -Eqi '^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$' \
    || die 'SourceInstanceId must be a canonical nonzero UUID'
[ "$SkyPulse__Durable__SourceInstanceId" != '00000000-0000-0000-0000-000000000000' ] \
    || die 'SourceInstanceId cannot be empty UUID'

pg_password=$(read_hex_secret "${SKYPULSE_PG_PASSWORD_FILE:-/run/secrets/postgres-password}")
tap_password=$(read_hex_secret "${SKYPULSE_TAP_ADMIN_PASSWORD_FILE:-/run/secrets/tap-admin-password}")
growth_token=$(read_hex_secret "${SKYPULSE_GROWTH_ADMIN_TOKEN_FILE:-/run/secrets/corpus-growth-admin-token}")

export ConnectionStrings__SkyPulsePostgreSql="Host=${SKYPULSE_PG_HOST};Port=${SKYPULSE_PG_PORT};Database=${SKYPULSE_PG_DATABASE};Username=${SKYPULSE_PG_USERNAME};Password=${pg_password};SSL Mode=VerifyFull;Root Certificate=/run/tls/pg-ca.crt;Include Error Detail=false;Application Name=SkyPulse;Maximum Pool Size=50;Minimum Pool Size=0"
export SkyPulse__Durable__TapAdminPassword="$tap_password"
export SkyPulse__Durable__CorpusGrowthAdminToken="$growth_token"

unset pg_password tap_password growth_token
unset expected_route
exec dotnet Orleans.SearchableStorage.Qualification.SkyPulse.Web.dll
