#!/usr/bin/env bash
set -euo pipefail
umask 077

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
DEPLOY_DIR=$(cd -- "$SCRIPT_DIR/.." && pwd)
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

require_command docker
require_command python3
require_command stat
require_command cmp
require_command nft
require_command systemctl
require_command ip
require_rootful_docker
load_deploy_env
"$SCRIPT_DIR/verify-journal-policy.sh"
require_private_topology
require_dns_hostname "$POSTGRES_PRIVATE_DNS"
require_expected_runtime_identity
verify_installed_firewall_assets app "$APP_PRIVATE_INTERFACE" "$APP_PRIVATE_IP" \
    "$DEPLOY_DIR/config/skypulse-app-firewall.service"
require_file /etc/skypulse-app-firewall.nft
require_mode /etc/skypulse-app-firewall.nft 600
[[ $(stat -c '%u' /etc/skypulse-app-firewall.nft) -eq 0 ]] \
    || die '/etc/skypulse-app-firewall.nft must be owned by root'
expected_firewall=$(mktemp)
trap 'rm -f "$expected_firewall"' EXIT
"$SCRIPT_DIR/render-app-firewall.sh" \
    "$APP_PRIVATE_IP" "$POSTGRES_PRIVATE_IP" "$APP_PRIVATE_INTERFACE" "$expected_firewall" >/dev/null
cmp --silent "$expected_firewall" /etc/skypulse-app-firewall.nft \
    || die 'installed app guest firewall is not the exact rendered rule set'
systemctl is-enabled --quiet skypulse-app-firewall.service \
    || die 'skypulse-app-firewall.service is not enabled'
systemctl is-active --quiet skypulse-app-firewall.service \
    || die 'skypulse-app-firewall.service is not active'
verify_firewall_boot_dependency skypulse-app-firewall.service \
    "$DEPLOY_DIR/config/require-skypulse-app-firewall.conf"
"$SCRIPT_DIR/verify-live-firewall.sh" /etc/skypulse-app-firewall.nft skypulse_app
require_digest_ref POSTGRES_IMAGE
[[ "$POSTGRES_IMAGE" == postgres:17.11-bookworm@sha256:* ]] \
    || die 'POSTGRES_IMAGE must be the reviewed PostgreSQL 17.11 bookworm image'
pg_version=$(docker run --rm --network none --entrypoint postgres "$POSTGRES_IMAGE" --version)
[[ "$pg_version" =~ ^postgres\ \(PostgreSQL\)\ 17\.11([[:space:]]|$) ]] \
    || die "POSTGRES_IMAGE digest is not PostgreSQL 17.11: $pg_version"
for name in APP_SECRET_DIR TAP_SECRET_DIR PUBLIC_CORPUS_DIR PRIVATE_ROUTING_ROOT APP_PG_CA_CERT; do
    [[ ${!name:-} == /* ]] || die "$name must be an absolute host path"
done
if [[ ${ENABLE_PUBLIC_PROXY:-false} == true ]]; then
    require_digest_ref CADDY_IMAGE
    [[ "$CADDY_IMAGE" == caddy:2.11.4-alpine@sha256:* ]] \
        || die 'CADDY_IMAGE must be the reviewed 2.11.4-alpine image'
    caddy_version=$(docker run --rm --network none --entrypoint caddy "$CADDY_IMAGE" version)
    [[ "$caddy_version" =~ ^v2\.11\.4([[:space:]]|$) ]] \
        || die "CADDY_IMAGE digest is not Caddy 2.11.4: $caddy_version"
    for name in CADDYFILE_PATH CADDY_DATA_DIR CADDY_CONFIG_DIR; do
        [[ ${!name:-} == /* ]] || die "$name must be an absolute host path"
    done
    require_file "$CADDYFILE_PATH"
    require_mode "$CADDYFILE_PATH" 600
    [[ $(stat -c '%u' "$CADDYFILE_PATH") -eq 0 ]] \
        || die "$CADDYFILE_PATH must be owned by root"
    require_exact_owner_mode "$(dirname -- "$CADDYFILE_PATH")" 0 700
    [[ -n ${SKYPULSE_CURL_CONFIG:-} ]] \
        || die 'SKYPULSE_CURL_CONFIG is required when the public proxy is enabled'
    require_curl_basic_config "$SKYPULSE_CURL_CONFIG"
    python3 - "$CADDYFILE_PATH" "$DEPLOY_DIR/config/Caddyfile.template" \
        "${SKYPULSE_PUBLIC_BASE_URL:-}" <<'PY'
import pathlib
import re
import sys

actual_path, template_path, public_origin = sys.argv[1:]
origin = re.fullmatch(
    r"https://([a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?(?:\.[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?)+)",
    public_origin,
)
if origin is None:
    raise SystemExit("SKYPULSE_PUBLIC_BASE_URL must be one canonical lowercase HTTPS origin")
actual = pathlib.Path(actual_path).read_text(encoding="utf-8")
match = re.search(r"^[ \t]+operator (\$2[aby]\$(\d{2})\$[./A-Za-z0-9]{53})$", actual, re.MULTILINE)
if match is None or not 12 <= int(match.group(2)) <= 16:
    raise SystemExit("Caddyfile must contain the reviewed operator bcrypt policy")
template = pathlib.Path(template_path).read_text(encoding="utf-8")
expected = (template.replace("__DOMAIN__", origin.group(1))
    .replace("__USERNAME__", "operator")
    .replace("__PASSWORD_HASH__", match.group(1)))
if actual != expected:
    raise SystemExit("Caddyfile differs from the exact reviewed rendered template")
PY
    require_exact_owner_mode "$CADDY_DATA_DIR" 0 700
    require_exact_owner_mode "$CADDY_CONFIG_DIR" 0 700
    docker run --rm --network none --read-only \
        --tmpfs /tmp:rw,noexec,nosuid,nodev,size=32m \
        --env XDG_CONFIG_HOME=/tmp --env XDG_DATA_HOME=/tmp \
        --volume "$CADDYFILE_PATH:/etc/caddy/Caddyfile:ro" \
        --entrypoint caddy "$CADDY_IMAGE" \
        validate --config /etc/caddy/Caddyfile --adapter caddyfile >/dev/null
fi

validate_control_env app "$DEPLOY_DIR/runtime/app.env"
validate_control_env images "$DEPLOY_DIR/runtime/images.env"
if grep -Eq 'REPLACE_|PLACEHOLDER' "$DEPLOY_DIR/runtime/app.env"; then
    die 'runtime/app.env contains an unresolved placeholder'
fi

set -a
# shellcheck disable=SC1091
source "$DEPLOY_DIR/runtime/app.env"
# shellcheck disable=SC1091
source "$DEPLOY_DIR/runtime/images.env"
set +a

[[ ${SKYPULSE_APP_IMAGE:-} == "skypulse-app:$DEPLOYMENT_ID" ]] \
    || die 'app image tag does not match DEPLOYMENT_ID'
[[ ${SKYPULSE_TAP_IMAGE:-} == "skypulse-tap:$DEPLOYMENT_ID" ]] \
    || die 'TAP image tag does not match DEPLOYMENT_ID'

[[ ${SkyPulse__Mode:-} == Durable ]] || die 'runtime/app.env must select Durable mode'
[[ ${ASPNETCORE_ENVIRONMENT:-} == Production ]] || die 'runtime/app.env must select Production'
[[ ${ASPNETCORE_URLS:-} == http://127.0.0.1:5080 ]] || die 'application must bind 127.0.0.1:5080'
[[ ${SkyPulse__Durable__TapEndpoint:-} == ws://127.0.0.1:2480/channel ]] || die 'TAP endpoint must be loopback'
[[ ${SkyPulse__Durable__ProfileId:-} == "$EXPECTED_BASE_PROFILE_ID" ]] \
    || die 'base profile ID differs between .env and runtime/app.env'
[[ ${SkyPulse__Durable__CorpusCap:-} == "$EXPECTED_BASE_CORPUS_CAP" ]] \
    || die 'base corpus cap differs between .env and runtime/app.env'
[[ ${SkyPulse__Durable__SourceInstanceId:-} == "$EXPECTED_SOURCE_INSTANCE_ID" ]] \
    || die 'source instance ID differs between .env and runtime/app.env'
[[ ${SkyPulse__Durable__CorpusManifestPath:-} == /var/lib/skypulse/corpus/corpus.manifest.json ]] \
    || die 'CorpusManifestPath differs from the read-only mount'
expected_route="/var/lib/skypulse/routes-root/${SkyPulse__Durable__ProfileId:-}/routing.private.manifest.json"
[[ ${SkyPulse__Durable__RoutingManifestPath:-} == "$expected_route" ]] \
    || die 'RoutingManifestPath must use the exact configured profile directory'
[[ ${SKYPULSE_PG_HOST:-} == "$POSTGRES_PRIVATE_DNS" ]] || die 'app PG DNS differs from deployment PG DNS'
[[ ${SKYPULSE_PG_PORT:-} == 5432 && ${SKYPULSE_PG_DATABASE:-} == skypulse && ${SKYPULSE_PG_USERNAME:-} == skypulse_app ]] \
    || die 'app PostgreSQL database/role/port differ from the reviewed topology'

for file in postgres-password tap-admin-password corpus-growth-admin-token; do
    require_hex_secret "$APP_SECRET_DIR/$file"
    [[ $(stat -c '%u' "$APP_SECRET_DIR/$file") -eq 10001 ]] || die "$APP_SECRET_DIR/$file must be owned by UID 10001"
done
for file in postgres-password tap-admin-password; do
    require_hex_secret "$TAP_SECRET_DIR/$file"
    [[ $(stat -c '%u' "$TAP_SECRET_DIR/$file") -eq 65534 ]] || die "$TAP_SECRET_DIR/$file must be owned by UID 65534"
done
require_exact_owner_mode "$APP_SECRET_DIR" 10001 700
require_exact_owner_mode "$TAP_SECRET_DIR" 65534 700
require_file "$APP_PG_CA_CERT"
ca_mode=$(stat -c '%a' "$APP_PG_CA_CERT")
[[ "$ca_mode" == 444 || "$ca_mode" == 644 ]] || die "$APP_PG_CA_CERT must be publicly readable certificate data (0444/0644)"
[[ $(stat -c '%u' "$APP_PG_CA_CERT") -eq 0 ]] \
    || die "$APP_PG_CA_CERT must be owned by root"

require_directory "$PUBLIC_CORPUS_DIR"
require_mode "$PUBLIC_CORPUS_DIR" 555
[[ $(stat -c '%u' "$PUBLIC_CORPUS_DIR") -eq 0 ]] \
    || die "$PUBLIC_CORPUS_DIR must be owned by root"
require_file "$PUBLIC_CORPUS_DIR/corpus.manifest.json"
require_file "$PUBLIC_CORPUS_DIR/accounts.ak32"
require_mode "$PUBLIC_CORPUS_DIR/corpus.manifest.json" 444
require_mode "$PUBLIC_CORPUS_DIR/accounts.ak32" 444
for file in "$PUBLIC_CORPUS_DIR/corpus.manifest.json" "$PUBLIC_CORPUS_DIR/accounts.ak32"; do
    [[ $(stat -c '%u' "$file") -eq 0 ]] || die "$file must be owned by root"
done
require_directory "$PRIVATE_ROUTING_ROOT"
require_mode "$PRIVATE_ROUTING_ROOT" 700
[[ $(stat -c '%u' "$PRIVATE_ROUTING_ROOT") -eq 10001 ]] \
    || die "$PRIVATE_ROUTING_ROOT must be owned by UID 10001"

route_prefix=/var/lib/skypulse/routes-root/
route_in_container=${SkyPulse__Durable__RoutingManifestPath:-}
[[ "$route_in_container" == "$route_prefix"* ]] || die 'RoutingManifestPath must be below the read-only route mount'
route_manifest="$PRIVATE_ROUTING_ROOT/${route_in_container#"$route_prefix"}"
route_dir=$(dirname -- "$route_manifest")
require_directory "$route_dir"
require_mode "$route_dir" 700
require_file "$route_manifest"
require_file "$route_dir/routing.private.ndjson"
require_mode "$route_manifest" 600
require_mode "$route_dir/routing.private.ndjson" 600
for path in "$route_dir" "$route_manifest" "$route_dir/routing.private.ndjson"; do
    [[ $(stat -c '%u' "$path") -eq 10001 ]] || die "$path must be owned by UID 10001"
done

artifact_paths=$(mktemp)
trap 'rm -f "$artifact_paths" "$expected_firewall"' EXIT
python3 "$SCRIPT_DIR/profile-artifacts.py" \
    --public-manifest "$PUBLIC_CORPUS_DIR/corpus.manifest.json" \
    --private-root "$PRIVATE_ROUTING_ROOT" \
    --expected-uid 10001 --paths0 > "$artifact_paths"

[[ ${SKYPULSE_APP_IMAGE_ID:-} =~ ^sha256:[0-9a-f]{64}$ ]] || die 'invalid app image ID lock'
[[ ${SKYPULSE_TAP_IMAGE_ID:-} =~ ^sha256:[0-9a-f]{64}$ ]] || die 'invalid TAP image ID lock'
[[ $(docker image inspect --format '{{.Id}}' "$SKYPULSE_APP_IMAGE") == "$SKYPULSE_APP_IMAGE_ID" ]] \
    || die 'local app image tag moved after build'
[[ $(docker image inspect --format '{{.Id}}' "$SKYPULSE_TAP_IMAGE") == "$SKYPULSE_TAP_IMAGE_ID" ]] \
    || die 'local TAP image tag moved after build'

artifact_docker=(
    docker run --rm --network none --read-only
    --tmpfs '/tmp:rw,noexec,nosuid,nodev,size=256m'
    --env HOME=/tmp --env DOTNET_CLI_HOME=/tmp --env DOTNET_EnableDiagnostics=0
    --volume "$PUBLIC_CORPUS_DIR:/runtime/corpus:ro"
    --volume "$PRIVATE_ROUTING_ROOT:/runtime/routes:ro"
    --entrypoint dotnet "$SKYPULSE_APP_IMAGE"
)
"${artifact_docker[@]}" \
    /opt/skypulse-tools/corpus-builder/Orleans.SearchableStorage.Qualification.SkyPulse.CorpusBuilder.dll \
    verify --manifest /runtime/corpus/corpus.manifest.json --deep >/dev/null
while IFS= read -r -d '' configured_manifest \
    && IFS= read -r -d '' configured_data; do
    : "$configured_data"
    relative_manifest=${configured_manifest#"$PRIVATE_ROUTING_ROOT"/}
    [[ "$relative_manifest" != "$configured_manifest" ]] \
        || die 'configured route is outside PRIVATE_ROUTING_ROOT'
    "${artifact_docker[@]}" \
        /opt/skypulse-tools/corpus-acquisition/Orleans.SearchableStorage.Qualification.SkyPulse.CorpusAcquisition.dll \
        verify-route --manifest "/runtime/routes/$relative_manifest" >/dev/null
done < "$artifact_paths"

resolved=$(mktemp)
trap 'rm -f "$resolved" "$artifact_paths" "$expected_firewall"; if [[ -n ${pgpass:-} ]]; then rm -f "$pgpass"; fi' EXIT
compose_app config > "$resolved"
grep -Fq 'network_mode: host' "$resolved" || die 'resolved app stack lost host networking'
if grep -Eq 'TAP_(FULL_NETWORK|SIGNAL_COLLECTION|DISABLE_ACKS|WEBHOOK_URL|NO_REPLAY|COLLECTION_FILTERS|OUTBOX_ONLY)' "$resolved"; then
    die 'resolved production TAP environment contains a forbidden mode'
fi
if grep -Fq 'ws://tap' "$resolved"; then
    die 'bridge-name plaintext TAP endpoint is forbidden'
fi

for service in app tap; do
    mapfile -t running_services < <(docker ps --filter "label=com.docker.compose.service=$service" \
        --format '{{.Names}}|{{.Label "com.docker.compose.project"}}')
    if [[ ${#running_services[@]} -gt 1 ]]; then
        die "more than one Compose service named $service is already running"
    fi
    expected="skypulse-$service|skypulse-app"
    if [[ ${#running_services[@]} -eq 1 && ${running_services[0]} != "$expected" ]]; then
        die "a foreign Compose $service service is already running: ${running_services[0]}"
    fi
done

check_database() {
    local user=$1 database=$2 secret_file=$3
    local password
    password=$(<"$secret_file")
    pgpass=$(mktemp)
    chmod 0600 "$pgpass"
    printf '%s:%s:%s:%s:%s\n' "$POSTGRES_PRIVATE_DNS" 5432 "$database" "$user" "$password" > "$pgpass"
    docker run --rm --network host \
        --add-host "$POSTGRES_PRIVATE_DNS:$POSTGRES_PRIVATE_IP" \
        --env PGPASSFILE=/run/secrets/pgpass \
        --volume "$pgpass:/run/secrets/pgpass:ro" \
        --volume "$APP_PG_CA_CERT:/run/tls/pg-ca.crt:ro" \
        "$POSTGRES_IMAGE" \
        psql "host=$POSTGRES_PRIVATE_DNS port=5432 dbname=$database user=$user sslmode=verify-full sslrootcert=/run/tls/pg-ca.crt" \
        --no-psqlrc --tuples-only --command 'SELECT 1' >/dev/null
    rm -f "$pgpass"
    pgpass=
}

check_database skypulse_app skypulse "$APP_SECRET_DIR/postgres-password"
check_database skypulse_tap skypulse_tap "$TAP_SECRET_DIR/postgres-password"
note 'Application-host preflight passed, including base and every configured growth route byte proof.'
