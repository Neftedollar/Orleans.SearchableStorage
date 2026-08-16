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
require_command openssl
require_command sha256sum
require_command ip
require_command realpath
require_rootful_docker
load_deploy_env
"$SCRIPT_DIR/verify-journal-policy.sh"
require_private_topology
require_dns_hostname "$POSTGRES_PRIVATE_DNS"
require_expected_runtime_identity
verify_installed_firewall_assets postgres "$POSTGRES_PRIVATE_INTERFACE" \
    "$POSTGRES_PRIVATE_IP" "$DEPLOY_DIR/config/skypulse-postgres-firewall.service"
require_postgres_17_11_image
for name in POSTGRES_SECRET_DIR POSTGRES_TLS_DIR POSTGRES_DATA_DIR POSTGRES_BACKUP_DIR; do
    [[ ${!name:-} == /* ]] || die "$name must be an absolute host path"
done
postgres_data_real=$(realpath -e -- "$POSTGRES_DATA_DIR")
postgres_backup_real=$(realpath -e -- "$POSTGRES_BACKUP_DIR")
[[ "$postgres_data_real" != "$postgres_backup_real" \
    && "$postgres_data_real/" != "$postgres_backup_real/"* \
    && "$postgres_backup_real/" != "$postgres_data_real/"* ]] \
    || die 'POSTGRES_DATA_DIR and POSTGRES_BACKUP_DIR must be disjoint, non-nested directories'
validate_control_env postgres "$DEPLOY_DIR/runtime/postgres.env"
require_file "$DEPLOY_DIR/runtime/pg_hba.conf"
require_mode "$DEPLOY_DIR/runtime/pg_hba.conf" 644
expected_hba=$(mktemp)
trap 'rm -f "$expected_hba"' EXIT
"$SCRIPT_DIR/render-pg-hba.sh" "$APP_PRIVATE_IP" "$expected_hba" >/dev/null
cmp --silent "$expected_hba" "$DEPLOY_DIR/runtime/pg_hba.conf" \
    || die 'runtime/pg_hba.conf is not the exact rendered reviewed template'

pg_uid=$(docker run --rm --entrypoint id "$POSTGRES_IMAGE" -u postgres)
pg_gid=$(docker run --rm --entrypoint id "$POSTGRES_IMAGE" -g postgres)
[[ "$pg_uid" =~ ^[0-9]+$ ]] || die 'could not determine postgres UID from the pinned image'
[[ "$pg_gid" =~ ^[0-9]+$ ]] || die 'could not determine postgres GID from the pinned image'

require_exact_owner_mode "$POSTGRES_SECRET_DIR" "$pg_uid" 700
require_exact_owner_mode "$POSTGRES_TLS_DIR" "$pg_uid" 700

for file in admin-password app-password tap-password; do
    path="$POSTGRES_SECRET_DIR/$file"
    require_hex_secret "$path"
    [[ $(stat -c '%u' "$path") -eq "$pg_uid" ]] || die "$path must be owned by postgres UID $pg_uid"
done

require_exact_owner_mode "$POSTGRES_DATA_DIR" "$pg_uid" 700
require_exact_owner_mode "$POSTGRES_BACKUP_DIR" "$pg_uid" 700

require_file "$POSTGRES_TLS_DIR/server.crt"
require_file "$POSTGRES_TLS_DIR/server.key"
require_file "$POSTGRES_TLS_DIR/ca.crt"
require_mode "$POSTGRES_TLS_DIR/server.key" 600
server_cert_mode=$(stat -c '%a' "$POSTGRES_TLS_DIR/server.crt")
ca_cert_mode=$(stat -c '%a' "$POSTGRES_TLS_DIR/ca.crt")
[[ "$server_cert_mode" == 444 || "$server_cert_mode" == 644 ]] || die 'server.crt must have mode 0444 or 0644'
[[ "$ca_cert_mode" == 444 || "$ca_cert_mode" == 644 ]] || die 'ca.crt must have mode 0444 or 0644'
[[ $(stat -c '%u' "$POSTGRES_TLS_DIR/server.key") -eq "$pg_uid" ]] \
    || die "server.key must be owned by postgres UID $pg_uid"
for certificate in "$POSTGRES_TLS_DIR/server.crt" "$POSTGRES_TLS_DIR/ca.crt"; do
    [[ $(stat -c '%u' "$certificate") -eq "$pg_uid" ]] \
        || die "$certificate must be owned by postgres UID $pg_uid"
done
openssl verify -CAfile "$POSTGRES_TLS_DIR/ca.crt" "$POSTGRES_TLS_DIR/server.crt" >/dev/null
openssl x509 -in "$POSTGRES_TLS_DIR/server.crt" -noout -checkhost "$POSTGRES_PRIVATE_DNS" >/dev/null \
    || die 'PostgreSQL server certificate does not cover POSTGRES_PRIVATE_DNS'
cert_public_key=$(openssl x509 -in "$POSTGRES_TLS_DIR/server.crt" -pubkey -noout \
    | openssl pkey -pubin -outform DER | sha256sum | awk '{print $1}')
private_public_key=$(openssl pkey -in "$POSTGRES_TLS_DIR/server.key" -pubout \
    | openssl pkey -pubin -outform DER | sha256sum | awk '{print $1}')
[[ "$cert_public_key" == "$private_public_key" ]] \
    || die 'PostgreSQL server.key does not match server.crt'

resolved=$(mktemp)
trap 'rm -f "$resolved" "$expected_hba"' EXIT
compose_postgres config --format json > "$resolved"
python3 - "$resolved" "$POSTGRES_PRIVATE_IP" <<'PY'
import json, sys
config = json.load(open(sys.argv[1], encoding='utf-8'))
service = config['services']['postgres']
if service.get('network_mode') != 'host' or service.get('ports'):
    raise SystemExit('PostgreSQL must use host networking without Docker-published ports')
if service.get('restart') not in (None, 'no'):
    raise SystemExit('PostgreSQL must remain operator-started so restore-pending cannot auto-restart')
command = service.get('command', [])
if f'listen_addresses={sys.argv[2]}' not in command:
    raise SystemExit('PostgreSQL must listen on the exact private IPv4 address')
PY

require_file /etc/skypulse-postgres-firewall.nft
require_mode /etc/skypulse-postgres-firewall.nft 600
[[ $(stat -c '%u' /etc/skypulse-postgres-firewall.nft) -eq 0 ]] \
    || die '/etc/skypulse-postgres-firewall.nft must be owned by root'
expected_firewall=$(mktemp)
trap 'rm -f "$resolved" "$expected_hba" "$expected_firewall"' EXIT
"$SCRIPT_DIR/render-postgres-firewall.sh" \
    "$APP_PRIVATE_IP" "$POSTGRES_PRIVATE_IP" "$POSTGRES_PRIVATE_INTERFACE" "$expected_firewall" >/dev/null
cmp --silent "$expected_firewall" /etc/skypulse-postgres-firewall.nft \
    || die 'installed guest firewall is not the exact rendered deployment rule set'
systemctl is-enabled --quiet skypulse-postgres-firewall.service \
    || die 'skypulse-postgres-firewall.service is not enabled'
systemctl is-active --quiet skypulse-postgres-firewall.service \
    || die 'skypulse-postgres-firewall.service is not active'
verify_firewall_boot_dependency skypulse-postgres-firewall.service \
    "$DEPLOY_DIR/config/require-skypulse-postgres-firewall.conf"
"$SCRIPT_DIR/verify-live-firewall.sh" \
    /etc/skypulse-postgres-firewall.nft skypulse_postgres
note 'PostgreSQL-host preflight passed, including the required guest private-network firewall.'
