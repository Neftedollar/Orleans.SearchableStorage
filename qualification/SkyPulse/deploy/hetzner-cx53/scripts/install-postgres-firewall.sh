#!/usr/bin/env bash
set -euo pipefail
umask 077

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
DEPLOY_DIR=$(cd -- "$SCRIPT_DIR/.." && pwd)
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

[[ $(id -u) -eq 0 ]] || die 'install-postgres-firewall.sh must run as root'
[[ ${SKYPULSE_FIREWALL_CONFIRMATION:-} == I_AM_ON_THE_POSTGRES_HOST ]] \
    || die 'set SKYPULSE_FIREWALL_CONFIRMATION=I_AM_ON_THE_POSTGRES_HOST'
require_command nft
require_command systemctl
require_command ip
load_deploy_env
require_private_topology
preflight_classic_ssh_conversion
require_interface_address "$POSTGRES_PRIVATE_INTERFACE" "$POSTGRES_PRIVATE_IP"

rendered=$(mktemp)
binding=$(mktemp)
trap 'rm -f "$rendered" "$binding"' EXIT
"$SCRIPT_DIR/render-postgres-firewall.sh" \
    "$APP_PRIVATE_IP" "$POSTGRES_PRIVATE_IP" "$POSTGRES_PRIVATE_INTERFACE" "$rendered" >/dev/null
nft -c -f "$rendered"
printf '%s %s\n' "$POSTGRES_PRIVATE_INTERFACE" "$POSTGRES_PRIVATE_IP" > "$binding"
install -o root -g root -m 0600 "$rendered" /etc/skypulse-postgres-firewall.nft
install -o root -g root -m 0600 "$binding" /etc/skypulse-postgres-firewall.interface
install -d -o root -g root -m 0755 /usr/local/libexec
install -o root -g root -m 0755 "$SCRIPT_DIR/firewall-interface-check.sh" \
    /usr/local/libexec/skypulse-firewall-interface-check
install -o root -g root -m 0644 \
    "$DEPLOY_DIR/config/skypulse-postgres-firewall.service" \
    /etc/systemd/system/skypulse-postgres-firewall.service
for unit in docker.service ssh.service; do
    install -d -o root -g root -m 0755 "/etc/systemd/system/$unit.d"
    install -o root -g root -m 0644 \
        "$DEPLOY_DIR/config/require-skypulse-postgres-firewall.conf" \
        "/etc/systemd/system/$unit.d/90-skypulse-firewall.conf"
done
systemctl daemon-reload
systemctl enable skypulse-postgres-firewall.service
if systemctl is-active --quiet skypulse-postgres-firewall.service; then
    systemctl reload skypulse-postgres-firewall.service
else
    systemctl start skypulse-postgres-firewall.service
fi
configure_classic_ssh
systemctl is-active --quiet skypulse-postgres-firewall.service \
    || die 'guest PostgreSQL firewall did not become active'
"$SCRIPT_DIR/verify-live-firewall.sh" \
    /etc/skypulse-postgres-firewall.nft skypulse_postgres
note 'Installed the exact persistent PostgreSQL guest firewall.'
