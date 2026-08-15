#!/usr/bin/env bash
set -euo pipefail
umask 077

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
DEPLOY_DIR=$(cd -- "$SCRIPT_DIR/.." && pwd)
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

[[ $(id -u) -eq 0 ]] || die 'install-app-firewall.sh must run as root'
[[ ${SKYPULSE_FIREWALL_CONFIRMATION:-} == I_AM_ON_THE_APP_HOST ]] \
    || die 'set SKYPULSE_FIREWALL_CONFIRMATION=I_AM_ON_THE_APP_HOST'
require_command nft
require_command systemctl
require_command ip
load_deploy_env
require_private_topology
preflight_classic_ssh_conversion
require_interface_address "$APP_PRIVATE_INTERFACE" "$APP_PRIVATE_IP"

rendered=$(mktemp)
binding=$(mktemp)
trap 'rm -f "$rendered" "$binding"' EXIT
"$SCRIPT_DIR/render-app-firewall.sh" \
    "$APP_PRIVATE_IP" "$POSTGRES_PRIVATE_IP" "$APP_PRIVATE_INTERFACE" "$rendered" >/dev/null
nft -c -f "$rendered"
printf '%s %s\n' "$APP_PRIVATE_INTERFACE" "$APP_PRIVATE_IP" > "$binding"
install -o root -g root -m 0600 "$rendered" /etc/skypulse-app-firewall.nft
install -o root -g root -m 0600 "$binding" /etc/skypulse-app-firewall.interface
install -d -o root -g root -m 0755 /usr/local/libexec
install -o root -g root -m 0755 "$SCRIPT_DIR/firewall-interface-check.sh" \
    /usr/local/libexec/skypulse-firewall-interface-check
install -o root -g root -m 0644 "$DEPLOY_DIR/config/skypulse-app-firewall.service" \
    /etc/systemd/system/skypulse-app-firewall.service
for unit in docker.service ssh.service; do
    install -d -o root -g root -m 0755 "/etc/systemd/system/$unit.d"
    install -o root -g root -m 0644 \
        "$DEPLOY_DIR/config/require-skypulse-app-firewall.conf" \
        "/etc/systemd/system/$unit.d/90-skypulse-firewall.conf"
done
systemctl daemon-reload
systemctl enable skypulse-app-firewall.service
if systemctl is-active --quiet skypulse-app-firewall.service; then
    systemctl reload skypulse-app-firewall.service
else
    systemctl start skypulse-app-firewall.service
fi
configure_classic_ssh
"$SCRIPT_DIR/verify-live-firewall.sh" /etc/skypulse-app-firewall.nft skypulse_app
note 'Installed the exact persistent application-host guest firewall.'
