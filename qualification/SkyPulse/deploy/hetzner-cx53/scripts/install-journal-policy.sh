#!/usr/bin/env bash
set -euo pipefail
umask 077

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
DEPLOY_DIR=$(cd -- "$SCRIPT_DIR/.." && pwd)
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

[[ $(id -u) -eq 0 ]] || die 'install-journal-policy.sh must run as root'
[[ ${SKYPULSE_JOURNAL_CONFIRMATION:-} == I_ACCEPT_SEVEN_DAY_TWO_GIB_HOST_LOG_RETENTION ]] \
    || die 'set the exact journal-policy confirmation'
require_command systemctl
require_command journalctl
install -d -o root -g root -m 0755 /etc/systemd/journald.conf.d
install -d -o root -g systemd-journal -m 2755 /var/log/journal
install -o root -g root -m 0644 "$DEPLOY_DIR/config/90-skypulse-journald.conf" \
    /etc/systemd/journald.conf.d/90-skypulse.conf
systemctl restart systemd-journald.service
journalctl --flush
"$SCRIPT_DIR/verify-journal-policy.sh"
note 'Installed persistent journald retention: at most seven days and two GiB per host.'
