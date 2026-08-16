#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
DEPLOY_DIR=$(cd -- "$SCRIPT_DIR/.." && pwd)
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

require_command cmp
require_command systemctl
require_command systemd-analyze
require_file /etc/systemd/journald.conf.d/90-skypulse.conf
require_mode /etc/systemd/journald.conf.d/90-skypulse.conf 644
[[ $(stat -c '%u' /etc/systemd/journald.conf.d/90-skypulse.conf) -eq 0 ]] \
    || die 'journald policy must be owned by root'
cmp --silent "$DEPLOY_DIR/config/90-skypulse-journald.conf" \
    /etc/systemd/journald.conf.d/90-skypulse.conf \
    || die 'installed journald policy differs from the reviewed deployment policy'
systemctl is-active --quiet systemd-journald.service \
    || die 'systemd-journald is not active'
merged=$(mktemp)
trap 'rm -f "$merged"' EXIT
systemd-analyze cat-config systemd/journald.conf > "$merged"
python3 - "$merged" <<'PY'
import sys

wanted = {
    "Storage": "persistent",
    "Compress": "yes",
    "Seal": "yes",
    "SystemMaxUse": "2G",
    "SystemKeepFree": "5G",
    "MaxRetentionSec": "7day",
}
actual = {}
in_journal = False
for raw in open(sys.argv[1], encoding="utf-8"):
    line = raw.strip()
    if not line or line.startswith(("#", ";")):
        continue
    if line.startswith("[") and line.endswith("]"):
        in_journal = line == "[Journal]"
        continue
    if in_journal and "=" in line:
        key, value = line.split("=", 1)
        if key in wanted:
            actual[key] = value
if actual != wanted:
    raise SystemExit(f"effective journald retention differs from reviewed policy: {actual}")
PY
