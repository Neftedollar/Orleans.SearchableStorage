#!/usr/bin/env bash
set -euo pipefail
umask 077

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
DEPLOY_DIR=$(cd -- "$SCRIPT_DIR/.." && pwd)
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

[[ $# -eq 4 ]] || die 'usage: render-app-firewall.sh APP_PRIVATE_IP POSTGRES_PRIVATE_IP PRIVATE_INTERFACE OUTPUT'
app_ip=$1
postgres_ip=$2
private_interface=$3
output=$4
APP_PRIVATE_IP=$app_ip
POSTGRES_PRIVATE_IP=$postgres_ip
require_private_topology
require_interface_address "$private_interface" "$app_ip"
[[ "$output" == /* ]] || die 'firewall output path must be absolute'
require_directory "$(dirname -- "$output")"
[[ ! -e "$output" || ( -f "$output" && ! -L "$output" ) ]] \
    || die 'refusing to overwrite a non-regular firewall path'

temporary=$(mktemp "${output}.tmp.XXXXXX")
trap 'rm -f "$temporary"' EXIT
python3 - "$DEPLOY_DIR/config/skypulse-app-firewall.nft.template" \
    "$app_ip" "$postgres_ip" "$private_interface" "$temporary" <<'PY'
import pathlib
import re
import sys

template = pathlib.Path(sys.argv[1]).read_text(encoding="utf-8")
if (template.count("__APP_PRIVATE_IP__") != 2
        or template.count("__POSTGRES_PRIVATE_IP__") != 3
        or template.count("__PRIVATE_INTERFACE__") != 9):
    raise SystemExit("unexpected app-firewall template placeholders")
rendered = template.replace("__APP_PRIVATE_IP__", sys.argv[2]).replace(
        "__POSTGRES_PRIVATE_IP__", sys.argv[3]
    ).replace("__PRIVATE_INTERFACE__", sys.argv[4])
if re.search(r"__[A-Z_]+__", rendered):
    raise SystemExit("unresolved app-firewall template placeholder")
pathlib.Path(sys.argv[5]).write_text(rendered, encoding="utf-8", newline="\n")
PY
chmod 0600 "$temporary"
mv -f -- "$temporary" "$output"
trap - EXIT
note "Rendered application-host guest firewall: $output"
