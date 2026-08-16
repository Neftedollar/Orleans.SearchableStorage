#!/usr/bin/env bash
set -euo pipefail
umask 077

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
DEPLOY_DIR=$(cd -- "$SCRIPT_DIR/.." && pwd)

[[ $# -eq 2 ]] || { printf '%s\n' 'usage: render-pg-hba.sh APP_PRIVATE_IP OUTPUT' >&2; exit 1; }
app_ip=$1
output=$2

python3 - "$app_ip" "$DEPLOY_DIR/config/pg_hba.conf.template" "$output" <<'PY'
import ipaddress
import os
import pathlib
import sys
import tempfile

ip = ipaddress.ip_address(sys.argv[1])
private = tuple(map(ipaddress.IPv4Network, ('10.0.0.0/8', '172.16.0.0/12', '192.168.0.0/16')))
if ip.version != 4 or not any(ip in network for network in private):
    raise SystemExit('APP_PRIVATE_IP must be an RFC1918 IPv4 address')
template = pathlib.Path(sys.argv[2]).read_text(encoding='utf-8')
target = pathlib.Path(sys.argv[3]).resolve()
target.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
rendered = template.replace('__APP_PRIVATE_IP__', str(ip))
fd, tmp_name = tempfile.mkstemp(prefix='.pg_hba.', dir=target.parent)
try:
    os.fchmod(fd, 0o644)
    with os.fdopen(fd, 'w', encoding='utf-8', newline='\n') as stream:
        stream.write(rendered)
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(tmp_name, target)
finally:
    if os.path.exists(tmp_name):
        os.unlink(tmp_name)
PY

printf 'Rendered %s for app host %s\n' "$output" "$app_ip"
