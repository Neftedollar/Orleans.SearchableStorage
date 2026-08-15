#!/usr/bin/env bash
set -euo pipefail
umask 077

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
DEPLOY_DIR=$(cd -- "$SCRIPT_DIR/.." && pwd)

[[ $# -eq 4 ]] || {
    printf '%s\n' 'usage: render-caddyfile.sh DOMAIN USERNAME PASSWORD_HASH_FILE OUTPUT' >&2
    exit 1
}

python3 - "$1" "$2" "$3" "$DEPLOY_DIR/config/Caddyfile.template" "$4" <<'PY'
import os
import pathlib
import re
import sys
import tempfile

domain, username, hash_path, template_path, output_path = sys.argv[1:]
label = re.compile(r'^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$')
if len(domain) > 253 or '.' not in domain or any(
    label.fullmatch(part) is None for part in domain.split('.')
):
    raise SystemExit('DOMAIN must be a canonical lowercase dotted DNS name')
if username != 'operator':
    raise SystemExit('USERNAME must be the reviewed fixed value operator')
source = pathlib.Path(hash_path)
if source.is_symlink() or not source.is_file():
    raise SystemExit('PASSWORD_HASH_FILE must be a regular non-symlink file')
if source.stat().st_mode & 0o777 not in (0o400, 0o600):
    raise SystemExit('PASSWORD_HASH_FILE must have mode 0400 or 0600')
password_hash = source.read_text(encoding='utf-8').strip()
match = re.fullmatch(r'\$2[aby]\$(\d{2})\$[./A-Za-z0-9]{53}', password_hash)
if match is None or not 12 <= int(match.group(1)) <= 16:
    raise SystemExit('expected one bcrypt hash with cost 12..16')
rendered = pathlib.Path(template_path).read_text(encoding='utf-8')
rendered = rendered.replace('__DOMAIN__', domain).replace('__USERNAME__', username).replace('__PASSWORD_HASH__', password_hash)
target = pathlib.Path(output_path).resolve()
target.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
fd, tmp_name = tempfile.mkstemp(prefix='.Caddyfile.', dir=target.parent)
try:
    os.fchmod(fd, 0o600)
    with os.fdopen(fd, 'w', encoding='utf-8', newline='\n') as stream:
        stream.write(rendered)
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(tmp_name, target)
finally:
    if os.path.exists(tmp_name):
        os.unlink(tmp_name)
PY

printf 'Rendered %s\n' "$4"
