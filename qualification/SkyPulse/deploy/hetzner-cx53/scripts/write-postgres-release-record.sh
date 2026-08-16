#!/usr/bin/env bash
set -euo pipefail
umask 077

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
DEPLOY_DIR=$(cd -- "$SCRIPT_DIR/.." && pwd)
SKYPULSE_ROOT=$(cd -- "$DEPLOY_DIR/../.." && pwd)
REPOSITORY_ROOT=$(cd -- "$SKYPULSE_ROOT/../.." && pwd)
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

require_command git
require_command python3
load_deploy_env
validate_control_env postgres "$DEPLOY_DIR/runtime/postgres.env"
require_file "$DEPLOY_DIR/runtime/pg_hba.conf"

head=$(git -C "$REPOSITORY_ROOT" rev-parse HEAD)
[[ "$head" == "$DEPLOYMENT_ID" ]] || die "checkout HEAD $head does not match DEPLOYMENT_ID $DEPLOYMENT_ID"
[[ -z $(git -C "$REPOSITORY_ROOT" status --porcelain --untracked-files=all) ]] \
    || die 'tracked or untracked checkout changes invalidate the release record'

files=(
    "$DEPLOY_DIR/.env"
    "$DEPLOY_DIR/runtime/postgres.env"
    "$DEPLOY_DIR/runtime/pg_hba.conf"
    "$DEPLOY_DIR/compose.postgres.yaml"
    "$DEPLOY_DIR/config/postgresql.conf"
    /etc/skypulse-postgres-firewall.nft
    /etc/skypulse-postgres-firewall.interface
    /etc/systemd/system/skypulse-postgres-firewall.service
    /usr/local/libexec/skypulse-firewall-interface-check
    /etc/systemd/system/docker.service.d/90-skypulse-firewall.conf
    /etc/systemd/system/ssh.service.d/90-skypulse-firewall.conf
    /etc/systemd/journald.conf.d/90-skypulse.conf
    "$DEPLOY_DIR/postgres/initdb/10-create-databases.sh"
    "$POSTGRES_TLS_DIR/server.crt"
    "$POSTGRES_TLS_DIR/server.key"
    "$POSTGRES_TLS_DIR/ca.crt"
    "$POSTGRES_SECRET_DIR/admin-password"
    "$POSTGRES_SECRET_DIR/app-password"
    "$POSTGRES_SECRET_DIR/tap-password"
)
for file in "${files[@]}"; do require_file "$file"; done
ssh_generator_mask=/etc/systemd/system-generators/sshd-socket-generator
[[ -L "$ssh_generator_mask" && $(readlink "$ssh_generator_mask") == /dev/null ]] \
    || die 'Ubuntu sshd-socket-generator must be masked with the reviewed /dev/null symlink'
[[ $(stat -c '%u' "$ssh_generator_mask") -eq 0 ]] \
    || die 'sshd-socket-generator mask must be owned by root'

mkdir -p "$DEPLOY_DIR/runtime"
chmod 0700 "$DEPLOY_DIR/runtime"
output="$DEPLOY_DIR/runtime/release-record.postgres.json"
tmp=$(mktemp "$DEPLOY_DIR/runtime/.release-record.postgres.XXXXXX")
trap 'rm -f "$tmp"' EXIT
chmod 0600 "$tmp"

python3 - "$tmp" "$REPOSITORY_ROOT" "$ssh_generator_mask" \
    "$DEPLOYMENT_ID" "$POSTGRES_IMAGE" "${files[@]}" <<'PY'
import datetime, hashlib, json, pathlib, subprocess, sys

output, repo, ssh_generator_mask, deployment, postgres, *files = sys.argv[1:]

def digest(path):
    h = hashlib.sha256()
    with open(path, 'rb') as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b''):
            h.update(chunk)
    return h.hexdigest()

record = {
    'format': 'skypulse-postgres-release-record/v1',
    'generatedAtUtc': datetime.datetime.now(datetime.timezone.utc).isoformat().replace('+00:00', 'Z'),
    'gitCommit': subprocess.check_output(['git', '-C', repo, 'rev-parse', 'HEAD'], text=True).strip(),
    'deploymentId': deployment,
    'postgresImage': postgres,
    'sshSocketGeneratorMask': {
        'path': ssh_generator_mask,
        'target': '/dev/null',
    },
    'files': [
        {'path': str(pathlib.Path(path).resolve()), 'sha256': digest(path)}
        for path in files
    ],
}
pathlib.Path(output).write_text(json.dumps(record, sort_keys=True, indent=2) + '\n', encoding='utf-8')
PY

mv -f "$tmp" "$output"
trap - EXIT
note "Wrote private database-host release record: $output"
note 'Pair it with the app-host record and encrypted whole-cluster backup.'
