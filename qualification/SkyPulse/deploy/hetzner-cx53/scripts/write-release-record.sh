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
validate_control_env app "$DEPLOY_DIR/runtime/app.env"
validate_control_env images "$DEPLOY_DIR/runtime/images.env"

set -a
# shellcheck disable=SC1091
source "$DEPLOY_DIR/runtime/app.env"
# shellcheck disable=SC1091
source "$DEPLOY_DIR/runtime/images.env"
set +a

head=$(git -C "$REPOSITORY_ROOT" rev-parse HEAD)
[[ "$head" == "$DEPLOYMENT_ID" ]] || die "checkout HEAD $head does not match DEPLOYMENT_ID $DEPLOYMENT_ID"
[[ -z $(git -C "$REPOSITORY_ROOT" status --porcelain --untracked-files=all) ]] \
    || die 'tracked or untracked checkout changes invalidate the release record'
if [[ ${ENABLE_PUBLIC_PROXY:-false} == true ]]; then
    caddy_record=$CADDY_IMAGE
else
    caddy_record=
fi

profiles_json=$(mktemp)
profile_paths=$(mktemp)
tmp=
cleanup() {
    rm -f "$profiles_json" "$profile_paths"
    [[ -z "$tmp" ]] || rm -f "$tmp"
}
trap cleanup EXIT

python3 "$SCRIPT_DIR/profile-artifacts.py" \
    --public-manifest "$PUBLIC_CORPUS_DIR/corpus.manifest.json" \
    --private-root "$PRIVATE_ROUTING_ROOT" --expected-uid 10001 > "$profiles_json"
python3 "$SCRIPT_DIR/profile-artifacts.py" \
    --public-manifest "$PUBLIC_CORPUS_DIR/corpus.manifest.json" \
    --private-root "$PRIVATE_ROUTING_ROOT" --expected-uid 10001 --paths0 > "$profile_paths"

files=(
    "$DEPLOY_DIR/.env"
    "$DEPLOY_DIR/runtime/app.env"
    "$DEPLOY_DIR/runtime/images.env"
    "$DEPLOY_DIR/compose.app.yaml"
    /etc/skypulse-app-firewall.nft
    /etc/skypulse-app-firewall.interface
    /etc/systemd/system/skypulse-app-firewall.service
    /usr/local/libexec/skypulse-firewall-interface-check
    /etc/systemd/system/docker.service.d/90-skypulse-firewall.conf
    /etc/systemd/system/ssh.service.d/90-skypulse-firewall.conf
    /etc/systemd/journald.conf.d/90-skypulse.conf
    "$APP_PG_CA_CERT"
    "$APP_SECRET_DIR/postgres-password"
    "$APP_SECRET_DIR/tap-admin-password"
    "$APP_SECRET_DIR/corpus-growth-admin-token"
    "$TAP_SECRET_DIR/postgres-password"
    "$TAP_SECRET_DIR/tap-admin-password"
    "$PUBLIC_CORPUS_DIR/corpus.manifest.json"
    "$PUBLIC_CORPUS_DIR/accounts.ak32"
)
while IFS= read -r -d '' path; do
    files+=("$path")
done < "$profile_paths"
if [[ ${ENABLE_PUBLIC_PROXY:-false} == true ]]; then
    files+=("$CADDYFILE_PATH" "$SKYPULSE_CURL_CONFIG")
fi
for file in "${files[@]}"; do require_file "$file"; done
ssh_generator_mask=/etc/systemd/system-generators/sshd-socket-generator
[[ -L "$ssh_generator_mask" && $(readlink "$ssh_generator_mask") == /dev/null ]] \
    || die 'Ubuntu sshd-socket-generator must be masked with the reviewed /dev/null symlink'
[[ $(stat -c '%u' "$ssh_generator_mask") -eq 0 ]] \
    || die 'sshd-socket-generator mask must be owned by root'

mkdir -p "$DEPLOY_DIR/runtime"
chmod 0700 "$DEPLOY_DIR/runtime"
output="$DEPLOY_DIR/runtime/release-record.app.json"
tmp=$(mktemp "$DEPLOY_DIR/runtime/.release-record.app.XXXXXX")
chmod 0600 "$tmp"

# Values below are loaded from the reviewed runtime/app.env contract.
# shellcheck disable=SC2154
python3 - "$tmp" "$REPOSITORY_ROOT" "$profiles_json" "$ssh_generator_mask" \
    "$DEPLOYMENT_ID" "$DOTNET_SDK_IMAGE" "$DOTNET_ASPNET_IMAGE" "$POSTGRES_IMAGE" \
    "$caddy_record" "$SKYPULSE_APP_IMAGE" "$SKYPULSE_APP_IMAGE_ID" \
    "$SKYPULSE_TAP_IMAGE" "$SKYPULSE_TAP_IMAGE_ID" \
    "${SkyPulse__Durable__ProfileVersion}" "${SkyPulse__Durable__SourceInstanceId}" \
    "${files[@]}" <<'PY'
import datetime, hashlib, json, pathlib, subprocess, sys

(output, repo, profiles_path, ssh_generator_mask, deployment, sdk, aspnet, postgres, caddy,
 app_image, app_id, tap_image, tap_id, version, source, *files) = sys.argv[1:]

def digest(path):
    h = hashlib.sha256()
    with open(path, 'rb') as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b''):
            h.update(chunk)
    return h.hexdigest()

record = {
    'format': 'skypulse-app-release-record/v1',
    'generatedAtUtc': datetime.datetime.now(datetime.timezone.utc).isoformat().replace('+00:00', 'Z'),
    'gitCommit': subprocess.check_output(['git', '-C', repo, 'rev-parse', 'HEAD'], text=True).strip(),
    'deploymentId': deployment,
    'images': {
        'dotnetSdk': sdk,
        'dotnetAspNet': aspnet,
        'postgres': postgres,
        'caddy': caddy or None,
        'app': {'name': app_image, 'id': app_id},
        'tap': {'name': tap_image, 'id': tap_id},
    },
    'profileVersion': int(version),
    'sourceInstanceId': source,
    'sshSocketGeneratorMask': {
        'path': ssh_generator_mask,
        'target': '/dev/null',
    },
    'configuredProfiles': json.loads(pathlib.Path(profiles_path).read_text(encoding='utf-8')),
    'files': [
        {'path': str(pathlib.Path(path).resolve()), 'sha256': digest(path)}
        for path in files
    ],
}
pathlib.Path(output).write_text(json.dumps(record, sort_keys=True, indent=2) + '\n', encoding='utf-8')
PY

mv -f "$tmp" "$output"
tmp=
note "Wrote private app-host release record: $output"
note 'Pair it with the database-host record and encrypted whole-cluster backup.'
