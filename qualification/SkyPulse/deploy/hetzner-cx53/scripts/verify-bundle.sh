#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
DEPLOY_DIR=$(cd -- "$SCRIPT_DIR/.." && pwd)
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

require_command bash
require_command python3
require_file "$DEPLOY_DIR/infra/.terraform.lock.hcl"
shopt -s nullglob
infra_overrides=("$DEPLOY_DIR"/infra/*_override.tf "$DEPLOY_DIR"/infra/*_override.tf.json)
for exact_override in "$DEPLOY_DIR/infra/override.tf" "$DEPLOY_DIR/infra/override.tf.json"; do
    [[ ! -e "$exact_override" && ! -L "$exact_override" ]] || infra_overrides+=("$exact_override")
done
(( ${#infra_overrides[@]} == 0 )) \
    || die 'OpenTofu override files are forbidden in the reviewed release and operator checkout'
infra_tfvars=("$DEPLOY_DIR"/infra/*.tfvars "$DEPLOY_DIR"/infra/*.tfvars.json)
for tfvars in "${infra_tfvars[@]}"; do
    [[ $(basename "$tfvars") == terraform.tfvars ]] \
        || die "unexpected auto-loaded or alternate OpenTofu variable file: $tfvars"
    require_control_file "$tfvars"
done
shopt -u nullglob

while IFS= read -r -d '' script; do
    bash -n "$script"
    [[ $(stat -c '%a' "$script") == 755 ]] \
        || die "deployment script must have mode 0755: $script"
done < <(find "$DEPLOY_DIR" -type f -name '*.sh' -print0)

python3 - "$DEPLOY_DIR" <<'PY'
import pathlib, sys
root = pathlib.Path(sys.argv[1])
for path in root.rglob('*'):
    if '.terraform' in path.parts:
        continue
    if path.is_file():
        data = path.read_bytes()
        if b'\r\n' in data:
            raise SystemExit(f'CRLF is forbidden: {path}')
        if (b'did' + b':plc:') in data:
            raise SystemExit(f'a raw DID-like value is forbidden in the deployment bundle: {path}')

firewalls = {
    "skypulse-app-firewall.nft.template": {
        "__APP_PRIVATE_IP__": 2,
        "__POSTGRES_PRIVATE_IP__": 3,
        "__PRIVATE_INTERFACE__": 9,
    },
    "skypulse-postgres-firewall.nft.template": {
        "__APP_PRIVATE_IP__": 3,
        "__POSTGRES_PRIVATE_IP__": 1,
        "__PRIVATE_INTERFACE__": 9,
    },
}
for name, counts in firewalls.items():
    value = (root / "config" / name).read_text(encoding="utf-8")
    for token, wanted in counts.items():
        if value.count(token) != wanted:
            raise SystemExit(f"unexpected {token} count in {name}")
    rendered = value
    for token in counts:
        rendered = rendered.replace(token, "verified")
    if "__" in rendered:
        raise SystemExit(f"unresolved firewall placeholder in {name}")

try:
    import yaml
except ImportError:
    print('PyYAML unavailable; skipping syntax-only YAML parse', file=sys.stderr)
else:
    for name in ('compose.app.yaml', 'compose.postgres.yaml'):
        value = yaml.safe_load((root / name).read_text(encoding='utf-8'))
        if not isinstance(value, dict) or 'services' not in value:
            raise SystemExit(f'invalid Compose structure: {name}')
PY

grep -Fxq '**' "$DEPLOY_DIR/Dockerfile.app.dockerignore" \
    || die 'application Docker context must start deny-by-default'
grep -Fq '!lock/package-source/Orleans.SearchableStorage.1.0.0-rc.2.nupkg' \
    "$DEPLOY_DIR/Dockerfile.app.dockerignore" || die 'verified package is absent from app context'
if grep -Eq '^COPY[[:space:]]+\.[[:space:]]+\.' "$DEPLOY_DIR/Dockerfile.app"; then
    die 'application Dockerfile may not broadly COPY the build context'
fi
if grep -Eq 'TAP_(FULL_NETWORK|SIGNAL_COLLECTION|DISABLE_ACKS|WEBHOOK_URL|NO_REPLAY|COLLECTION_FILTERS|OUTBOX_ONLY)' \
    "$DEPLOY_DIR/compose.app.yaml"; then
    die 'production Compose contains a forbidden TAP setting'
fi
if grep -Fq 'ws://tap' "$DEPLOY_DIR/compose.app.yaml"; then
    die 'production Compose contains a forbidden bridge TAP endpoint'
fi
if grep -Fq 'ports:' "$DEPLOY_DIR/compose.app.yaml"; then
    die 'app/TAP/Caddy host-network stack must not publish bridge ports'
fi
grep -Eq '^local[[:space:]]+replication[[:space:]]+skypulse_admin[[:space:]]+trust$' \
    "$DEPLOY_DIR/config/pg_hba.conf.template" \
    || die 'HBA template must permit local whole-cluster pg_basebackup'

if command -v shellcheck >/dev/null 2>&1; then
    find "$DEPLOY_DIR" -type f -name '*.sh' -print0 \
        | xargs -0 shellcheck -x -P "$DEPLOY_DIR/scripts"
else
    note 'shellcheck is unavailable; bash syntax and custom static checks still ran.'
fi

if command -v tofu >/dev/null 2>&1; then
    infra_check=$(mktemp -d)
    cleanup_infra_check() {
        rm -rf -- "$infra_check"
    }
    trap cleanup_infra_check EXIT
    cp "$DEPLOY_DIR/infra/"*.tf "$DEPLOY_DIR/infra/.terraform.lock.hcl" "$infra_check/"
    tofu -chdir="$DEPLOY_DIR/infra" fmt -check -recursive
    tofu -chdir="$infra_check" init -backend=false -input=false -lockfile=readonly
    tofu -chdir="$infra_check" validate
else
    note 'OpenTofu is unavailable; the committed provider lock is present, but infra validation remains a hard local gate.'
fi

note 'Static deployment-bundle checks passed.'
