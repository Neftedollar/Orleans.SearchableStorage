#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
DEPLOY_DIR=$(cd -- "$SCRIPT_DIR/.." && pwd)
SKYPULSE_ROOT=$(cd -- "$DEPLOY_DIR/../.." && pwd)
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

runtime_only=false
if [[ ${1:-} == --runtime ]]; then
    runtime_only=true
    shift
fi
[[ $# -eq 0 ]] || die 'usage: preflight-artifacts.sh [--runtime]'

require_command dotnet
load_deploy_env
validate_control_env app "$DEPLOY_DIR/runtime/app.env"
set -a
# shellcheck disable=SC1091
source "$DEPLOY_DIR/runtime/app.env"
set +a

manifest="$PUBLIC_CORPUS_DIR/corpus.manifest.json"
accounts="$PUBLIC_CORPUS_DIR/accounts.ak32"
require_file "$manifest"
require_file "$accounts"

profile_paths=$(mktemp)
trap 'rm -f "$profile_paths"' EXIT
expected_uid=$(id -u)
if [[ "$runtime_only" == true ]]; then
    expected_uid=10001
fi
python3 "$SCRIPT_DIR/profile-artifacts.py" \
    --public-manifest "$manifest" \
    --private-root "$PRIVATE_ROUTING_ROOT" \
    --expected-uid "$expected_uid" --paths0 > "$profile_paths"

cd "$SKYPULSE_ROOT"
corpus_args=(verify --manifest "$manifest" --deep)
if [[ "$runtime_only" == false ]]; then
    [[ -n ${PRIVATE_OBSERVATION_JOURNAL:-} ]] || die 'PRIVATE_OBSERVATION_JOURNAL is required for the release artifact proof'
    require_file "$PRIVATE_OBSERVATION_JOURNAL"
    corpus_args+=(--journal "$PRIVATE_OBSERVATION_JOURNAL")
else
    note 'Runtime preflight omits the private source-journal proof; it must already have passed before transfer.'
fi

dotnet run \
    --project src/Orleans.SearchableStorage.Qualification.SkyPulse.CorpusBuilder \
    -c Release --no-build -- "${corpus_args[@]}"

while IFS= read -r -d '' route_manifest \
    && IFS= read -r -d '' route_data; do
    : "$route_data"
    dotnet run \
        --project src/Orleans.SearchableStorage.Qualification.SkyPulse.CorpusAcquisition \
        -c Release --no-build -- verify-route --manifest "$route_manifest"
done < "$profile_paths"

note 'Base and every configured growth-profile artifact check passed.'
