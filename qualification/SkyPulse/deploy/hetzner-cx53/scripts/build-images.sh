#!/usr/bin/env bash
set -euo pipefail
umask 077

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
DEPLOY_DIR=$(cd -- "$SCRIPT_DIR/.." && pwd)
SKYPULSE_ROOT=$(cd -- "$DEPLOY_DIR/../.." && pwd)
REPOSITORY_ROOT=$(cd -- "$SKYPULSE_ROOT/../.." && pwd)
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

require_command docker
require_command git
require_command python3
require_command sha256sum
require_command tar
load_deploy_env
require_digest_ref DOTNET_SDK_IMAGE
require_digest_ref DOTNET_ASPNET_IMAGE
[[ "$DOTNET_SDK_IMAGE" == mcr.microsoft.com/dotnet/sdk:10.0.303-noble@sha256:* ]] \
    || die 'DOTNET_SDK_IMAGE must be the reviewed 10.0.303 noble image'
[[ "$DOTNET_ASPNET_IMAGE" == mcr.microsoft.com/dotnet/aspnet:10.0.11-noble@sha256:* ]] \
    || die 'DOTNET_ASPNET_IMAGE must be the reviewed 10.0.11 noble image'

# Current buildx releases (verified with v0.36.1) have no `inspect --format`;
# parse the stable text field instead. awk must consume the whole stream: an
# early exit closes the pipe and fails the pipeline under pipefail via SIGPIPE.
buildx_driver=$(docker buildx inspect --bootstrap | awk '/^Driver:/ && !seen { print $2; seen=1 }')
[[ "$buildx_driver" == docker ]] \
    || die "the TAP wrapper requires the buildx docker driver (found $buildx_driver)"

[[ "$PLATFORM" == linux/amd64 ]] || die 'the reviewed first deployment platform is linux/amd64'
[[ "$DEPLOYMENT_ID" =~ ^[0-9a-f]{40}$ ]] || die 'DEPLOYMENT_ID must be the full release Git SHA'
head=$(git -C "$REPOSITORY_ROOT" rev-parse HEAD)
[[ "$head" == "$DEPLOYMENT_ID" ]] || die "checkout HEAD $head does not match DEPLOYMENT_ID $DEPLOYMENT_ID"
[[ -z "$(git -C "$REPOSITORY_ROOT" status --porcelain --untracked-files=all)" ]] \
    || die 'checkout has tracked or untracked changes; build from an exact clean commit'

build_context=$(mktemp -d)
tmp=
cleanup() {
    [[ -z "$tmp" ]] || rm -f -- "$tmp"
    [[ -z "$build_context" ]] || rm -rf -- "$build_context"
}
trap cleanup EXIT
git -C "$REPOSITORY_ROOT" archive "$DEPLOYMENT_ID:qualification/SkyPulse" \
    | tar -x -C "$build_context"

package="$build_context/lock/package-source/Orleans.SearchableStorage.1.0.0-rc.2.nupkg"
printf '%s  %s\n' \
    d9c05681a0866f027d394843089d6534d06d151f18f611dce3f1e7b5f1e9331c \
    "$package" | sha256sum --check --strict
python3 "$REPOSITORY_ROOT/eng/validate-package.py" "$package" \
    --expected-version 1.0.0-rc.2 \
    --expected-commit 6301f8b676edcc6ae0936ead38927f45adb99b00 \
    --expected-package-sha256 d9c05681a0866f027d394843089d6534d06d151f18f611dce3f1e7b5f1e9331c \
    --expected-canonical-sha256 c711886b0559b2e667ffa43c8628aaa3088ee32fe64ce4363230ba4e1b52d983

app_image="skypulse-app:${DEPLOYMENT_ID}"
tap_base_image="skypulse-tap-base:${DEPLOYMENT_ID}"
tap_image="skypulse-tap:${DEPLOYMENT_ID}"

docker buildx build --platform "$PLATFORM" --load \
    --build-arg "DOTNET_SDK_IMAGE=$DOTNET_SDK_IMAGE" \
    --build-arg "DOTNET_ASPNET_IMAGE=$DOTNET_ASPNET_IMAGE" \
    --file "$build_context/deploy/hetzner-cx53/Dockerfile.app" \
    --tag "$app_image" "$build_context"

docker buildx build --platform "$PLATFORM" --load \
    --file "$build_context/provision/tap/Dockerfile" \
    --tag "$tap_base_image" "$build_context/provision/tap"

tap_sha=$(docker run --rm --entrypoint sha256sum "$tap_base_image" /usr/local/bin/tap | awk '{print $1}')
[[ "$tap_sha" == 0142caff15f321cdabe68761f2cbf5e9f85cfbb8f8eb21787b72987666a368f2 ]] \
    || die "unexpected TAP binary SHA-256: $tap_sha"

docker buildx build --platform "$PLATFORM" --load \
    --build-arg "TAP_BASE_IMAGE=$tap_base_image" \
    --file "$build_context/deploy/hetzner-cx53/Dockerfile.tap" \
    --tag "$tap_image" "$build_context/deploy/hetzner-cx53"

app_id=$(docker image inspect --format '{{.Id}}' "$app_image")
tap_id=$(docker image inspect --format '{{.Id}}' "$tap_image")
mkdir -p "$DEPLOY_DIR/runtime"
chmod 0700 "$DEPLOY_DIR/runtime"
tmp=$(mktemp "$DEPLOY_DIR/runtime/.images.env.XXXXXX")
chmod 0600 "$tmp"
printf 'SKYPULSE_APP_IMAGE=%s\nSKYPULSE_TAP_IMAGE=%s\nSKYPULSE_APP_IMAGE_ID=%s\nSKYPULSE_TAP_IMAGE_ID=%s\n' \
    "$app_image" "$tap_image" "$app_id" "$tap_id" > "$tmp"
mv -f "$tmp" "$DEPLOY_DIR/runtime/images.env"
tmp=

note "Built $app_image ($app_id)"
note "Built $tap_image ($tap_id; TAP binary $tap_sha)"
