#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
DEPLOY_DIR=$(cd -- "$SCRIPT_DIR/.." && pwd)
SKYPULSE_ROOT=$(cd -- "$DEPLOY_DIR/../.." && pwd)
REPOSITORY_ROOT=$(cd -- "$SKYPULSE_ROOT/../.." && pwd)
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

require_command dotnet
require_command python3
require_command sha256sum
require_command git

[[ "$(dotnet --version)" == 10.0.303 ]] || die 'exact .NET SDK 10.0.303 is required'

package="$SKYPULSE_ROOT/lock/package-source/Orleans.SearchableStorage.1.0.0-rc.2.nupkg"
printf '%s  %s\n' \
    d9c05681a0866f027d394843089d6534d06d151f18f611dce3f1e7b5f1e9331c \
    "$package" | sha256sum --check --strict

python3 "$REPOSITORY_ROOT/eng/validate-package.py" "$package" \
    --expected-version 1.0.0-rc.2 \
    --expected-commit 6301f8b676edcc6ae0936ead38927f45adb99b00 \
    --expected-package-sha256 d9c05681a0866f027d394843089d6534d06d151f18f611dce3f1e7b5f1e9331c \
    --expected-canonical-sha256 c711886b0559b2e667ffa43c8628aaa3088ee32fe64ce4363230ba4e1b52d983

cd "$SKYPULSE_ROOT"
dotnet restore Orleans.SearchableStorage.Qualification.slnx \
    --locked-mode --configfile NuGet.Config
dotnet build Orleans.SearchableStorage.Qualification.slnx \
    -c Release --no-restore
dotnet test Orleans.SearchableStorage.Qualification.slnx \
    -c Release --no-build --no-restore

git -C "$REPOSITORY_ROOT" diff --check
note 'Source/package verification passed.'
note 'The solution command may skip opt-in PostgreSQL tests; run-postgres-integration.sh is a separate mandatory gate.'

