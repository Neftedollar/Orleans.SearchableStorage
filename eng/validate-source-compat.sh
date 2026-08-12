#!/usr/bin/env bash
set -euo pipefail

repository_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$repository_root"

dotnet build src/Orleans.SearchableStorage/Orleans.SearchableStorage.csproj \
  --configuration Release \
  "$@"

dotnet build eng/source-compat-probe/SourceCompatProbe.csproj \
  --configuration Release \
  "$@"

probe_dir=$(mktemp -d)
cleanup_probe_dir() {
  if [[ -d "$probe_dir" ]]; then
    rm -rf -- "$probe_dir"
  fi
}
trap cleanup_probe_dir EXIT

set +e
dotnet build eng/source-compat-probe/SourceCompatProbe.csproj \
  --configuration Release \
  --property:DefineConstants=OSS_SOURCE_COMPAT_NEGATIVE \
  "$@" >"$probe_dir/negative.log" 2>&1
negative_status=$?
set -e

if [[ $negative_status -eq 0 ]]; then
  echo "The source-compatibility canary unexpectedly accepted removal of 'notnull'." >&2
  exit 1
fi

actual_diagnostics=$(
  sed -n 's/.*error \([A-Z][A-Z0-9]*\):.*/\1/p' "$probe_dir/negative.log" \
    | LC_ALL=C sort --unique
)
expected_diagnostics=$'OSSAPI001\nOSSAPI002'
if [[ "$actual_diagnostics" != "$expected_diagnostics" ]]; then
  echo "The source-compatibility canary failed for an unexpected reason." >&2
  cat "$probe_dir/negative.log" >&2
  exit 1
fi

if ! grep --fixed-strings --quiet \
  "SourceCompatibilityProbe<T> :: T = <none>" \
  "$probe_dir/negative.log"; then
  echo "The source-compatibility canary did not report the removed 'notnull' constraint." >&2
  cat "$probe_dir/negative.log" >&2
  exit 1
fi

echo "C# source-compatibility baselines and the notnull canary passed."
