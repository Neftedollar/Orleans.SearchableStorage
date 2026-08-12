#!/usr/bin/env bash
set -euo pipefail

repository_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$repository_root"

if [[ "${OSS_RELEASE_ALLOW_DIRTY:-false}" != "true" ]] && [[ -n "$(git status --porcelain=v1)" ]]; then
  echo "Release dry-run requires a clean repository. Set OSS_RELEASE_ALLOW_DIRTY=true only for local development." >&2
  exit 1
fi

release_commit=${OSS_RELEASE_GIT_COMMIT:-$(git rev-parse HEAD)}
if [[ ! "$release_commit" =~ ^[0-9a-fA-F]{40,64}$ ]]; then
  echo "OSS_RELEASE_GIT_COMMIT must be a full hexadecimal revision." >&2
  exit 1
fi
actual_commit=$(git rev-parse HEAD)
if [[ "${release_commit,,}" != "${actual_commit,,}" ]]; then
  echo "Release provenance $release_commit does not match checked-out HEAD $actual_commit." >&2
  exit 1
fi

release_version=${OSS_RELEASE_PACKAGE_VERSION:-$(python3 - <<'PY'
import xml.etree.ElementTree as ET
root = ET.parse("src/Orleans.SearchableStorage/Orleans.SearchableStorage.csproj").getroot()
value = root.findtext(".//VersionPrefix")
if not value:
    raise SystemExit("VersionPrefix is missing")
print(value)
PY
)}

release_temp_dir=$(mktemp -d)
cleanup_release_temp_dir() {
  if [[ -n "${release_temp_dir:-}" ]] && [[ -d "$release_temp_dir" ]]; then
    rm -rf -- "$release_temp_dir"
  fi
}
trap cleanup_release_temp_dir EXIT

if [[ "${OSS_RELEASE_NO_BUILD:-false}" != "true" ]]; then
  dotnet restore Orleans.SearchableStorage.slnx
  dotnet build src/Orleans.SearchableStorage/Orleans.SearchableStorage.csproj \
    --configuration Release \
    --no-restore
fi

for ordinal in first second; do
  output="$release_temp_dir/$ordinal"
  mkdir -p "$output"
  dotnet pack src/Orleans.SearchableStorage/Orleans.SearchableStorage.csproj \
    --configuration Release \
    --no-build \
    --no-restore \
    --output "$output" \
    -p:ContinuousIntegrationBuild=true \
    -p:RepositoryCommit="$release_commit" \
    -p:PackageVersion="$release_version"
  package="$output/Orleans.SearchableStorage.$release_version.nupkg"
  python3 eng/validate-package.py "$package" \
    --expected-version "$release_version" \
    --expected-commit "$release_commit" \
    --canonical-output "$release_temp_dir/$ordinal.canonical.json"
done

if ! cmp --silent "$release_temp_dir/first.canonical.json" "$release_temp_dir/second.canonical.json"; then
  echo "Two packs from the same source produced different canonical entries or metadata." >&2
  diff --unified "$release_temp_dir/first.canonical.json" "$release_temp_dir/second.canonical.json" >&2 || true
  exit 1
fi

cat >"$release_temp_dir/NuGet.Config" <<EOF
<?xml version="1.0" encoding="utf-8"?>
<configuration>
  <packageSources>
    <clear />
    <add key="release-candidate" value="$release_temp_dir/first" />
    <add key="nuget.org" value="https://api.nuget.org/v3/index.json" protocolVersion="3" />
  </packageSources>
  <packageSourceMapping>
    <clear />
    <packageSource key="release-candidate">
      <package pattern="Orleans.SearchableStorage" />
    </packageSource>
    <packageSource key="nuget.org">
      <package pattern="Microsoft.*" />
      <package pattern="PolyType" />
      <package pattern="System.*" />
      <package pattern="Humanizer.Core" />
      <package pattern="Newtonsoft.Json" />
    </packageSource>
  </packageSourceMapping>
</configuration>
EOF

consumer=eng/package-smoke/PackageConsumer.csproj
dotnet restore "$consumer" \
  --configfile "$release_temp_dir/NuGet.Config" \
  --force-evaluate \
  -p:BaseIntermediateOutputPath="$release_temp_dir/consumer-obj/" \
  -p:RestorePackagesPath="$release_temp_dir/package-cache" \
  -p:SearchableStoragePackageVersion="$release_version"
dotnet build "$consumer" \
  --configuration Release \
  --no-restore \
  -p:BaseIntermediateOutputPath="$release_temp_dir/consumer-obj/" \
  -p:OutputPath="$release_temp_dir/consumer-bin/" \
  -p:RestorePackagesPath="$release_temp_dir/package-cache" \
  -p:SearchableStoragePackageVersion="$release_version"

echo "Release dry-run passed for Orleans.SearchableStorage $release_version at $release_commit."
