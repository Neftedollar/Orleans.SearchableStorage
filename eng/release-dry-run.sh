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
explicit_version = root.findtext(".//Version")
if explicit_version:
    print(explicit_version.strip())
else:
    prefix = root.findtext(".//VersionPrefix")
    if not prefix:
        raise SystemExit("Version or VersionPrefix is missing")
    suffix = root.findtext(".//VersionSuffix")
    print(f"{prefix.strip()}-{suffix.strip()}" if suffix and suffix.strip() else prefix.strip())
PY
)}

release_output_directory=${OSS_RELEASE_OUTPUT_DIRECTORY:-}
release_output_package=
release_output_manifest=
if [[ -n "$release_output_directory" ]]; then
  mkdir -p -- "$release_output_directory"
  release_output_directory=$(realpath --canonicalize-existing -- "$release_output_directory")
  release_output_package="$release_output_directory/Orleans.SearchableStorage.$release_version.nupkg"
  release_output_manifest="$release_output_directory/package.canonical.json"
  if [[ -e "$release_output_package" || -e "$release_output_manifest" ]]; then
    echo "Release output already exists; refusing to overwrite $release_output_directory." >&2
    exit 1
  fi
fi

release_temp_dir=$(mktemp -d)
cleanup_release_temp_dir() {
  if [[ -n "${release_temp_dir:-}" ]] && [[ -d "$release_temp_dir" ]]; then
    rm -rf -- "$release_temp_dir"
  fi
}
trap cleanup_release_temp_dir EXIT

release_input_package=${OSS_RELEASE_INPUT_PACKAGE:-}
release_input_repository_signed=${OSS_RELEASE_INPUT_REPOSITORY_SIGNED:-false}
if [[ "$release_input_repository_signed" != "true" && "$release_input_repository_signed" != "false" ]]; then
  echo "OSS_RELEASE_INPUT_REPOSITORY_SIGNED must be true or false." >&2
  exit 1
fi
if [[ "$release_input_repository_signed" == "true" && -z "$release_input_package" ]]; then
  echo "OSS_RELEASE_INPUT_REPOSITORY_SIGNED=true requires OSS_RELEASE_INPUT_PACKAGE." >&2
  exit 1
fi
if [[ -n "$release_input_package" ]]; then
  release_input_source=$(realpath --no-symlinks "$release_input_package")
  if [[ ! -f "$release_input_source" ]]; then
    echo "OSS_RELEASE_INPUT_PACKAGE does not name a package file: $release_input_source" >&2
    exit 1
  fi
  mkdir -p "$release_temp_dir/input"
  release_input_package="$release_temp_dir/input/Orleans.SearchableStorage.$release_version.nupkg"
  input_validator_args=(
    "$release_input_source"
    --expected-version "$release_version"
    --expected-commit "$release_commit"
    --canonical-output "$release_temp_dir/input.canonical.json"
    --snapshot-output "$release_input_package"
  )
  if [[ "$release_input_repository_signed" == "true" ]]; then
    input_validator_args+=(--repository-signed)
  fi
  python3 eng/validate-package.py "${input_validator_args[@]}"
fi

if [[ "${OSS_RELEASE_NO_BUILD:-false}" != "true" ]]; then
  dotnet restore Orleans.SearchableStorage.slnx
  dotnet build src/Orleans.SearchableStorage/Orleans.SearchableStorage.csproj \
    --configuration Release \
    --no-restore
  bash eng/validate-source-compat.sh --no-restore
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

if [[ -n "$release_input_package" ]] \
  && ! cmp --silent "$release_temp_dir/input.canonical.json" "$release_temp_dir/first.canonical.json"; then
  echo "The supplied release package differs from a canonical repack of the same source." >&2
  diff --unified "$release_temp_dir/input.canonical.json" "$release_temp_dir/first.canonical.json" >&2 || true
  exit 1
fi

if [[ -z "$release_input_package" ]]; then
  release_input_package="$release_temp_dir/first/Orleans.SearchableStorage.$release_version.nupkg"
fi
release_input_directory=$(dirname "$release_input_package")

cat >"$release_temp_dir/NuGet.Config" <<EOF
<?xml version="1.0" encoding="utf-8"?>
<configuration>
  <packageSources>
    <clear />
    <add key="release-candidate" value="$release_input_directory" />
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

if [[ -n "$release_output_directory" ]]; then
  cp -- "$release_input_package" "$release_output_package"
  cp -- "$release_temp_dir/first.canonical.json" "$release_output_manifest"
  chmod a-w -- "$release_output_package" "$release_output_manifest"
  echo "Retained validated package and canonical manifest in $release_output_directory."
fi

echo "Release dry-run passed for Orleans.SearchableStorage $release_version at $release_commit."
