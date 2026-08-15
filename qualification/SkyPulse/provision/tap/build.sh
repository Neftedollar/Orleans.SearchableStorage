#!/usr/bin/env bash
set -euo pipefail

readonly INDIGO_COMMIT="52c38ce3daca2e85a9f70cf052b475506463018e"
readonly REQUIRED_GO_VERSION="go1.26.1"
readonly OVERLAY_PATCH_SHA256="17575e48b5762616fe0e7c6fc56ebe23d442df3a4cf60d35d5377193b6a36056"
readonly HARDENING_PATCH_SHA256="2b8be0ceb8e2a71d15710199e545579a82a70ac2428d89bb88d1e74825c20101"
readonly PRIVACY_PATCH_SHA256="63ff8131d6fe838f92464f4b133b798bbb29e902e954ee42cb1660af5cf9ceb0"

script_directory=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
readonly script_directory
readonly overlay_patch="${script_directory}/indigo-52c38ce3-skypulse-metadata.patch"
readonly hardening_patch="${script_directory}/qualification-startup-hardening.patch"
readonly privacy_patch="${script_directory}/qualification-privacy-logging.patch"
requested_output_path=${TAP_OUTPUT:-"${script_directory}/dist/tap"}
if [[ "${requested_output_path}" = /* ]]; then
    output_path=${requested_output_path}
else
    output_path="$(pwd -P)/${requested_output_path}"
fi
readonly output_path

for command_name in git go awk; do
    if ! command -v "${command_name}" >/dev/null 2>&1; then
        echo "Required command is unavailable: ${command_name}" >&2
        exit 1
    fi
done

if ! command -v sha256sum >/dev/null 2>&1 && ! command -v shasum >/dev/null 2>&1; then
    echo "Required checksum command is unavailable: sha256sum or shasum" >&2
    exit 1
fi

actual_go_version=$(go env GOVERSION)
if [[ "${actual_go_version}" != "${REQUIRED_GO_VERSION}" ]]; then
    echo "Expected ${REQUIRED_GO_VERSION}, found ${actual_go_version}." >&2
    exit 1
fi

calculate_sha256() {
    if command -v sha256sum >/dev/null 2>&1; then
        sha256sum "$1" | awk '{print $1}'
    else
        shasum -a 256 "$1" | awk '{print $1}'
    fi
}

actual_patch_sha256=$(calculate_sha256 "${overlay_patch}")
if [[ "${actual_patch_sha256}" != "${OVERLAY_PATCH_SHA256}" ]]; then
    echo "Overlay patch SHA-256 mismatch: ${actual_patch_sha256}" >&2
    exit 1
fi
echo "${overlay_patch}: OK"

actual_hardening_sha256=$(calculate_sha256 "${hardening_patch}")
if [[ "${actual_hardening_sha256}" != "${HARDENING_PATCH_SHA256}" ]]; then
    echo "Hardening patch SHA-256 mismatch: ${actual_hardening_sha256}" >&2
    exit 1
fi
echo "${hardening_patch}: OK"

actual_privacy_sha256=$(calculate_sha256 "${privacy_patch}")
if [[ "${actual_privacy_sha256}" != "${PRIVACY_PATCH_SHA256}" ]]; then
    echo "Privacy patch SHA-256 mismatch: ${actual_privacy_sha256}" >&2
    exit 1
fi
echo "${privacy_patch}: OK"

temporary_root=${TMPDIR:-/tmp}
work_directory=$(mktemp -d "${temporary_root%/}/skypulse-tap.XXXXXXXX")
readonly work_directory

cleanup() {
    if [[ -n "${work_directory}" && -d "${work_directory}" ]]; then
        rm -rf -- "${work_directory}"
    fi
}
trap cleanup EXIT

git -C "${work_directory}" init --quiet
git -C "${work_directory}" remote add origin https://github.com/bluesky-social/indigo.git
git -C "${work_directory}" fetch --quiet --depth=1 origin "${INDIGO_COMMIT}"
git -C "${work_directory}" checkout --quiet --detach FETCH_HEAD

actual_commit=$(git -C "${work_directory}" rev-parse HEAD)
if [[ "${actual_commit}" != "${INDIGO_COMMIT}" ]]; then
    echo "Expected Indigo ${INDIGO_COMMIT}, found ${actual_commit}." >&2
    exit 1
fi

git -C "${work_directory}" apply --check "${overlay_patch}"
git -C "${work_directory}" apply "${overlay_patch}"
git -C "${work_directory}" apply --check "${hardening_patch}"
git -C "${work_directory}" apply "${hardening_patch}"
git -C "${work_directory}" apply --check "${privacy_patch}"
git -C "${work_directory}" apply "${privacy_patch}"

(
    cd "${work_directory}"
    GOTOOLCHAIN=local go test ./cmd/tap
    GOTOOLCHAIN=local go test ./atproto/atdata
    GOTOOLCHAIN=local go vet ./cmd/tap
)

mkdir -p -- "$(dirname -- "${output_path}")"
(
    cd "${work_directory}"
    GOTOOLCHAIN=local CGO_ENABLED=1 go build \
        -trimpath \
        -buildvcs=false \
        -ldflags='-buildid=' \
        -o "${output_path}" \
        ./cmd/tap
)

echo "$(calculate_sha256 "${output_path}")  ${output_path}"
