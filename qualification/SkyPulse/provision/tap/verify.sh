#!/usr/bin/env bash
set -euo pipefail

script_directory=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
readonly script_directory
first_binary="${script_directory}/dist/tap.first"
second_binary="${script_directory}/dist/tap.second"

TAP_OUTPUT="${first_binary}" "${script_directory}/build.sh"
TAP_OUTPUT="${second_binary}" "${script_directory}/build.sh"

if ! cmp -s "${first_binary}" "${second_binary}"; then
    echo "Two clean host builds produced different TAP binaries." >&2
    exit 1
fi

if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "${first_binary}" "${second_binary}"
else
    shasum -a 256 "${first_binary}" "${second_binary}"
fi

