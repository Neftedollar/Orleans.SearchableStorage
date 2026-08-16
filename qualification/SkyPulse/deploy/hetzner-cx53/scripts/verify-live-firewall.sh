#!/usr/bin/env bash
set -euo pipefail
umask 077

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

[[ $# -eq 2 ]] || die 'usage: verify-live-firewall.sh /absolute/config table-name'
config=$1
table_name=$2
[[ "$table_name" =~ ^skypulse_(app|postgres)$ ]] || die 'unexpected firewall table name'
require_file "$config"
require_command cmp
require_command ip
require_command nft
[[ $(id -u) -eq 0 ]] || die 'live firewall verification must run as root'

namespace="skypulse-nft-check-$$-$RANDOM"
expected=$(mktemp)
actual=$(mktemp)
cleanup() {
    ip netns delete "$namespace" >/dev/null 2>&1 || true
    rm -f "$expected" "$actual"
}
trap cleanup EXIT
ip netns add "$namespace"
ip netns exec "$namespace" nft -f "$config"
ip netns exec "$namespace" nft --stateless list table inet "$table_name" > "$expected"
nft --stateless list table inet "$table_name" > "$actual"
cmp --silent "$expected" "$actual" \
    || die "live nftables table $table_name differs from the reviewed file"
