#!/bin/sh
set -eu

fail() {
    printf 'SkyPulse firewall interface check failed: %s\n' "$*" >&2
    exit 1
}

[ "$#" -eq 1 ] || fail 'one interface-binding file is required'
binding=$1
if [ ! -f "$binding" ] || [ -L "$binding" ]; then
    fail 'binding must be a regular non-symlink file'
fi
[ "$(wc -l < "$binding")" -eq 1 ] || fail 'binding must contain exactly one line'
IFS=' ' read -r interface address extra < "$binding"
if [ -z "$interface" ] || [ -z "$address" ] || [ -n "${extra:-}" ]; then
    fail 'binding must contain exactly interface and IPv4 address'
fi
[ "$interface" != lo ] || fail 'loopback cannot be the private interface'
printf '%s' "$interface" | grep -Eq '^[A-Za-z0-9_.:-]{1,15}$' \
    || fail 'invalid interface name'
printf '%s' "$address" | grep -Eq '^([0-9]{1,3}[.]){3}[0-9]{1,3}$' \
    || fail 'invalid IPv4 text'
link=$(ip -o link show dev "$interface") || fail "interface does not exist: $interface"
printf '%s\n' "$link" \
    | awk -F '[<>]' '{n=split($2, flags, ","); for (i=1; i<=n; i++) if (flags[i] == "UP") found=1} END {exit !found}' \
    || fail "interface is not administratively up: $interface"
ip -4 -o address show dev "$interface" \
    | awk -v expected="$address/" 'index($4, expected) == 1 { found=1 } END { exit !found }' \
    || fail "$interface does not own $address"
