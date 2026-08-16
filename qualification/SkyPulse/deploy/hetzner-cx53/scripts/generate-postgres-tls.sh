#!/usr/bin/env bash
set -euo pipefail
umask 077

die() {
    printf 'ERROR: %s\n' "$*" >&2
    exit 1
}

[[ $# -eq 3 ]] || die 'usage: generate-postgres-tls.sh DNS_NAME PRIVATE_IP /absolute/output'
dns_name=$1
private_ip=$2
out=$3

python3 - "$dns_name" "$private_ip" <<'PY' || exit 1
import ipaddress, re, sys
dns_name, value = sys.argv[1:]
label = re.compile(r"^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$")
if len(dns_name) > 253 or "." not in dns_name or any(
    label.fullmatch(part) is None for part in dns_name.split(".")
):
    raise SystemExit("DNS_NAME must be a canonical lowercase dotted hostname")
ip = ipaddress.IPv4Address(value)
private = tuple(map(ipaddress.IPv4Network, ("10.0.0.0/8", "172.16.0.0/12", "192.168.0.0/16")))
if not any(ip in network for network in private):
    raise SystemExit("PRIVATE_IP must be an RFC1918 IPv4 address")
PY
[[ "$out" = /* ]] || die 'output path must be absolute'
[[ ! -e "$out" ]] || die "refusing to overwrite existing path: $out"
command -v openssl >/dev/null 2>&1 || die 'openssl is required'

install -d -m 0700 "$out" "$out/ca" "$out/server"

openssl req -x509 -newkey rsa:3072 -nodes -sha256 -days 3650 \
    -subj '/CN=SkyPulse private PostgreSQL CA' \
    -keyout "$out/ca/ca.key" -out "$out/ca/ca.crt"

openssl req -new -newkey rsa:3072 -nodes -sha256 \
    -subj "/CN=$dns_name" \
    -addext "subjectAltName=DNS:$dns_name,IP:$private_ip" \
    -keyout "$out/server/server.key" -out "$out/server/server.csr"

openssl x509 -req -sha256 -days 825 \
    -in "$out/server/server.csr" \
    -CA "$out/ca/ca.crt" -CAkey "$out/ca/ca.key" -CAcreateserial \
    -copy_extensions copy -out "$out/server/server.crt"

openssl verify -CAfile "$out/ca/ca.crt" "$out/server/server.crt"
openssl x509 -in "$out/server/server.crt" -noout -checkhost "$dns_name"
openssl x509 -in "$out/server/server.crt" -noout -checkip "$private_ip"

chmod 0600 "$out/ca/ca.key" "$out/server/server.key"
chmod 0644 "$out/ca/ca.crt" "$out/server/server.crt"
rm -f "$out/server/server.csr" "$out/ca/ca.srl"

printf '%s\n' "TLS material created under $out"
printf '%s\n' 'Keep ca/ca.key offline. Host B receives server.{crt,key} + ca.crt; host A receives ca.crt only.'
