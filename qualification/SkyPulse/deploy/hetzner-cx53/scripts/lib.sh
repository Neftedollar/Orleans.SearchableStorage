#!/usr/bin/env bash
set -euo pipefail

die() {
    printf 'ERROR: %s\n' "$*" >&2
    exit 1
}

note() {
    printf '%s\n' "$*" >&2
}

require_command() {
    command -v "$1" >/dev/null 2>&1 || die "required command not found: $1"
}

require_file() {
    local path=$1
    [[ -f "$path" && ! -L "$path" ]] || die "regular non-symlink file required: $path"
}

require_directory() {
    local path=$1
    [[ -d "$path" && ! -L "$path" ]] || die "real directory required: $path"
}

require_mode() {
    local path=$1 expected=$2 actual
    actual=$(stat -c '%a' "$path")
    [[ "$actual" == "$expected" ]] || die "$path must have mode $expected (found $actual)"
}

require_private_file() {
    local path=$1 mode
    require_file "$path"
    mode=$(stat -c '%a' "$path")
    [[ "$mode" == 400 || "$mode" == 600 ]] || die "$path must have mode 0400 or 0600"
}

require_curl_basic_config() {
    local path=$1 value
    require_private_file "$path"
    value=$(<"$path")
    [[ "$value" =~ ^user[[:space:]]*=[[:space:]]*\"operator:[0-9a-f]{64}\"$ ]] \
        || die "$path must contain only: user = \"operator:<64-lowercase-hex>\""
}

require_control_file() {
    local path=$1 owner current checkout_owner
    require_private_file "$path"
    owner=$(stat -c '%u' "$path")
    current=$(id -u)
    checkout_owner=$(stat -c '%u' "$DEPLOY_DIR")
    [[ "$owner" -eq 0 || "$owner" -eq "$current" || "$owner" -eq "$checkout_owner" ]] \
        || die "$path must be owned by root, current UID $current, or checkout owner UID $checkout_owner"
}

validate_control_env() {
    local kind=$1 path=$2
    require_control_file "$path"
    require_command python3
    python3 "$DEPLOY_DIR/scripts/validate-runtime-env.py" "$kind" "$path"
}

require_hex_secret() {
    local path=$1 size value
    require_private_file "$path"
    size=$(wc -c < "$path")
    [[ "$size" -eq 64 ]] || die "$path must be exactly 64 bytes with no newline"
    value=$(<"$path")
    [[ "$value" =~ ^[0-9a-f]{64}$ ]] || die "$path must contain lowercase 64-hex"
}

require_digest_ref() {
    local name=$1 value=${!1:-}
    [[ "$value" =~ ^[^[:space:]]+@sha256:[0-9a-f]{64}$ ]] \
        || die "$name must be an exact image@sha256:... reference"
    [[ "$value" != *REPLACE_* ]] || die "$name still contains a placeholder"
}

require_postgres_17_11_image() {
    local version
    require_digest_ref POSTGRES_IMAGE
    [[ "$POSTGRES_IMAGE" == postgres:17.11-bookworm@sha256:* ]] \
        || die 'POSTGRES_IMAGE must be the reviewed PostgreSQL 17.11 bookworm image'
    version=$(docker run --rm --network none --entrypoint postgres "$POSTGRES_IMAGE" --version)
    [[ "$version" =~ ^postgres\ \(PostgreSQL\)\ 17\.11([[:space:]]|$) ]] \
        || die "POSTGRES_IMAGE digest is not PostgreSQL 17.11: $version"
    docker run --rm --network none --entrypoint bash "$POSTGRES_IMAGE" \
        -ceu 'command -v tar >/dev/null && command -v pg_verifybackup >/dev/null' \
        || die 'POSTGRES_IMAGE lacks the reviewed Bash/tar/pg_verifybackup recovery tools'
}

load_deploy_env() {
    [[ -e "$DEPLOY_DIR/.env" ]] || die "copy $DEPLOY_DIR/.env.example to .env and fill it first"
    validate_control_env deploy "$DEPLOY_DIR/.env"
    set -a
    # shellcheck disable=SC1091
    source "$DEPLOY_DIR/.env"
    set +a
    # MSBuild lifts exported environment variables into global build
    # properties, so an exported PLATFORM=linux/amd64 breaks every dotnet
    # invocation with the invalid solution configuration "Release|linux/amd64".
    # Keep it as an unexported shell variable for docker buildx arguments.
    export -n PLATFORM 2>/dev/null || true
}

require_rootful_docker() {
    local security_options
    security_options=$(docker info --format '{{json .SecurityOptions}}')
    [[ "$security_options" != *rootless* ]] \
        || die 'rootless Docker is unsupported by this numeric-UID/host-network deployment contract'
    [[ "$security_options" != *userns* ]] \
        || die 'Docker userns-remap is unsupported by this exact numeric-UID bind-mount contract'
}

require_exact_owner_mode() {
    local path=$1 expected_uid=$2 expected_mode=$3
    require_directory "$path"
    require_mode "$path" "$expected_mode"
    [[ $(stat -c '%u' "$path") -eq "$expected_uid" ]] \
        || die "$path must be owned by UID $expected_uid"
}

verify_postgres_backup_tree() {
    local backup=$1
    require_directory "$backup"
    require_file "$backup/SHA256SUMS"
    python3 - "$backup" <<'PY'
import hashlib
import os
import pathlib
import re
import stat
import sys

root = pathlib.Path(sys.argv[1])
expected: dict[str, str] = {}
line_re = re.compile(r"^([0-9a-f]{64})  (\./[A-Za-z0-9_.\-/]+)$")
for line in (root / "SHA256SUMS").read_text(encoding="ascii").splitlines():
    match = line_re.fullmatch(line)
    if match is None or match.group(2) == "./SHA256SUMS":
        raise SystemExit("backup SHA256SUMS has an invalid or self-referential entry")
    if match.group(2) in expected:
        raise SystemExit(f"duplicate backup checksum path: {match.group(2)}")
    expected[match.group(2)] = match.group(1)
if not expected:
    raise SystemExit("backup SHA256SUMS is empty")

actual: set[str] = set()
for directory, directories, files in os.walk(root, followlinks=False):
    for name in directories + files:
        path = pathlib.Path(directory, name)
        mode = path.lstat().st_mode
        if stat.S_ISLNK(mode) or not (stat.S_ISDIR(mode) or stat.S_ISREG(mode)):
            raise SystemExit(f"unsupported backup entry type: {path}")
    for name in files:
        path = pathlib.Path(directory, name)
        relative = "./" + path.relative_to(root).as_posix()
        if relative != "./SHA256SUMS":
            actual.add(relative)

if actual != set(expected):
    missing = sorted(set(expected) - actual)
    extra = sorted(actual - set(expected))
    raise SystemExit(f"backup file set mismatch; missing={missing[:5]} extra={extra[:5]}")

for relative, wanted in expected.items():
    digest = hashlib.sha256()
    with (root / relative[2:]).open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    if digest.hexdigest() != wanted:
        raise SystemExit(f"backup checksum mismatch: {relative}")
PY
}

verify_postgres_backup_identity() {
    local backup=$1
    require_file "$backup/SKYPULSE_BACKUP_IDENTITY.json"
    python3 - "$backup/SKYPULSE_BACKUP_IDENTITY.json" \
        "${EXPECTED_SOURCE_INSTANCE_ID:-}" "${EXPECTED_BASE_PROFILE_ID:-}" \
        "${EXPECTED_BASE_CORPUS_CAP:-}" "${EXPECTED_ACTIVE_PROFILE_ID:-}" \
        "${EXPECTED_ACTIVE_CORPUS_CAP:-}" <<'PY'
import json
import pathlib
import sys

path, source, base_profile, base_cap, active_profile, active_cap = sys.argv[1:]
expected = {
    "sourceInstanceId": source,
    "baseProfileId": base_profile,
    "baseCorpusCap": int(base_cap),
    "activeProfileId": active_profile,
    "activeCorpusCap": int(active_cap),
    "targetProfileId": None,
    "targetCorpusCap": None,
    "migrationVersions": [1, 2],
    "tapOverlayVersion": 3,
}
actual = json.loads(pathlib.Path(path).read_text(encoding="utf-8"))
if actual != expected:
    raise SystemExit(f"backup identity does not match the selected stable release: {actual}")
PY
}

verify_firewall_boot_dependency() {
    local firewall_service=$1 expected_file=$2 unit dependency_file dependencies
    for unit in docker.service ssh.service; do
        dependency_file="/etc/systemd/system/$unit.d/90-skypulse-firewall.conf"
        require_file "$dependency_file"
        require_mode "$dependency_file" 644
        cmp --silent "$expected_file" "$dependency_file" \
            || die "$unit does not have the exact reviewed firewall dependency"
        dependencies=$(systemctl show --property=Requires --value "$unit")
        tr ' ' '\n' <<<"$dependencies" | grep -Fxq "$firewall_service" \
            || die "$unit does not require $firewall_service at boot"
    done
    [[ $(systemctl is-enabled ssh.socket 2>/dev/null || true) == masked ]] \
        || die 'ssh.socket must be masked; socket activation creates an unsafe firewall boot-order cycle'
    systemctl is-active --quiet ssh.socket \
        && die 'ssh.socket must be inactive; classic ssh.service is required'
    systemctl is-enabled --quiet ssh.service \
        || die 'classic ssh.service must be enabled'
    systemctl is-active --quiet ssh.service \
        || die 'classic ssh.service must be active'
    [[ -L /etc/systemd/system-generators/sshd-socket-generator \
        && $(readlink /etc/systemd/system-generators/sshd-socket-generator) == /dev/null ]] \
        || die 'Ubuntu sshd-socket-generator must be masked with an exact /dev/null symlink'
    [[ $(stat -c '%u' /etc/systemd/system-generators/sshd-socket-generator) -eq 0 ]] \
        || die 'sshd-socket-generator mask must be owned by root'
}

preflight_classic_ssh_conversion() {
    [[ ${SKYPULSE_SSH_MODE_CONFIRMATION:-} == I_HAVE_HETZNER_CONSOLE_AND_ACCEPT_CLASSIC_SSH ]] \
        || die 'set SKYPULSE_SSH_MODE_CONFIRMATION=I_HAVE_HETZNER_CONSOLE_AND_ACCEPT_CLASSIC_SSH'
    systemctl list-unit-files --no-legend ssh.service 2>/dev/null \
        | awk '$1 == "ssh.service" { found=1 } END { exit !found }' \
        || die 'the pinned Ubuntu host has no ssh.service'
    systemctl list-unit-files --no-legend ssh.socket 2>/dev/null \
        | awk '$1 == "ssh.socket" { found=1 } END { exit !found }' \
        || die 'the pinned Ubuntu host has no ssh.socket to disable explicitly'
    require_command ln
    require_command readlink
    local generator_mask=/etc/systemd/system-generators/sshd-socket-generator
    if [[ -e "$generator_mask" || -L "$generator_mask" ]]; then
        [[ -L "$generator_mask" && $(readlink "$generator_mask") == /dev/null ]] \
            || die "$generator_mask exists but is not the reviewed /dev/null mask"
        [[ $(stat -c '%u' "$generator_mask") -eq 0 ]] \
            || die "$generator_mask must be owned by root"
    fi
}

configure_classic_ssh() {
    local generator_mask
    preflight_classic_ssh_conversion
    require_command ln
    require_command readlink
    install -d -o root -g root -m 0755 /etc/systemd/system-generators
    generator_mask=/etc/systemd/system-generators/sshd-socket-generator
    if [[ -e "$generator_mask" || -L "$generator_mask" ]]; then
        [[ -L "$generator_mask" && $(readlink "$generator_mask") == /dev/null ]] \
            || die "$generator_mask exists but is not the reviewed /dev/null mask"
    else
        ln -s /dev/null "$generator_mask"
    fi
    [[ $(stat -c '%u' "$generator_mask") -eq 0 ]] \
        || die "$generator_mask must be owned by root"
    systemctl daemon-reload
    systemctl disable --now ssh.socket
    systemctl mask ssh.socket
    rm -f /etc/systemd/system/ssh.socket.d/90-skypulse-firewall.conf
    systemctl daemon-reload
    systemctl enable ssh.service
    # A socket-activated sshd instance from the pre-conversion socket can still
    # hold port 22 and make a plain start fail. ssh.service uses KillMode=process,
    # so restart re-execs only the listener and keeps established sessions.
    systemctl restart ssh.service
    [[ $(systemctl is-enabled ssh.socket 2>/dev/null || true) == masked ]] \
        || die 'failed to mask ssh.socket'
    systemctl is-active --quiet ssh.socket \
        && die 'ssh.socket remained active after conversion'
    systemctl is-enabled --quiet ssh.service \
        || die 'classic ssh.service is not enabled'
    systemctl is-active --quiet ssh.service \
        || die 'classic ssh.service is not active'
}

require_private_topology() {
    python3 - "${APP_PRIVATE_IP:-}" "${POSTGRES_PRIVATE_IP:-}" <<'PY'
import ipaddress, sys

try:
    app = ipaddress.IPv4Address(sys.argv[1])
    postgres = ipaddress.IPv4Address(sys.argv[2])
except ipaddress.AddressValueError as error:
    raise SystemExit(f'invalid topology IPv4 address: {error}') from error
private = tuple(map(ipaddress.IPv4Network, ('10.0.0.0/8', '172.16.0.0/12', '192.168.0.0/16')))
if app == postgres or not all(any(value in network for network in private) for value in (app, postgres)):
    raise SystemExit('APP_PRIVATE_IP and POSTGRES_PRIVATE_IP must be distinct RFC1918 unicast addresses')
PY
}

require_dns_hostname() {
    local value=$1
    python3 - "$value" <<'PY'
import re
import sys

value = sys.argv[1]
label = re.compile(r"^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$")
if len(value) > 253 or "." not in value or any(not label.fullmatch(part) for part in value.split(".")):
    raise SystemExit("POSTGRES_PRIVATE_DNS must be a canonical lowercase dotted DNS hostname")
PY
}

require_network_interface() {
    local value=$1
    [[ "$value" =~ ^[A-Za-z0-9_.:-]{1,15}$ ]] \
        || die "invalid network interface name: $value"
    ip link show dev "$value" >/dev/null 2>&1 \
        || die "required network interface does not exist: $value"
}

require_interface_address() {
    local interface=$1 address=$2 binding status
    require_network_interface "$interface"
    binding=$(mktemp)
    printf '%s %s\n' "$interface" "$address" > "$binding"
    if "$DEPLOY_DIR/scripts/firewall-interface-check.sh" "$binding"; then
        :
    else
        status=$?
        rm -f "$binding"
        return "$status"
    fi
    rm -f "$binding"
}

verify_installed_firewall_assets() {
    local role=$1 interface=$2 address=$3 service_template=$4
    local binding="/etc/skypulse-$role-firewall.interface"
    local service="/etc/systemd/system/skypulse-$role-firewall.service"
    local helper=/usr/local/libexec/skypulse-firewall-interface-check

    require_interface_address "$interface" "$address"
    require_file "$binding"
    require_mode "$binding" 600
    [[ $(stat -c '%u' "$binding") -eq 0 ]] \
        || die "$binding must be owned by root"
    cmp --silent "$binding" <(printf '%s %s\n' "$interface" "$address") \
        || die "$binding does not contain the exact reviewed interface binding"

    require_file "$helper"
    require_mode "$helper" 755
    [[ $(stat -c '%u' "$helper") -eq 0 ]] \
        || die "$helper must be owned by root"
    cmp --silent "$DEPLOY_DIR/scripts/firewall-interface-check.sh" "$helper" \
        || die 'installed firewall interface checker differs from this release'

    require_file "$service"
    require_mode "$service" 644
    [[ $(stat -c '%u' "$service") -eq 0 ]] \
        || die "$service must be owned by root"
    cmp --silent "$service_template" "$service" \
        || die "$service differs from this release"
}

require_expected_runtime_identity() {
    [[ ${EXPECTED_BASE_PROFILE_ID:-} =~ ^[a-z0-9]([a-z0-9._-]{0,78}[a-z0-9])?$ ]] \
        || die 'EXPECTED_BASE_PROFILE_ID must be canonical'
    [[ ${EXPECTED_ACTIVE_PROFILE_ID:-} =~ ^[a-z0-9]([a-z0-9._-]{0,78}[a-z0-9])?$ ]] \
        || die 'EXPECTED_ACTIVE_PROFILE_ID must be canonical'
    [[ ${EXPECTED_BASE_CORPUS_CAP:-} =~ ^[1-9][0-9]*$ ]] \
        || die 'EXPECTED_BASE_CORPUS_CAP must be positive'
    [[ ${EXPECTED_ACTIVE_CORPUS_CAP:-} =~ ^[1-9][0-9]*$ ]] \
        || die 'EXPECTED_ACTIVE_CORPUS_CAP must be positive'
    (( EXPECTED_ACTIVE_CORPUS_CAP >= EXPECTED_BASE_CORPUS_CAP )) \
        || die 'expected active corpus cap cannot be below the immutable base cap'
    [[ ${EXPECTED_SOURCE_INSTANCE_ID:-} =~ ^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$ ]] \
        || die 'EXPECTED_SOURCE_INSTANCE_ID must be a canonical nonzero UUID'
    [[ "$EXPECTED_SOURCE_INSTANCE_ID" != 00000000-0000-0000-0000-000000000000 ]] \
        || die 'EXPECTED_SOURCE_INSTANCE_ID cannot be empty'
}

compose_app() (
    validate_control_env deploy "$DEPLOY_DIR/.env"
    validate_control_env app "$DEPLOY_DIR/runtime/app.env"
    validate_control_env images "$DEPLOY_DIR/runtime/images.env"
    set -a
    # shellcheck disable=SC1091
    source "$DEPLOY_DIR/.env"
    # shellcheck disable=SC1091
    source "$DEPLOY_DIR/runtime/images.env"
    set +a
    docker compose \
        --project-name skypulse-app \
        --env-file "$DEPLOY_DIR/.env" \
        --env-file "$DEPLOY_DIR/runtime/images.env" \
        -f "$DEPLOY_DIR/compose.app.yaml" "$@"
)

compose_postgres() (
    validate_control_env deploy "$DEPLOY_DIR/.env"
    validate_control_env postgres "$DEPLOY_DIR/runtime/postgres.env"
    set -a
    # shellcheck disable=SC1091
    source "$DEPLOY_DIR/.env"
    set +a
    docker compose \
        --project-name skypulse-postgres \
        --env-file "$DEPLOY_DIR/.env" \
        -f "$DEPLOY_DIR/compose.postgres.yaml" "$@"
)

stop_exact_compose_container() {
    local name=$1 service=$2 timeout=$3 expected_project=${4:-skypulse-app} project actual_service
    if ! docker container inspect "$name" >/dev/null 2>&1; then
        return 0
    fi
    project=$(docker container inspect --format '{{index .Config.Labels "com.docker.compose.project"}}' "$name")
    actual_service=$(docker container inspect --format '{{index .Config.Labels "com.docker.compose.service"}}' "$name")
    [[ "$project" == "$expected_project" && "$actual_service" == "$service" ]] \
        || die "refusing to stop foreign container $name ($project/$actual_service)"
    if [[ $(docker container inspect --format '{{.State.Running}}' "$name") == true ]]; then
        docker stop --time "$timeout" "$name" >/dev/null
    fi
    [[ $(docker container inspect --format '{{.State.Running}}' "$name") == false ]] \
        || die "$name is still running"
}

require_running_release_container() {
    local name=$1 service=$2 expected_image_id=$3 expected_project=${4:-skypulse-app}
    local project actual_service actual_image
    docker container inspect "$name" >/dev/null 2>&1 \
        || die "required release container is absent: $name"
    [[ $(docker container inspect --format '{{.State.Running}}' "$name") == true ]] \
        || die "required release container is not running: $name"
    project=$(docker container inspect --format '{{index .Config.Labels "com.docker.compose.project"}}' "$name")
    actual_service=$(docker container inspect --format '{{index .Config.Labels "com.docker.compose.service"}}' "$name")
    actual_image=$(docker container inspect --format '{{.Image}}' "$name")
    [[ "$project" == "$expected_project" && "$actual_service" == "$service" ]] \
        || die "unexpected Compose identity for $name: $project/$actual_service"
    [[ "$actual_image" == "$expected_image_id" ]] \
        || die "$name runs image $actual_image instead of locked $expected_image_id"
}

require_container_bind_source() {
    local name=$1 destination=$2 expected_source=$3 mount_value actual_source actual_real expected_real
    require_command realpath
    mount_value=$(docker container inspect --format \
        "{{range .Mounts}}{{if eq .Destination \"$destination\"}}{{.Type}}|{{.Source}}{{end}}{{end}}" "$name")
    [[ "$mount_value" == bind\|* ]] || die "$name must use a bind mount at $destination"
    actual_source=${mount_value#bind|}
    [[ -n "$actual_source" ]] || die "$name has no bind mount at $destination"
    actual_real=$(realpath -e -- "$actual_source")
    expected_real=$(realpath -e -- "$expected_source")
    [[ "$actual_real" == "$expected_real" ]] \
        || die "$name mounts $actual_real at $destination instead of reviewed $expected_real"
}

require_container_restart_policy() {
    local name=$1 expected=$2 actual
    actual=$(docker container inspect --format '{{.HostConfig.RestartPolicy.Name}}' "$name")
    [[ "$actual" == "$expected" ]] \
        || die "$name restart policy is $actual instead of reviewed $expected"
}

stop_caddy_if_present() {
    stop_exact_compose_container skypulse-caddy caddy 30
}
