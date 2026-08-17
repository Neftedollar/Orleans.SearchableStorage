#!/usr/bin/env bash
set -euo pipefail
umask 077

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
DEPLOY_DIR=$(cd -- "$SCRIPT_DIR/.." && pwd)
SKYPULSE_ROOT=$(cd -- "$DEPLOY_DIR/../.." && pwd)
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

require_command docker
require_command dotnet
require_command openssl
require_command python3
require_rootful_docker
load_deploy_env
require_postgres_17_11_image
# load_deploy_env already un-exports PLATFORM (see lib.sh); nothing extra here.
[[ "$(dotnet --version)" == 10.0.303 ]] || die 'exact .NET SDK 10.0.303 is required'
[[ ${SKYPULSE_ALLOW_DESTRUCTIVE_POSTGRES_TESTS:-} == I_UNDERSTAND_THIS_USES_A_DISPOSABLE_CONTAINER ]] \
    || die 'set the exact disposable-container test confirmation'

container="skypulse-pg-integration-${BASHPID}-${RANDOM}"
scratch=$(mktemp -d)
password=$(openssl rand -hex 32)
printf '%s' "$password" > "$scratch/postgres-password"
chmod 0600 "$scratch/postgres-password"

created_container=false
cleanup() {
    if [[ "$created_container" == true ]]; then
        docker rm -f "$container" >/dev/null 2>&1 || true
    fi
    find "$scratch" -type f -delete 2>/dev/null || true
    rmdir "$scratch" 2>/dev/null || true
    unset password connection
}
trap cleanup EXIT INT TERM

docker container inspect "$container" >/dev/null 2>&1 \
    && die "refusing to reuse existing disposable test container $container"

docker run --detach --rm --name "$container" \
    --publish 127.0.0.1::5432 \
    --tmpfs /var/lib/postgresql/data:rw,nosuid,nodev,size=4g \
    --env POSTGRES_PASSWORD_FILE=/run/secrets/postgres-password \
    --env POSTGRES_DB=skypulse_qualification_test \
    --env 'POSTGRES_INITDB_ARGS=--auth-host=scram-sha-256 --data-checksums' \
    --volume "$scratch/postgres-password:/run/secrets/postgres-password:ro" \
    "$POSTGRES_IMAGE" >/dev/null
created_container=true

port=
for _ in $(seq 1 90); do
    port_line=$(docker port "$container" 5432/tcp 2>/dev/null || true)
    port=${port_line##*:}
    if [[ "$port" =~ ^[0-9]+$ ]] \
        && docker exec "$container" pg_isready \
            --username postgres --dbname skypulse_qualification_test >/dev/null 2>&1; then
        break
    fi
    port=
    sleep 1
done
[[ "$port" =~ ^[0-9]+$ ]] || die 'disposable PostgreSQL did not become ready'

actual_database=$(docker exec "$container" psql --no-psqlrc --tuples-only --no-align \
    --username postgres --dbname skypulse_qualification_test \
    --command 'SELECT current_database()')
[[ "$actual_database" == skypulse_qualification_test ]] \
    || die "unexpected disposable database: $actual_database"

connection="Host=127.0.0.1;Port=${port};Database=skypulse_qualification_test;Username=postgres;Password=${password};SSL Mode=Disable;Include Error Detail=false;Maximum Pool Size=20"
results="$SKYPULSE_ROOT/artifacts/test-results"
mkdir -p "$results"
find "$results" -maxdepth 1 -type f -name postgresql.trx -delete

cd "$SKYPULSE_ROOT"
dotnet restore Orleans.SearchableStorage.Qualification.slnx \
    --locked-mode --configfile NuGet.Config
dotnet build Orleans.SearchableStorage.Qualification.slnx \
    -c Release --no-restore
SKYPULSE_POSTGRES_CONNECTION_STRING="$connection" dotnet test \
    tests/Orleans.SearchableStorage.Qualification.SkyPulse.Persistence.PostgreSql.IntegrationTests \
    -c Release --no-build --no-restore \
    --results-directory "$results" \
    --logger 'trx;LogFileName=postgresql.trx'

python3 - "$results/postgresql.trx" <<'PY'
import sys
import xml.etree.ElementTree as ET

root = ET.parse(sys.argv[1]).getroot()
counters = next((x for x in root.iter() if x.tag.endswith('Counters')), None)
if counters is None:
    raise SystemExit('TRX counters are missing')
values = {k: int(v) for k, v in counters.attrib.items() if v.isdigit()}
total = values.get('total', 0)
executed = values.get('executed', 0)
passed = values.get('passed', 0)
failed = values.get('failed', 0)
not_executed = values.get('notExecuted', 0)
if total < 34 or executed != total or passed != total or failed != 0 or not_executed != 0:
    raise SystemExit(f'PostgreSQL test gate failed: {values}')
print(f'PostgreSQL integration gate passed: {passed}/{total}, no skips')
PY

note 'Disposable PostgreSQL integration gate passed; the container will now be destroyed.'
