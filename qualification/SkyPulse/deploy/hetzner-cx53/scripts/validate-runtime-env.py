#!/usr/bin/env python3
"""Strict, non-executing validation for shell/Compose control files."""

from __future__ import annotations

import pathlib
import re
import sys


DEPLOY_KEYS = {
    "DEPLOYMENT_ID", "PLATFORM", "DOTNET_SDK_IMAGE", "DOTNET_ASPNET_IMAGE",
    "POSTGRES_IMAGE", "CADDY_IMAGE", "APP_PRIVATE_IP", "POSTGRES_PRIVATE_IP",
    "POSTGRES_PRIVATE_DNS", "APP_PRIVATE_INTERFACE", "POSTGRES_PRIVATE_INTERFACE",
    "EXPECTED_BASE_PROFILE_ID", "EXPECTED_BASE_CORPUS_CAP",
    "EXPECTED_SOURCE_INSTANCE_ID", "EXPECTED_ACTIVE_PROFILE_ID",
    "EXPECTED_ACTIVE_CORPUS_CAP", "APP_SECRET_DIR", "TAP_SECRET_DIR",
    "POSTGRES_SECRET_DIR", "PUBLIC_CORPUS_DIR", "PRIVATE_ROUTING_ROOT",
    "APP_PG_CA_CERT", "POSTGRES_TLS_DIR", "POSTGRES_DATA_DIR",
    "POSTGRES_BACKUP_DIR", "CADDY_DATA_DIR", "CADDY_CONFIG_DIR",
    "ENABLE_PUBLIC_PROXY", "CADDYFILE_PATH", "SKYPULSE_PUBLIC_BASE_URL",
    "SKYPULSE_CURL_CONFIG",
}

APP_KEYS = {
    "ASPNETCORE_ENVIRONMENT", "ASPNETCORE_URLS", "DOTNET_EnableDiagnostics",
    "Logging__LogLevel__Default", "Logging__LogLevel__Microsoft",
    "SkyPulse__Mode", "SkyPulse__Durable__ProfileId",
    "SkyPulse__Durable__ProfileVersion", "SkyPulse__Durable__CorpusCap",
    "SkyPulse__Durable__ProfilePrefixSha256",
    "SkyPulse__Durable__SourceInstanceId",
    "SkyPulse__Durable__CorpusManifestPath", "SkyPulse__Durable__TapEndpoint",
    "SkyPulse__Durable__RoutingManifestPath",
    "SkyPulse__Durable__ExclusiveRepositoryAdministrationConfirmed",
    "SkyPulse__Durable__FullNetworkModeDisabledConfirmed",
    "SkyPulse__Durable__AutomaticRepositoryDiscoveryDisabledConfirmed",
    "SKYPULSE_PG_HOST", "SKYPULSE_PG_PORT", "SKYPULSE_PG_DATABASE",
    "SKYPULSE_PG_USERNAME",
}

IMAGE_KEYS = {
    "SKYPULSE_APP_IMAGE", "SKYPULSE_TAP_IMAGE",
    "SKYPULSE_APP_IMAGE_ID", "SKYPULSE_TAP_IMAGE_ID",
}

POSTGRES_VALUES = {
    "POSTGRES_USER": "skypulse_admin",
    "POSTGRES_DB": "postgres",
    "POSTGRES_INITDB_ARGS": "--auth-host=scram-sha-256 --auth-local=trust --data-checksums",
    "PGDATA": "/var/lib/postgresql/data/pgdata",
}

SAFE_VALUE = re.compile(r"[A-Za-z0-9._:/@=-]*")
GROWTH_KEY = re.compile(
    r"SkyPulse__Durable__GrowthProfiles__([0-9]+)__"
    r"(ProfileId|CorpusCap|ProfilePrefixSha256|RoutingManifestPath)"
)


def fail(message: str) -> "None":
    raise SystemExit(message)


def parse(path: pathlib.Path) -> dict[str, str]:
    result: dict[str, str] = {}
    for number, raw in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        if not raw or raw.startswith("#"):
            continue
        if raw != raw.strip() or "=" not in raw:
            fail(f"{path}:{number}: expected an unquoted KEY=value line")
        key, value = raw.split("=", 1)
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_.]*", key):
            fail(f"{path}:{number}: invalid key")
        if key in result:
            fail(f"{path}:{number}: duplicate key {key}")
        safe_postgres_args = (
            key == "POSTGRES_INITDB_ARGS"
            and value == POSTGRES_VALUES["POSTGRES_INITDB_ARGS"]
        )
        if SAFE_VALUE.fullmatch(value) is None and not safe_postgres_args:
            fail(f"{path}:{number}: unsafe or shell-active value for {key}")
        result[key] = value
    return result


def require_exact_keys(path: pathlib.Path, actual: dict[str, str], expected: set[str]) -> None:
    if set(actual) != expected:
        fail(
            f"{path}: exact key set mismatch; "
            f"missing={sorted(expected - set(actual))} extra={sorted(set(actual) - expected)}"
        )


def validate_app(path: pathlib.Path, values: dict[str, str]) -> None:
    growth: dict[int, set[str]] = {}
    unexpected: set[str] = set()
    for key in values:
        if key in APP_KEYS:
            continue
        match = GROWTH_KEY.fullmatch(key)
        if match is None:
            unexpected.add(key)
            continue
        growth.setdefault(int(match.group(1)), set()).add(match.group(2))
    if unexpected or not APP_KEYS.issubset(values):
        fail(
            f"{path}: app key set mismatch; missing={sorted(APP_KEYS - set(values))} "
            f"extra={sorted(unexpected)}"
        )
    expected_fields = {"ProfileId", "CorpusCap", "ProfilePrefixSha256", "RoutingManifestPath"}
    if sorted(growth) != list(range(len(growth))):
        fail(f"{path}: growth profile indexes must be contiguous from zero")
    for index, fields in growth.items():
        if fields != expected_fields:
            fail(f"{path}: growth profile {index} has an incomplete field set")


def main() -> None:
    if len(sys.argv) != 3 or sys.argv[1] not in {"deploy", "app", "images", "postgres"}:
        fail("usage: validate-runtime-env.py deploy|app|images|postgres PATH")
    kind = sys.argv[1]
    path = pathlib.Path(sys.argv[2])
    values = parse(path)
    if kind == "deploy":
        require_exact_keys(path, values, DEPLOY_KEYS)
    elif kind == "app":
        validate_app(path, values)
    elif kind == "images":
        require_exact_keys(path, values, IMAGE_KEYS)
        if re.fullmatch(r"skypulse-app:[0-9a-f]{40}", values["SKYPULSE_APP_IMAGE"]) is None:
            fail(f"{path}: invalid SKYPULSE_APP_IMAGE")
        if re.fullmatch(r"skypulse-tap:[0-9a-f]{40}", values["SKYPULSE_TAP_IMAGE"]) is None:
            fail(f"{path}: invalid SKYPULSE_TAP_IMAGE")
        for key in ("SKYPULSE_APP_IMAGE_ID", "SKYPULSE_TAP_IMAGE_ID"):
            if re.fullmatch(r"sha256:[0-9a-f]{64}", values[key]) is None:
                fail(f"{path}: invalid {key}")
    else:
        if values != POSTGRES_VALUES:
            fail(f"{path}: PostgreSQL control file differs from the exact reviewed contract")


if __name__ == "__main__":
    main()
