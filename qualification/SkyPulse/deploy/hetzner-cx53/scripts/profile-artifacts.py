#!/usr/bin/env python3
"""Validate every configured base/growth corpus profile and private route."""

import argparse
import json
import os
import pathlib
import re
import stat
import sys


PROFILE_ID = re.compile(r"[a-z0-9](?:[a-z0-9._-]{0,78}[a-z0-9])?")
HEX64 = re.compile(r"[0-9a-f]{64}")
GROWTH_KEY = re.compile(
    r"SkyPulse__Durable__GrowthProfiles__(\d+)__(ProfileId|CorpusCap|ProfilePrefixSha256|RoutingManifestPath)"
)
REQUIRED_GROWTH_FIELDS = {
    "ProfileId",
    "CorpusCap",
    "ProfilePrefixSha256",
    "RoutingManifestPath",
}
ROUTE_PREFIX = "/var/lib/skypulse/routes-root/"


def fail(message: str) -> "None":
    raise SystemExit(message)


def required_environment(name: str) -> str:
    value = os.environ.get(name, "")
    if not value:
        fail(f"missing configuration: {name}")
    return value


def positive_integer(value: str, name: str) -> int:
    if not re.fullmatch(r"[1-9][0-9]*", value):
        fail(f"{name} must be a positive integer")
    return int(value)


def exact_object(path: pathlib.Path, *, mode: int, uid: int | None = None) -> os.stat_result:
    if path.is_symlink():
        fail(f"symlink is forbidden: {path}")
    try:
        info = path.stat()
    except FileNotFoundError:
        fail(f"required path is missing: {path}")
    expected_kind = stat.S_IFDIR if mode == 0o700 else stat.S_IFREG
    if stat.S_IFMT(info.st_mode) != expected_kind:
        fail(f"unexpected path type: {path}")
    actual_mode = stat.S_IMODE(info.st_mode)
    if actual_mode != mode:
        fail(f"{path} must have mode {mode:04o} (found {actual_mode:04o})")
    if uid is not None and info.st_uid != uid:
        fail(f"{path} must be owned by UID {uid} (found {info.st_uid})")
    if expected_kind == stat.S_IFREG and info.st_nlink != 1:
        fail(f"hard-linked private file is forbidden: {path}")
    return info


def exact_child_names(directory: pathlib.Path, expected: set[str]) -> None:
    actual = {entry.name for entry in directory.iterdir()}
    if actual != expected:
        fail(
            f"{directory} has an unexpected child set; "
            f"missing={sorted(expected - actual)} extra={sorted(actual - expected)}"
        )


def configured_profiles() -> list[dict[str, object]]:
    base = {
        "profileId": required_environment("SkyPulse__Durable__ProfileId"),
        "corpusCap": positive_integer(
            required_environment("SkyPulse__Durable__CorpusCap"),
            "SkyPulse__Durable__CorpusCap",
        ),
        "prefixSha256": required_environment("SkyPulse__Durable__ProfilePrefixSha256"),
        "routingManifestPath": required_environment(
            "SkyPulse__Durable__RoutingManifestPath"
        ),
        "kind": "base",
    }

    growth: dict[int, dict[str, str]] = {}
    for name, value in os.environ.items():
        if not name.startswith("SkyPulse__Durable__GrowthProfiles__"):
            continue
        match = GROWTH_KEY.fullmatch(name)
        if match is None:
            fail(f"unsupported growth-profile setting: {name}")
        index = int(match.group(1))
        growth.setdefault(index, {})[match.group(2)] = value

    expected_indices = list(range(len(growth)))
    if sorted(growth) != expected_indices:
        fail("growth-profile indexes must be contiguous from zero")

    profiles = [base]
    for index in expected_indices:
        values = growth[index]
        if set(values) != REQUIRED_GROWTH_FIELDS or any(not value for value in values.values()):
            fail(f"growth profile {index} must define exactly {sorted(REQUIRED_GROWTH_FIELDS)}")
        profiles.append(
            {
                "profileId": values["ProfileId"],
                "corpusCap": positive_integer(
                    values["CorpusCap"], f"growth profile {index} CorpusCap"
                ),
                "prefixSha256": values["ProfilePrefixSha256"],
                "routingManifestPath": values["RoutingManifestPath"],
                "kind": f"growth-{index}",
            }
        )

    ids: set[str] = set()
    caps: set[int] = set()
    previous_cap = 0
    for profile in profiles:
        profile_id = str(profile["profileId"])
        cap = int(profile["corpusCap"])
        prefix = str(profile["prefixSha256"])
        if PROFILE_ID.fullmatch(profile_id) is None:
            fail(f"non-canonical profile id: {profile_id!r}")
        if HEX64.fullmatch(prefix) is None:
            fail(f"invalid prefix SHA-256 for profile {profile_id}")
        if profile_id in ids or cap in caps:
            fail("profile IDs and corpus caps must be unique")
        if cap <= previous_cap:
            fail("configured profiles must be strictly increasing by corpus cap")
        ids.add(profile_id)
        caps.add(cap)
        previous_cap = cap
    expected_active_id = required_environment("EXPECTED_ACTIVE_PROFILE_ID")
    expected_active_cap = positive_integer(
        required_environment("EXPECTED_ACTIVE_CORPUS_CAP"),
        "EXPECTED_ACTIVE_CORPUS_CAP",
    )
    if not any(
        profile["profileId"] == expected_active_id
        and profile["corpusCap"] == expected_active_cap
        for profile in profiles
    ):
        fail("expected active profile/cap is absent from the configured profile catalog")
    return profiles


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--public-manifest", required=True)
    parser.add_argument("--private-root", required=True)
    parser.add_argument("--expected-uid", type=int, default=10001)
    parser.add_argument("--paths0", action="store_true")
    args = parser.parse_args()

    public_path = pathlib.Path(args.public_manifest)
    if public_path.name != "corpus.manifest.json" or public_path.is_symlink() or not public_path.is_file():
        fail("public corpus manifest must be a regular non-symlink file")
    public_accounts = public_path.parent / "accounts.ak32"
    if public_accounts.is_symlink() or not public_accounts.is_file():
        fail("public accounts.ak32 must be a regular non-symlink file")
    if public_path.stat().st_nlink != 1 or public_accounts.stat().st_nlink != 1:
        fail("hard-linked public corpus files are forbidden")
    exact_child_names(public_path.parent, {"corpus.manifest.json", "accounts.ak32"})
    public = json.loads(public_path.read_text(encoding="utf-8"))
    public_profiles = public.get("profiles")
    if not isinstance(public_profiles, list):
        fail("public corpus manifest has no profiles array")

    private_root = pathlib.Path(args.private_root)
    exact_object(private_root, mode=0o700, uid=args.expected_uid)
    profiles = configured_profiles()
    exact_child_names(private_root, {str(profile["profileId"]) for profile in profiles})

    results: list[dict[str, object]] = []
    for profile in profiles:
        profile_id = str(profile["profileId"])
        cap = int(profile["corpusCap"])
        prefix = str(profile["prefixSha256"])
        route_in_container = str(profile["routingManifestPath"])
        expected_container = (
            f"{ROUTE_PREFIX}{profile_id}/routing.private.manifest.json"
        )
        if route_in_container != expected_container:
            fail(f"profile {profile_id} route must be {expected_container}")

        route_dir = private_root / profile_id
        route_manifest = route_dir / "routing.private.manifest.json"
        route_data = route_dir / "routing.private.ndjson"
        exact_object(route_dir, mode=0o700, uid=args.expected_uid)
        exact_child_names(
            route_dir, {"routing.private.manifest.json", "routing.private.ndjson"}
        )
        exact_object(route_manifest, mode=0o600, uid=args.expected_uid)
        exact_object(route_data, mode=0o600, uid=args.expected_uid)

        matches = [item for item in public_profiles if item.get("name") == profile_id]
        if len(matches) != 1:
            fail(f"profile {profile_id} is not unique in the public manifest")
        public_profile = matches[0]
        if (
            public_profile.get("accountCount") != cap
            or public_profile.get("prefixSha256") != prefix
        ):
            fail(f"public profile metadata mismatch: {profile_id}")

        private = json.loads(route_manifest.read_text(encoding="utf-8"))
        private_profile = private.get("profile")
        if not isinstance(private_profile, dict):
            fail(f"private route manifest has no profile object: {route_manifest}")
        private_id = private_profile.get("name", private_profile.get("profileId"))
        private_cap = private_profile.get(
            "accountCount", private_profile.get("corpusCap")
        )
        if (
            private_id != profile_id
            or private_cap != cap
            or private_profile.get("prefixSha256") != prefix
        ):
            fail(f"private profile metadata mismatch: {profile_id}")

        results.append(
            {
                **profile,
                "manifest": str(route_manifest),
                "data": str(route_data),
            }
        )

    if args.paths0:
        for result in results:
            for key in ("manifest", "data"):
                sys.stdout.buffer.write(os.fsencode(str(result[key])) + b"\0")
    else:
        json.dump(results, sys.stdout, sort_keys=True, indent=2)
        sys.stdout.write("\n")


if __name__ == "__main__":
    main()
