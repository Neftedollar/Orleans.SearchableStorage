#!/usr/bin/env python3
"""Validate package target, qualification verdict, and publication identity records."""

from __future__ import annotations

import argparse
import datetime
import hashlib
import json
import os
import re
import stat
import sys
from pathlib import Path, PurePosixPath
from urllib.parse import urlsplit


LIBRARY_REPOSITORY = "https://github.com/Neftedollar/Orleans.SearchableStorage"
QUALIFICATION_REPOSITORY = (
    "https://github.com/Neftedollar/Orleans.SearchableStorage.Qualification"
)
MAX_RECORD_BYTES = 1024 * 1024
SHA256_PATTERN = re.compile(r"[0-9a-f]{64}")
COMMIT_PATTERN = re.compile(r"[0-9a-f]{40}")
RELEASE_NAME_PATTERN = re.compile(r"[0-9A-Za-z][0-9A-Za-z._-]{0,127}")
RFC3339_UTC_PATTERN = re.compile(
    r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(?:\.[0-9]+)?Z"
)
TARGET_KEYS = {
    "schema",
    "packageId",
    "packageVersion",
    "packageKind",
    "artifactUrl",
    "artifactFileName",
    "nupkgSha256",
    "canonicalManifestUrl",
    "canonicalManifestSha256",
    "repositoryUrl",
    "repositoryCommit",
    "packageValidatorPassed",
    "packageOnlyConsumerPassed",
    "recordedAtUtc",
}
VERDICT_KEYS = {
    "schema",
    "outcome",
    "packageId",
    "packageVersion",
    "qualifiedNupkgSha256",
    "qualifiedCanonicalManifestSha256",
    "libraryRepositoryCommit",
    "qualificationRepository",
    "qualificationRepositoryCommit",
    "targetRecordUrl",
    "targetRecordSha256",
    "evidenceReleaseUrl",
    "verifiedEvidenceManifestUrl",
    "verifiedEvidenceManifestSha256",
    "recordedAtUtc",
}
PUBLICATION_KEYS = {
    "schema",
    "packageId",
    "packageVersion",
    "qualifiedNupkgSha256",
    "qualifiedCanonicalManifestSha256",
    "qualificationVerdictUrl",
    "qualificationVerdictSha256",
    "qualificationVerdictVerified",
    "publishedNupkgSha256",
    "repositoryCommit",
    "repositorySignatureVerified",
    "canonicalPayloadEquivalent",
    "packageOnlyConsumerPassed",
    "recordedAtUtc",
}


def require_string(record: dict[str, object], name: str) -> str:
    value = record.get(name)
    if not isinstance(value, str) or not value:
        raise ValueError(f"{name} must be a nonempty string")
    return value


def require_true(record: dict[str, object], name: str) -> None:
    if record.get(name) is not True:
        raise ValueError(f"{name} must be the JSON boolean true")


def require_sha256(value: str, name: str) -> None:
    if SHA256_PATTERN.fullmatch(value) is None:
        raise ValueError(f"{name} must be exactly 64 lowercase hexadecimal characters")


def require_commit(value: str, name: str) -> None:
    if COMMIT_PATTERN.fullmatch(value) is None:
        raise ValueError(f"{name} must be exactly 40 lowercase hexadecimal characters")


def qualification_release_parts(url: str, kind: str) -> tuple[str, ...]:
    parsed = urlsplit(url)
    if (
        parsed.scheme != "https"
        or parsed.netloc != "github.com"
        or parsed.query
        or parsed.fragment
    ):
        raise ValueError(f"{kind} must be a query-free HTTPS URL on github.com")
    parts = PurePosixPath(parsed.path).parts
    expected_prefix = (
        "/",
        "Neftedollar",
        "Orleans.SearchableStorage.Qualification",
        "releases",
    )
    if parts[:4] != expected_prefix:
        raise ValueError(f"{kind} must belong to the reviewed qualification repository")
    return parts


def release_asset_tag(url: str, expected_filename: str, kind: str) -> str:
    parts = qualification_release_parts(url, kind)
    if (
        len(parts) != 7
        or parts[4] != "download"
        or RELEASE_NAME_PATTERN.fullmatch(parts[5]) is None
        or parts[6] != expected_filename
    ):
        raise ValueError(f"{kind} must name the exact expected qualification release asset")
    return parts[5]


def release_page_tag(url: str, kind: str) -> str:
    parts = qualification_release_parts(url, kind)
    if (
        len(parts) != 6
        or parts[4] != "tag"
        or RELEASE_NAME_PATTERN.fullmatch(parts[5]) is None
    ):
        raise ValueError(f"{kind} must name an exact qualification release tag")
    return parts[5]


def validate_recorded_at_utc(value: str) -> None:
    if RFC3339_UTC_PATTERN.fullmatch(value) is None:
        raise ValueError("recordedAtUtc must use an RFC 3339 UTC timestamp with a Z suffix")
    try:
        parsed = datetime.datetime.fromisoformat(value[:-1] + "+00:00")
    except ValueError as error:
        raise ValueError("recordedAtUtc must be a valid RFC 3339 UTC timestamp") from error
    if parsed.utcoffset() != datetime.timedelta(0):
        raise ValueError("recordedAtUtc must be UTC")


def strict_json_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for name, value in pairs:
        if name in result:
            raise ValueError(f"release record contains duplicate key {name!r}")
        result[name] = value
    return result


def load_record(path: Path, expected_sha256: str) -> dict[str, object]:
    require_sha256(expected_sha256, "expected record SHA-256")
    opened = path.open("rb")
    with opened:
        path_metadata = path.lstat()
        opened_metadata = os.fstat(opened.fileno())
        if (
            not stat.S_ISREG(path_metadata.st_mode)
            or not stat.S_ISREG(opened_metadata.st_mode)
            or path_metadata.st_ino != opened_metadata.st_ino
            or path_metadata.st_dev != opened_metadata.st_dev
            or opened_metadata.st_size > MAX_RECORD_BYTES
        ):
            raise ValueError("release record must be one bounded regular file")
        data = opened.read(MAX_RECORD_BYTES + 1)
        final_metadata = os.fstat(opened.fileno())
        if (
            len(data) != opened_metadata.st_size
            or final_metadata.st_size != opened_metadata.st_size
            or final_metadata.st_mtime_ns != opened_metadata.st_mtime_ns
        ):
            raise ValueError("release record changed while it was read")
    actual_sha256 = hashlib.sha256(data).hexdigest()
    if actual_sha256 != expected_sha256:
        raise ValueError(f"release record SHA-256 {actual_sha256} != expected {expected_sha256}")
    try:
        record = json.loads(data, object_pairs_hook=strict_json_object)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError("release record must be one UTF-8 JSON document") from error
    if not isinstance(record, dict):
        raise ValueError("release record root must be an object")
    return record


def require_exact_keys(record: dict[str, object], expected: set[str], kind: str) -> None:
    if set(record) != expected:
        missing = sorted(expected - set(record))
        extra = sorted(set(record) - expected)
        raise ValueError(f"{kind} keys differ; missing={missing}, extra={extra}")


def require_exact_values(record: dict[str, object], expected: dict[str, str], kind: str) -> None:
    for name, expected_value in expected.items():
        actual = require_string(record, name)
        if actual != expected_value:
            raise ValueError(f"{kind} {name} {actual!r} != expected {expected_value!r}")


def validate_expected_identity(args: argparse.Namespace) -> None:
    if not args.expected_package_id or not args.expected_version:
        raise ValueError("expected package id and version must be nonempty")
    require_sha256(args.expected_package_sha256, "expected package SHA-256")
    require_sha256(args.expected_canonical_sha256, "expected canonical SHA-256")
    require_commit(args.expected_library_commit, "expected library commit")


def validate_target(record: dict[str, object], args: argparse.Namespace) -> None:
    require_exact_keys(record, TARGET_KEYS, "package target")
    package_filename = f"{args.expected_package_id}.{args.expected_version}.nupkg"
    require_exact_values(
        record,
        {
            "schema": "oss-package-target/v2",
            "packageId": args.expected_package_id,
            "packageVersion": args.expected_version,
            "packageKind": "unsigned-qualification-target",
            "artifactFileName": package_filename,
            "nupkgSha256": args.expected_package_sha256,
            "canonicalManifestSha256": args.expected_canonical_sha256,
            "repositoryUrl": LIBRARY_REPOSITORY,
            "repositoryCommit": args.expected_library_commit,
        },
        "package target",
    )
    package_tag = release_asset_tag(
        require_string(record, "artifactUrl"), package_filename, "artifactUrl"
    )
    canonical_tag = release_asset_tag(
        require_string(record, "canonicalManifestUrl"),
        "package.canonical.json",
        "canonicalManifestUrl",
    )
    if package_tag != canonical_tag:
        raise ValueError("package and canonical manifest must belong to the same release")
    require_true(record, "packageValidatorPassed")
    require_true(record, "packageOnlyConsumerPassed")
    validate_recorded_at_utc(require_string(record, "recordedAtUtc"))


def validate_verdict(record: dict[str, object], args: argparse.Namespace) -> None:
    require_exact_keys(record, VERDICT_KEYS, "qualification verdict")
    require_exact_values(
        record,
        {
            "schema": "oss-qualification-verdict/v1",
            "outcome": "pass",
            "packageId": args.expected_package_id,
            "packageVersion": args.expected_version,
            "qualifiedNupkgSha256": args.expected_package_sha256,
            "qualifiedCanonicalManifestSha256": args.expected_canonical_sha256,
            "libraryRepositoryCommit": args.expected_library_commit,
            "qualificationRepository": QUALIFICATION_REPOSITORY,
        },
        "qualification verdict",
    )
    qualification_commit = require_string(record, "qualificationRepositoryCommit")
    require_commit(qualification_commit, "qualificationRepositoryCommit")
    target_record_sha256 = require_string(record, "targetRecordSha256")
    require_sha256(target_record_sha256, "targetRecordSha256")
    evidence_manifest_sha256 = require_string(record, "verifiedEvidenceManifestSha256")
    require_sha256(evidence_manifest_sha256, "verifiedEvidenceManifestSha256")
    release_asset_tag(
        require_string(record, "targetRecordUrl"),
        "target-package.json",
        "targetRecordUrl",
    )
    evidence_release_tag = release_page_tag(
        require_string(record, "evidenceReleaseUrl"), "evidenceReleaseUrl"
    )
    evidence_manifest_tag = release_asset_tag(
        require_string(record, "verifiedEvidenceManifestUrl"),
        "verified-evidence.json",
        "verifiedEvidenceManifestUrl",
    )
    if evidence_release_tag != evidence_manifest_tag:
        raise ValueError("evidence release and verified manifest must name the same release")
    validate_recorded_at_utc(require_string(record, "recordedAtUtc"))


def validate_publication(record: dict[str, object], args: argparse.Namespace) -> None:
    require_sha256(
        args.expected_verdict_sha256,
        "expected qualification verdict SHA-256",
    )
    require_sha256(
        args.expected_published_package_sha256,
        "expected published package SHA-256",
    )
    release_asset_tag(
        args.expected_verdict_url,
        "qualification-verdict.json",
        "expected qualification verdict URL",
    )
    require_exact_keys(record, PUBLICATION_KEYS, "package publication")
    require_exact_values(
        record,
        {
            "schema": "oss-package-publication/v1",
            "packageId": args.expected_package_id,
            "packageVersion": args.expected_version,
            "qualifiedNupkgSha256": args.expected_package_sha256,
            "qualifiedCanonicalManifestSha256": args.expected_canonical_sha256,
            "qualificationVerdictUrl": args.expected_verdict_url,
            "qualificationVerdictSha256": args.expected_verdict_sha256,
            "publishedNupkgSha256": args.expected_published_package_sha256,
            "repositoryCommit": args.expected_library_commit,
        },
        "package publication",
    )
    require_true(record, "qualificationVerdictVerified")
    require_true(record, "repositorySignatureVerified")
    require_true(record, "canonicalPayloadEquivalent")
    require_true(record, "packageOnlyConsumerPassed")
    validate_recorded_at_utc(require_string(record, "recordedAtUtc"))


def add_identity_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("path", type=Path)
    parser.add_argument("--expected-record-sha256", required=True)
    parser.add_argument("--expected-package-id", required=True)
    parser.add_argument("--expected-version", required=True)
    parser.add_argument("--expected-package-sha256", required=True)
    parser.add_argument("--expected-canonical-sha256", required=True)
    parser.add_argument("--expected-library-commit", required=True)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="kind", required=True)
    add_identity_arguments(subparsers.add_parser("target"))
    add_identity_arguments(subparsers.add_parser("verdict"))
    publication = subparsers.add_parser("publication")
    add_identity_arguments(publication)
    publication.add_argument("--expected-verdict-url", required=True)
    publication.add_argument("--expected-verdict-sha256", required=True)
    publication.add_argument("--expected-published-package-sha256", required=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        validate_expected_identity(args)
        record = load_record(args.path, args.expected_record_sha256)
        if args.kind == "target":
            validate_target(record, args)
        elif args.kind == "verdict":
            validate_verdict(record, args)
        else:
            validate_publication(record, args)
    except (OSError, ValueError) as error:
        print(f"Release record validation failed: {error}", file=sys.stderr)
        return 1
    print(f"Validated {args.kind} release record {args.path}.", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
