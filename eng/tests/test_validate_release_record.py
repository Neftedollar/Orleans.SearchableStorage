from __future__ import annotations

import contextlib
import hashlib
import importlib.util
import io
import json
import tempfile
import unittest
from pathlib import Path


SCRIPT = Path(__file__).resolve().parents[1] / "validate-release-record.py"
SPEC = importlib.util.spec_from_file_location("validate_release_record", SCRIPT)
if SPEC is None or SPEC.loader is None:
    raise RuntimeError(f"Cannot load {SCRIPT}")
VALIDATOR = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(VALIDATOR)

PACKAGE_ID = "Orleans.SearchableStorage"
VERSION = "1.0.0-rc.2"
PACKAGE_SHA256 = "1" * 64
CANONICAL_SHA256 = "2" * 64
LIBRARY_COMMIT = "3" * 40
VERDICT_SHA256 = "4" * 64
PUBLISHED_SHA256 = "5" * 64
TARGET_TAG = "target-rc2"
EVIDENCE_TAG = "evidence-rc2"
PUBLICATION_TAG = "publication-rc2"


class ReleaseRecordTests(unittest.TestCase):
    def test_target_sdk_binding_matches_global_policy(self) -> None:
        global_policy = json.loads(
            (SCRIPT.parents[1] / "global.json").read_text(encoding="utf-8")
        )
        self.assertEqual(
            {
                "sdk": {
                    "version": VALIDATOR.BUILD_SDK_VERSION,
                    "rollForward": "disable",
                }
            },
            global_policy,
        )

    def test_accepts_exact_target_verdict_and_publication_records(self) -> None:
        cases = {
            "target": valid_target(),
            "verdict": valid_verdict(),
            "publication": valid_publication(),
        }
        for kind, record in cases.items():
            with self.subTest(kind=kind), tempfile.TemporaryDirectory() as temporary:
                path = Path(temporary) / f"{kind}.json"
                write_record(path, record)
                code, error = run_validator(kind, path)
                self.assertEqual(0, code, error)

    def test_target_rejects_mismatch_false_gate_and_split_release(self) -> None:
        cases = {
            "hash": {**valid_target(), "nupkgSha256": "6" * 64},
            "sdk": {**valid_target(), "buildSdkVersion": "10.0.302"},
            "boolean": {**valid_target(), "packageValidatorPassed": 1},
            "release": {
                **valid_target(),
                "canonicalManifestUrl": asset_url("other", "package.canonical.json"),
            },
        }
        for expected, record in cases.items():
            with self.subTest(expected=expected), tempfile.TemporaryDirectory() as temporary:
                path = Path(temporary) / "target-package.json"
                write_record(path, record)
                code, error = run_validator("target", path)
                self.assertEqual(1, code)
                self.assertTrue("!= expected" in error or "must" in error, error)

    def test_verdict_rejects_nonpass_mutable_url_and_misbound_evidence(self) -> None:
        cases = {
            "outcome": {**valid_verdict(), "outcome": "fail"},
            "query": {
                **valid_verdict(),
                "targetRecordUrl": valid_verdict()["targetRecordUrl"] + "?download=1",
            },
            "evidence": {
                **valid_verdict(),
                "verifiedEvidenceManifestUrl": asset_url(
                    "different-evidence", "verified-evidence.json"
                ),
            },
        }
        for expected, record in cases.items():
            with self.subTest(expected=expected), tempfile.TemporaryDirectory() as temporary:
                path = Path(temporary) / "qualification-verdict.json"
                write_record(path, record)
                code, error = run_validator("verdict", path)
                self.assertEqual(1, code)
                self.assertTrue("!= expected" in error or "must" in error, error)

    def test_publication_rejects_unknown_key_false_proof_and_signed_hash(self) -> None:
        cases = {
            "extra": {**valid_publication(), "unexpected": True},
            "boolean": {**valid_publication(), "canonicalPayloadEquivalent": False},
            "signed hash": {**valid_publication(), "publishedNupkgSha256": "6" * 64},
        }
        for expected, record in cases.items():
            with self.subTest(expected=expected), tempfile.TemporaryDirectory() as temporary:
                path = Path(temporary) / "package-publication.json"
                write_record(path, record)
                code, error = run_validator("publication", path)
                self.assertEqual(1, code)
                self.assertTrue(
                    "keys differ" in error or "!= expected" in error or "must" in error,
                    error,
                )

    def test_rejects_duplicate_key_wrong_digest_and_invalid_timestamp(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "qualification-verdict.json"
            rendered = json.dumps(valid_verdict(), sort_keys=True)
            path.write_text(rendered[:-1] + ', "outcome": "pass"}\n', encoding="utf-8")
            code, error = run_validator("verdict", path)
            self.assertEqual(1, code)
            self.assertIn("duplicate key 'outcome'", error)

            write_record(path, valid_verdict())
            digest_code, digest_error = run_validator(
                "verdict", path, record_sha256="0" * 64
            )
            self.assertEqual(1, digest_code)
            self.assertIn("record SHA-256", digest_error)

            invalid = {**valid_verdict(), "recordedAtUtc": "2026-08-14"}
            write_record(path, invalid)
            timestamp_code, timestamp_error = run_validator("verdict", path)
            self.assertEqual(1, timestamp_code)
            self.assertIn("recordedAtUtc", timestamp_error)


def asset_url(tag: str, filename: str) -> str:
    return f"{VALIDATOR.QUALIFICATION_REPOSITORY}/releases/download/{tag}/{filename}"


def release_url(tag: str) -> str:
    return f"{VALIDATOR.QUALIFICATION_REPOSITORY}/releases/tag/{tag}"


def valid_target() -> dict[str, object]:
    filename = f"{PACKAGE_ID}.{VERSION}.nupkg"
    return {
        "schema": "oss-package-target/v3",
        "packageId": PACKAGE_ID,
        "packageVersion": VERSION,
        "packageKind": "unsigned-qualification-target",
        "buildSdkVersion": VALIDATOR.BUILD_SDK_VERSION,
        "artifactUrl": asset_url(TARGET_TAG, filename),
        "artifactFileName": filename,
        "nupkgSha256": PACKAGE_SHA256,
        "canonicalManifestUrl": asset_url(TARGET_TAG, "package.canonical.json"),
        "canonicalManifestSha256": CANONICAL_SHA256,
        "repositoryUrl": VALIDATOR.LIBRARY_REPOSITORY,
        "repositoryCommit": LIBRARY_COMMIT,
        "packageValidatorPassed": True,
        "packageOnlyConsumerPassed": True,
        "recordedAtUtc": "2026-08-14T10:00:00Z",
    }


def valid_verdict() -> dict[str, object]:
    return {
        "schema": "oss-qualification-verdict/v1",
        "outcome": "pass",
        "packageId": PACKAGE_ID,
        "packageVersion": VERSION,
        "qualifiedNupkgSha256": PACKAGE_SHA256,
        "qualifiedCanonicalManifestSha256": CANONICAL_SHA256,
        "libraryRepositoryCommit": LIBRARY_COMMIT,
        "qualificationRepository": VALIDATOR.QUALIFICATION_REPOSITORY,
        "qualificationRepositoryCommit": "6" * 40,
        "targetRecordUrl": asset_url(TARGET_TAG, "target-package.json"),
        "targetRecordSha256": "7" * 64,
        "evidenceReleaseUrl": release_url(EVIDENCE_TAG),
        "verifiedEvidenceManifestUrl": asset_url(
            EVIDENCE_TAG, "verified-evidence.json"
        ),
        "verifiedEvidenceManifestSha256": "8" * 64,
        "recordedAtUtc": "2026-08-14T10:00:00Z",
    }


def valid_publication() -> dict[str, object]:
    return {
        "schema": "oss-package-publication/v1",
        "packageId": PACKAGE_ID,
        "packageVersion": VERSION,
        "qualifiedNupkgSha256": PACKAGE_SHA256,
        "qualifiedCanonicalManifestSha256": CANONICAL_SHA256,
        "qualificationVerdictUrl": asset_url(
            PUBLICATION_TAG, "qualification-verdict.json"
        ),
        "qualificationVerdictSha256": VERDICT_SHA256,
        "qualificationVerdictVerified": True,
        "publishedNupkgSha256": PUBLISHED_SHA256,
        "repositoryCommit": LIBRARY_COMMIT,
        "repositorySignatureVerified": True,
        "canonicalPayloadEquivalent": True,
        "packageOnlyConsumerPassed": True,
        "recordedAtUtc": "2026-08-14T10:00:00Z",
    }


def write_record(path: Path, record: dict[str, object]) -> None:
    path.write_text(json.dumps(record, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def run_validator(
    kind: str,
    path: Path,
    *,
    record_sha256: str | None = None,
) -> tuple[int, str]:
    if record_sha256 is None:
        record_sha256 = hashlib.sha256(path.read_bytes()).hexdigest()
    arguments = [
        kind,
        str(path),
        "--expected-record-sha256",
        record_sha256,
        "--expected-package-id",
        PACKAGE_ID,
        "--expected-version",
        VERSION,
        "--expected-package-sha256",
        PACKAGE_SHA256,
        "--expected-canonical-sha256",
        CANONICAL_SHA256,
        "--expected-library-commit",
        LIBRARY_COMMIT,
    ]
    if kind == "publication":
        arguments.extend(
            [
                "--expected-verdict-url",
                asset_url(PUBLICATION_TAG, "qualification-verdict.json"),
                "--expected-verdict-sha256",
                VERDICT_SHA256,
                "--expected-published-package-sha256",
                PUBLISHED_SHA256,
            ]
        )
    output = io.StringIO()
    errors = io.StringIO()
    with contextlib.redirect_stdout(output), contextlib.redirect_stderr(errors):
        result = VALIDATOR.main(arguments)
    return result, errors.getvalue()


if __name__ == "__main__":
    unittest.main()
