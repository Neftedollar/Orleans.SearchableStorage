from __future__ import annotations

import contextlib
import importlib.util
import io
import json
import os
import stat
import struct
import subprocess
import tempfile
import unittest
import zipfile
from pathlib import Path
from unittest import mock


SCRIPT = Path(__file__).resolve().parents[1] / "validate-package.py"
SPEC = importlib.util.spec_from_file_location("validate_package", SCRIPT)
if SPEC is None or SPEC.loader is None:
    raise RuntimeError(f"Cannot load {SCRIPT}")
VALIDATOR = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(VALIDATOR)


class PackageAllowlistTests(unittest.TestCase):
    def test_prerelease_nupkg_requires_exact_version_and_every_warning_surface(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            allowlist = root / "allowlist.txt"
            allowlist.write_text(
                "Orleans.SearchableStorage.nuspec\nREADME.md\nRELEASE_NOTES.md\n",
                encoding="utf-8",
            )
            package = root / "Orleans.SearchableStorage.1.0.0-rc.1.nupkg"
            valid_entries = {
                "Orleans.SearchableStorage.nuspec": valid_nuspec(
                    version="1.0.0-rc.1",
                    description=(
                        f"{VALIDATOR.PRERELEASE_WARNING} "
                        f"{VALIDATOR.PACKAGE_DESCRIPTION}"
                    ),
                    release_notes=VALIDATOR.PRERELEASE_WARNING,
                ),
                "README.md": VALIDATOR.PRERELEASE_WARNING.encode(),
                "RELEASE_NOTES.md": VALIDATOR.PRERELEASE_WARNING.encode(),
            }
            write_zip_bytes(package, valid_entries)

            code, error = run_validator(
                package,
                allowlist,
                "--expected-version",
                "1.0.0-rc.1",
            )
            self.assertEqual(0, code, error)

            wrong_version_code, wrong_version_error = run_validator(
                package,
                allowlist,
                "--expected-version",
                "1.0.0-rc.2",
            )
            self.assertEqual(1, wrong_version_code)
            self.assertIn("!= expected '1.0.0-rc.2'", wrong_version_error)

            invalid_surfaces = {
                "nuspec <description>": {
                    **valid_entries,
                    "Orleans.SearchableStorage.nuspec": valid_nuspec(
                        version="1.0.0-rc.1",
                        release_notes=VALIDATOR.PRERELEASE_WARNING,
                    ),
                },
                "nuspec <releaseNotes>": {
                    **valid_entries,
                    "Orleans.SearchableStorage.nuspec": valid_nuspec(
                        version="1.0.0-rc.1",
                        description=(
                            f"{VALIDATOR.PRERELEASE_WARNING} "
                            f"{VALIDATOR.PACKAGE_DESCRIPTION}"
                        ),
                    ),
                },
                "package entry README.md": {
                    **valid_entries,
                    "README.md": b"prerelease without the required warning",
                },
                "package entry RELEASE_NOTES.md": {
                    **valid_entries,
                    "RELEASE_NOTES.md": b"prerelease without the required warning",
                },
            }
            for expected_surface, entries in invalid_surfaces.items():
                with self.subTest(surface=expected_surface):
                    write_zip_bytes(package, entries)
                    invalid_code, invalid_error = run_validator(package, allowlist)
                    self.assertEqual(1, invalid_code)
                    self.assertIn(expected_surface, invalid_error)

    def test_validator_binds_exact_package_and_canonical_sha256(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            allowlist = root / "allowlist.txt"
            allowlist.write_text(
                "Orleans.SearchableStorage.nuspec\nREADME.md\nRELEASE_NOTES.md\n",
                encoding="utf-8",
            )
            package = root / "Orleans.SearchableStorage.1.0.0-rc.2.nupkg"
            write_zip_bytes(
                package,
                {
                    "Orleans.SearchableStorage.nuspec": valid_nuspec(
                        version="1.0.0-rc.2",
                        description=(
                            f"{VALIDATOR.PRERELEASE_WARNING} "
                            f"{VALIDATOR.PACKAGE_DESCRIPTION}"
                        ),
                        release_notes=VALIDATOR.PRERELEASE_WARNING,
                    ),
                    "README.md": VALIDATOR.PRERELEASE_WARNING.encode(),
                    "RELEASE_NOTES.md": VALIDATOR.PRERELEASE_WARNING.encode(),
                },
            )
            canonical = root / "package.canonical.json"
            baseline_code, baseline_error = run_validator(
                package,
                allowlist,
                "--canonical-output",
                str(canonical),
            )
            self.assertEqual(0, baseline_code, baseline_error)
            package_sha256 = VALIDATOR.hashlib.sha256(package.read_bytes()).hexdigest()
            canonical_sha256 = VALIDATOR.hashlib.sha256(canonical.read_bytes()).hexdigest()

            accepted_code, accepted_error = run_validator(
                package,
                allowlist,
                "--expected-package-sha256",
                package_sha256,
                "--expected-canonical-sha256",
                canonical_sha256,
            )
            self.assertEqual(0, accepted_code, accepted_error)

            package_code, package_error = run_validator(
                package,
                allowlist,
                "--expected-package-sha256",
                "0" * 64,
            )
            self.assertEqual(1, package_code)
            self.assertIn("package SHA-256", package_error)

            canonical_code, canonical_error = run_validator(
                package,
                allowlist,
                "--expected-canonical-sha256",
                "0" * 64,
            )
            self.assertEqual(1, canonical_code)
            self.assertIn("canonical SHA-256", canonical_error)

            malformed_code, malformed_error = run_validator(
                package,
                allowlist,
                "--expected-package-sha256",
                "A" * 64,
            )
            self.assertEqual(1, malformed_code)
            self.assertIn("64 lowercase hexadecimal", malformed_error)

    def test_literal_content_types_name_is_not_treated_as_a_glob(self) -> None:
        self.assertTrue(VALIDATOR.matches("[Content_Types].xml", "[Content_Types].xml"))
        self.assertFalse(VALIDATOR.matches("[Content_Types].xml", "C.xml"))
        self.assertTrue(VALIDATOR.matches("core/*.psmdcp", "core/generated.psmdcp"))
        self.assertFalse(VALIDATOR.matches("core/*.psmdcp", "core/nested/evil.psmdcp"))
        self.assertFalse(
            VALIDATOR.matches(
                "package/services/metadata/core-properties/*.psmdcp",
                "package/services/metadata/core-properties/nested/evil.psmdcp",
            )
        )

    def test_validator_accepts_literal_content_types_and_rejects_extra_entry(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            allowlist = root / "allowlist.txt"
            allowlist.write_text("[Content_Types].xml\npayload.txt\n", encoding="utf-8")
            accepted = root / "accepted.nupkg"
            rejected = root / "rejected.nupkg"
            write_zip(accepted, ["[Content_Types].xml", "payload.txt"])
            write_zip(rejected, ["[Content_Types].xml", "payload.txt", "extra.txt"])

            # Stop after allowlist processing so this focused test does not have to duplicate a
            # production nuspec. Reaching the nuspec error proves every expected entry was accepted.
            accepted_code, accepted_error = run_validator(accepted, allowlist)
            self.assertEqual(1, accepted_code)
            self.assertIn("package nuspec was not found", accepted_error)

            rejected_code, rejected_error = run_validator(rejected, allowlist)
            self.assertEqual(1, rejected_code)
            self.assertIn("package entry 'extra.txt' must match exactly one", rejected_error)

    def test_generated_core_properties_relationship_is_canonical(self) -> None:
        first = relationship_xml("abc.psmdcp", "R111")
        second = relationship_xml("def.psmdcp", "R222")

        first_name, first_data = VALIDATOR.canonical_entry("_rels/.rels", first)
        second_name, second_data = VALIDATOR.canonical_entry("_rels/.rels", second)

        self.assertEqual("_rels/.rels", first_name)
        self.assertEqual(first_name, second_name)
        self.assertEqual(first_data, second_data)

    def test_validator_rejects_entry_count_before_reading_entries(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            allowlist = root / "allowlist.txt"
            allowlist.write_text("*.txt\n", encoding="utf-8")
            package = root / "too-many.nupkg"
            write_zip(
                package,
                [f"entry-{index:03}.txt" for index in range(VALIDATOR.MAX_PACKAGE_ENTRIES + 1)],
            )

            with mock.patch.object(
                VALIDATOR.zipfile,
                "ZipFile",
                side_effect=AssertionError("ZipFile must not run before the entry-count guard"),
            ):
                code, error = run_validator(package, allowlist)
            self.assertEqual(1, code)
            self.assertIn("ZIP entries", error)

    def test_preflight_rejects_later_fake_eocd_embedded_in_zip_comment(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            allowlist = root / "allowlist.txt"
            allowlist.write_text("payload.txt\n", encoding="utf-8")
            package = root / "fake-eocd.nupkg"
            with zipfile.ZipFile(package, "w") as archive:
                archive.writestr("payload.txt", b"test")
                archive.comment = b"prefix" + struct.pack(
                    "<4s4H2LH",
                    VALIDATOR.ZIP_EOCD_SIGNATURE,
                    0,
                    0,
                    1,
                    1,
                    46,
                    0,
                    0,
                )

            with mock.patch.object(
                VALIDATOR.zipfile,
                "ZipFile",
                side_effect=AssertionError("ZipFile must not run after ambiguous EOCD preflight"),
            ):
                code, error = run_validator(package, allowlist)
            self.assertEqual(1, code)
            self.assertIn("ZIP central-directory", error)

    def test_preflight_rejects_zip64_locator_before_constructing_zipfile(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            allowlist = root / "allowlist.txt"
            allowlist.write_text("payload.txt\n", encoding="utf-8")
            package = root / "zip64-locator.nupkg"
            write_zip(package, ["payload.txt"])
            data = bytearray(package.read_bytes())
            eocd = data.rfind(VALIDATOR.ZIP_EOCD_SIGNATURE)
            self.assertGreaterEqual(eocd, 20)
            data[eocd - 20 : eocd - 16] = VALIDATOR.ZIP64_EOCD_LOCATOR_SIGNATURE
            package.write_bytes(data)

            with mock.patch.object(
                VALIDATOR.zipfile,
                "ZipFile",
                side_effect=AssertionError("ZipFile must not run for ZIP64 input"),
            ):
                code, error = run_validator(package, allowlist)
            self.assertEqual(1, code)
            self.assertIn("ZIP64", error)

    def test_validator_rejects_oversized_package_before_zip_inspection(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            allowlist = root / "allowlist.txt"
            allowlist.write_text("payload.txt\n", encoding="utf-8")
            package = root / "oversized.nupkg"
            with package.open("wb") as stream:
                stream.truncate(VALIDATOR.MAX_PACKAGE_FILE_BYTES + 1)

            code, error = run_validator(package, allowlist)
            self.assertEqual(1, code)
            self.assertIn("before ZIP inspection", error)

    def test_validator_rejects_symlink_metadata(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            allowlist = root / "allowlist.txt"
            allowlist.write_text("payload.txt\n", encoding="utf-8")
            package = root / "symlink.nupkg"
            with zipfile.ZipFile(package, "w") as archive:
                entry = zipfile.ZipInfo("payload.txt")
                entry.create_system = 3
                entry.external_attr = (stat.S_IFLNK | 0o777) << 16
                archive.writestr(entry, b"target")

            code, error = run_validator(package, allowlist)
            self.assertEqual(1, code)
            self.assertIn("not a regular file", error)

    def test_validator_rejects_non_regular_package_input_before_open(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            allowlist = root / "allowlist.txt"
            allowlist.write_text("payload.txt\n", encoding="utf-8")
            target = root / "target.nupkg"
            link = root / "linked.nupkg"
            write_zip(target, ["payload.txt"])
            os.symlink(target.name, link)

            code, error = run_validator(link, allowlist)
            self.assertEqual(1, code)
            self.assertIn("regular file", error)

    def test_repository_signed_mode_verifies_and_excludes_only_signature_from_canonical(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            allowlist = root / "allowlist.txt"
            allowlist.write_text(
                "Orleans.SearchableStorage.nuspec\npayload.txt\n",
                encoding="utf-8",
            )
            unsigned = root / "unsigned.nupkg"
            signed = root / "signed.nupkg"
            nuspec = valid_nuspec()
            write_zip_bytes(
                unsigned,
                {
                    "Orleans.SearchableStorage.nuspec": nuspec,
                    "payload.txt": b"same semantic payload",
                },
            )
            write_zip_bytes(
                signed,
                {
                    "Orleans.SearchableStorage.nuspec": nuspec,
                    "payload.txt": b"same semantic payload",
                    ".signature.p7s": b"synthetic signature container",
                },
            )
            unsigned_canonical = root / "unsigned.json"
            signed_canonical = root / "signed.json"

            unsigned_code, unsigned_error = run_validator(
                unsigned,
                allowlist,
                "--canonical-output",
                str(unsigned_canonical),
            )
            self.assertEqual(0, unsigned_code, unsigned_error)

            ordinary_code, ordinary_error = run_validator(signed, allowlist)
            self.assertEqual(1, ordinary_code)
            self.assertIn("only with --repository-signed", ordinary_error)

            with mock.patch.object(VALIDATOR, "verify_package_signature") as verify:
                signed_code, signed_error = run_validator(
                    signed,
                    allowlist,
                    "--repository-signed",
                    "--canonical-output",
                    str(signed_canonical),
            )
            self.assertEqual(0, signed_code, signed_error)
            verify.assert_called_once()
            verified_snapshot = verify.call_args.args[0]
            self.assertEqual("package.nupkg", verified_snapshot.name)
            self.assertNotEqual(signed, verified_snapshot)
            self.assertEqual(
                unsigned_canonical.read_text(encoding="utf-8"),
                signed_canonical.read_text(encoding="utf-8"),
            )

    def test_repository_signed_mode_requires_signature_and_successful_dotnet_verification(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            allowlist = root / "allowlist.txt"
            allowlist.write_text("Orleans.SearchableStorage.nuspec\n", encoding="utf-8")
            unsigned = root / "unsigned.nupkg"
            signed = root / "signed.nupkg"
            write_zip_bytes(unsigned, {"Orleans.SearchableStorage.nuspec": valid_nuspec()})
            write_zip_bytes(
                signed,
                {
                    "Orleans.SearchableStorage.nuspec": valid_nuspec(),
                    ".signature.p7s": b"synthetic signature container",
                },
            )

            missing_code, missing_error = run_validator(
                unsigned,
                allowlist,
                "--repository-signed",
            )
            self.assertEqual(1, missing_code)
            self.assertIn("requires one root .signature.p7s", missing_error)

            failed_verification = subprocess.CompletedProcess([], 1, "", "invalid")
            with mock.patch.object(
                VALIDATOR.subprocess,
                "run",
                return_value=failed_verification,
            ) as run:
                failed_code, failed_error = run_validator(
                    signed,
                    allowlist,
                    "--repository-signed",
                )
            self.assertEqual(1, failed_code)
            self.assertIn("dotnet nuget verify rejected", failed_error)
            command = run.call_args.args[0]
            self.assertEqual(["nuget", "verify", "--all"], command[1:4])
            self.assertEqual("package.nupkg", Path(command[4]).name)
            self.assertEqual(["--verbosity", "quiet"], command[5:])

    def test_signature_verifier_uses_reviewed_trusted_signer_policy_and_online_revocation(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            package = Path(temporary) / "release.nupkg"
            package.write_bytes(b"snapshot")
            observed: dict[str, object] = {}

            def verify_process(command: list[str], **arguments: object) -> subprocess.CompletedProcess[str]:
                observed["command"] = command
                observed["environment"] = arguments["env"]
                config = Path(arguments["cwd"]) / "NuGet.Config"
                observed["cwd"] = arguments["cwd"]
                observed["config"] = config.read_text(encoding="utf-8")
                observed["global_json"] = (Path(arguments["cwd"]) / "global.json").read_text(
                    encoding="utf-8"
                )
                return subprocess.CompletedProcess(command, 0, "", "")

            with mock.patch.object(VALIDATOR.subprocess, "run", side_effect=verify_process):
                VALIDATOR.verify_package_signature(package)

            command = observed["command"]
            self.assertEqual(["nuget", "verify", "--all"], command[1:4])
            self.assertTrue(Path(command[4]).is_absolute())
            self.assertEqual(["--verbosity", "quiet"], command[5:])
            environment = observed["environment"]
            self.assertEqual("online", environment["NUGET_CERT_REVOCATION_MODE"])
            self.assertEqual("true", environment["DOTNET_NUGET_SIGNATURE_VERIFICATION"])
            self.assertEqual("1", environment["DOTNET_SKIP_FIRST_TIME_EXPERIENCE"])
            self.assertEqual("false", environment["DOTNET_GENERATE_ASPNET_CERTIFICATE"])
            self.assertEqual("false", environment["DOTNET_ADD_GLOBAL_TOOLS_TO_PATH"])
            self.assertEqual(
                str(Path(observed["cwd"]) / "dotnet-home"),
                environment["DOTNET_CLI_HOME"],
            )
            config = observed["config"]
            self.assertIn('key="signatureValidationMode" value="require"', config)
            self.assertIn("<clear", config)
            self.assertIn('serviceIndex="https://api.nuget.org/v3/index.json"', config)
            self.assertIn("<owners>neftedollar</owners>", config)
            self.assertIn(
                'fingerprint="1F4B311D9ACC115C8DC8018B5A49E00FCE6DA8E2855F9F014CA6F34570BC482D"',
                config,
            )
            self.assertIn('allowUntrustedRoot="false"', config)
            self.assertEqual(
                {"sdk": {"version": "10.0.302", "rollForward": "latestPatch"}},
                json.loads(observed["global_json"]),
            )

    def test_snapshot_output_never_deletes_a_preexisting_file_or_the_input(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            allowlist = root / "allowlist.txt"
            allowlist.write_text("payload.txt\n", encoding="utf-8")
            package = root / "input.nupkg"
            destination = root / "existing.nupkg"
            write_zip(package, ["payload.txt"])
            original = package.read_bytes()
            destination.write_bytes(b"keep me")

            code, _ = run_validator(
                package,
                allowlist,
                "--snapshot-output",
                str(destination),
            )
            self.assertEqual(1, code)
            self.assertEqual(b"keep me", destination.read_bytes())

            same_code, _ = run_validator(
                package,
                allowlist,
                "--snapshot-output",
                str(package),
            )
            self.assertEqual(1, same_code)
            self.assertEqual(original, package.read_bytes())

    def test_invalid_package_removes_only_the_new_snapshot(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            allowlist = root / "allowlist.txt"
            allowlist.write_text("payload.txt\n", encoding="utf-8")
            package = root / "invalid.nupkg"
            snapshot = root / "snapshot.nupkg"
            write_zip(package, ["unexpected.txt"])

            code, _ = run_validator(
                package,
                allowlist,
                "--snapshot-output",
                str(snapshot),
            )
            self.assertEqual(1, code)
            self.assertFalse(snapshot.exists())
            self.assertTrue(package.exists())

    def test_signature_verifier_rejects_any_diagnostic_even_on_zero_exit(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            package = Path(temporary) / "release.nupkg"
            package.write_bytes(b"snapshot")
            warning = subprocess.CompletedProcess([], 0, "", "warning NU3027")
            with mock.patch.object(VALIDATOR.subprocess, "run", return_value=warning):
                with self.assertRaisesRegex(ValueError, "warning or unexpected diagnostic"):
                    VALIDATOR.verify_package_signature(package)

    def test_repository_policy_is_strict_and_explicit(self) -> None:
        policy = VALIDATOR.load_repository_policy()
        self.assertEqual(["neftedollar"], policy["owners"])
        self.assertEqual(1, len(policy["certificates"]))
        certificate = policy["certificates"][0]
        self.assertEqual("SHA256", certificate["hashAlgorithm"])
        self.assertFalse(certificate["allowUntrustedRoot"])

    def test_malformed_repository_policy_fails_cleanly_and_removes_snapshot(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            policy = root / "bad-policy.json"
            policy.write_text(
                """{
  "schemaVersion": "oss-nuget-repository-policy/v1",
  "serviceIndex": "https://api.nuget.org/v3/index.json",
  "owners": [{"not": "a string"}],
  "certificates": [{
    "fingerprint": "1F4B311D9ACC115C8DC8018B5A49E00FCE6DA8E2855F9F014CA6F34570BC482D",
    "hashAlgorithm": "SHA256",
    "allowUntrustedRoot": false
  }]
}
""",
                encoding="utf-8",
            )
            with self.assertRaisesRegex(ValueError, "owners"):
                VALIDATOR.load_repository_policy(policy)

    def test_repository_signature_entry_must_use_frozen_container_shape(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            allowlist = root / "allowlist.txt"
            allowlist.write_text("Orleans.SearchableStorage.nuspec\n", encoding="utf-8")
            invalid = root / "invalid-signature.nupkg"
            with zipfile.ZipFile(invalid, "w", compression=zipfile.ZIP_DEFLATED) as package:
                package.writestr("Orleans.SearchableStorage.nuspec", valid_nuspec())
                package.writestr(".signature.p7s", b"not stored")

            code, error = run_validator(invalid, allowlist, "--repository-signed")
            self.assertEqual(1, code)
            self.assertIn("repository signature must be", error)


def write_zip(path: Path, entries: list[str]) -> None:
    with zipfile.ZipFile(path, "w") as package:
        for entry in entries:
            package.writestr(entry, b"test")


def write_zip_bytes(path: Path, entries: dict[str, bytes]) -> None:
    with zipfile.ZipFile(path, "w") as package:
        for name, data in entries.items():
            package.writestr(name, data)


def valid_nuspec(
    version: str = "0.1.0",
    description: str = VALIDATOR.PACKAGE_DESCRIPTION,
    release_notes: str | None = None,
) -> bytes:
    release_notes_xml = (
        f"    <releaseNotes>{release_notes}</releaseNotes>\n"
        if release_notes is not None
        else ""
    )
    return f"""<?xml version="1.0" encoding="utf-8"?>
<package xmlns="http://schemas.microsoft.com/packaging/2013/05/nuspec.xsd">
  <metadata>
    <id>{VALIDATOR.PACKAGE_ID}</id>
    <version>{version}</version>
    <authors>Orleans.SearchableStorage contributors</authors>
    <description>{description}</description>
    <readme>README.md</readme>
{release_notes_xml}    <license type="expression">MIT</license>
    <repository type="git" url="{VALIDATOR.REPOSITORY_URL}" commit="{'a' * 40}" />
    <dependencies>
      <group targetFramework="net10.0">
        <dependency id="Microsoft.Orleans.Runtime" version="[9.0.0,)" />
        <dependency id="PolyType" version="[1.0.0,)" />
      </group>
    </dependencies>
  </metadata>
</package>
""".encode()


def relationship_xml(core_name: str, relationship_id: str) -> bytes:
    return f"""<?xml version="1.0" encoding="utf-8"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
  <Relationship Type="http://schemas.openxmlformats.org/package/2006/relationships/metadata/core-properties" Target="/package/services/metadata/core-properties/{core_name}" Id="{relationship_id}" />
</Relationships>
""".encode()


def run_validator(package: Path, allowlist: Path, *extra: str) -> tuple[int, str]:
    output = io.StringIO()
    errors = io.StringIO()
    with contextlib.redirect_stdout(output), contextlib.redirect_stderr(errors):
        result = VALIDATOR.main([str(package), "--allowlist", str(allowlist), *extra])
    return result, errors.getvalue()


if __name__ == "__main__":
    unittest.main()
