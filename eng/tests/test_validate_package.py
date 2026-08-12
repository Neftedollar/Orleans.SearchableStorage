from __future__ import annotations

import contextlib
import importlib.util
import io
import stat
import tempfile
import unittest
import zipfile
from pathlib import Path


SCRIPT = Path(__file__).resolve().parents[1] / "validate-package.py"
SPEC = importlib.util.spec_from_file_location("validate_package", SCRIPT)
if SPEC is None or SPEC.loader is None:
    raise RuntimeError(f"Cannot load {SCRIPT}")
VALIDATOR = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(VALIDATOR)


class PackageAllowlistTests(unittest.TestCase):
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

            code, error = run_validator(package, allowlist)
            self.assertEqual(1, code)
            self.assertIn("ZIP entries", error)

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


def write_zip(path: Path, entries: list[str]) -> None:
    with zipfile.ZipFile(path, "w") as package:
        for entry in entries:
            package.writestr(entry, b"test")


def relationship_xml(core_name: str, relationship_id: str) -> bytes:
    return f"""<?xml version="1.0" encoding="utf-8"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
  <Relationship Type="http://schemas.openxmlformats.org/package/2006/relationships/metadata/core-properties" Target="/package/services/metadata/core-properties/{core_name}" Id="{relationship_id}" />
</Relationships>
""".encode()


def run_validator(package: Path, allowlist: Path) -> tuple[int, str]:
    errors = io.StringIO()
    with contextlib.redirect_stderr(errors):
        result = VALIDATOR.main([str(package), "--allowlist", str(allowlist)])
    return result, errors.getvalue()


if __name__ == "__main__":
    unittest.main()
