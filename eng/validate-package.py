#!/usr/bin/env python3
"""Validate package contents, metadata, provenance, and canonical entry hashes."""

from __future__ import annotations

import argparse
import fnmatch
import hashlib
import json
import os
import re
import stat
import struct
import subprocess
import sys
import tempfile
import xml.etree.ElementTree as ET
import zipfile
from pathlib import Path, PurePosixPath
from typing import BinaryIO


PACKAGE_ID = "Orleans.SearchableStorage"
REPOSITORY_URL = "https://github.com/Neftedollar/Orleans.SearchableStorage"
PACKAGE_DESCRIPTION = "Orleans-native persistent storage with searchable secondary indexes."
PRERELEASE_WARNING = "NOT PRODUCTION-QUALIFIED. DO NOT USE THIS PACKAGE IN PRODUCTION."
PRERELEASE_WARNING_ENTRIES = ("README.md", "RELEASE_NOTES.md")
REPOSITORY_POLICY = Path(__file__).with_name("nuget-repository-policy.json")
REPOSITORY_ROOT = Path(__file__).resolve().parent.parent
GLOBAL_JSON = REPOSITORY_ROOT / "global.json"
CORE_PROPERTIES_PATTERN = "package/services/metadata/core-properties/*.psmdcp"
CORE_PROPERTIES_CANONICAL = "package/services/metadata/core-properties/{generated}.psmdcp"
MAX_ENTRY_BYTES = 64 * 1024 * 1024
MAX_PACKAGE_BYTES = 128 * 1024 * 1024
MAX_PACKAGE_FILE_BYTES = 128 * 1024 * 1024
MAX_PACKAGE_ENTRIES = 128
MAX_SIGNATURE_BYTES = 1024 * 1024
MAX_ZIP_COMMENT_BYTES = 65_535
ZIP_EOCD_SIGNATURE = b"PK\x05\x06"
ZIP64_EOCD_LOCATOR_SIGNATURE = b"PK\x06\x07"
ZIP_CENTRAL_DIRECTORY_SIGNATURE = b"PK\x01\x02"
ZIP_EOCD = struct.Struct("<4s4H2LH")
ZIP_CENTRAL_DIRECTORY_FIXED_BYTES = 46
SIGNATURE_ENTRY = ".signature.p7s"


def child(element: ET.Element, name: str) -> ET.Element:
    result = element.find(f"{{*}}{name}")
    if result is None:
        raise ValueError(f"nuspec is missing <{name}>")
    return result


def child_text(element: ET.Element, name: str) -> str:
    value = (child(element, name).text or "").strip()
    if not value:
        raise ValueError(f"nuspec <{name}> must not be empty")
    return value


def normalize_xml(element: ET.Element) -> bytes:
    for node in element.iter():
        attributes = sorted(node.attrib.items())
        node.attrib.clear()
        node.attrib.update(attributes)
    return ET.tostring(element, encoding="utf-8", xml_declaration=True)


def canonical_entry(name: str, data: bytes) -> tuple[str, bytes]:
    if matches(CORE_PROPERTIES_PATTERN, name):
        root = ET.fromstring(data)
        for node in root.iter():
            if node.tag.endswith("}created") or node.tag == "created":
                node.text = "{normalized-package-created-time}"
        return CORE_PROPERTIES_CANONICAL, normalize_xml(root)
    if name == "_rels/.rels":
        root = ET.fromstring(data)
        for relationship in root.iter():
            target = relationship.attrib.get("Target")
            if target and matches(CORE_PROPERTIES_PATTERN, target.lstrip("/")):
                relationship.attrib["Target"] = "/" + CORE_PROPERTIES_CANONICAL
                # NuGet generates both the core-properties path and its relationship id afresh for
                # each pack. Neither carries semantic package content; normalize the pair together.
                relationship.attrib["Id"] = "{generated-core-properties-relationship}"
        return name, normalize_xml(root)
    return name, data


def matches(pattern: str, name: str) -> bool:
    """Match exact entries literally and glob each path segment independently."""
    if "*" not in pattern and "?" not in pattern:
        return name == pattern

    pattern_parts = PurePosixPath(pattern).parts
    name_parts = PurePosixPath(name).parts
    return len(pattern_parts) == len(name_parts) and all(
        fnmatch.fnmatchcase(name_part, pattern_part)
        for pattern_part, name_part in zip(pattern_parts, name_parts, strict=True)
    )


def is_prerelease(version: str) -> bool:
    """Return whether a normalized NuGet/SemVer version has a prerelease component."""
    return "-" in version.split("+", maxsplit=1)[0]


def require_prerelease_warning(value: str, surface: str) -> None:
    if PRERELEASE_WARNING not in value:
        raise ValueError(
            f"prerelease {surface} must contain the exact non-production warning"
        )


def validate_nuspec(
    data: bytes,
    expected_version: str | None,
    expected_commit: str | None,
) -> dict[str, object]:
    package = ET.fromstring(data)
    metadata = child(package, "metadata")
    actual_id = child_text(metadata, "id")
    version = child_text(metadata, "version")
    if actual_id != PACKAGE_ID:
        raise ValueError(f"unexpected package id {actual_id!r}")
    if expected_version and version != expected_version:
        raise ValueError(f"package version {version!r} != expected {expected_version!r}")
    if child_text(metadata, "authors") != "Orleans.SearchableStorage contributors":
        raise ValueError("unexpected package authors")
    description = child_text(metadata, "description")
    if is_prerelease(version):
        require_prerelease_warning(description, "nuspec <description>")
        if PACKAGE_DESCRIPTION not in description:
            raise ValueError("prerelease package description must retain the product description")
    elif description != PACKAGE_DESCRIPTION:
        raise ValueError("unexpected package description")
    if child_text(metadata, "readme") != "README.md":
        raise ValueError("package readme must be README.md")
    release_notes_element = metadata.find("{*}releaseNotes")
    release_notes = (
        (release_notes_element.text or "").strip()
        if release_notes_element is not None
        else ""
    )
    if is_prerelease(version):
        require_prerelease_warning(release_notes, "nuspec <releaseNotes>")

    license_element = child(metadata, "license")
    if license_element.attrib.get("type") != "expression" or (license_element.text or "").strip() != "MIT":
        raise ValueError("package license must be the MIT expression")

    repository = child(metadata, "repository")
    repository_url = repository.attrib.get("url", "").rstrip("/")
    repository_type = repository.attrib.get("type")
    repository_commit = repository.attrib.get("commit", "").lower()
    if repository_url != REPOSITORY_URL or repository_type != "git":
        raise ValueError("unexpected repository URL or type in package provenance")
    if expected_commit and repository_commit != expected_commit.lower():
        raise ValueError(
            f"package repository commit {repository_commit!r} != expected {expected_commit.lower()!r}"
        )
    if not re.fullmatch(r"[0-9a-f]{40,64}", repository_commit):
        raise ValueError("package repository commit is absent or is not a full hexadecimal revision")

    dependency_groups = child(metadata, "dependencies").findall("{*}group")
    if len(dependency_groups) != 1:
        raise ValueError("package must contain exactly one dependency group")
    target_framework = dependency_groups[0].attrib.get("targetFramework", "")
    if target_framework.lower() not in {"net10.0", ".netcoreapp10.0"}:
        raise ValueError(f"unexpected dependency target framework {target_framework!r}")
    dependencies = {
        dependency.attrib.get("id"): dependency.attrib.get("version")
        for dependency in dependency_groups[0].findall("{*}dependency")
    }
    if set(dependencies) != {"Microsoft.Orleans.Runtime", "PolyType"}:
        raise ValueError(f"unexpected package dependency set: {sorted(dependencies)}")
    if any(not version_range for version_range in dependencies.values()):
        raise ValueError("every package dependency must have an explicit version range")

    return {
        "id": actual_id,
        "version": version,
        "authors": child_text(metadata, "authors"),
        "description": description,
        "license": "MIT",
        "readme": child_text(metadata, "readme"),
        "releaseNotes": release_notes,
        "repository": {
            "type": repository_type,
            "url": repository_url,
            "commit": repository_commit,
        },
        "targetFramework": target_framework,
        "dependencies": dependencies,
    }


def load_allowlist(path: Path) -> list[str]:
    patterns = [
        line.strip()
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip() and not line.lstrip().startswith("#")
    ]
    if len(patterns) != len(set(patterns)):
        raise ValueError("package allowlist contains duplicate patterns")
    return patterns


def preflight_zip(stream: BinaryIO, file_size: int) -> int:
    """Count a single-disk, non-ZIP64 central directory before ZipFile allocates ZipInfo objects."""
    if file_size < ZIP_EOCD.size:
        raise ValueError("package is too small to contain a ZIP end record")

    tail_size = min(file_size, ZIP_EOCD.size + MAX_ZIP_COMMENT_BYTES)
    stream.seek(file_size - tail_size)
    tail = stream.read(tail_size)
    # CPython's ZipFile finds the final EOCD signature. Validate exactly that record so a fake
    # signature embedded later in a ZIP comment cannot make preflight and ZipFile parse different
    # central directories (and allocate unbounded ZipInfo objects before our entry cap).
    position = tail.rfind(ZIP_EOCD_SIGNATURE)
    if position < 0 or position + ZIP_EOCD.size > len(tail):
        raise ValueError("package has no valid ZIP end record")
    values = ZIP_EOCD.unpack_from(tail, position)
    if position + ZIP_EOCD.size + values[-1] != len(tail):
        raise ValueError("final ZIP end record has an inconsistent comment length")

    end_offset = file_size - tail_size + position
    if end_offset >= 20:
        stream.seek(end_offset - 20)
        if stream.read(4) == ZIP64_EOCD_LOCATOR_SIGNATURE:
            raise ValueError("ZIP64 packages are not supported")
    _, disk_number, directory_disk, disk_entries, total_entries, directory_size, directory_offset, _ = values
    if disk_number != 0 or directory_disk != 0 or disk_entries != total_entries:
        raise ValueError("multi-disk ZIP packages are not supported")
    if total_entries == 0xFFFF or directory_size == 0xFFFFFFFF or directory_offset == 0xFFFFFFFF:
        raise ValueError("ZIP64 packages are not supported")
    directory_end = directory_offset + directory_size
    if directory_offset < 0 or directory_end != end_offset:
        raise ValueError("ZIP central-directory bounds are inconsistent")

    stream.seek(directory_offset)
    parsed_entries = 0
    cursor = directory_offset
    while cursor < directory_end:
        if parsed_entries >= MAX_PACKAGE_ENTRIES:
            raise ValueError(f"package contains more than {MAX_PACKAGE_ENTRIES} ZIP entries")
        fixed = stream.read(ZIP_CENTRAL_DIRECTORY_FIXED_BYTES)
        if len(fixed) != ZIP_CENTRAL_DIRECTORY_FIXED_BYTES or fixed[:4] != ZIP_CENTRAL_DIRECTORY_SIGNATURE:
            raise ValueError("ZIP central directory contains an invalid record")
        name_length, extra_length, comment_length, start_disk = struct.unpack_from("<4H", fixed, 28)
        if start_disk != 0:
            raise ValueError("multi-disk ZIP packages are not supported")
        variable_length = name_length + extra_length + comment_length
        cursor += ZIP_CENTRAL_DIRECTORY_FIXED_BYTES + variable_length
        if cursor > directory_end:
            raise ValueError("ZIP central-directory record exceeds its declared bounds")
        stream.seek(variable_length, os.SEEK_CUR)
        parsed_entries += 1

    if cursor != directory_end or parsed_entries != total_entries:
        raise ValueError("ZIP central-directory entry count is inconsistent")
    stream.seek(0)
    return parsed_entries


def create_package_snapshot(source: Path, destination: Path) -> tuple[int, str]:
    """Copy one regular input handle into a private immutable snapshot while hashing it."""
    source_stat = source.lstat()
    if not stat.S_ISREG(source_stat.st_mode):
        raise ValueError("package input must be a regular file, not a link, device, or pipe")
    if source_stat.st_size > MAX_PACKAGE_FILE_BYTES:
        raise ValueError(f"package file exceeds {MAX_PACKAGE_FILE_BYTES} bytes before ZIP inspection")

    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NONBLOCK", 0)
    flags |= getattr(os, "O_NOFOLLOW", 0)
    destination.parent.mkdir(parents=True, exist_ok=True)
    descriptor = os.open(source, flags)
    digest = hashlib.sha256()
    copied = 0
    destination_created = False
    try:
        with os.fdopen(descriptor, "rb", closefd=True) as input_stream:
            opened_stat = os.fstat(input_stream.fileno())
            if not stat.S_ISREG(opened_stat.st_mode):
                raise ValueError("package input changed to a non-regular file")
            if (
                opened_stat.st_dev != source_stat.st_dev
                or opened_stat.st_ino != source_stat.st_ino
                or opened_stat.st_size != source_stat.st_size
            ):
                raise ValueError("package input changed before snapshot creation")
            with destination.open("xb") as output_stream:
                destination_created = True
                while chunk := input_stream.read(1024 * 1024):
                    copied += len(chunk)
                    if copied > MAX_PACKAGE_FILE_BYTES or copied > opened_stat.st_size:
                        raise ValueError("package input grew while its snapshot was created")
                    digest.update(chunk)
                    output_stream.write(chunk)
                output_stream.flush()
                os.fsync(output_stream.fileno())
                os.fchmod(output_stream.fileno(), stat.S_IRUSR)
            final_stat = os.fstat(input_stream.fileno())
        if copied != source_stat.st_size:
            raise ValueError("package input changed size while its snapshot was created")
        if (
            final_stat.st_dev != opened_stat.st_dev
            or final_stat.st_ino != opened_stat.st_ino
            or final_stat.st_size != opened_stat.st_size
            or final_stat.st_mtime_ns != opened_stat.st_mtime_ns
            or final_stat.st_ctime_ns != opened_stat.st_ctime_ns
        ):
            raise ValueError("package input changed while its snapshot was created")
        return copied, digest.hexdigest()
    except BaseException:
        if destination_created:
            destination.unlink(missing_ok=True)
        raise


def load_repository_policy(path: Path = REPOSITORY_POLICY) -> dict[str, object]:
    def reject_duplicate_keys(pairs: list[tuple[str, object]]) -> dict[str, object]:
        result: dict[str, object] = {}
        for key, value in pairs:
            if key in result:
                raise ValueError(f"NuGet repository policy repeats property {key!r}")
            result[key] = value
        return result

    policy = json.loads(
        path.read_text(encoding="utf-8"),
        object_pairs_hook=reject_duplicate_keys,
    )
    if not isinstance(policy, dict):
        raise ValueError("NuGet repository policy root must be an object")
    if set(policy) != {"schemaVersion", "serviceIndex", "owners", "certificates"}:
        raise ValueError("NuGet repository policy has unexpected or missing properties")
    if policy["schemaVersion"] != "oss-nuget-repository-policy/v1":
        raise ValueError("unsupported NuGet repository policy schema")
    service_index = policy["serviceIndex"]
    if service_index != "https://api.nuget.org/v3/index.json":
        raise ValueError("NuGet repository policy must target the reviewed NuGet.org service index")
    owners = policy["owners"]
    if (
        not isinstance(owners, list)
        or not owners
        or any(not isinstance(owner, str) or not re.fullmatch(r"[A-Za-z0-9_.-]+", owner) for owner in owners)
        or len(owners) != len(set(owners))
    ):
        raise ValueError("NuGet repository policy owners must be unique explicit account names")
    certificates = policy["certificates"]
    if not isinstance(certificates, list) or not certificates:
        raise ValueError("NuGet repository policy must pin at least one certificate")
    fingerprints: set[str] = set()
    for certificate in certificates:
        if not isinstance(certificate, dict) or set(certificate) != {
            "fingerprint",
            "hashAlgorithm",
            "allowUntrustedRoot",
        }:
            raise ValueError("NuGet repository policy certificate has an invalid shape")
        if certificate["hashAlgorithm"] != "SHA256" or certificate["allowUntrustedRoot"] is not False:
            raise ValueError("NuGet repository certificates require SHA256 and a trusted root")
        if not isinstance(certificate["fingerprint"], str) or not re.fullmatch(
            r"[0-9A-F]{64}", certificate["fingerprint"]
        ):
            raise ValueError("NuGet repository certificate fingerprint must be uppercase SHA256")
        if certificate["fingerprint"] in fingerprints:
            raise ValueError("NuGet repository policy repeats a certificate fingerprint")
        fingerprints.add(certificate["fingerprint"])
    return policy


def write_nuget_trust_config(path: Path, policy: dict[str, object]) -> None:
    configuration = ET.Element("configuration")
    config = ET.SubElement(configuration, "config")
    ET.SubElement(config, "clear")
    ET.SubElement(config, "add", key="signatureValidationMode", value="require")
    package_sources = ET.SubElement(configuration, "packageSources")
    ET.SubElement(package_sources, "clear")
    trusted_signers = ET.SubElement(configuration, "trustedSigners")
    ET.SubElement(trusted_signers, "clear")
    repository = ET.SubElement(
        trusted_signers,
        "repository",
        name="nuget.org",
        serviceIndex=str(policy["serviceIndex"]),
    )
    for certificate in policy["certificates"]:
        ET.SubElement(
            repository,
            "certificate",
            fingerprint=str(certificate["fingerprint"]),
            hashAlgorithm=str(certificate["hashAlgorithm"]),
            allowUntrustedRoot="false",
        )
    owners = ET.SubElement(repository, "owners")
    owners.text = ";".join(str(owner) for owner in policy["owners"])
    ET.ElementTree(configuration).write(path, encoding="utf-8", xml_declaration=True)


def verify_package_signature(package_path: Path) -> None:
    """Validate the snapshot against the reviewed NuGet.org certificate and owner policy."""
    policy = load_repository_policy()
    environment = os.environ.copy()
    environment["DOTNET_CLI_TELEMETRY_OPTOUT"] = "1"
    environment["DOTNET_NOLOGO"] = "1"
    environment["DOTNET_SKIP_FIRST_TIME_EXPERIENCE"] = "1"
    environment["DOTNET_GENERATE_ASPNET_CERTIFICATE"] = "false"
    environment["DOTNET_ADD_GLOBAL_TOOLS_TO_PATH"] = "false"
    environment["DOTNET_NUGET_SIGNATURE_VERIFICATION"] = "true"
    environment["NUGET_CERT_REVOCATION_MODE"] = "online"
    dotnet = environment.get("DOTNET_HOST_PATH", "dotnet")
    with tempfile.TemporaryDirectory(prefix="oss-nuget-trust-") as temporary:
        trust_directory = Path(temporary)
        environment["DOTNET_CLI_HOME"] = str(trust_directory / "dotnet-home")
        global_json = json.loads(GLOBAL_JSON.read_text(encoding="utf-8"))
        if global_json != {
            "sdk": {
                "version": "10.0.302",
                "rollForward": "latestPatch",
            }
        }:
            raise ValueError("global.json no longer matches the reviewed release verifier SDK policy")
        (trust_directory / "global.json").write_text(
            json.dumps(global_json, indent=2) + "\n",
            encoding="utf-8",
        )
        write_nuget_trust_config(trust_directory / "NuGet.Config", policy)
        try:
            result = subprocess.run(
                [
                    dotnet,
                    "nuget",
                    "verify",
                    "--all",
                    str(package_path.resolve(strict=True)),
                    "--verbosity",
                    "quiet",
                ],
                check=False,
                capture_output=True,
                text=True,
                timeout=120,
                cwd=trust_directory,
                env=environment,
            )
        except (OSError, subprocess.TimeoutExpired) as error:
            raise ValueError("could not execute dotnet nuget verify for the signed package") from error
    if result.returncode != 0:
        raise ValueError("dotnet nuget verify rejected the signed package")
    if result.stdout.strip() or result.stderr.strip():
        raise ValueError("dotnet nuget verify emitted a warning or unexpected diagnostic")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("package", type=Path)
    parser.add_argument("--allowlist", type=Path, default=Path(__file__).with_name("package-allowlist.txt"))
    parser.add_argument("--expected-version")
    parser.add_argument("--expected-commit")
    parser.add_argument("--canonical-output", type=Path)
    parser.add_argument(
        "--snapshot-output",
        type=Path,
        help="retain the exact validated input bytes at this new path for later consumer smoke",
    )
    parser.add_argument(
        "--repository-signed",
        action="store_true",
        help="require and verify a registry-added .signature.p7s entry, excluding it from semantic comparison",
    )
    args = parser.parse_args(argv)

    temporary_snapshot: tempfile.TemporaryDirectory[str] | None = None
    snapshot_path: Path | None = None
    snapshot_created = False
    try:
        if args.snapshot_output:
            snapshot_path = args.snapshot_output
        else:
            temporary_snapshot = tempfile.TemporaryDirectory(prefix="oss-package-snapshot-")
            snapshot_path = Path(temporary_snapshot.name) / "package.nupkg"
        package_file_bytes, container_sha256 = create_package_snapshot(args.package, snapshot_path)
        snapshot_created = True
        patterns = load_allowlist(args.allowlist)
        canonical_entries: dict[str, dict[str, object]] = {}
        warning_entry_text: dict[str, str] = {}
        total_bytes = 0
        nuspec_data: bytes | None = None
        signature_seen = False
        with snapshot_path.open("rb") as package_stream:
            opened_stat = os.fstat(package_stream.fileno())
            if not stat.S_ISREG(opened_stat.st_mode) or opened_stat.st_size != package_file_bytes:
                raise ValueError("package input changed before ZIP inspection")
            preflight_entries = preflight_zip(package_stream, package_file_bytes)
            with zipfile.ZipFile(package_stream) as package:
                entries = package.infolist()
                if len(entries) != preflight_entries:
                    raise ValueError("ZIP entry count changed after bounded preflight")
                names = [entry.filename for entry in entries]
                if len(names) != len(set(names)):
                    raise ValueError("package contains duplicate ZIP entry names")
                for info in entries:
                    name = info.filename
                    path = PurePosixPath(name)
                    if name.endswith("/") or path.is_absolute() or ".." in path.parts or "\\" in name:
                        raise ValueError(f"unsafe or unexpected package entry name: {name!r}")
                    unix_mode = (info.external_attr >> 16) & 0xFFFF
                    file_type = stat.S_IFMT(unix_mode)
                    if file_type not in {0, stat.S_IFREG}:
                        raise ValueError(f"package entry {name!r} is not a regular file")
                    if info.flag_bits & 0x1:
                        raise ValueError(f"package entry {name!r} is encrypted")
                    if info.compress_type not in {zipfile.ZIP_STORED, zipfile.ZIP_DEFLATED}:
                        raise ValueError(f"package entry {name!r} uses an unsupported compression method")
                    is_signature = name == SIGNATURE_ENTRY
                    if is_signature:
                        if not args.repository_signed:
                            raise ValueError(
                                "package signature entry is accepted only with --repository-signed"
                            )
                        if (
                            info.compress_type != zipfile.ZIP_STORED
                            or info.flag_bits != 0
                            or info.file_size <= 0
                            or info.file_size > MAX_SIGNATURE_BYTES
                        ):
                            raise ValueError(
                                "repository signature must be a nonempty, unencrypted, stored root entry "
                                f"no larger than {MAX_SIGNATURE_BYTES} bytes"
                            )
                        signature_seen = True
                    else:
                        matched = [pattern for pattern in patterns if matches(pattern, name)]
                        if len(matched) != 1:
                            raise ValueError(
                                f"package entry {name!r} must match exactly one allowlist pattern; matched {matched}"
                            )
                    if info.file_size > MAX_ENTRY_BYTES:
                        raise ValueError(f"package entry {name!r} exceeds {MAX_ENTRY_BYTES} bytes")
                    total_bytes += info.file_size
                    if total_bytes > MAX_PACKAGE_BYTES:
                        raise ValueError(f"package expands beyond {MAX_PACKAGE_BYTES} bytes")
                    data = package.read(name)
                    if is_signature:
                        continue
                    canonical_name, canonical_data = canonical_entry(name, data)
                    if canonical_name in canonical_entries:
                        raise ValueError(f"canonical package entry {canonical_name!r} is duplicated")
                    canonical_entries[canonical_name] = {
                        "sha256": hashlib.sha256(canonical_data).hexdigest(),
                        "bytes": len(canonical_data),
                    }
                    if name == f"{PACKAGE_ID}.nuspec":
                        nuspec_data = data
                    if name in PRERELEASE_WARNING_ENTRIES:
                        warning_entry_text[name] = data.decode("utf-8")

                payload_names = [name for name in names if name != SIGNATURE_ENTRY]
                unmatched_patterns = [
                    pattern
                    for pattern in patterns
                    if not any(matches(pattern, name) for name in payload_names)
                ]
                if unmatched_patterns:
                    raise ValueError(f"package is missing allowlisted entries: {unmatched_patterns}")

        if args.repository_signed:
            if not signature_seen:
                raise ValueError("--repository-signed requires one root .signature.p7s entry")
            verify_package_signature(snapshot_path)

        if nuspec_data is None:
            raise ValueError("package nuspec was not found")
        metadata = validate_nuspec(nuspec_data, args.expected_version, args.expected_commit)
        if is_prerelease(str(metadata["version"])):
            for entry_name in PRERELEASE_WARNING_ENTRIES:
                if entry_name not in warning_entry_text:
                    raise ValueError(f"prerelease package is missing {entry_name}")
                require_prerelease_warning(
                    warning_entry_text[entry_name],
                    f"package entry {entry_name}",
                )
        canonical = {
            "format": "orleans-searchable-storage-package/v1",
            "metadata": metadata,
            "entries": [
                {"path": name, **canonical_entries[name]}
                for name in sorted(canonical_entries)
            ],
        }
        rendered = json.dumps(canonical, indent=2, sort_keys=True) + "\n"
        if args.canonical_output:
            args.canonical_output.parent.mkdir(parents=True, exist_ok=True)
            args.canonical_output.write_text(rendered, encoding="utf-8")
        else:
            print(rendered, end="")
    except (OSError, ValueError, ET.ParseError, zipfile.BadZipFile, json.JSONDecodeError) as error:
        if args.snapshot_output and snapshot_created and snapshot_path is not None:
            snapshot_path.unlink(missing_ok=True)
        print(f"Package validation failed: {error}", file=sys.stderr)
        return 1
    finally:
        if temporary_snapshot is not None:
            temporary_snapshot.cleanup()

    print(
        f"Validated {args.package} ({len(canonical_entries)} canonical entries; "
        f"container sha256 {container_sha256}).",
        file=sys.stderr,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
