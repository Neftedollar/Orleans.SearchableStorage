#!/usr/bin/env python3
"""Validate package contents, metadata, provenance, and canonical entry hashes."""

from __future__ import annotations

import argparse
import fnmatch
import hashlib
import json
import re
import sys
import xml.etree.ElementTree as ET
import zipfile
from pathlib import Path, PurePosixPath


PACKAGE_ID = "Orleans.SearchableStorage"
REPOSITORY_URL = "https://github.com/Neftedollar/Orleans.SearchableStorage"
CORE_PROPERTIES_PATTERN = "package/services/metadata/core-properties/*.psmdcp"
CORE_PROPERTIES_CANONICAL = "package/services/metadata/core-properties/{generated}.psmdcp"
MAX_ENTRY_BYTES = 64 * 1024 * 1024
MAX_PACKAGE_BYTES = 128 * 1024 * 1024


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
    if fnmatch.fnmatchcase(name, CORE_PROPERTIES_PATTERN):
        root = ET.fromstring(data)
        for node in root.iter():
            if node.tag.endswith("}created") or node.tag == "created":
                node.text = "{normalized-package-created-time}"
        return CORE_PROPERTIES_CANONICAL, normalize_xml(root)
    if name == "_rels/.rels":
        root = ET.fromstring(data)
        for relationship in root.iter():
            target = relationship.attrib.get("Target")
            if target and fnmatch.fnmatchcase(target.lstrip("/"), CORE_PROPERTIES_PATTERN):
                relationship.attrib["Target"] = "/" + CORE_PROPERTIES_CANONICAL
                # NuGet generates both the core-properties path and its relationship id afresh for
                # each pack. Neither carries semantic package content; normalize the pair together.
                relationship.attrib["Id"] = "{generated-core-properties-relationship}"
        return name, normalize_xml(root)
    return name, data


def matches(pattern: str, name: str) -> bool:
    """Match exact allowlist entries literally; only * and ? opt into a glob."""
    return fnmatch.fnmatchcase(name, pattern) if "*" in pattern or "?" in pattern else name == pattern


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
    if child_text(metadata, "description") != (
        "Orleans-native persistent storage with searchable secondary indexes."
    ):
        raise ValueError("unexpected package description")
    if child_text(metadata, "readme") != "README.md":
        raise ValueError("package readme must be README.md")

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
        "license": "MIT",
        "readme": child_text(metadata, "readme"),
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


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("package", type=Path)
    parser.add_argument("--allowlist", type=Path, default=Path(__file__).with_name("package-allowlist.txt"))
    parser.add_argument("--expected-version")
    parser.add_argument("--expected-commit")
    parser.add_argument("--canonical-output", type=Path)
    args = parser.parse_args(argv)

    try:
        patterns = load_allowlist(args.allowlist)
        canonical_entries: dict[str, dict[str, object]] = {}
        total_bytes = 0
        nuspec_data: bytes | None = None
        with zipfile.ZipFile(args.package) as package:
            names = package.namelist()
            if len(names) != len(set(names)):
                raise ValueError("package contains duplicate ZIP entry names")
            for name in names:
                path = PurePosixPath(name)
                if name.endswith("/") or path.is_absolute() or ".." in path.parts or "\\" in name:
                    raise ValueError(f"unsafe or unexpected package entry name: {name!r}")
                matched = [pattern for pattern in patterns if matches(pattern, name)]
                if len(matched) != 1:
                    raise ValueError(
                        f"package entry {name!r} must match exactly one allowlist pattern; matched {matched}"
                    )
                info = package.getinfo(name)
                if info.file_size > MAX_ENTRY_BYTES:
                    raise ValueError(f"package entry {name!r} exceeds {MAX_ENTRY_BYTES} bytes")
                total_bytes += info.file_size
                if total_bytes > MAX_PACKAGE_BYTES:
                    raise ValueError(f"package expands beyond {MAX_PACKAGE_BYTES} bytes")
                data = package.read(name)
                canonical_name, canonical_data = canonical_entry(name, data)
                if canonical_name in canonical_entries:
                    raise ValueError(f"canonical package entry {canonical_name!r} is duplicated")
                canonical_entries[canonical_name] = {
                    "sha256": hashlib.sha256(canonical_data).hexdigest(),
                    "bytes": len(canonical_data),
                }
                if name == f"{PACKAGE_ID}.nuspec":
                    nuspec_data = data

            unmatched_patterns = [
                pattern
                for pattern in patterns
                if not any(matches(pattern, name) for name in names)
            ]
            if unmatched_patterns:
                raise ValueError(f"package is missing allowlisted entries: {unmatched_patterns}")

        if nuspec_data is None:
            raise ValueError("package nuspec was not found")
        metadata = validate_nuspec(nuspec_data, args.expected_version, args.expected_commit)
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
    except (OSError, ValueError, ET.ParseError, zipfile.BadZipFile) as error:
        print(f"Package validation failed: {error}", file=sys.stderr)
        return 1

    print(f"Validated {args.package} ({len(canonical_entries)} canonical entries).", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
