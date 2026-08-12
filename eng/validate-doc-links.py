#!/usr/bin/env python3
"""Validate repository-local Markdown links and GitHub-style heading anchors."""

from __future__ import annotations

import argparse
import re
import sys
import unicodedata
from collections import defaultdict
from pathlib import Path
from urllib.parse import unquote, urlsplit


LINK_PATTERN = re.compile(r"(?<!!)\[[^\]]*\]\(([^)]+)\)")
HEADING_PATTERN = re.compile(r"^(#{1,6})\s+(.+?)\s*#*\s*$")
EXPLICIT_ANCHOR_PATTERN = re.compile(
    r"<(?:a\s+(?:name|id)|[^>]+\sid)=[\"']([^\"']+)[\"']",
    re.IGNORECASE,
)


def github_slug(value: str) -> str:
    value = re.sub(r"<[^>]+>", "", value)
    value = re.sub(r"[`*_~]", "", value)
    value = unicodedata.normalize("NFKC", value).strip().lower()
    value = "".join(
        character
        for character in value
        if character.isalnum() or character in {" ", "-", "_"}
    )
    return re.sub(r"\s+", "-", value)


def markdown_anchors(path: Path) -> set[str]:
    anchors: set[str] = set()
    occurrences: defaultdict[str, int] = defaultdict(int)
    in_fence = False
    fence_marker = ""
    for line in path.read_text(encoding="utf-8").splitlines():
        stripped = line.lstrip()
        if stripped.startswith(("```", "~~~")):
            marker = stripped[:3]
            if not in_fence:
                in_fence = True
                fence_marker = marker
            elif marker == fence_marker:
                in_fence = False
            continue
        if in_fence:
            continue

        match = HEADING_PATTERN.match(line)
        if match:
            base = github_slug(match.group(2))
            if base:
                suffix = occurrences[base]
                occurrences[base] += 1
                anchors.add(base if suffix == 0 else f"{base}-{suffix}")
        anchors.update(EXPLICIT_ANCHOR_PATTERN.findall(line))
    return anchors


def link_destination(raw: str) -> str:
    raw = raw.strip()
    if raw.startswith("<") and ">" in raw:
        return raw[1 : raw.index(">")]
    # Markdown permits an optional title after a whitespace-delimited destination.
    return raw.split(maxsplit=1)[0]


def discover_markdown(root: Path, explicit: list[str]) -> list[Path]:
    if explicit:
        return [Path(value).resolve() for value in explicit]
    excluded = {".git", "bin", "obj", "artifacts", "BenchmarkDotNet.Artifacts"}
    return sorted(
        path
        for path in root.rglob("*.md")
        if not any(component in excluded for component in path.relative_to(root).parts)
    )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("paths", nargs="*", help="Markdown files; defaults to the repository tree")
    parser.add_argument("--root", default=Path(__file__).resolve().parent.parent)
    args = parser.parse_args()

    root = Path(args.root).resolve()
    markdown_files = discover_markdown(root, args.paths)
    anchor_cache: dict[Path, set[str]] = {}
    errors: list[str] = []

    for source in markdown_files:
        if not source.is_file():
            errors.append(f"{source}: Markdown source does not exist")
            continue
        for line_number, line in enumerate(source.read_text(encoding="utf-8").splitlines(), 1):
            for match in LINK_PATTERN.finditer(line):
                destination = link_destination(match.group(1))
                split = urlsplit(destination)
                if split.scheme or split.netloc:
                    continue

                target_path = source if not split.path else (source.parent / unquote(split.path))
                target_path = target_path.resolve()
                try:
                    target_path.relative_to(root)
                except ValueError:
                    errors.append(
                        f"{source.relative_to(root)}:{line_number}: local link escapes repository: "
                        f"{destination}"
                    )
                    continue
                if not target_path.is_file():
                    errors.append(
                        f"{source.relative_to(root)}:{line_number}: missing local target: {destination}"
                    )
                    continue

                fragment = unquote(split.fragment).lower()
                if fragment and target_path.suffix.lower() == ".md":
                    anchors = anchor_cache.setdefault(target_path, markdown_anchors(target_path))
                    if fragment not in anchors:
                        errors.append(
                            f"{source.relative_to(root)}:{line_number}: missing anchor "
                            f"'{fragment}' in {target_path.relative_to(root)}"
                        )

    if errors:
        print("Markdown link validation failed:", file=sys.stderr)
        for error in errors:
            print(f"  - {error}", file=sys.stderr)
        return 1

    print(f"Validated local links and anchors in {len(markdown_files)} Markdown files.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
