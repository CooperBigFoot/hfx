#!/usr/bin/env python3
"""Verify canonical repository vision metadata and file shape."""

from __future__ import annotations

import argparse
import re
import sys
from datetime import date
from pathlib import Path

FILENAME_RE = re.compile(
    r"(?P<date>[0-9]{4}-[0-9]{2}-[0-9]{2})-[a-z0-9]+(?:-[a-z0-9]+)*\.md"
)
REPOSITORY_RE = re.compile(
    r"(?![A-Za-z0-9-]*--)[A-Za-z0-9](?:[A-Za-z0-9-]{0,37}[A-Za-z0-9])?/[A-Za-z0-9._-]{1,100}"
)


def verify(root: Path, repository: str) -> list[str]:
    """Return every canonical-vision violation under root."""
    errors: list[str] = []
    if REPOSITORY_RE.fullmatch(repository) is None:
        return [f"malformed canonical repository: {repository}"]
    if not root.is_dir():
        return [f"missing canonical vision directory: {root}"]

    issue_prefix = f"https://github.com/{repository}/issues/"
    declaration_re = re.compile(re.escape(issue_prefix) + r"(?P<number>[1-9][0-9]*)")
    paths = sorted(root.rglob("*"))
    visions = [path for path in paths if path.is_file()]
    if not visions:
        errors.append(f"{root}: contains no vision files")

    for path in paths:
        if path.is_symlink():
            errors.append(f"{path}: symlinks are forbidden")
        elif path.is_dir():
            errors.append(f"{path}: nested directories are forbidden")
        elif path.suffix != ".md":
            errors.append(f"{path}: only Markdown files are allowed")

    seen_efforts: dict[str, Path] = {}
    for path in visions:
        filename = FILENAME_RE.fullmatch(path.name)
        if filename is None:
            errors.append(f"{path}: filename must use YYYY-MM-DD-lowercase-slug.md")
        else:
            try:
                date.fromisoformat(filename.group("date"))
            except ValueError:
                errors.append(f"{path}: filename contains an invalid calendar date")

        lines = path.read_text(encoding="utf-8").splitlines()
        if not lines or not lines[0].startswith("# "):
            errors.append(f"{path}: first line must be a Markdown title")
            continue

        program_lines = [line for line in lines if line.startswith("Program: ")]
        effort_lines = [line for line in lines if line.startswith("Effort: ")]
        if len(program_lines) != 1:
            errors.append(f"{path}: expected exactly one Program declaration")
            continue
        if len(effort_lines) != 1:
            errors.append(f"{path}: expected exactly one Effort declaration")
            continue
        if len(lines) < 3 or lines[1] != program_lines[0] or lines[2] != effort_lines[0]:
            errors.append(f"{path}: Program and Effort must immediately follow the title")

        program = declaration_re.fullmatch(program_lines[0].removeprefix("Program: "))
        effort = declaration_re.fullmatch(effort_lines[0].removeprefix("Effort: "))
        if program is None:
            errors.append(f"{path}: Program URL must belong to {repository}")
        if effort is None:
            errors.append(f"{path}: Effort URL must belong to {repository}")
        else:
            effort_url = effort_lines[0].removeprefix("Effort: ")
            if effort_url in seen_efforts:
                errors.append(f"{path}: Effort is already linked by {seen_efforts[effort_url]}")
            else:
                seen_efforts[effort_url] = path
        if program and effort and program.group("number") == effort.group("number"):
            errors.append(f"{path}: Program and Effort must identify different issues")

    return errors


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--repository", required=True, help="canonical GitHub owner/repository")
    parser.add_argument("--root", type=Path, default=Path("planning/visions"))
    args = parser.parse_args(argv)

    errors = verify(args.root, args.repository)
    if errors:
        for error in errors:
            print(f"error: {error}", file=sys.stderr)
        return 1

    count = sum(1 for path in args.root.iterdir() if path.is_file())
    print(f"verified {count} canonical vision files for {args.repository}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
