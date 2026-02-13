#!/usr/bin/env python3
"""Fail on multiline Django variable tags like '{{ ...' on one line and '... }}' on another."""

from __future__ import annotations

import sys
from pathlib import Path


def main() -> int:
    root = Path(__file__).resolve().parents[1]
    template_paths = sorted(
        set(root.glob("apps/**/templates/**/*.html")) | set(root.glob("templates/**/*.html"))
    )
    failures: list[tuple[Path, int, str]] = []

    for path in template_paths:
        try:
            lines = path.read_text(encoding="utf-8").splitlines()
        except UnicodeDecodeError:
            continue

        for line_no, line in enumerate(lines, start=1):
            if "{{" in line and "}}" not in line:
                failures.append((path, line_no, line.rstrip()))

    if not failures:
        return 0

    print("Found possible multiline Django variable tags. Keep each '{{ ... }}' on one line:")
    for path, line_no, line in failures:
        print(f"  - {path.relative_to(root)}:{line_no}: {line}")
    return 1


if __name__ == "__main__":
    sys.exit(main())
