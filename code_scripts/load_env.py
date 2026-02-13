"""Lightweight .env loader for script entrypoints."""

from __future__ import annotations

import os
from pathlib import Path

from code_scripts.paths import BASE_DIR, OPS_ROOT


def _candidate_paths(env_file: str) -> list[Path]:
    return [
        OPS_ROOT / env_file,
        BASE_DIR / env_file,
    ]


def load_env_file(env_file: str = ".env") -> None:
    """
    Load environment variables from `.env`.

    Search order:
    1) `code_scripts/.env`
    2) repo-root `.env` (for backward compatibility)
    """
    env_path = next((path for path in _candidate_paths(env_file) if path.exists()), None)
    if env_path is None:
        return

    try:
        with open(env_path, "r", encoding="utf-8") as handle:
            for raw_line in handle:
                line = raw_line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue

                key, value = line.split("=", 1)
                key = key.strip()
                value = value.strip()
                if value.startswith('"') and value.endswith('"'):
                    value = value[1:-1]
                elif value.startswith("'") and value.endswith("'"):
                    value = value[1:-1]

                if key and key not in os.environ:
                    os.environ[key] = value
    except OSError:
        # Non-fatal: scripts should still use process environment variables.
        return
