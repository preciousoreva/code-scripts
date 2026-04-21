"""Lightweight .env loader for script entrypoints."""

from __future__ import annotations

import os
from pathlib import Path

from code_scripts.paths import REPO_ROOT, REPO_CODE_SCRIPTS_DIR


def _resolve_env_path(path_str: str) -> Path:
    path = Path(path_str)
    if path.is_absolute():
        return path
    return (REPO_ROOT / path).resolve()


def resolve_env_file_path(env_file: str = ".env") -> Path | None:
    explicit_env_file = os.getenv("OIAT_ENV_FILE")
    candidates: list[Path] = []

    if explicit_env_file:
        candidates.append(_resolve_env_path(explicit_env_file))

    if env_file and env_file != ".env":
        candidates.append(_resolve_env_path(env_file))

    candidates.extend(
        [
            REPO_CODE_SCRIPTS_DIR / ".env",
            REPO_ROOT / ".env",
        ]
    )

    seen: set[Path] = set()
    for candidate in candidates:
        if candidate in seen:
            continue
        seen.add(candidate)
        if candidate.exists():
            return candidate
    return None


def load_env_file(env_file: str = ".env") -> None:
    """
    Load environment variables from `.env`.

    Search order:
    1) `OIAT_ENV_FILE`
    2) explicit `env_file` path (when not `.env`)
    3) `code_scripts/.env`
    4) repo-root `.env` (for backward compatibility)
    """
    env_path = resolve_env_file_path(env_file)
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
