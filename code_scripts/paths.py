from __future__ import annotations

import os
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
REPO_ROOT = BASE_DIR
REPO_CODE_SCRIPTS_DIR = REPO_ROOT / "code_scripts"


def _seed_path_env_from_explicit_env_file() -> None:
    raw_env_file = os.getenv("OIAT_ENV_FILE")
    if not raw_env_file:
        return

    env_path = Path(raw_env_file)
    if not env_path.is_absolute():
        env_path = (REPO_ROOT / env_path).resolve()
    if not env_path.exists():
        return

    try:
        with open(env_path, "r", encoding="utf-8") as handle:
            for raw_line in handle:
                line = raw_line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                key, value = line.split("=", 1)
                key = key.strip()
                value = value.strip().strip('"').strip("'")
                if key in {"STATE_ROOT", "OIAT_STATE_ROOT", "OIAT_COMPANIES_DIR"} and key not in os.environ:
                    os.environ[key] = value
    except OSError:
        return


def _resolve_path(raw_value: str | None, default_path: Path) -> Path:
    if not raw_value:
        return default_path
    path = Path(raw_value).expanduser()
    if path.is_absolute():
        return path
    return (REPO_ROOT / path).resolve()


_seed_path_env_from_explicit_env_file()

STATE_ROOT = _resolve_path(os.getenv("STATE_ROOT") or os.getenv("OIAT_STATE_ROOT"), REPO_ROOT / "runtime")
OPS_ROOT = STATE_ROOT / "code_scripts"
OPS_COMPANIES_DIR = _resolve_path(os.getenv("OIAT_COMPANIES_DIR"), OPS_ROOT / "companies")
OPS_LOGS_DIR = OPS_ROOT / "logs"
OPS_RUN_LOGS_DIR = OPS_LOGS_DIR / "runs"
OPS_UPLOADED_DIR = OPS_ROOT / "Uploaded"
OPS_REPORTS_DIR = OPS_ROOT / "reports"
