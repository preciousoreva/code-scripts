from __future__ import annotations

from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
OPS_ROOT = BASE_DIR / "code_scripts"
OPS_COMPANIES_DIR = OPS_ROOT / "companies"
OPS_LOGS_DIR = OPS_ROOT / "logs"
OPS_RUN_LOGS_DIR = OPS_LOGS_DIR / "runs"
OPS_UPLOADED_DIR = OPS_ROOT / "Uploaded"
OPS_REPORTS_DIR = OPS_ROOT / "reports"
