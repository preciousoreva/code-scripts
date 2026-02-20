"""Env/CLI config parsing and defaults for reconciliation."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from code_scripts.paths import OPS_REPORTS_DIR, OPS_UPLOADED_DIR, REPO_ROOT


def resolve_default_base_dir(company: str) -> Path:
    """
    Resolve default base input dir for company-aware layouts.

    Preferred: docs/<company>/bank-reconciliation/
    Supported legacy: docs/bank-reconciliation/<company>/
    """
    preferred = REPO_ROOT / "docs" / company / "bank-reconciliation"
    legacy = REPO_ROOT / "docs" / "bank-reconciliation" / company
    if preferred.exists():
        return preferred
    if legacy.exists():
        return legacy
    return preferred


def resolve_default_out_dir(
    company: Optional[str],
    date: str,
    out_dir_override: Optional[Path] = None,
) -> Path:
    """
    Resolve output base dir. If override provided, use it. Else if company provided,
    use code_scripts/reports/reconciliation/<company>/; else reports/reconciliation/.
    """
    if out_dir_override is not None:
        return Path(out_dir_override).resolve()
    if company:
        return (OPS_REPORTS_DIR / "reconciliation" / company).resolve()
    return (OPS_REPORTS_DIR / "reconciliation").resolve()


@dataclass
class ReconciliationConfig:
    """Configuration for EPOS-to-bank reconciliation run."""

    date: str  # YYYY-MM-DD
    epos_file: Optional[Path] = None
    epos_dir: Optional[Path] = None
    statements_dir: Optional[Path] = None
    account_mapping_path: Optional[Path] = None
    out_dir: Path = OPS_REPORTS_DIR / "reconciliation"
    amount_tolerance: float = 1.0
    time_window_mins: int = 180
    prefer_original: bool = True
    emtl_link_minutes: int = 10
    company: Optional[str] = None  # used for default paths and out_dir scoping

    def output_date_dir(self) -> Path:
        """Output folder for this run: out_dir / YYYY-MM-DD."""
        return Path(self.out_dir) / self.date

    def epos_dir_resolved(self) -> Optional[Path]:
        """Resolved epos_dir; if only epos_file set, use its parent."""
        if self.epos_dir is not None:
            return Path(self.epos_dir).resolve()
        if self.epos_file is not None:
            return Path(self.epos_file).resolve().parent
        return None

    @staticmethod
    def default_epos_dir() -> Path:
        return OPS_UPLOADED_DIR
