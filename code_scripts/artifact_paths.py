from __future__ import annotations

from datetime import datetime
from pathlib import Path

from code_scripts.paths import OPS_REPORTS_DIR, OPS_ROOT


def artifact_day_stamp(now: datetime | None = None) -> str:
    return (now or datetime.now()).strftime("%Y-%m-%d")


def stock_exports_dir(now: datetime | None = None) -> Path:
    return OPS_ROOT / "exports" / "stock_reports" / artifact_day_stamp(now)


def qbo_snapshots_dir() -> Path:
    return OPS_ROOT / "exports" / "qbo_snapshots"


def inventory_audit_reports_dir(now: datetime | None = None) -> Path:
    return OPS_REPORTS_DIR / "inventory_sync" / artifact_day_stamp(now)


def qbo_pack_variant_reports_dir(now: datetime | None = None) -> Path:
    return OPS_REPORTS_DIR / "qbo_pack_variant_audit" / artifact_day_stamp(now)


def qbo_pack_variant_migration_reports_dir(now: datetime | None = None) -> Path:
    return OPS_REPORTS_DIR / "qbo_pack_variant_migration" / artifact_day_stamp(now)


def qbo_pack_variant_consolidation_reports_dir(now: datetime | None = None) -> Path:
    return OPS_REPORTS_DIR / "qbo_pack_variant_consolidation" / artifact_day_stamp(now)
