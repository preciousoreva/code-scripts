from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from code_scripts.artifact_paths import qbo_snapshots_dir


def get_qbo_snapshot_path(company_key: str) -> Path:
    return qbo_snapshots_dir() / f"{company_key}_products.csv"


def get_qbo_snapshot_invalidation_path(company_key: str) -> Path:
    return qbo_snapshots_dir() / f"{company_key}_products.invalidate.json"


def mark_qbo_snapshot_stale(company_key: str, *, reason: str) -> Path:
    path = get_qbo_snapshot_invalidation_path(company_key)
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "company_key": company_key,
        "reason": str(reason).strip() or "unknown",
        "invalidated_at": datetime.now(tz=timezone.utc).isoformat(),
    }
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, sort_keys=True)
    return path


def clear_qbo_snapshot_stale_marker(company_key: str) -> None:
    get_qbo_snapshot_invalidation_path(company_key).unlink(missing_ok=True)


def get_qbo_snapshot_stale_reason(company_key: str, snapshot_path: Path) -> Optional[str]:
    marker = get_qbo_snapshot_invalidation_path(company_key)
    if not marker.exists():
        return None
    if not snapshot_path.exists():
        return "snapshot_missing"
    try:
        if marker.stat().st_mtime <= snapshot_path.stat().st_mtime:
            return None
        payload = json.loads(marker.read_text(encoding="utf-8"))
        reason = str(payload.get("reason", "")).strip()
        return reason or "invalidated"
    except Exception:
        if marker.stat().st_mtime > snapshot_path.stat().st_mtime:
            return "invalidated"
        return None
