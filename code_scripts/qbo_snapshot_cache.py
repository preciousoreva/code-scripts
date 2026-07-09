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
    snapshot_mtime = snapshot_path.stat().st_mtime
    marker_mtime = marker.stat().st_mtime
    try:
        payload = json.loads(marker.read_text(encoding="utf-8"))
        invalidated_at = str(payload.get("invalidated_at", "")).strip()
        if invalidated_at:
            marker_time = datetime.fromisoformat(invalidated_at.replace("Z", "+00:00"))
            if marker_time.tzinfo is None:
                marker_time = marker_time.replace(tzinfo=timezone.utc)
            snapshot_time = datetime.fromtimestamp(snapshot_mtime, tz=timezone.utc)
            if marker_time < snapshot_time:
                return None
        elif marker_mtime < snapshot_mtime:
            return None
        reason = str(payload.get("reason", "")).strip()
        return reason or "invalidated"
    except Exception:
        if marker_mtime >= snapshot_mtime:
            return "invalidated"
        return None
