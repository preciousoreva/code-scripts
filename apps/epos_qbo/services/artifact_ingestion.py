from __future__ import annotations

import hashlib
import json
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from django.utils import timezone as dj_timezone

from oiat_portal.paths import OPS_LOGS_DIR, OPS_REPORTS_DIR, OPS_UPLOADED_DIR

from ..models import RunArtifact, RunJob

logger = logging.getLogger(__name__)


@dataclass
class ParsedArtifact:
    company_key: str
    target_date: datetime.date | None
    processed_at: datetime | None
    source_path: str
    source_hash: str
    reliability_status: str
    rows_total: int | None
    rows_kept: int | None
    rows_non_target: int | None
    upload_stats_json: dict[str, Any]
    reconcile_status: str
    reconcile_difference: float | None
    reconcile_epos_total: float | None
    reconcile_qbo_total: float | None
    reconcile_epos_count: int | None
    reconcile_qbo_count: int | None
    raw_file: str
    processed_files_json: list[str]
    nearest_log_file: str


def _sha256(path: Path) -> str:
    hasher = hashlib.sha256()
    with open(path, "rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            hasher.update(chunk)
    return hasher.hexdigest()


def _parse_date(value: str | None) -> datetime.date | None:
    if not value:
        return None
    try:
        return datetime.strptime(value[:10], "%Y-%m-%d").date()
    except ValueError:
        return None


def _parse_dt(value: str | None) -> datetime | None:
    if not value:
        return None
    normalized = value.replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(normalized)
    except ValueError:
        return None
    if dt.tzinfo is None:
        logger.warning("Artifact metadata processed_at is naive; assuming UTC: %s", value)
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _log_mentions_company(path: Path, company_key: str) -> bool:
    if not company_key:
        return False
    try:
        with open(path, "r", encoding="utf-8", errors="ignore") as handle:
            return company_key in handle.read(50000)
    except OSError:
        return False


def _nearest_log(processed_at: datetime | None, company_key: str) -> str:
    if not processed_at:
        return ""

    candidates = sorted(OPS_LOGS_DIR.glob("pipeline_*.log"))
    best: tuple[float, Path] | None = None
    for path in candidates:
        try:
            ts = datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc)
        except OSError:
            continue
        delta_seconds = abs((ts - processed_at).total_seconds())
        if delta_seconds > 12 * 3600:
            continue

        score = delta_seconds
        if _log_mentions_company(path, company_key):
            score -= 60
        if best is None or score < best[0]:
            best = (score, path)
    return str(best[1]) if best else ""


def _safe_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _safe_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _safe_int_dict(values: dict[str, Any]) -> dict[str, int]:
    parsed: dict[str, int] = {}
    for key, value in values.items():
        int_value = _safe_int(value)
        if int_value is not None:
            parsed[str(key)] = int_value
    return parsed


def _reliability_for(path: Path) -> str:
    if path.name.startswith("last_"):
        return RunArtifact.RELIABILITY_WARNING
    return RunArtifact.RELIABILITY_HIGH


def parse_metadata_file(path: Path) -> ParsedArtifact | None:
    try:
        with open(path, "r", encoding="utf-8") as handle:
            data = json.load(handle)
    except (OSError, json.JSONDecodeError):
        return None

    company_key = str(data.get("company_key") or "").strip()
    if not company_key:
        return None

    reconcile = data.get("reconcile") or {}
    if not isinstance(reconcile, dict):
        reconcile = {}

    processed_at = _parse_dt(data.get("processed_at"))
    return ParsedArtifact(
        company_key=company_key,
        target_date=_parse_date(data.get("target_date")),
        processed_at=processed_at,
        source_path=str(path),
        source_hash=_sha256(path),
        reliability_status=_reliability_for(path),
        rows_total=_safe_int(data.get("rows_total")),
        rows_kept=_safe_int(data.get("rows_kept")),
        rows_non_target=_safe_int(data.get("rows_non_target")),
        upload_stats_json=data.get("upload_stats") if isinstance(data.get("upload_stats"), dict) else {},
        reconcile_status=str(reconcile.get("status") or ""),
        reconcile_difference=_safe_float(reconcile.get("difference")),
        reconcile_epos_total=_safe_float(reconcile.get("epos_total")),
        reconcile_qbo_total=_safe_float(reconcile.get("qbo_total")),
        reconcile_epos_count=_safe_int(reconcile.get("epos_count")),
        reconcile_qbo_count=_safe_int(reconcile.get("qbo_count")),
        raw_file=str(data.get("raw_file") or ""),
        processed_files_json=data.get("processed_files")
        if isinstance(data.get("processed_files"), list)
        else [],
        nearest_log_file=_nearest_log(processed_at, company_key),
    )


def ingest_metadata_file(path: Path, run_job: RunJob | None = None) -> tuple[RunArtifact | None, bool]:
    parsed = parse_metadata_file(path)
    if parsed is None:
        return None, False

    artifact, created = RunArtifact.objects.get_or_create(
        company_key=parsed.company_key,
        target_date=parsed.target_date,
        processed_at=parsed.processed_at,
        source_hash=parsed.source_hash,
        defaults={
            "run_job": run_job,
            "source_path": parsed.source_path,
            "reliability_status": parsed.reliability_status,
            "rows_total": parsed.rows_total,
            "rows_kept": parsed.rows_kept,
            "rows_non_target": parsed.rows_non_target,
            "upload_stats_json": parsed.upload_stats_json,
            "reconcile_status": parsed.reconcile_status,
            "reconcile_difference": parsed.reconcile_difference,
            "reconcile_epos_total": parsed.reconcile_epos_total,
            "reconcile_qbo_total": parsed.reconcile_qbo_total,
            "reconcile_epos_count": parsed.reconcile_epos_count,
            "reconcile_qbo_count": parsed.reconcile_qbo_count,
            "raw_file": parsed.raw_file,
            "processed_files_json": parsed.processed_files_json,
            "nearest_log_file": parsed.nearest_log_file,
        },
    )

    updated_fields: list[str] = []
    if run_job and artifact.run_job_id is None:
        artifact.run_job = run_job
        updated_fields.append("run_job")
    if not artifact.source_path:
        artifact.source_path = parsed.source_path
        updated_fields.append("source_path")
    if artifact.reliability_status != parsed.reliability_status:
        artifact.reliability_status = parsed.reliability_status
        updated_fields.append("reliability_status")
    for field_name in (
        "reconcile_epos_total",
        "reconcile_qbo_total",
        "reconcile_epos_count",
        "reconcile_qbo_count",
    ):
        current_value = getattr(artifact, field_name, None)
        parsed_value = getattr(parsed, field_name, None)
        if current_value is None and parsed_value is not None:
            setattr(artifact, field_name, parsed_value)
            updated_fields.append(field_name)
    # Refresh upload stats from metadata so Companies page and Run Detail show skipped/uploaded correctly
    if parsed.upload_stats_json and isinstance(parsed.upload_stats_json, dict):
        artifact.upload_stats_json = parsed.upload_stats_json
        updated_fields.append("upload_stats_json")
    if updated_fields:
        artifact.save(update_fields=updated_fields)

    return artifact, created


def ingest_history(days: int = 60) -> int:
    cutoff = dj_timezone.now() - timedelta(days=days)
    created_count = 0
    for path in sorted(OPS_UPLOADED_DIR.rglob("last_*_transform.json")):
        try:
            modified = datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc)
        except OSError:
            continue
        if modified < cutoff:
            continue
        _, created = ingest_metadata_file(path)
        if created:
            created_count += 1
    return created_count


def attach_recent_artifacts_to_job(run_job: RunJob) -> int:
    if run_job.scope == RunJob.SCOPE_INVENTORY_PIPELINE:
        return _attach_inventory_artifacts_to_job(run_job) + _attach_inventory_pipeline_artifacts_to_job(
            run_job
        )
    if run_job.scope == RunJob.SCOPE_INVENTORY_SYNC:
        return _attach_inventory_artifacts_to_job(run_job)

    attached = 0
    for path in sorted(OPS_UPLOADED_DIR.rglob("last_*_transform.json")):
        artifact, _ = ingest_metadata_file(path)
        if artifact is None:
            continue
        if run_job.scope == RunJob.SCOPE_SINGLE and artifact.company_key != run_job.company_key:
            # Defensive cleanup for legacy bad links from earlier matching behavior.
            if artifact.run_job_id == run_job.id:
                artifact.run_job = None
                artifact.save(update_fields=["run_job"])
            continue
        if artifact.run_job_id is None:
            artifact.run_job = run_job
            artifact.save(update_fields=["run_job"])
        if artifact.run_job_id == run_job.id:
            attached += 1
    return attached


def parse_inventory_audit_metadata(path: Path) -> dict[str, Any] | None:
    try:
        with open(path, "r", encoding="utf-8") as handle:
            data = json.load(handle)
    except (OSError, json.JSONDecodeError):
        return None
    if not isinstance(data, dict):
        return None
    if not str(data.get("company_key") or "").strip():
        return None
    return data


def parse_inventory_pipeline_metadata(path: Path) -> dict[str, Any] | None:
    try:
        with open(path, "r", encoding="utf-8") as handle:
            data = json.load(handle)
    except (OSError, json.JSONDecodeError):
        return None
    if not isinstance(data, dict):
        return None
    if data.get("run_type") != RunJob.SCOPE_INVENTORY_PIPELINE:
        return None
    if not str(data.get("company_key") or "").strip():
        return None
    return data


def ingest_inventory_audit_file(path: Path, run_job: RunJob | None = None) -> tuple[RunArtifact | None, bool]:
    """Create/update a RunArtifact row for an inventory audit sidecar JSON.

    The audit's report CSV lives next to the sidecar (same stem, .csv). The
    sidecar itself is what we hash/store as the canonical source.
    """
    data = parse_inventory_audit_metadata(path)
    if data is None:
        return None, False

    company_key = str(data["company_key"]).strip()
    processed_at = _parse_dt(data.get("processed_at"))
    source_hash = _sha256(path)
    apply_stats = data.get("apply") if isinstance(data.get("apply"), dict) else {}
    status_counts = data.get("status_counts") if isinstance(data.get("status_counts"), dict) else {}

    upload_stats_json = {
        "status_counts": {str(k): int(v) for k, v in status_counts.items()},
        "total_groups": _safe_int(data.get("total_groups")),
        "apply": apply_stats,
        "report_csv": str(data.get("report_csv") or ""),
        "stock_csv": str(data.get("stock_csv") or ""),
        "qbo_csv": str(data.get("qbo_csv") or ""),
    }

    artifact, created = RunArtifact.objects.get_or_create(
        company_key=company_key,
        target_date=None,
        processed_at=processed_at,
        source_hash=source_hash,
        defaults={
            "kind": RunArtifact.KIND_INVENTORY_AUDIT,
            "run_job": run_job,
            "source_path": str(path),
            "reliability_status": RunArtifact.RELIABILITY_HIGH,
            "upload_stats_json": upload_stats_json,
            "raw_file": str(data.get("stock_csv") or ""),
            "processed_files_json": [str(data.get("report_csv") or "")],
            "nearest_log_file": _nearest_log(processed_at, company_key),
        },
    )

    updated_fields: list[str] = []
    if artifact.kind != RunArtifact.KIND_INVENTORY_AUDIT:
        artifact.kind = RunArtifact.KIND_INVENTORY_AUDIT
        updated_fields.append("kind")
    if run_job and artifact.run_job_id is None:
        artifact.run_job = run_job
        updated_fields.append("run_job")
    if not artifact.source_path:
        artifact.source_path = str(path)
        updated_fields.append("source_path")
    artifact.upload_stats_json = upload_stats_json
    updated_fields.append("upload_stats_json")
    if updated_fields:
        artifact.save(update_fields=updated_fields)

    return artifact, created


def ingest_inventory_pipeline_file(path: Path, run_job: RunJob | None = None) -> tuple[RunArtifact | None, bool]:
    """Create/update a RunArtifact row for an inventory pipeline summary JSON."""
    data = parse_inventory_pipeline_metadata(path)
    if data is None:
        return None, False

    company_key = str(data["company_key"]).strip()
    processed_at = _parse_dt(data.get("finished_at") or data.get("started_at"))
    source_hash = _sha256(path)
    child_reports = data.get("child_reports") if isinstance(data.get("child_reports"), dict) else {}
    summary_json = str(data.get("summary_json") or path)
    summary_csv = str(data.get("summary_csv") or "")
    final_status_counts = (
        data.get("final_status_counts") if isinstance(data.get("final_status_counts"), dict) else {}
    )
    unsupported = (
        data.get("unsupported_catalog_issues")
        if isinstance(data.get("unsupported_catalog_issues"), dict)
        else {}
    )

    upload_stats_json = {
        "report_type": "inventory_pipeline",
        "summary_json": summary_json,
        "summary_csv": summary_csv,
        "products_checked": _safe_int(data.get("products_checked")),
        "already_correct": _safe_int(data.get("already_correct")),
        "catalog_fixes_applied": _safe_int(data.get("catalog_fixes_applied")),
        "quantity_updates_applied": _safe_int(data.get("quantity_updates_applied")),
        "skipped_unsupported": _safe_int(data.get("skipped_unsupported")),
        "skipped_safely": _safe_int(data.get("skipped_safely")),
        "still_needs_review": _safe_int(data.get("still_needs_review")),
        "max_catalog_fixes": _safe_int(data.get("max_catalog_fixes")),
        "max_quantity_adjustments": _safe_int(data.get("max_quantity_adjustments")),
        "dry_run": bool(data.get("dry_run")),
        "stock_csv": str(data.get("stock_csv") or ""),
        "qbo_csv": str(data.get("qbo_csv") or ""),
        "final_status_counts": _safe_int_dict(final_status_counts),
        "unsupported_catalog_issues": _safe_int_dict(unsupported),
        "child_reports": {str(k): str(v) for k, v in child_reports.items()},
    }
    processed_files = [
        value
        for value in [summary_csv, *[str(v) for v in child_reports.values()]]
        if value
    ]

    artifact, created = RunArtifact.objects.get_or_create(
        company_key=company_key,
        target_date=None,
        processed_at=processed_at,
        source_hash=source_hash,
        defaults={
            "kind": RunArtifact.KIND_INVENTORY_AUDIT,
            "run_job": run_job,
            "source_path": str(path),
            "reliability_status": RunArtifact.RELIABILITY_HIGH,
            "rows_total": _safe_int(data.get("products_checked")),
            "rows_kept": _safe_int(data.get("already_correct")),
            "rows_non_target": _safe_int(data.get("still_needs_review")),
            "upload_stats_json": upload_stats_json,
            "raw_file": str(data.get("stock_csv") or ""),
            "processed_files_json": processed_files,
            "nearest_log_file": _nearest_log(processed_at, company_key),
        },
    )

    updated_fields: list[str] = []
    if artifact.kind != RunArtifact.KIND_INVENTORY_AUDIT:
        artifact.kind = RunArtifact.KIND_INVENTORY_AUDIT
        updated_fields.append("kind")
    if run_job and artifact.run_job_id is None:
        artifact.run_job = run_job
        updated_fields.append("run_job")
    if not artifact.source_path:
        artifact.source_path = str(path)
        updated_fields.append("source_path")
    artifact.rows_total = _safe_int(data.get("products_checked"))
    artifact.rows_kept = _safe_int(data.get("already_correct"))
    artifact.rows_non_target = _safe_int(data.get("still_needs_review"))
    artifact.upload_stats_json = upload_stats_json
    artifact.processed_files_json = processed_files
    updated_fields.extend(
        ["rows_total", "rows_kept", "rows_non_target", "upload_stats_json", "processed_files_json"]
    )
    if updated_fields:
        artifact.save(update_fields=updated_fields)

    return artifact, created


def _attach_inventory_artifacts_to_job(run_job: RunJob) -> int:
    attached = 0
    if not OPS_REPORTS_DIR.exists():
        return 0
    for path in sorted(OPS_REPORTS_DIR.rglob("inventory_audit_*.json")):
        data = parse_inventory_audit_metadata(path)
        if data is None:
            continue
        meta_job_id = str(data.get("run_job_id") or "").strip()
        if meta_job_id and meta_job_id != str(run_job.id):
            continue
        if not meta_job_id:
            if run_job.company_key and str(data.get("company_key") or "") != run_job.company_key:
                continue
            processed_at = _parse_dt(data.get("processed_at"))
            anchor = run_job.dispatched_at or run_job.started_at or run_job.created_at
            if processed_at is None or anchor is None:
                continue
            if abs((processed_at - anchor).total_seconds()) > 12 * 3600:
                continue
        artifact, _ = ingest_inventory_audit_file(path, run_job=run_job)
        if artifact is None:
            continue
        if artifact.run_job_id is None:
            artifact.run_job = run_job
            artifact.save(update_fields=["run_job"])
        if artifact.run_job_id == run_job.id:
            attached += 1
    return attached


def _attach_inventory_pipeline_artifacts_to_job(run_job: RunJob) -> int:
    attached = 0
    if not OPS_REPORTS_DIR.exists():
        return 0
    for path in sorted(OPS_REPORTS_DIR.rglob("inventory_pipeline_*.json")):
        data = parse_inventory_pipeline_metadata(path)
        if data is None:
            continue
        meta_job_id = str(data.get("run_job_id") or "").strip()
        if meta_job_id and meta_job_id != str(run_job.id):
            continue
        if not meta_job_id:
            if run_job.company_key and str(data.get("company_key") or "") != run_job.company_key:
                continue
            processed_at = _parse_dt(data.get("finished_at") or data.get("started_at"))
            anchor = run_job.dispatched_at or run_job.started_at or run_job.created_at
            if processed_at is None or anchor is None:
                continue
            if abs((processed_at - anchor).total_seconds()) > 12 * 3600:
                continue
        artifact, _ = ingest_inventory_pipeline_file(path, run_job=run_job)
        if artifact is None:
            continue
        if artifact.run_job_id is None:
            artifact.run_job = run_job
            artifact.save(update_fields=["run_job"])
        if artifact.run_job_id == run_job.id:
            attached += 1
    return attached


def ingest_inventory_audit_history(days: int = 60) -> int:
    cutoff = dj_timezone.now() - timedelta(days=days)
    created_count = 0
    if not OPS_REPORTS_DIR.exists():
        return 0
    for path in sorted(OPS_REPORTS_DIR.rglob("inventory_audit_*.json")):
        try:
            modified = datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc)
        except OSError:
            continue
        if modified < cutoff:
            continue
        _, created = ingest_inventory_audit_file(path)
        if created:
            created_count += 1
    return created_count
