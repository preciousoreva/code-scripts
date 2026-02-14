from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from django.utils import timezone as dj_timezone

from oiat_portal.paths import OPS_LOGS_DIR, OPS_UPLOADED_DIR

from ..models import RunArtifact, RunJob


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
