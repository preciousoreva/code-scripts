from __future__ import annotations

import logging
from collections import defaultdict
from datetime import datetime, timedelta
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from typing import Any

from django.db.models import Q
from django.utils import timezone

from apps.epos_qbo.models import RunArtifact, RunJob

logger = logging.getLogger(__name__)

AMOUNT_KEYS = (
    "total_amount",
    "total_sales",
    "amount_total",
    "uploaded_total",
    "grand_total",
)


def _to_decimal(value: Any) -> Decimal | None:
    if value is None:
        return None
    if isinstance(value, Decimal):
        return value
    if isinstance(value, (int, float)):
        return Decimal(str(value))
    if isinstance(value, str):
        cleaned = value.strip().replace(",", "")
        if not cleaned:
            return None
        try:
            return Decimal(cleaned)
        except InvalidOperation:
            return None
    return None


def _artifact_anchor(artifact: RunArtifact) -> datetime | None:
    return artifact.processed_at or artifact.imported_at


def _artifact_sort_key(artifact: RunArtifact) -> tuple[datetime, datetime]:
    floor = timezone.make_aware(datetime(1970, 1, 1))
    anchor = _artifact_anchor(artifact) or floor
    imported = artifact.imported_at or floor
    return anchor, imported


def _artifact_day_key(artifact: RunArtifact):
    if artifact.target_date:
        return artifact.target_date
    anchor = _artifact_anchor(artifact)
    if not anchor:
        return None
    return anchor.date()


def _format_currency(value: Decimal, symbol: str = "₦") -> str:
    rounded = value.quantize(Decimal("1"), rounding=ROUND_HALF_UP)
    return f"{symbol}{int(rounded):,}"


def extract_amount(artifact: RunArtifact) -> Decimal:
    stats = artifact.upload_stats_json if isinstance(artifact.upload_stats_json, dict) else {}
    for key in AMOUNT_KEYS:
        parsed = _to_decimal(stats.get(key))
        if parsed is not None:
            return parsed

    reconcile_total = _to_decimal(artifact.reconcile_epos_total)
    if reconcile_total is not None:
        return reconcile_total

    anchor = _artifact_anchor(artifact)
    logger.warning(
        "No monetary amount found for artifact id=%s company_key=%s processed_at=%s",
        artifact.id,
        artifact.company_key,
        anchor.isoformat() if anchor else None,
    )
    return Decimal("0")


def _choose_day_artifact(artifacts: list[RunArtifact]) -> RunArtifact | None:
    by_hash: dict[str, RunArtifact] = {}
    no_hash: list[RunArtifact] = []
    for artifact in artifacts:
        if artifact.source_hash:
            current = by_hash.get(artifact.source_hash)
            if current is None or _artifact_sort_key(artifact) > _artifact_sort_key(current):
                by_hash[artifact.source_hash] = artifact
        else:
            no_hash.append(artifact)
    candidates = list(by_hash.values()) + no_hash
    if not candidates:
        return None

    succeeded = [
        artifact
        for artifact in candidates
        if artifact.run_job_id and artifact.run_job and artifact.run_job.status == RunJob.STATUS_SUCCEEDED
    ]
    if succeeded:
        return max(succeeded, key=_artifact_sort_key)

    unlinked = [artifact for artifact in candidates if artifact.run_job_id is None]
    if unlinked:
        return max(unlinked, key=_artifact_sort_key)

    return None


def _window_total(company_key: str, *, start_at: datetime, end_at: datetime) -> Decimal:
    queryset = (
        RunArtifact.objects.filter(company_key=company_key)
        .filter(
            Q(processed_at__gte=start_at, processed_at__lt=end_at)
            | Q(processed_at__isnull=True, imported_at__gte=start_at, imported_at__lt=end_at)
        )
        .select_related("run_job")
        .order_by("-processed_at", "-imported_at")
    )

    grouped: dict[Any, list[RunArtifact]] = defaultdict(list)
    for artifact in queryset:
        day_key = _artifact_day_key(artifact)
        if day_key is None:
            continue
        grouped[day_key].append(artifact)

    total = Decimal("0")
    for day_artifacts in grouped.values():
        selected = _choose_day_artifact(day_artifacts)
        if selected is None:
            continue
        total += extract_amount(selected)
    return total


def compute_sales_trend(company_key: str, *, now: datetime | None = None) -> dict[str, Any]:
    current = now or timezone.now()
    if timezone.is_naive(current):
        current = timezone.make_aware(current)

    this_week_start = current - timedelta(days=7)
    prev_week_start = current - timedelta(days=14)

    this_week_total = _window_total(company_key, start_at=this_week_start, end_at=current)
    prev_week_total = _window_total(company_key, start_at=prev_week_start, end_at=this_week_start)
    delta = this_week_total - prev_week_total

    is_new = False
    if prev_week_total > 0:
        pct_change = float((delta / prev_week_total) * Decimal("100"))
    elif this_week_total > 0:
        pct_change = 100.0
        is_new = True
    else:
        pct_change = 0.0

    if abs(pct_change) < 1.0:
        trend_dir = "flat"
    elif pct_change > 0:
        trend_dir = "up"
    else:
        trend_dir = "down"

    trend_color = {
        "up": "emerald",
        "down": "red",
        "flat": "slate",
    }[trend_dir]
    trend_arrow = {
        "up": "↑",
        "down": "↓",
        "flat": "→",
    }[trend_dir]
    if is_new:
        trend_text = "↑ New vs last week"
    else:
        trend_text = f"{trend_arrow} {abs(pct_change):.1f}% vs last week"

    return {
        "sales_7d_total": this_week_total,
        "sales_7d_prev_total": prev_week_total,
        "sales_7d_pct_change": pct_change,
        "sales_7d_trend_dir": trend_dir,
        "sales_7d_is_new": is_new,
        "sales_7d_total_display": _format_currency(this_week_total),
        "sales_7d_trend_text": trend_text,
        "sales_7d_trend_color": trend_color,
    }
