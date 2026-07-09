from __future__ import annotations

import logging
from collections import defaultdict
from datetime import date, datetime, timedelta
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


def extract_amount_hybrid(artifact: RunArtifact, *, prefer_reconcile: bool = False) -> Decimal:
    stats = artifact.upload_stats_json if isinstance(artifact.upload_stats_json, dict) else {}
    reconcile_total = _to_decimal(artifact.reconcile_epos_total)
    if prefer_reconcile and reconcile_total is not None:
        return reconcile_total

    for key in AMOUNT_KEYS:
        parsed = _to_decimal(stats.get(key))
        if parsed is not None:
            return parsed

    if not prefer_reconcile and reconcile_total is not None:
        return reconcile_total

    anchor = _artifact_anchor(artifact)
    logger.warning(
        "No monetary amount found for artifact id=%s company_key=%s processed_at=%s",
        artifact.id,
        artifact.company_key,
        anchor.isoformat() if anchor else None,
    )
    return Decimal("0")


def extract_amount(artifact: RunArtifact) -> Decimal:
    return extract_amount_hybrid(artifact, prefer_reconcile=False)


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


def _window_total_for_companies(
    company_keys: list[str] | None,
    *,
    start_at: datetime,
    end_at: datetime,
    prefer_reconcile: bool = False,
) -> Decimal:
    queryset = RunArtifact.objects.filter(
        Q(processed_at__gte=start_at, processed_at__lt=end_at)
        | Q(processed_at__isnull=True, imported_at__gte=start_at, imported_at__lt=end_at)
    )
    if company_keys is not None:
        queryset = queryset.filter(company_key__in=company_keys)
    queryset = queryset.select_related("run_job").order_by("company_key", "-processed_at", "-imported_at")

    grouped: dict[tuple[str, Any], list[RunArtifact]] = defaultdict(list)
    for artifact in queryset:
        day_key = _artifact_day_key(artifact)
        if day_key is None:
            continue
        grouped[(artifact.company_key, day_key)].append(artifact)

    total = Decimal("0")
    for day_artifacts in grouped.values():
        selected = _choose_day_artifact(day_artifacts)
        if selected is None:
            continue
        total += extract_amount_hybrid(selected, prefer_reconcile=prefer_reconcile)
    return total


def _snapshot_total_for_companies(
    company_keys: list[str] | None,
    *,
    start_at: datetime,
    end_at: datetime,
    prefer_reconcile: bool = False,
) -> tuple[Decimal, int]:
    """
    Sum monetary totals for a time window using one artifact per company (latest successful).

    Windows are by processed_at (sync completion time), not target_date. For each company,
    we keep the single best artifact in the window (succeeded preferred, then latest by
    processed_at/imported_at). Uses one query per window with select_related to avoid N+1.
    """
    queryset = RunArtifact.objects.filter(
        Q(processed_at__gte=start_at, processed_at__lt=end_at)
        | Q(processed_at__isnull=True, imported_at__gte=start_at, imported_at__lt=end_at)
    )
    if company_keys is not None:
        queryset = queryset.filter(company_key__in=company_keys)
    queryset = queryset.select_related("run_job").order_by("company_key", "-processed_at", "-imported_at")

    grouped: dict[str, list[RunArtifact]] = defaultdict(list)
    for artifact in queryset:
        grouped[artifact.company_key].append(artifact)

    total = Decimal("0")
    selected_count = 0
    for company_artifacts in grouped.values():
        selected = _choose_day_artifact(company_artifacts)
        if selected is None:
            continue
        selected_count += 1
        total += extract_amount_hybrid(selected, prefer_reconcile=prefer_reconcile)
    return total, selected_count


def compute_sales_day_snapshot_for_companies(
    company_keys: list[str] | None,
    *,
    now: datetime | None = None,
    prefer_reconcile: bool = False,
    comparison_label: str = "vs yesterday",
    flat_symbol: str = "—",
    currency_symbol: str = "₦",
) -> dict[str, Any]:
    """
    Today vs previous target day snapshot for the overview "Sales Synced" KPI.

    Uses calendar-day windows by processed_at (when the run completed), not target_date.
    One artifact per company per window (latest successful). Totals are from reconcile_epos_total
    when prefer_reconcile=True. Compare to revenue chart which is by target_date over a range.
    """
    current = now or timezone.now()
    if timezone.is_naive(current):
        current = timezone.make_aware(current)
    local_now = timezone.localtime(current)
    today_start = local_now.replace(hour=0, minute=0, second=0, microsecond=0)
    yesterday_start = today_start - timedelta(days=1)

    this_total, this_samples = _snapshot_total_for_companies(
        company_keys,
        start_at=today_start,
        end_at=current,
        prefer_reconcile=prefer_reconcile,
    )
    prev_total, prev_samples = _snapshot_total_for_companies(
        company_keys,
        start_at=yesterday_start,
        end_at=today_start,
        prefer_reconcile=prefer_reconcile,
    )
    delta = this_total - prev_total

    is_new = False
    if prev_total > 0:
        pct_change = float((delta / prev_total) * Decimal("100"))
    elif this_total > 0:
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
        "flat": flat_symbol,
    }[trend_dir]
    if is_new:
        trend_text = f"↑ New {comparison_label}"
    else:
        trend_text = f"{trend_arrow} {abs(pct_change):.1f}% {comparison_label}"

    return {
        "total": this_total,
        "prev_total": prev_total,
        "pct_change": pct_change,
        "trend_dir": trend_dir,
        "is_new": is_new,
        "total_display": _format_currency(this_total, symbol=currency_symbol),
        "trend_text": trend_text,
        "trend_color": trend_color,
        "sample_count": this_samples,
        "prev_sample_count": prev_samples,
    }


def compute_sales_trend_for_companies(
    company_keys: list[str] | None,
    *,
    now: datetime | None = None,
    window_hours: int = 24,
    prefer_reconcile: bool = False,
    comparison_label: str = "vs previous period",
    flat_symbol: str = "→",
    currency_symbol: str = "₦",
) -> dict[str, Any]:
    current = now or timezone.now()
    if timezone.is_naive(current):
        current = timezone.make_aware(current)

    this_window_start = current - timedelta(hours=window_hours)
    prev_window_start = current - timedelta(hours=window_hours * 2)

    this_total = _window_total_for_companies(
        company_keys,
        start_at=this_window_start,
        end_at=current,
        prefer_reconcile=prefer_reconcile,
    )
    prev_total = _window_total_for_companies(
        company_keys,
        start_at=prev_window_start,
        end_at=this_window_start,
        prefer_reconcile=prefer_reconcile,
    )
    delta = this_total - prev_total

    is_new = False
    if prev_total > 0:
        pct_change = float((delta / prev_total) * Decimal("100"))
    elif this_total > 0:
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
        "flat": flat_symbol,
    }[trend_dir]
    if is_new:
        trend_text = f"↑ New {comparison_label}"
    else:
        trend_text = f"{trend_arrow} {abs(pct_change):.1f}% {comparison_label}"

    return {
        "total": this_total,
        "prev_total": prev_total,
        "pct_change": pct_change,
        "trend_dir": trend_dir,
        "is_new": is_new,
        "total_display": _format_currency(this_total, symbol=currency_symbol),
        "trend_text": trend_text,
        "trend_color": trend_color,
    }


def _latest_artifact_per_company_for_target_date(
    company_keys: list[str] | None,
    target_date: date,
    *,
    prefer_reconcile: bool = True,
) -> tuple[Decimal, int]:
    """
    Sum reconcile_epos_total (or fallback) for the latest artifact per company
    for the given target_date. Returns (total, company_count).
    """
    qs = RunArtifact.objects.filter(target_date=target_date).select_related("run_job")
    if company_keys is not None:
        qs = qs.filter(company_key__in=company_keys)
    qs = qs.order_by("company_key", "-processed_at", "-imported_at")

    grouped: dict[str, list[RunArtifact]] = defaultdict(list)
    for artifact in qs:
        grouped[artifact.company_key].append(artifact)

    total = Decimal("0")
    count = 0
    for company_artifacts in grouped.values():
        chosen = _choose_day_artifact(company_artifacts)
        if chosen is None:
            continue
        total += extract_amount_hybrid(chosen, prefer_reconcile=prefer_reconcile)
        count += 1
    return total, count


def compute_sales_snapshot_by_target_date(
    company_keys: list[str] | None,
    target_date: date,
    prev_target_date: date,
    *,
    prefer_reconcile: bool = True,
    comparison_label: str = "vs previous day",
    flat_symbol: str = "—",
    currency_symbol: str = "₦",
) -> dict[str, Any]:
    """
    Sales Synced KPI by data target date: compare latest run totals for target_date
    vs prev_target_date (e.g. yesterday vs day before yesterday), not by run completion calendar day.
    """
    this_total, this_samples = _latest_artifact_per_company_for_target_date(
        company_keys, target_date, prefer_reconcile=prefer_reconcile
    )
    prev_total, prev_samples = _latest_artifact_per_company_for_target_date(
        company_keys, prev_target_date, prefer_reconcile=prefer_reconcile
    )
    delta = this_total - prev_total

    is_new = False
    if prev_total > 0:
        pct_change = float((delta / prev_total) * Decimal("100"))
    elif this_total > 0:
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

    trend_color = {"up": "emerald", "down": "red", "flat": "slate"}[trend_dir]
    trend_arrow = {"up": "↑", "down": "↓", "flat": flat_symbol}[trend_dir]
    if is_new:
        trend_text = f"↑ New {comparison_label}"
    else:
        trend_text = f"{trend_arrow} {abs(pct_change):.1f}% {comparison_label}"

    return {
        "total": this_total,
        "prev_total": prev_total,
        "pct_change": pct_change,
        "trend_dir": trend_dir,
        "is_new": is_new,
        "total_display": _format_currency(this_total, symbol=currency_symbol),
        "trend_text": trend_text,
        "trend_color": trend_color,
        "sample_count": this_samples,
        "prev_sample_count": prev_samples,
    }


def compute_avg_runtime_by_target_date(
    company_keys: list[str] | None,
    target_date: date,
    prev_target_date: date,
    *,
    prev_date_display: str,
) -> dict[str, Any]:
    """
    Avg Runtime KPI by data target date: average duration of runs that produced
    artifacts for target_date vs prev_target_date (same comparison logic as Sales Synced).

    Includes all successful runs linked to artifacts for each target date.
    """
    artifact_qs = RunArtifact.objects.filter(
        target_date=target_date,
        run_job_id__isnull=False,
    )
    if company_keys is not None:
        artifact_qs = artifact_qs.filter(company_key__in=company_keys)
    job_ids_this = list(artifact_qs.values_list("run_job_id", flat=True).distinct())

    prev_qs = RunArtifact.objects.filter(
        target_date=prev_target_date,
        run_job_id__isnull=False,
    )
    if company_keys is not None:
        prev_qs = prev_qs.filter(company_key__in=company_keys)
    job_ids_prev = list(prev_qs.values_list("run_job_id", flat=True).distinct())

    jobs_this = list(
        RunJob.objects.filter(
            id__in=job_ids_this,
            status=RunJob.STATUS_SUCCEEDED,
            started_at__isnull=False,
            finished_at__isnull=False,
        )
    )
    jobs_prev = list(
        RunJob.objects.filter(
            id__in=job_ids_prev,
            status=RunJob.STATUS_SUCCEEDED,
            started_at__isnull=False,
            finished_at__isnull=False,
        )
    )

    duration_this = [
        int((j.finished_at - j.started_at).total_seconds())
        for j in jobs_this
        if j.finished_at and j.started_at and j.finished_at >= j.started_at
    ]
    duration_prev = [
        int((j.finished_at - j.started_at).total_seconds())
        for j in jobs_prev
        if j.finished_at and j.started_at and j.finished_at >= j.started_at
    ]

    avg_this = int(sum(duration_this) / len(duration_this)) if duration_this else 0
    avg_prev = int(sum(duration_prev) / len(duration_prev)) if duration_prev else 0

    if avg_prev > 0:
        runtime_delta = avg_this - avg_prev
        runtime_pct = (runtime_delta / avg_prev) * 100
        pct_abs = abs(runtime_pct)
        if pct_abs < 1.0:
            trend_dir = "flat"
            trend_color = "slate"
            trend_text = f"— {pct_abs:.1f}% change vs {prev_date_display}"
        elif runtime_delta > 0:
            trend_dir = "up"
            trend_color = "red"
            trend_text = f"↑ {pct_abs:.1f}% slower vs {prev_date_display}"
        else:
            trend_dir = "down"
            trend_color = "emerald"
            trend_text = f"↓ {pct_abs:.1f}% faster vs {prev_date_display}"
    elif avg_this > 0:
        trend_dir = "up"
        trend_color = "slate"
        trend_text = f"↑ New runtime vs {prev_date_display}"
    else:
        # No successful runs linked to artifacts for either target date.
        trend_dir = "flat"
        trend_color = "slate"
        if len(duration_this) == 0 and len(duration_prev) == 0:
            trend_text = f"— No successful runs vs {prev_date_display}"
        elif len(duration_this) == 0:
            trend_text = f"— No successful runs (vs {prev_date_display})"
        else:
            trend_text = f"— 0.0% change vs {prev_date_display}"

    return {
        "avg_seconds": avg_this,
        "prev_avg_seconds": avg_prev,
        "samples": len(duration_this),
        "prev_samples": len(duration_prev),
        "trend_dir": trend_dir,
        "trend_color": trend_color,
        "trend_text": trend_text,
    }


def compute_run_success_by_target_date(
    company_keys: list[str] | None,
    target_date: date,
) -> dict[str, Any]:
    artifact_qs = RunArtifact.objects.filter(
        target_date=target_date,
        run_job_id__isnull=False,
    )
    if company_keys is not None:
        artifact_qs = artifact_qs.filter(company_key__in=company_keys)

    job_ids = list(artifact_qs.values_list("run_job_id", flat=True).distinct())
    if not job_ids:
        return {"successful": 0, "completed": 0, "pct": 0.0, "ratio": "0/0"}

    completed_qs = RunJob.objects.filter(
        id__in=job_ids,
        status__in=[
            RunJob.STATUS_SUCCEEDED,
            RunJob.STATUS_FAILED,
            RunJob.STATUS_CANCELLED,
        ],
    )
    successful = completed_qs.filter(status=RunJob.STATUS_SUCCEEDED).count()
    completed = completed_qs.count()
    pct = round((successful / completed) * 100, 1) if completed > 0 else 0.0
    ratio = f"{successful}/{completed}" if completed > 0 else "0/0"
    return {"successful": successful, "completed": completed, "pct": pct, "ratio": ratio}


def compute_sales_trend(company_key: str, *, now: datetime | None = None) -> dict[str, Any]:
    trend = compute_sales_trend_for_companies(
        [company_key],
        now=now,
        window_hours=24 * 7,
        prefer_reconcile=False,
        comparison_label="vs last week",
        flat_symbol="→",
    )
    return {
        "sales_7d_total": trend["total"],
        "sales_7d_prev_total": trend["prev_total"],
        "sales_7d_pct_change": trend["pct_change"],
        "sales_7d_trend_dir": trend["trend_dir"],
        "sales_7d_is_new": trend["is_new"],
        "sales_7d_total_display": trend["total_display"],
        "sales_7d_trend_text": trend["trend_text"],
        "sales_7d_trend_color": trend["trend_color"],
    }
