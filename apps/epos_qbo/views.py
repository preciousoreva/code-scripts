from __future__ import annotations

import json
import os
from datetime import datetime, timedelta
from decimal import Decimal
from math import ceil

from django.conf import settings
from django.contrib import messages
from django.contrib.auth.decorators import login_required, permission_required
from django.db.models import Q
from django.http import Http404, JsonResponse
from django.shortcuts import get_object_or_404, redirect, render
from django.urls import reverse
from django.utils import timezone
from django.views.decorators.http import require_GET, require_POST

from code_scripts.token_manager import load_tokens

from .forms import CompanyAdvancedForm, CompanyBasicForm, RunTriggerForm
from .models import CompanyConfigRecord, RunArtifact, RunJob
from .services.config_sync import (
    apply_advanced_payload,
    build_basic_payload,
    import_all_company_json,
    sync_record_to_json,
    validate_company_config,
)
from .services.job_runner import dispatch_next_queued_job, read_log_chunk
from .dashboard_timezone import get_dashboard_date_bounds, get_dashboard_timezone_display, get_dashboard_timezone_name
from .business_date import (
    get_business_day_cutoff,
    get_business_timezone_display,
    get_target_trading_date,
)
from .services.metrics import (
    compute_avg_runtime_by_target_date,
    compute_run_success_by_target_date,
    compute_sales_snapshot_by_target_date,
    compute_sales_trend,
)

ACCESS_REFRESH_MARGIN_SECONDS = 60
REVENUE_PERIOD_DAYS = {
    "yesterday": 1,
    "7d": 7,
    "30d": 30,
    "90d": 90,
}
REVENUE_PERIOD_OPTIONS = [
    ("yesterday", "Yesterday"),
    ("7d", "Last 7D"),
    ("30d", "Last 30D"),
    ("90d", "Last 90D"),
]
DEFAULT_REAUTH_GUIDANCE = (
    "QBO re-authentication required. Run OAuth flow and store tokens using "
    "code_scripts/store_tokens.py."
)
EXIT_CODE_REFERENCE = [
    {"code": "0", "message": "Success."},
    {"code": "1", "message": "Pipeline failed during execution. Check Live Log for root cause."},
    {"code": "2", "message": "Run blocked by active lock or invalid CLI usage."},
    {"code": "3", "message": "Dashboard failed to start the subprocess."},
    {"code": "-1", "message": "Run reconciler marked stale process as failed (PID not alive)."},
    {"code": "126", "message": "Subprocess command invoked but not executable."},
    {"code": "127", "message": "Subprocess command/dependency not found."},
]


def _int_setting(name: str, default: int, *, minimum: int = 0) -> int:
    raw = getattr(settings, name, default)
    try:
        value = int(raw)
    except (TypeError, ValueError):
        return default
    if value < minimum:
        return default
    return value


def _dashboard_default_parallel() -> int:
    return _int_setting("OIAT_DASHBOARD_DEFAULT_PARALLEL", 2, minimum=1)


def _dashboard_default_stagger_seconds() -> int:
    return _int_setting("OIAT_DASHBOARD_DEFAULT_STAGGER_SECONDS", 2, minimum=0)


def _dashboard_stale_hours_warning() -> int:
    return _int_setting("OIAT_DASHBOARD_STALE_HOURS_WARNING", 48, minimum=1)


def _dashboard_refresh_expiring_days() -> int:
    return _int_setting("OIAT_DASHBOARD_REFRESH_EXPIRING_DAYS", 7, minimum=1)


def _reauth_guidance() -> str:
    text = str(getattr(settings, "OIAT_DASHBOARD_REAUTH_GUIDANCE", DEFAULT_REAUTH_GUIDANCE)).strip()
    return text or DEFAULT_REAUTH_GUIDANCE


def _ensure_company_records() -> None:
    if CompanyConfigRecord.objects.exists():
        return
    import_all_company_json()


def _nav_context() -> dict:
    ui_debug_beacon_enabled = os.getenv("OIAT_UI_DEBUG_BEACON", "0").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }
    return {
        "company_count": CompanyConfigRecord.objects.filter(is_active=True).count(),
        "ui_debug_beacon_enabled": ui_debug_beacon_enabled,
    }


def _breadcrumb_context(breadcrumbs, *, back_url=None, back_label=None, show_overview_actions=False):
    """Add breadcrumbs and optional back link for topbar."""
    out = {"breadcrumbs": breadcrumbs, "show_overview_actions": show_overview_actions}
    if back_url and back_label:
        out["back_url"] = back_url
        out["back_label"] = back_label
    return out


def _format_duration(seconds: int | None) -> str:
    if not seconds or seconds <= 0:
        return "0 minutes"
    if seconds < 3600:
        minutes = max(1, seconds // 60)
        return f"{minutes} minute{'s' if minutes != 1 else ''}"
    if seconds < 86400:
        hours = max(1, seconds // 3600)
        return f"{hours} hour{'s' if hours != 1 else ''}"
    days = max(1, ceil(seconds / 86400))
    return f"{days} day{'s' if days != 1 else ''}"


def _format_runtime_compact(seconds: int | None) -> str:
    if seconds is None or seconds <= 0:
        return "0s"
    if seconds < 60:
        return f"{seconds}s"
    if seconds < 3600:
        minutes = max(1, round(seconds / 60))
        return f"{minutes}m"
    if seconds < 86400:
        hours = seconds / 3600
        return f"{hours:.1f}h".replace(".0h", "h")
    days = seconds / 86400
    return f"{days:.1f}d".replace(".0d", "d")


def _format_day_count(seconds: int) -> int:
    return max(1, ceil(seconds / 86400))


def _normalize_revenue_period(value: str | None) -> str:
    selected = (value or "").strip().lower()
    return selected if selected in REVENUE_PERIOD_DAYS else "7d"


def _quick_sync_default_target_date(*, now: datetime | None = None) -> str:
    return get_target_trading_date(now=now).isoformat()


def _exit_code_info(exit_code: int | None) -> dict | None:
    if exit_code is None:
        return None
    mapping = {
        0: {
            "label": "Success",
            "description": "Process completed normally.",
        },
        1: {
            "label": "Pipeline failure",
            "description": "The pipeline reported an execution error. Check Live Log for the underlying phase error.",
        },
        2: {
            "label": "Blocked or invalid invocation",
            "description": "Usually means a run lock blocked execution or CLI arguments were invalid.",
        },
        3: {
            "label": "Subprocess start failure",
            "description": "Dashboard could not start the runner subprocess.",
        },
        -1: {
            "label": "Reconciled stale run",
            "description": "Reaper marked a stuck running job as failed because the PID was no longer alive.",
        },
        126: {
            "label": "Not executable",
            "description": "Command exists but is not executable in current environment.",
        },
        127: {
            "label": "Command missing",
            "description": "Command or required runtime dependency could not be found.",
        },
    }
    if exit_code in mapping:
        return mapping[exit_code]
    if exit_code < 0:
        return {
            "label": "Terminated by signal",
            "description": f"Process ended from OS signal {-exit_code}.",
        }
    return {
        "label": "Unhandled non-zero exit",
        "description": "Process returned a non-zero code. Check Live Log and failure reason for details.",
    }


def _company_token_health(company: CompanyConfigRecord) -> dict:
    guidance = _reauth_guidance()
    cfg = company.config_json or {}
    realm_id = (cfg.get("qbo") or {}).get("realm_id")
    if not realm_id:
        return {
            "valid": False,
            "severity": "critical",
            "status_color": "red",
            "token_unknown": True,
            "connection_state": "missing_tokens",
            "access_state": "unknown",
            "display_label": "QBO re-authentication required",
            "display_subtext": guidance,
            "status_message": "QBO re-authentication required",
            "days_remaining": None,
            "expiring_soon": False,
            "expires_at": None,
            "token_days": None,
            "reauth_guidance": guidance,
            "issues": [
                {
                    "severity": "red",
                    "icon": "solar:shield-warning-linear",
                    "message": "QBO re-authentication required",
                    "action": "refresh_token",
                }
            ],
        }

    tokens = load_tokens(company.company_key, realm_id)
    if not tokens:
        return {
            "valid": False,
            "severity": "critical",
            "status_color": "red",
            "token_unknown": True,
            "connection_state": "missing_tokens",
            "access_state": "unknown",
            "display_label": "QBO re-authentication required",
            "display_subtext": guidance,
            "status_message": "QBO re-authentication required",
            "days_remaining": None,
            "expiring_soon": False,
            "expires_at": None,
            "token_days": None,
            "reauth_guidance": guidance,
            "issues": [
                {
                    "severity": "red",
                    "icon": "solar:shield-warning-linear",
                    "message": "QBO re-authentication required",
                    "action": "refresh_token",
                }
            ],
        }

    access_expires_at = tokens.get("expires_at")
    refresh_expires_at = tokens.get("refresh_expires_at")
    refresh_token = tokens.get("refresh_token")
    now_ts = int(timezone.now().timestamp())
    access_seconds_left = int(access_expires_at - now_ts) if access_expires_at else None
    refresh_seconds_left = int(refresh_expires_at - now_ts) if refresh_expires_at else None

    if not refresh_token:
        return {
            "valid": False,
            "severity": "critical",
            "status_color": "red",
            "token_unknown": False,
            "connection_state": "missing_refresh_token",
            "access_state": "unknown",
            "display_label": "QBO re-authentication required",
            "display_subtext": guidance,
            "status_message": "QBO re-authentication required",
            "days_remaining": None,
            "expiring_soon": False,
            "expires_at": access_expires_at,
            "token_days": None,
            "reauth_guidance": guidance,
            "issues": [
                {
                    "severity": "red",
                    "icon": "solar:shield-warning-linear",
                    "message": "QBO re-authentication required",
                    "action": "refresh_token",
                }
            ],
        }

    if access_seconds_left is None:
        access_state = "unknown"
        access_subtext = "Access token expiry unknown (auto-refreshes during sync)"
    elif access_seconds_left <= ACCESS_REFRESH_MARGIN_SECONDS:
        access_state = "expired"
        access_subtext = "Access token expired (will refresh on next sync)"
    else:
        access_state = "active"
        access_subtext = (
            f"Access token expires in {_format_duration(access_seconds_left)} "
            "(auto-refreshes during sync)"
        )

    if refresh_expires_at is not None and refresh_seconds_left is not None and refresh_seconds_left <= 0:
        return {
            "valid": False,
            "severity": "critical",
            "status_color": "red",
            "token_unknown": False,
            "connection_state": "refresh_expired",
            "access_state": access_state,
            "display_label": "QBO re-authentication required",
            "display_subtext": guidance,
            "status_message": "QBO re-authentication required",
            "days_remaining": 0,
            "expiring_soon": False,
            "expires_at": access_expires_at,
            "token_days": 0,
            "reauth_guidance": guidance,
            "issues": [
                {
                    "severity": "red",
                    "icon": "solar:shield-warning-linear",
                    "message": "QBO re-authentication required",
                    "action": "refresh_token",
                }
            ],
        }

    if (
        refresh_expires_at is not None
        and refresh_seconds_left is not None
        and refresh_seconds_left <= _dashboard_refresh_expiring_days() * 86400
    ):
        days_left = _format_day_count(refresh_seconds_left)
        message = f"Refresh token expires in {days_left} day{'s' if days_left != 1 else ''}"
        return {
            "valid": True,
            "severity": "warning",
            "status_color": "amber",
            "token_unknown": False,
            "connection_state": "refresh_expiring",
            "access_state": access_state,
            "display_label": "Connected",
            "display_subtext": message,
            "status_message": message,
            "days_remaining": days_left,
            "expiring_soon": True,
            "expires_at": access_expires_at,
            "token_days": days_left,
            "reauth_guidance": guidance,
            "issues": [
                {
                    "severity": "amber",
                    "icon": "solar:key-minimalistic-linear",
                    "message": message,
                    "action": "refresh_token",
                }
            ],
        }

    return {
        "valid": True,
        "severity": "healthy",
        "status_color": "emerald",
        "token_unknown": False,
        "connection_state": "connected",
        "access_state": access_state,
        "display_label": "Connected",
        "display_subtext": access_subtext,
        "status_message": "Connected",
        "days_remaining": _format_day_count(refresh_seconds_left) if refresh_seconds_left else None,
        "expiring_soon": False,
        "expires_at": access_expires_at,
        "token_days": _format_day_count(refresh_seconds_left) if refresh_seconds_left else None,
        "reauth_guidance": guidance,
        "issues": [],
    }


def _overview_live_log_message(job: RunJob, company_display: str) -> str:
    run_label = job.display_label
    if job.status == RunJob.STATUS_SUCCEEDED:
        return f"{company_display}: Run {run_label} succeeded"
    if job.status == RunJob.STATUS_FAILED:
        if job.failure_reason:
            return f"{company_display}: Run {run_label} failed ({job.failure_reason})"
        return f"{company_display}: Run {run_label} failed"
    if job.status == RunJob.STATUS_RUNNING:
        return f"{company_display}: Run {run_label} is running"
    if job.status == RunJob.STATUS_CANCELLED:
        return f"{company_display}: Run {run_label} was cancelled"
    return f"{company_display}: Run {run_label} queued"


def _status_for_company(
    company: CompanyConfigRecord,
    latest_artifact: RunArtifact | None,
    latest_job: RunJob | None,
    token_info: dict | None = None,
) -> tuple[str, str]:
    cfg = company.config_json or {}
    epos = cfg.get("epos") or {}
    token_info = token_info or _company_token_health(company)

    if not epos.get("username_env_key") or not epos.get("password_env_key"):
        return "warning", "Missing EPOS env key names in company config."
    if token_info["severity"] == "critical":
        return "critical", token_info["status_message"]

    if latest_job and latest_job.status == RunJob.STATUS_FAILED:
        return "critical", latest_job.failure_reason or "Latest run failed."
    if latest_job and latest_job.status in (RunJob.STATUS_QUEUED, RunJob.STATUS_RUNNING):
        return "warning", "Run currently queued/running."

    if token_info["severity"] == "warning":
        return "warning", token_info["status_message"]

    if latest_artifact:
        failed_uploads = int((latest_artifact.upload_stats_json or {}).get("failed", 0))
        if failed_uploads > 0:
            return "critical", f"{failed_uploads} upload(s) failed in latest run."
        if latest_artifact.reconcile_difference and abs(latest_artifact.reconcile_difference) > 1.0:
            return "warning", "Reconciliation mismatch above threshold."
    else:
        return "warning", "No artifact metadata ingested yet."

    return "healthy", "Last run succeeded."


def _classify_system_health(healthy_count: int, warning_count: int, critical_count: int) -> dict:
    if critical_count > 0:
        return {
            "label": "Degraded",
            "severity": "critical",
            "color": "red",
            "icon": "solar:close-circle-linear",
        }
    if warning_count > 0:
        return {
            "label": "Warning",
            "severity": "warning",
            "color": "amber",
            "icon": "solar:danger-triangle-linear",
        }
    return {
        "label": "All Operational",
        "severity": "healthy",
        "color": "emerald",
        "icon": "solar:shield-check-linear",
    }


def _format_relative_age(delta_seconds: float) -> str:
    seconds = max(0, int(delta_seconds))
    if seconds < 60:
        return "just now"
    if seconds < 3600:
        minutes = max(1, seconds // 60)
        return f"{minutes}m"
    if seconds < 86400:
        hours = max(1, seconds // 3600)
        return f"{hours}h"
    days = max(1, ceil(seconds / 86400))
    return f"{days}d"


def resolve_overview_target_date(company_keys: list[str] | None = None) -> dict:
    artifacts = RunArtifact.objects.filter(
        target_date__isnull=False,
        run_job_id__isnull=False,
        run_job__status=RunJob.STATUS_SUCCEEDED,
    )
    if company_keys is not None:
        artifacts = artifacts.filter(company_key__in=company_keys)
    latest = artifacts.select_related("run_job").order_by("-processed_at", "-imported_at", "-id").first()
    if latest is None or latest.target_date is None:
        return {
            "target_date": None,
            "prev_target_date": None,
            "last_successful_at": None,
            "has_data": False,
        }

    return {
        "target_date": latest.target_date,
        "prev_target_date": latest.target_date - timedelta(days=1),
        "last_successful_at": latest.processed_at or latest.imported_at,
        "has_data": True,
    }


def _overview_context(revenue_period: str = "7d") -> dict:
    now = timezone.now()
    target_date = None
    prev_target_date = None
    target_date_display = ""
    prev_target_date_display = ""
    business_timezone_display = get_business_timezone_display(now=now)
    cutoff_hour, cutoff_minute = get_business_day_cutoff()
    business_cutoff_display = f"{cutoff_hour:02d}:{cutoff_minute:02d}"
    since_7d = now - timedelta(days=7)
    revenue_period = _normalize_revenue_period(revenue_period)

    companies = list(CompanyConfigRecord.objects.filter(is_active=True).order_by("company_key"))
    company_keys = [company.company_key for company in companies]
    date_resolution = resolve_overview_target_date(company_keys)
    has_overview_target_date = bool(date_resolution["has_data"])
    target_date = date_resolution["target_date"]
    prev_target_date = date_resolution["prev_target_date"]
    last_successful_sync_at = date_resolution["last_successful_at"]
    if target_date is not None:
        target_date_display = target_date.strftime("%b %d, %Y")
    if prev_target_date is not None:
        prev_target_date_display = prev_target_date.strftime("%b %d")

    latest_artifacts: dict[str, RunArtifact] = {}
    latest_jobs: dict[str, RunJob] = {}

    for artifact in RunArtifact.objects.filter(company_key__in=company_keys).order_by(
        "company_key", "-processed_at", "-imported_at"
    ):
        if artifact.company_key not in latest_artifacts:
            latest_artifacts[artifact.company_key] = artifact
    for job in RunJob.objects.filter(company_key__in=company_keys).order_by("company_key", "-created_at"):
        if job.company_key and job.company_key not in latest_jobs:
            latest_jobs[job.company_key] = job

    companies_context = []
    healthy_count = warning_count = critical_count = 0

    for company in companies:
        latest_artifact = latest_artifacts.get(company.company_key)
        latest_job = latest_jobs.get(company.company_key)
        token_info = _company_token_health(company)
        status, summary = _status_for_company(company, latest_artifact, latest_job, token_info)
        latest_job_time = None
        if latest_job:
            latest_job_time = latest_job.finished_at or latest_job.started_at or latest_job.created_at
        latest_artifact_time = None
        if latest_artifact:
            latest_artifact_time = latest_artifact.processed_at or latest_artifact.imported_at
        last_run_time = latest_job_time or latest_artifact_time

        if status == "healthy":
            healthy_count += 1
        elif status == "warning":
            warning_count += 1
        else:
            critical_count += 1

        companies_context.append(
            {
                "name": company.display_name,
                "company_key": company.company_key,
                "last_run": last_run_time,
                "status": status,
                "token_info": token_info,
                "records_synced": latest_artifact.rows_kept if latest_artifact else 0,
                "summary": summary,
            }
        )

    system_health = _classify_system_health(healthy_count, warning_count, critical_count)
    system_health_breakdown = f"{healthy_count} healthy • {warning_count} warning • {critical_count} critical"

    if has_overview_target_date and target_date is not None and prev_target_date is not None:
        sales_trend = compute_sales_snapshot_by_target_date(
            company_keys,
            target_date,
            prev_target_date,
            prefer_reconcile=True,
            comparison_label=f"vs {prev_target_date_display}",
            flat_symbol="—",
        )
        if sales_trend.get("sample_count", 0) > 0 and sales_trend["total"] <= 0:
            sales_trend["trend_color"] = "slate"
            sales_trend["trend_text"] = "No monetary totals found"
        else:
            pct = abs(float(sales_trend.get("pct_change", 0.0)))
            if sales_trend.get("trend_dir") == "up":
                sales_trend["trend_text"] = f"↑ {pct:.1f}% increase vs {prev_target_date_display}"
            elif sales_trend.get("trend_dir") == "down":
                sales_trend["trend_text"] = f"↓ {pct:.1f}% decrease vs {prev_target_date_display}"
            else:
                sales_trend["trend_text"] = f"— {pct:.1f}% change vs {prev_target_date_display}"

        run_success = compute_run_success_by_target_date(company_keys, target_date)
        successful_runs_24h = run_success["successful"]
        total_completed_runs_24h = run_success["completed"]
        run_success_pct_24h = run_success["pct"]
        run_success_ratio_24h = run_success["ratio"]
        runtime_trend = compute_avg_runtime_by_target_date(
            company_keys,
            target_date,
            prev_target_date,
            prev_date_display=prev_target_date_display,
        )
    else:
        sales_trend = {
            "total": Decimal("0"),
            "prev_total": Decimal("0"),
            "pct_change": 0.0,
            "trend_dir": "flat",
            "is_new": False,
            "total_display": "₦0",
            "trend_text": "No successful run data yet.",
            "trend_color": "slate",
            "sample_count": 0,
            "prev_sample_count": 0,
        }
        successful_runs_24h = 0
        total_completed_runs_24h = 0
        run_success_pct_24h = 0.0
        run_success_ratio_24h = "0/0"
        runtime_trend = {
            "avg_seconds": 0,
            "prev_avg_seconds": 0,
            "samples": 0,
            "prev_samples": 0,
            "trend_dir": "flat",
            "trend_color": "slate",
            "trend_text": "No successful run data yet.",
        }

    avg_runtime_today_seconds = runtime_trend["avg_seconds"]
    avg_runtime_today_display = _format_runtime_compact(avg_runtime_today_seconds)
    avg_runtime_yesterday_seconds = runtime_trend["prev_avg_seconds"]
    avg_runtime_today_trend_dir = runtime_trend["trend_dir"]
    avg_runtime_today_trend_color = runtime_trend["trend_color"]
    avg_runtime_today_trend_text = runtime_trend["trend_text"]
    duration_seconds = [avg_runtime_today_seconds] * max(1, runtime_trend["samples"])

    recent_jobs = RunJob.objects.order_by("-created_at")[:10]
    live_log = []
    company_map = {c.company_key: c.display_name for c in CompanyConfigRecord.objects.all()}
    for job in recent_jobs:
        company_display = company_map.get(job.company_key, job.company_key or "All Companies")
        if job.status == RunJob.STATUS_SUCCEEDED:
            level = "success"
        elif job.status == RunJob.STATUS_FAILED:
            level = "error"
        elif job.status == RunJob.STATUS_RUNNING:
            level = "info"
        elif job.status == RunJob.STATUS_CANCELLED:
            level = "warning"
        else:
            level = "warning"
        message = _overview_live_log_message(job, company_display)
        live_log.append({"timestamp": job.created_at, "level": level, "message": message})

    revenue_days = REVENUE_PERIOD_DAYS[revenue_period]
    if has_overview_target_date and target_date is not None:
        revenue_end_date = target_date
        revenue_start_date = revenue_end_date - timedelta(days=revenue_days - 1)
        revenue_dates = [revenue_start_date + timedelta(days=i) for i in range(revenue_days)]
    else:
        revenue_end_date = None
        revenue_start_date = None
        revenue_dates = []
    revenue_labels = [d.strftime("%b %d") for d in revenue_dates]
    revenue_index_by_date = {date: idx for idx, date in enumerate(revenue_dates)}
    revenue_series_map = {company.company_key: [0.0] * len(revenue_dates) for company in companies}
    revenue_totals_by_company = {company.company_key: 0.0 for company in companies}
    latest_reconciled_artifacts: dict[tuple[str, object], RunArtifact] = {}

    if company_keys and revenue_days > 0 and revenue_start_date is not None and revenue_end_date is not None:
        reconciled_qs = RunArtifact.objects.filter(
            company_key__in=company_keys,
            target_date__isnull=False,
            target_date__gte=revenue_start_date,
            target_date__lte=revenue_end_date,
            reconcile_status="MATCH",
            reconcile_epos_total__isnull=False,
        ).order_by("company_key", "target_date", "-processed_at", "-imported_at")
        for artifact in reconciled_qs:
            key = (artifact.company_key, artifact.target_date)
            if key not in latest_reconciled_artifacts:
                latest_reconciled_artifacts[key] = artifact

    matched_dates = set()
    for (company_key, artifact_target_date), artifact in latest_reconciled_artifacts.items():
        if artifact_target_date not in revenue_index_by_date:
            continue
        value = float(artifact.reconcile_epos_total or 0.0)
        idx = revenue_index_by_date[artifact_target_date]
        revenue_series_map[company_key][idx] += value
        revenue_totals_by_company[company_key] = revenue_totals_by_company.get(company_key, 0.0) + value
        matched_dates.add(artifact_target_date)

    revenue_series = [
        {
            "company_key": company.company_key,
            "name": company.display_name,
            "data": [round(v, 2) for v in revenue_series_map.get(company.company_key, [])],
        }
        for company in companies
    ]
    revenue_company_totals = sorted(
        [
            {
                "company_key": company.company_key,
                "name": company.display_name,
                "total": round(revenue_totals_by_company.get(company.company_key, 0.0), 2),
            }
            for company in companies
            if revenue_totals_by_company.get(company.company_key, 0.0) > 0
        ],
        key=lambda item: item["total"],
        reverse=True,
    )
    has_reconciled_revenue_data = bool(latest_reconciled_artifacts)
    revenue_start_date_display = revenue_start_date.strftime("%b %d") if revenue_start_date else ""
    revenue_end_date_display = revenue_end_date.strftime("%b %d") if revenue_end_date else ""
    revenue_chart_payload = {
        "labels": revenue_labels,
        "series": revenue_series if has_reconciled_revenue_data else [],
    }

    # Get active run IDs for polling
    active_runs = RunJob.objects.filter(
        status__in=[RunJob.STATUS_QUEUED, RunJob.STATUS_RUNNING]
    ).values_list('id', flat=True)[:10]  # Limit to 10 most recent
    
    if has_overview_target_date and target_date_display and last_successful_sync_at:
        age_text = _format_relative_age((now - last_successful_sync_at).total_seconds())
        metric_basis_line = (
            f"Metrics are based on Target Date: {target_date_display} • "
            f"Last successful sync {age_text} ago"
        )
    elif has_overview_target_date and target_date_display:
        metric_basis_line = f"Metrics are based on Target Date: {target_date_display}"
    else:
        metric_basis_line = "No successful run data yet."

    return {
        "target_date_display": target_date_display,
        "target_date_iso": target_date.isoformat() if target_date else "",
        "target_trading_date_display": target_date_display,
        "target_trading_date_iso": target_date.isoformat() if target_date else "",
        "business_timezone_display": business_timezone_display,
        "business_cutoff_display": business_cutoff_display,
        "metric_basis_line": metric_basis_line,
        "overview_has_data": has_overview_target_date,
        "last_successful_sync_at": last_successful_sync_at,
        "kpis": {
            "healthy_count": healthy_count,
            "warning_count": warning_count,
            "critical_count": critical_count,
            "system_health_label": system_health["label"],
            "system_health_severity": system_health["severity"],
            "system_health_color": system_health["color"],
            "system_health_icon": system_health["icon"],
            "system_health_breakdown": system_health_breakdown,
            "sales_24h_total": sales_trend["total"],
            "sales_prev_24h_total": sales_trend["prev_total"],
            "sales_24h_pct_change": sales_trend["pct_change"],
            "sales_24h_trend_dir": sales_trend["trend_dir"],
            "sales_24h_is_new": sales_trend["is_new"],
            "sales_24h_total_display": sales_trend["total_display"],
            "sales_24h_trend_text": sales_trend["trend_text"],
            "sales_24h_trend_color": sales_trend["trend_color"],
            "successful_runs_24h": successful_runs_24h,
            "total_completed_runs_24h": total_completed_runs_24h,
            "run_success_pct_24h": run_success_pct_24h,
            "run_success_ratio_24h": run_success_ratio_24h,
            "avg_runtime_today_seconds": avg_runtime_today_seconds,
            "avg_runtime_today_display": avg_runtime_today_display,
            "avg_runtime_today_samples": len(duration_seconds),
            "avg_runtime_yesterday_seconds": avg_runtime_yesterday_seconds,
            "avg_runtime_today_trend_dir": avg_runtime_today_trend_dir,
            "avg_runtime_today_trend_color": avg_runtime_today_trend_color,
            "avg_runtime_today_trend_text": avg_runtime_today_trend_text,
            # Backward-compatible keys for existing templates/tests.
            "avg_runtime_24h_seconds": avg_runtime_today_seconds,
            "avg_runtime_24h_display": avg_runtime_today_display,
            "avg_runtime_24h_samples": len(duration_seconds),
            "queued_or_running": RunJob.objects.filter(
                status__in=[RunJob.STATUS_QUEUED, RunJob.STATUS_RUNNING]
            ).count(),
            "runs_7d": RunJob.objects.filter(created_at__gte=since_7d).count(),
        },
        "companies": companies_context,
        "live_log": live_log,
        "company_count": len(companies_context),
        "revenue_period": revenue_period,
        "revenue_period_options": [
            {"value": value, "label": label, "selected": value == revenue_period}
            for value, label in REVENUE_PERIOD_OPTIONS
        ],
        "revenue_labels": revenue_labels,
        "revenue_series": revenue_series,
        "revenue_start_date_display": revenue_start_date_display,
        "revenue_end_date_display": revenue_end_date_display,
        "revenue_company_totals": revenue_company_totals,
        "revenue_matched_days": len(matched_dates),
        "has_reconciled_revenue_data": has_reconciled_revenue_data,
        "revenue_chart_payload": revenue_chart_payload,
        "active_run_ids": [str(id) for id in active_runs],
        "active_run_ids_json": json.dumps([str(id) for id in active_runs]),
    }


@login_required
def overview(request):
    _ensure_company_records()
    revenue_period = _normalize_revenue_period(request.GET.get("revenue_period"))
    context = _overview_context(revenue_period)
    context["quick_sync_target_date"] = _quick_sync_default_target_date()
    context["quick_sync_timezone"] = context.get("business_timezone_display", get_business_timezone_display())
    context["dashboard_timezone_display"] = get_dashboard_timezone_display()
    context.update(_nav_context())
    context.update(
        _breadcrumb_context(
            [
                {"label": "Dashboard", "url": reverse("epos_qbo:overview")},
                {"label": "Overview", "url": None},
            ],
            show_overview_actions=True,
        )
    )
    return render(request, "dashboard/overview.html", context)


@login_required
@require_GET
def overview_panels(request):
    _ensure_company_records()
    revenue_period = _normalize_revenue_period(request.GET.get("revenue_period"))
    context = _overview_context(revenue_period)
    return render(request, "components/overview_refresh.html", context)


@login_required
@require_GET
def settings_page(request):
    """Settings page: dashboard tuning (read-only) and appearance (theme)."""
    context = {
        "dashboard_timezone_display": get_dashboard_timezone_display(),
        "dashboard_timezone_name": get_dashboard_timezone_name(),
        "default_parallel": _dashboard_default_parallel(),
        "default_stagger_seconds": _dashboard_default_stagger_seconds(),
        "stale_hours_warning": _dashboard_stale_hours_warning(),
        "refresh_expiring_days": _dashboard_refresh_expiring_days(),
        "reauth_guidance": _reauth_guidance(),
    }
    context.update(_nav_context())
    context.update(
        _breadcrumb_context(
            [
                {"label": "Dashboard", "url": reverse("epos_qbo:overview")},
                {"label": "Settings", "url": None},
            ],
        )
    )
    return render(request, "epos_qbo/settings.html", context)


@login_required
def runs_list(request):
    _ensure_company_records()
    default_parallel = _dashboard_default_parallel()
    default_stagger_seconds = _dashboard_default_stagger_seconds()
    jobs = RunJob.objects.order_by("-created_at")[:100]
    form = RunTriggerForm(initial={"scope": RunJob.SCOPE_ALL, "date_mode": "yesterday"})
    companies = CompanyConfigRecord.objects.filter(is_active=True).order_by("display_name")
    
    # Get active run IDs for polling
    active_runs = RunJob.objects.filter(
        status__in=[RunJob.STATUS_QUEUED, RunJob.STATUS_RUNNING]
    ).values_list('id', flat=True)[:10]  # Limit to 10 most recent
    
    active_run_ids_list = [str(id) for id in active_runs]
    context = {
        "jobs": jobs, 
        "form": form, 
        "companies": companies,
        "default_parallel": default_parallel,
        "default_stagger_seconds": default_stagger_seconds,
        "active_run_ids": active_run_ids_list,
        "active_run_ids_json": json.dumps(active_run_ids_list),
    }
    context.update(_nav_context())
    context.update(
        _breadcrumb_context(
            [
                {"label": "Dashboard", "url": reverse("epos_qbo:overview")},
                {"label": "Runs", "url": None},
            ],
            back_url=reverse("epos_qbo:overview"),
            back_label="Overview",
        )
    )
    return render(request, "epos_qbo/runs.html", context)


@login_required
def logs_list(request):
    """Logs page showing structured run events with filters and statistics."""
    _ensure_company_records()
    
    # Get filter parameters
    company_key = request.GET.get("company", "")
    status_filter = request.GET.get("status", "")
    date_from = request.GET.get("date_from", "")
    date_to = request.GET.get("date_to", "")
    
    # Build query
    jobs_query = RunJob.objects.all()
    
    if company_key:
        jobs_query = jobs_query.filter(company_key=company_key)
    if status_filter:
        jobs_query = jobs_query.filter(status=status_filter)
    if date_from:
        try:
            date_from_obj = datetime.strptime(date_from, "%Y-%m-%d").date()
            jobs_query = jobs_query.filter(created_at__date__gte=date_from_obj)
        except ValueError:
            pass
    if date_to:
        try:
            date_to_obj = datetime.strptime(date_to, "%Y-%m-%d").date()
            jobs_query = jobs_query.filter(created_at__date__lte=date_to_obj)
        except ValueError:
            pass
    
    # Get jobs (limit to 200 most recent)
    jobs = jobs_query.order_by("-created_at")[:200]
    
    # Build structured log events
    log_events = []
    company_map = {c.company_key: c.display_name for c in CompanyConfigRecord.objects.filter(is_active=True)}
    
    for job in jobs:
        if job.status == RunJob.STATUS_SUCCEEDED:
            level = "success"
            message = f"{job.scope.replace('_', ' ')} run"
            if job.company_key:
                message += f" for {company_map.get(job.company_key, job.company_key)}"
            message += " succeeded"
        elif job.status == RunJob.STATUS_FAILED:
            level = "error"
            message = f"{job.scope.replace('_', ' ')} run"
            if job.company_key:
                message += f" for {company_map.get(job.company_key, job.company_key)}"
            message += " failed"
            if job.failure_reason:
                message += f": {job.failure_reason[:100]}"
        elif job.status == RunJob.STATUS_RUNNING:
            level = "info"
            message = f"{job.scope.replace('_', ' ')} run"
            if job.company_key:
                message += f" for {company_map.get(job.company_key, job.company_key)}"
            message += " is running"
        elif job.status == RunJob.STATUS_CANCELLED:
            level = "warning"
            message = f"{job.scope.replace('_', ' ')} run"
            if job.company_key:
                message += f" for {company_map.get(job.company_key, job.company_key)}"
            message += " was cancelled"
        else:  # queued
            level = "warning"
            message = f"{job.scope.replace('_', ' ')} run"
            if job.company_key:
                message += f" for {company_map.get(job.company_key, job.company_key)}"
            message += " queued"
        
        # Calculate duration if finished
        duration = None
        if job.started_at and job.finished_at:
            duration_seconds = int((job.finished_at - job.started_at).total_seconds())
            if duration_seconds < 60:
                duration = f"{duration_seconds}s"
            elif duration_seconds < 3600:
                duration = f"{duration_seconds // 60}m {duration_seconds % 60}s"
            else:
                hours = duration_seconds // 3600
                minutes = (duration_seconds % 3600) // 60
                duration = f"{hours}h {minutes}m"
        
        log_events.append({
            "job": job,
            "timestamp": job.created_at,
            "level": level,
            "message": message,
            "company_name": company_map.get(job.company_key, job.company_key or "all"),
            "duration": duration,
        })
    
    # Calculate statistics
    now = timezone.now()
    since_7d = now - timedelta(days=7)
    since_30d = now - timedelta(days=30)
    
    all_jobs_7d = RunJob.objects.filter(created_at__gte=since_7d)
    all_jobs_30d = RunJob.objects.filter(created_at__gte=since_30d)
    
    stats = {
        "total_runs_7d": all_jobs_7d.count(),
        "total_runs_30d": all_jobs_30d.count(),
        "success_rate_7d": round(
            (all_jobs_7d.filter(status=RunJob.STATUS_SUCCEEDED).count() * 100 / all_jobs_7d.count())
            if all_jobs_7d.count() > 0 else 100.0,
            1
        ),
        "error_count_7d": all_jobs_7d.filter(status=RunJob.STATUS_FAILED).count(),
        "active_runs": RunJob.objects.filter(
            status__in=[RunJob.STATUS_QUEUED, RunJob.STATUS_RUNNING]
        ).count(),
    }
    
    companies = CompanyConfigRecord.objects.filter(is_active=True).order_by("display_name")
    
    # Get active run IDs for polling
    active_runs = RunJob.objects.filter(
        status__in=[RunJob.STATUS_QUEUED, RunJob.STATUS_RUNNING]
    ).values_list('id', flat=True)[:10]  # Limit to 10 most recent
    
    context = {
        "log_events": log_events,
        "stats": stats,
        "companies": companies,
        "filters": {
            "company": company_key,
            "status": status_filter,
            "date_from": date_from,
            "date_to": date_to,
        },
        "active_run_ids": [str(id) for id in active_runs],
        "active_run_ids_json": json.dumps([str(id) for id in active_runs]),
    }
    context.update(_nav_context())
    context.update(
        _breadcrumb_context(
            [
                {"label": "Dashboard", "url": reverse("epos_qbo:overview")},
                {"label": "Logs", "url": None},
            ],
            back_url=reverse("epos_qbo:overview"),
            back_label="Overview",
        )
    )
    return render(request, "epos_qbo/logs.html", context)


@login_required
def run_detail(request, job_id):
    job = get_object_or_404(RunJob, id=job_id)
    artifacts = job.artifacts.order_by("-processed_at", "-imported_at")
    active_run_ids_list = [str(job.id)] if job.status in [RunJob.STATUS_QUEUED, RunJob.STATUS_RUNNING] else []
    context = {
        "job": job,
        "artifacts": artifacts,
        "active_run_ids": active_run_ids_list,
        "active_run_ids_json": json.dumps(active_run_ids_list),
        "exit_code_info": _exit_code_info(job.exit_code),
        "exit_code_reference": EXIT_CODE_REFERENCE,
    }
    context.update(_nav_context())
    context.update(
        _breadcrumb_context(
            [
                {"label": "Dashboard", "url": reverse("epos_qbo:overview")},
                {"label": "Runs", "url": reverse("epos_qbo:runs")},
                {"label": f"Run {job.display_label}", "url": None},
            ],
            back_url=reverse("epos_qbo:runs"),
            back_label="Runs",
        )
    )
    return render(request, "epos_qbo/run_detail.html", context)


@login_required
@require_GET
def run_logs(request, job_id):
    job = get_object_or_404(RunJob, id=job_id)
    try:
        offset = int(request.GET.get("offset", "0"))
    except ValueError as exc:
        raise Http404("Invalid offset") from exc
    if offset < 0:
        raise Http404("Invalid offset")
    chunk, next_offset = read_log_chunk(job, offset)
    return JsonResponse({"chunk": chunk, "next_offset": next_offset, "status": job.status})


@login_required
@require_GET
def run_active_ids(request):
    active_runs = (
        RunJob.objects.filter(status__in=[RunJob.STATUS_QUEUED, RunJob.STATUS_RUNNING])
        .order_by("-created_at")
        .values_list("id", flat=True)[:25]
    )
    return JsonResponse({"job_ids": [str(job_id) for job_id in active_runs]})


@login_required
@require_GET
def run_status_check(request):
    """API endpoint to check status of multiple runs."""
    job_ids_str = request.GET.get("job_ids", "")
    if not job_ids_str:
        return JsonResponse({}, status=400)
    
    try:
        from uuid import UUID
        job_ids = [UUID(id.strip()) for id in job_ids_str.split(",") if id.strip()]
    except ValueError:
        return JsonResponse({"error": "Invalid job IDs"}, status=400)
    
    if not job_ids:
        return JsonResponse({})
    
    jobs = RunJob.objects.filter(id__in=job_ids)
    result = {}
    for job in jobs:
        result[str(job.id)] = {
            "status": job.status,
            "finished_at": job.finished_at.isoformat() if job.finished_at else None,
            "failure_reason": job.failure_reason or None,
        }
    return JsonResponse(result)


@login_required
@permission_required("epos_qbo.can_trigger_runs", raise_exception=True)
@require_POST
def trigger_run(request):
    """Create a queued run from the trigger form. target_date/from_date/to_date come from the form (Quick Sync submits date_mode=target_date and target_date)."""
    form = RunTriggerForm(request.POST)
    if not form.is_valid():
        messages.error(request, f"Invalid trigger payload: {form.errors.as_text()}")
        return redirect("epos_qbo:runs")

    cleaned = form.cleaned_data
    if cleaned["scope"] == RunJob.SCOPE_SINGLE and not CompanyConfigRecord.objects.filter(
        company_key=cleaned.get("company_key") or ""
    ).exists():
        messages.error(request, "Unknown company key for single-company run.")
        return redirect("epos_qbo:runs")

    job = RunJob.objects.create(
        scope=cleaned["scope"],
        company_key=cleaned.get("company_key") or None,
        target_date=cleaned.get("target_date"),
        from_date=cleaned.get("from_date"),
        to_date=cleaned.get("to_date"),
        skip_download=bool(cleaned.get("skip_download")),
        parallel=int(cleaned.get("parallel") or _dashboard_default_parallel()),
        stagger_seconds=int(cleaned.get("stagger_seconds") or _dashboard_default_stagger_seconds()),
        continue_on_failure=bool(cleaned.get("continue_on_failure")),
        requested_by=request.user,
        status=RunJob.STATUS_QUEUED,
    )
    dispatch_next_queued_job()

    job.refresh_from_db()
    if job.status == RunJob.STATUS_RUNNING:
        messages.success(request, f"Run started: {job.display_label}")
        return redirect("epos_qbo:run-detail", job_id=job.id)

    messages.info(request, f"Run queued: {job.display_label}. It will start automatically.")
    return redirect("epos_qbo:runs")


@login_required
@permission_required("epos_qbo.can_edit_companies", raise_exception=True)
def company_new(request):
    if request.method == "POST":
        form = CompanyBasicForm(request.POST)
        if form.is_valid():
            payload = build_basic_payload(form)
            result = validate_company_config(payload)
            if not result.valid:
                messages.error(request, "; ".join(result.errors))
            else:
                record = CompanyConfigRecord.objects.create(
                    company_key=form.cleaned_data["company_key"],
                    display_name=form.cleaned_data["display_name"],
                    config_json=payload,
                    created_by=request.user,
                    updated_by=request.user,
                )
                sync_record_to_json(record)
                messages.success(request, "Company created. Continue with advanced settings.")
                return redirect("epos_qbo:company-advanced", company_key=record.company_key)
    else:
        form = CompanyBasicForm()
    context = {"form": form}
    context.update(_nav_context())
    context.update(
        _breadcrumb_context(
            [
                {"label": "Dashboard", "url": reverse("epos_qbo:overview")},
                {"label": "New Company", "url": None},
            ],
            back_url=reverse("epos_qbo:overview"),
            back_label="Overview",
        )
    )
    return render(request, "epos_qbo/company_form_basic.html", context)


@login_required
@permission_required("epos_qbo.can_edit_companies", raise_exception=True)
def company_advanced(request, company_key):
    record = get_object_or_404(CompanyConfigRecord, company_key=company_key)

    if request.method == "POST":
        form = CompanyAdvancedForm(request.POST)
        if form.is_valid():
            payload = apply_advanced_payload(record.config_json, form)
            result = validate_company_config(payload)
            if not result.valid:
                messages.error(request, "; ".join(result.errors))
            else:
                record.config_json = payload
                record.display_name = payload.get("display_name", record.display_name)
                record.config_version += 1
                record.updated_by = request.user
                record.save(update_fields=["config_json", "display_name", "config_version", "updated_by", "updated_at"])
                sync_record_to_json(record)
                messages.success(request, "Advanced settings saved.")
                return redirect("epos_qbo:overview")
    else:
        cfg = record.config_json or {}
        form = CompanyAdvancedForm(
            initial={
                "trading_day_enabled": (cfg.get("trading_day") or {}).get("enabled", False),
                "trading_day_start_hour": (cfg.get("trading_day") or {}).get("start_hour", 5),
                "trading_day_start_minute": (cfg.get("trading_day") or {}).get("start_minute", 0),
                "inventory_enabled": (cfg.get("inventory") or {}).get("enable_inventory_items", False),
                "allow_negative_inventory": (cfg.get("inventory") or {}).get("allow_negative_inventory", False),
                "inventory_start_date": (cfg.get("inventory") or {}).get("inventory_start_date", "today"),
                "default_qty_on_hand": (cfg.get("inventory") or {}).get("default_qty_on_hand", 0),
                "tax_rate": (cfg.get("qbo") or {}).get("tax_rate"),
                "tax_code_id": (cfg.get("qbo") or {}).get("tax_code_id", ""),
                "tax_code_name": (cfg.get("qbo") or {}).get("tax_code_name", ""),
                "date_format": (cfg.get("transform") or {}).get("date_format", "%Y-%m-%d"),
                "receipt_prefix": (cfg.get("transform") or {}).get("receipt_prefix", "SR"),
                "receipt_number_format": (cfg.get("transform") or {}).get("receipt_number_format", "date_tender_sequence"),
                "group_by": ",".join((cfg.get("transform") or {}).get("group_by", ["date", "tender"])),
            }
        )

    context = {"form": form, "record": record}
    context.update(_nav_context())
    context.update(
        _breadcrumb_context(
            [
                {"label": "Dashboard", "url": reverse("epos_qbo:overview")},
                {"label": record.display_name, "url": None},
            ],
            back_url=reverse("epos_qbo:overview"),
            back_label="Overview",
        )
    )
    return render(request, "epos_qbo/company_form_advanced.html", context)


@login_required
@permission_required("epos_qbo.can_edit_companies", raise_exception=True)
@require_POST
def sync_company_json(request, company_key):
    record = get_object_or_404(CompanyConfigRecord, company_key=company_key)
    sync_record_to_json(record)
    messages.success(request, f"Synced {company_key} to JSON")
    return redirect("epos_qbo:company-advanced", company_key=company_key)


# --- Companies list / detail helpers (no FK from RunJob to Company; use company_key) ---


def _get_token_info_for_display(company: CompanyConfigRecord) -> dict:
    """Return canonical token health info for templates."""
    return _company_token_health(company)


def _parse_config_for_display(config_json: dict | None) -> dict:
    """Parse company config JSON into display values for templates."""
    config = config_json or {}
    qbo = config.get("qbo") or {}
    transform = config.get("transform") or {}
    inventory = config.get("inventory") or {}
    return {
        "inventory_enabled": inventory.get("enable_inventory_items", False),
        "tax_rate": qbo.get("tax_rate"),
        "deposit_account": qbo.get("deposit_account", "Undeposited Funds"),
        "group_by": ", ".join(transform.get("group_by", ["date", "tender"])),
        "date_format": transform.get("date_format", "%Y-%m-%d"),
        "realm_id": qbo.get("realm_id", "Not set"),
    }


def _run_activity_time(job: RunJob | None):
    if not job:
        return None
    return job.finished_at or job.started_at or job.created_at


def _artifact_activity_time(artifact: RunArtifact | None):
    if not artifact:
        return None
    return artifact.processed_at or artifact.imported_at


def _artifact_order_key(artifact: RunArtifact):
    floor = timezone.make_aware(datetime(1970, 1, 1))
    anchor = artifact.processed_at or artifact.imported_at or floor
    imported = artifact.imported_at or floor
    return anchor, imported


def _artifact_day_bucket(artifact: RunArtifact):
    if artifact.target_date:
        return artifact.target_date
    anchor = artifact.processed_at or artifact.imported_at
    return anchor.date() if anchor else None


def _artifact_uploaded_count(artifact: RunArtifact) -> int:
    stats = artifact.upload_stats_json if isinstance(artifact.upload_stats_json, dict) else {}
    # Prefer explicit uploaded receipt count; support legacy metadata that used "created".
    for key in ("uploaded", "created"):
        raw = stats.get(key)
        try:
            count = int(raw)
        except (TypeError, ValueError):
            continue
        return max(0, count)
    return 0


def _select_day_artifact_for_uploaded_count(artifacts: list[RunArtifact]) -> RunArtifact | None:
    by_hash: dict[str, RunArtifact] = {}
    no_hash: list[RunArtifact] = []
    for artifact in artifacts:
        if artifact.source_hash:
            current = by_hash.get(artifact.source_hash)
            if current is None or _artifact_order_key(artifact) > _artifact_order_key(current):
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
        return max(succeeded, key=_artifact_order_key)

    unlinked = [artifact for artifact in candidates if artifact.run_job_id is None]
    if unlinked:
        return max(unlinked, key=_artifact_order_key)

    return None


def _format_last_run_time(last_activity_at) -> str:
    if not last_activity_at:
        return "Never run"
    diff = timezone.now() - last_activity_at
    if diff < timedelta(minutes=1):
        return "Just now"
    if diff < timedelta(hours=1):
        minutes = int(diff.total_seconds() / 60)
        return f"{minutes} minute{'s' if minutes != 1 else ''} ago"
    if diff < timedelta(days=1):
        hours = int(diff.total_seconds() / 3600)
        return f"{hours} hour{'s' if hours != 1 else ''} ago"
    if diff < timedelta(days=7):
        days = diff.days
        return f"{days} day{'s' if days != 1 else ''} ago"
    return last_activity_at.strftime("%b %d, %Y")


def _company_runs_queryset(company_key: str):
    run_ids_from_artifacts = RunArtifact.objects.filter(
        company_key=company_key,
        run_job__isnull=False,
    ).values_list("run_job_id", flat=True)
    return RunJob.objects.filter(
        Q(company_key=company_key)
        | (
            Q(id__in=run_ids_from_artifacts)
            & (
                Q(scope=RunJob.SCOPE_ALL)
                | Q(company_key=company_key)
                | Q(company_key__isnull=True)
            )
        )
    ).distinct()


def _status_display_from_canonical(
    status_str: str,
    latest_run: RunJob | None,
    latest_artifact: RunArtifact | None,
) -> dict:
    """Map canonical status from _status_for_company to display dict (level, label, color, icon)."""
    if status_str == "critical":
        return {"level": "critical", "label": "Critical", "color": "red", "icon": "solar:close-circle-linear"}
    if status_str == "healthy":
        return {"level": "healthy", "label": "Healthy", "color": "emerald", "icon": "solar:check-circle-linear"}
    # warning: show "Running" when job is running, "Never Run" when no run, else "Warning"
    if latest_run and latest_run.status == RunJob.STATUS_RUNNING:
        return {"level": "running", "label": "Running", "color": "blue", "icon": "solar:refresh-linear"}
    if not latest_run and not latest_artifact:
        return {"level": "warning", "label": "Never Run", "color": "amber", "icon": "solar:danger-triangle-linear"}
    return {"level": "warning", "label": "Warning", "color": "amber", "icon": "solar:danger-triangle-linear"}


def _get_company_issues_for_list(
    company: CompanyConfigRecord,
    latest_run: RunJob | None,
    latest_artifact: RunArtifact | None,
    token_info: dict,
) -> list:
    """Return list of issue dicts: severity, icon, message, action."""
    issues = list(token_info.get("issues", []))
    if latest_run and latest_run.status == RunJob.STATUS_FAILED:
        reason = (latest_run.failure_reason or "Unknown error")[:100]
        if len((latest_run.failure_reason or "")) > 100:
            reason += "..."
        issues.append({
            "severity": "red",
            "icon": "solar:close-circle-linear",
            "message": f"Last run failed: {reason}",
            "action": "view_run",
        })
    if not latest_run and not latest_artifact:
        issues.append({
            "severity": "amber",
            "icon": "solar:question-circle-linear",
            "message": "Company has never been synced",
            "action": "trigger_sync",
        })
    if latest_run and latest_run.started_at:
        hours_since = (timezone.now() - latest_run.started_at).total_seconds() / 3600
        if hours_since > _dashboard_stale_hours_warning():
            issues.append({
                "severity": "amber",
                "icon": "solar:clock-circle-linear",
                "message": f"No sync in {int(hours_since)} hours",
                "action": "trigger_sync",
            })
    return issues


def _enrich_company_data(
    company: CompanyConfigRecord,
    latest_run: RunJob | None,
) -> dict:
    """Build enriched company dict for list/detail templates. Uses same status logic as Overview."""
    latest_artifact = (
        RunArtifact.objects.filter(company_key=company.company_key)
        .order_by("-processed_at", "-imported_at")
        .first()
    )
    # Receipts uploaded (Today): calendar day in dashboard TZ, same definition as overview KPIs
    bounds = get_dashboard_date_bounds()
    today_start_utc = bounds["today_start_utc"]
    now_utc = bounds["now_utc"]
    artifacts_today = (
        RunArtifact.objects.filter(company_key=company.company_key)
        .filter(
            Q(processed_at__gte=today_start_utc, processed_at__lt=now_utc)
            | Q(
                processed_at__isnull=True,
                imported_at__gte=today_start_utc,
                imported_at__lt=now_utc,
            )
        )
        .select_related("run_job")
        .order_by("-processed_at", "-imported_at")
    )
    # Use canonical status from Overview so counts and labels match
    token_info = _get_token_info_for_display(company)
    status_str, _ = _status_for_company(company, latest_artifact, latest_run, token_info)
    status = _status_display_from_canonical(status_str, latest_run, latest_artifact)
    issues = _get_company_issues_for_list(company, latest_run, latest_artifact, token_info)
    config_display = _parse_config_for_display(company.config_json)
    artifacts_by_day: dict[object, list[RunArtifact]] = {}
    for artifact in artifacts_today:
        bucket = _artifact_day_bucket(artifact)
        if bucket is None:
            continue
        artifacts_by_day.setdefault(bucket, []).append(artifact)
    records_24h = 0
    for day_artifacts in artifacts_by_day.values():
        selected = _select_day_artifact_for_uploaded_count(day_artifacts)
        if selected is None:
            continue
        records_24h += _artifact_uploaded_count(selected)
    latest_run_time = _run_activity_time(latest_run)
    latest_artifact_time = _artifact_activity_time(latest_artifact)
    if latest_run_time and latest_artifact_time:
        last_activity_at = max(latest_run_time, latest_artifact_time)
    else:
        last_activity_at = latest_run_time or latest_artifact_time

    # Latest successful sync: receipt count and target date (aligned with overview)
    latest_successful_artifact = (
        RunArtifact.objects.filter(company_key=company.company_key)
        .filter(run_job__status=RunJob.STATUS_SUCCEEDED)
        .select_related("run_job")
        .order_by("-processed_at", "-imported_at", "-id")
        .first()
    )
    if latest_successful_artifact:
        records_latest_sync = _artifact_uploaded_count(latest_successful_artifact)
        latest_sync_target_date = latest_successful_artifact.target_date
    else:
        records_latest_sync = 0
        latest_sync_target_date = None

    return {
        "company": company,
        "status": status,
        "latest_run": latest_run,
        "latest_artifact": latest_artifact,
        "token_info": token_info,
        "issues": issues,
        "config_display": config_display,
        "records_24h": records_24h,
        "last_activity_at": last_activity_at,
        "last_run_display": _format_last_run_time(last_activity_at),
        "records_latest_sync": records_latest_sync,
        "latest_sync_target_date": latest_sync_target_date,
    }


def _sort_companies_data(companies_data: list, sort_by: str) -> list:
    if sort_by == "name":
        return sorted(companies_data, key=lambda c: (c["company"].display_name or "").lower())
    if sort_by == "last_run":
        return sorted(
            companies_data,
            key=lambda c: c["last_activity_at"] if c["last_activity_at"] else timezone.datetime.min.replace(tzinfo=timezone.utc),
            reverse=True,
        )
    if sort_by == "status":
        order = {"critical": 0, "warning": 1, "running": 2, "healthy": 3}
        return sorted(companies_data, key=lambda c: order.get(c["status"]["level"], 99))
    return companies_data


def _calculate_companies_summary(companies_data: list) -> dict:
    total = len(companies_data)
    healthy = sum(1 for c in companies_data if c["status"]["level"] == "healthy")
    # Count "running" as warning so totals match Overview (Overview has no separate running count)
    warning = sum(1 for c in companies_data if c["status"]["level"] in ("warning", "running"))
    critical = sum(1 for c in companies_data if c["status"]["level"] == "critical")
    return {"total": total, "healthy": healthy, "warning": warning, "critical": critical}


@login_required
def companies_list(request):
    """Companies management page with search, filter, sort; HTMX partial for list."""
    _ensure_company_records()
    search = request.GET.get("search", "").strip()
    filter_status = request.GET.get("filter", "all")
    sort_by = request.GET.get("sort", "name")
    view_mode = request.GET.get("view", "cards")

    companies = CompanyConfigRecord.objects.filter(is_active=True)
    if search:
        companies = companies.filter(
            Q(display_name__icontains=search) | Q(company_key__icontains=search)
        )

    companies_data = []
    for company in companies:
        latest_run = _company_runs_queryset(company.company_key).order_by("-started_at", "-created_at").first()
        company_data = _enrich_company_data(company, latest_run)
        companies_data.append(company_data)

    if filter_status != "all":
        companies_data = [c for c in companies_data if c["status"]["level"] == filter_status]
    companies_data = _sort_companies_data(companies_data, sort_by)
    summary = _calculate_companies_summary(companies_data)

    context = {
        "companies_data": companies_data,
        "search": search,
        "filter_status": filter_status,
        "sort_by": sort_by,
        "view_mode": view_mode,
        "summary": summary,
    }
    context.update(_nav_context())
    context.update(
        _breadcrumb_context(
            [
                {"label": "Dashboard", "url": reverse("epos_qbo:overview")},
                {"label": "Companies", "url": None},
            ],
            back_url=reverse("epos_qbo:overview"),
            back_label="Overview",
        )
    )

    if request.headers.get("HX-Request"):
        return render(request, "components/company_cards.html", context)
    return render(request, "epos_qbo/companies.html", context)


@login_required
def company_detail(request, company_key):
    """Detail view for a single company."""
    company = get_object_or_404(CompanyConfigRecord, company_key=company_key)
    recent_runs = list(_company_runs_queryset(company_key).order_by("-started_at", "-created_at")[:30])
    latest_run = recent_runs[0] if recent_runs else None
    company_data = _enrich_company_data(company, latest_run)
    company_data.update(compute_sales_trend(company_key, now=timezone.now()))
    company_data["config_json_pretty"] = json.dumps(company.config_json or {}, indent=2)
    recent_artifacts = RunArtifact.objects.filter(company_key=company_key).order_by("-processed_at")[:30]

    context = {
        "company_data": company_data,
        "recent_runs": recent_runs,
        "recent_artifacts": recent_artifacts,
    }
    context.update(_nav_context())
    context.update(
        _breadcrumb_context(
            [
                {"label": "Dashboard", "url": reverse("epos_qbo:overview")},
                {"label": "Companies", "url": reverse("epos_qbo:companies-list")},
                {"label": company.display_name, "url": None},
            ],
            back_url=reverse("epos_qbo:companies-list"),
            back_label="Companies",
        )
    )
    return render(request, "epos_qbo/company_detail.html", context)


@login_required
@permission_required("epos_qbo.can_edit_companies", raise_exception=True)
@require_POST
def company_toggle_active(request, company_key):
    """Toggle company is_active (soft delete/restore). Returns JSON for HTMX else redirects."""
    company = get_object_or_404(CompanyConfigRecord, company_key=company_key)
    company.is_active = not company.is_active
    company.save(update_fields=["is_active"])
    if request.headers.get("HX-Request") or request.accepts("application/json"):
        return JsonResponse({
            "success": True,
            "is_active": company.is_active,
            "message": f"Company {'activated' if company.is_active else 'deactivated'}",
        })
    msg = f"Company {company.display_name} has been {'activated' if company.is_active else 'deactivated'}."
    messages.success(request, msg)
    return redirect("epos_qbo:companies-list")
