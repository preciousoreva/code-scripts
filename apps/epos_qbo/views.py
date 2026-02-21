from __future__ import annotations

import json
import os
from collections import defaultdict
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

from code_scripts.token_manager import ensure_db_initialized, load_tokens, load_tokens_batch

from .forms import CompanyAdvancedForm, CompanyBasicForm, RunScheduleForm, RunTriggerForm
from .models import (
    CompanyConfigRecord,
    RunArtifact,
    RunJob,
    RunSchedule,
    RunScheduleEvent,
)
from .services.config_sync import (
    apply_advanced_payload,
    build_basic_payload,
    import_all_company_json,
    sync_record_to_json,
    validate_company_config,
)
from .services.job_runner import dispatch_next_queued_job, read_log_chunk
from .services.schedule_worker import enqueue_run_for_schedule, get_scheduler_status
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
    extract_amount_hybrid,
    _format_currency as _metrics_format_currency,
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
HEALTH_REASON_LABELS = {
    "EPOS_CONFIG_MISSING": "EPOS config/env keys missing",
    "TOKEN_CRITICAL": "QBO re-authentication required",
    "TOKEN_EXPIRING_SOON": "QBO refresh token expiring soon",
    "LATEST_RUN_FAILED": "Latest run failed",
    "UPLOAD_FAILURE": "Upload failures in latest run",
    "RECON_MISMATCH": "Reconciliation mismatch above threshold",
    "NO_ARTIFACT_METADATA": "No successful sync yet",
}
# Run detail: message when run succeeded but 0 Sales Receipts uploaded (all skipped). {skipped} placeholder.
RUN_DETAIL_ALL_SKIPPED_MESSAGE = (
    "QuickBooks: 0 new Sales Receipts uploaded; {skipped} Sales Receipt(s) skipped (already in QuickBooks)."
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


def _decimal_setting(name: str, default: Decimal, *, minimum: Decimal | None = None) -> Decimal:
    raw = getattr(settings, name, default)
    try:
        value = Decimal(str(raw))
    except Exception:
        return default
    if minimum is not None and value < minimum:
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


def _dashboard_reconcile_diff_warning_threshold() -> Decimal:
    return _decimal_setting("OIAT_DASHBOARD_RECON_DIFF_WARNING", Decimal("1.0"), minimum=Decimal("0"))


def _reauth_guidance() -> str:
    text = str(getattr(settings, "OIAT_DASHBOARD_REAUTH_GUIDANCE", DEFAULT_REAUTH_GUIDANCE)).strip()
    return text or DEFAULT_REAUTH_GUIDANCE


def _health_reason_labels(reason_codes: list[str] | None) -> list[str]:
    labels: list[str] = []
    for code in reason_codes or []:
        label = HEALTH_REASON_LABELS.get(code)
        if label and label not in labels:
            labels.append(label)
    return labels


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


def _company_token_health(company: CompanyConfigRecord, tokens: dict | None = None) -> dict:
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

    if tokens is None:
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
    health = _company_health_snapshot(
        company,
        latest_artifact=latest_artifact,
        latest_job=latest_job,
        token_info=token_info,
    )
    return health["level"], health["summary"]


def _run_activity_status(latest_job: RunJob | None) -> str:
    if latest_job and latest_job.status == RunJob.STATUS_RUNNING:
        return "running"
    if latest_job and latest_job.status == RunJob.STATUS_QUEUED:
        return "queued"
    return "idle"


def _company_health_snapshot(
    company: CompanyConfigRecord,
    latest_artifact: RunArtifact | None,
    latest_job: RunJob | None,
    token_info: dict | None = None,
) -> dict:
    """Canonical company health classification used by overview/list/detail views."""
    cfg = company.config_json or {}
    epos = cfg.get("epos") or {}
    token_info = token_info or _company_token_health(company)
    run_activity = _run_activity_status(latest_job)

    if not epos.get("username_env_key") or not epos.get("password_env_key"):
        return {
            "level": "warning",
            "summary": "Missing EPOS env key names in company config.",
            "reason_codes": ["EPOS_CONFIG_MISSING"],
            "run_activity": run_activity,
        }
    if token_info["severity"] == "critical":
        return {
            "level": "critical",
            "summary": token_info["status_message"],
            "reason_codes": ["TOKEN_CRITICAL"],
            "run_activity": run_activity,
        }

    if latest_job and latest_job.status == RunJob.STATUS_FAILED:
        return {
            "level": "critical",
            "summary": latest_job.failure_reason or "Latest run failed.",
            "reason_codes": ["LATEST_RUN_FAILED"],
            "run_activity": run_activity,
        }

    if token_info["severity"] == "warning":
        return {
            "level": "warning",
            "summary": token_info["status_message"],
            "reason_codes": ["TOKEN_EXPIRING_SOON"],
            "run_activity": run_activity,
        }

    if latest_artifact:
        failed_uploads = int((latest_artifact.upload_stats_json or {}).get("failed", 0))
        if failed_uploads > 0:
            return {
                "level": "critical",
                "summary": f"{failed_uploads} upload(s) failed in latest run.",
                "reason_codes": ["UPLOAD_FAILURE"],
                "run_activity": run_activity,
            }
        reconcile_diff = latest_artifact.reconcile_difference
        if reconcile_diff is not None and abs(reconcile_diff) > _dashboard_reconcile_diff_warning_threshold():
            return {
                "level": "warning",
                "summary": "Reconciliation mismatch above threshold.",
                "reason_codes": ["RECON_MISMATCH"],
                "run_activity": run_activity,
            }
    else:
        return {
            "level": "unknown",
            "summary": "No successful sync yet.",
            "reason_codes": ["NO_ARTIFACT_METADATA"],
            "run_activity": run_activity,
        }

    return {
        "level": "healthy",
        "summary": "Last run succeeded.",
        "reason_codes": [],
        "run_activity": run_activity,
    }


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


def _format_system_health_breakdown(
    healthy_count: int,
    warning_count: int,
    critical_count: int,
    unknown_count: int = 0,
) -> str:
    parts = [f"{healthy_count} healthy"]
    if warning_count > 0:
        parts.append(f"{warning_count} warning")
    if critical_count > 0:
        parts.append(f"{critical_count} critical")
    if unknown_count > 0:
        parts.append(f"{unknown_count} unknown")
    return " • ".join(parts)


def _normalize_summary_text(value: str | None) -> str:
    if not value:
        return ""
    # Normalize for lightweight equality checks: trim, strip trailing punctuation, and casefold.
    return value.strip().rstrip(".:").casefold()


def _should_show_company_summary(
    status: str,
    summary: str | None,
    health_reason_labels: list[str] | None = None,
) -> bool:
    """Render summary only when it adds value beyond status/reason labels."""
    norm_summary = _normalize_summary_text(summary)
    # Healthy/unknown rows already communicate enough via labels and run/activity lines.
    if status in {"healthy", "unknown"}:
        return False
    if not norm_summary:
        return False
    # Critical rows: always show summary when present (acts as primary error explanation).
    if status == "critical":
        return True
    # For warnings (and any other non-healthy/unknown/non-critical), hide if summary just repeats a label.
    labels = health_reason_labels or []
    for label in labels:
        if _normalize_summary_text(label) == norm_summary:
            return False
    return True


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
        # No artifact yet: still try to show last successful run time (e.g. All Companies just finished)
        latest_job = (
            RunJob.objects.filter(status=RunJob.STATUS_SUCCEEDED)
            .order_by("-finished_at", "-started_at", "-created_at")
            .first()
        )
        last_successful_at = (latest_job.finished_at or latest_job.started_at or latest_job.created_at) if latest_job else None
        return {
            "target_date": None,
            "prev_target_date": None,
            "last_successful_at": last_successful_at,
            "has_data": False,
        }

    artifact_time = latest.processed_at or latest.imported_at
    # Include latest succeeded RunJob so "Last successful sync X ago" matches runs (including All Companies) even if artifact ingest is delayed
    latest_succeeded_job = (
        RunJob.objects.filter(status=RunJob.STATUS_SUCCEEDED)
        .order_by("-finished_at", "-started_at", "-created_at")
        .first()
    )
    job_time = None
    if latest_succeeded_job:
        job_time = latest_succeeded_job.finished_at or latest_succeeded_job.started_at or latest_succeeded_job.created_at
    last_successful_at = max(
        (t for t in (artifact_time, job_time) if t is not None),
        key=lambda t: t,
    ) if (artifact_time or job_time) else artifact_time

    return {
        "target_date": latest.target_date,
        "prev_target_date": latest.target_date - timedelta(days=1),
        "last_successful_at": last_successful_at,
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

    # Build "latest run" per company including All Companies runs (same logic as _company_runs_queryset).
    # Jobs with company_key=company apply to that company; jobs with artifacts for a company also apply.
    job_id_to_company_keys: dict = defaultdict(set)
    for run_job_id, ck in RunArtifact.objects.filter(
        company_key__in=company_keys
    ).exclude(run_job_id__isnull=True).values_list("run_job_id", "company_key"):
        if run_job_id and ck:
            job_id_to_company_keys[run_job_id].add(ck)
    job_ids_with_artifacts = list(job_id_to_company_keys.keys())
    # Same ordering as _company_runs_queryset_ordered_by_latest so "latest run" is consistent app-wide
    # PRIORITIZE RUNNING/QUEUED JOBS: Query all relevant jobs, then process running/queued first
    all_relevant_jobs = list(RunJob.objects.filter(
        Q(company_key__in=company_keys) | Q(id__in=job_ids_with_artifacts) | Q(company_key__isnull=True, scope=RunJob.SCOPE_ALL)
    ).order_by("-finished_at", "-started_at", "-created_at"))
    # First pass: prioritize running/queued jobs
    for job in all_relevant_jobs:
        if job.status not in (RunJob.STATUS_RUNNING, RunJob.STATUS_QUEUED):
            continue
        candidates = []
        if job.company_key and job.company_key in company_keys:
            candidates.append(job.company_key)
        elif job.company_key is None and job.scope == RunJob.SCOPE_ALL:
            # All Companies run applies to all companies
            candidates.extend(company_keys)
        candidates.extend(job_id_to_company_keys.get(job.id, []))
        for ck in candidates:
            if ck not in latest_jobs:
                latest_jobs[ck] = job
    # Second pass: fill in completed jobs for companies without active runs
    for job in all_relevant_jobs:
        candidates = []
        if job.company_key and job.company_key in company_keys:
            candidates.append(job.company_key)
        elif job.company_key is None and job.scope == RunJob.SCOPE_ALL:
            # All Companies run applies to all companies
            candidates.extend(company_keys)
        candidates.extend(job_id_to_company_keys.get(job.id, []))
        for ck in candidates:
            if ck not in latest_jobs:
                latest_jobs[ck] = job

    # Reconciliation warnings for succeeded latest runs (overview company list)
    succeeded_job_ids = list({j.id for j in latest_jobs.values() if j.status == RunJob.STATUS_SUCCEEDED})
    artifacts_by_job_overview: dict = defaultdict(list)
    for run_job_id, status in RunArtifact.objects.filter(
        run_job_id__in=succeeded_job_ids
    ).values_list("run_job_id", "reconcile_status"):
        if run_job_id is not None:
            artifacts_by_job_overview[str(run_job_id)].append(status or "")
    reconciliation_warning_by_job_id: dict = {}
    for jid in succeeded_job_ids:
        label = _reconciliation_label_for_job(str(jid), artifacts_by_job_overview)
        if label == "Mismatch":
            reconciliation_warning_by_job_id[jid] = "Reconciliation mismatch"
        elif label == "Not reconciled":
            reconciliation_warning_by_job_id[jid] = "Not reconciled"

    ensure_db_initialized()
    token_pairs = [
        (c.company_key, ((c.config_json or {}).get("qbo") or {}).get("realm_id"))
        for c in companies
    ]
    token_pairs = [(k, r) for k, r in token_pairs if r]
    token_batch = load_tokens_batch(token_pairs)

    companies_context = []
    healthy_count = warning_count = critical_count = unknown_count = 0

    for company in companies:
        latest_artifact = latest_artifacts.get(company.company_key)
        latest_job = latest_jobs.get(company.company_key)
        realm_id = ((company.config_json or {}).get("qbo") or {}).get("realm_id")
        preloaded_tokens = token_batch.get((company.company_key, realm_id)) if realm_id else None
        token_info = _company_token_health(company, tokens=preloaded_tokens)
        health = _company_health_snapshot(
            company,
            latest_artifact=latest_artifact,
            latest_job=latest_job,
            token_info=token_info,
        )
        status = health["level"]
        summary = health["summary"]
        run_activity = _run_activity_display(health["run_activity"])
        health_reason_labels = _health_reason_labels(health.get("reason_codes"))
        show_summary = _should_show_company_summary(status, summary, health_reason_labels)
        latest_job_time = None
        if latest_job:
            latest_job_time = latest_job.finished_at or latest_job.started_at or latest_job.created_at
        latest_artifact_time = None
        if latest_artifact:
            latest_artifact_time = latest_artifact.processed_at or latest_artifact.imported_at
        last_run_time = latest_job_time or latest_artifact_time

        last_run_reconciliation_warning = None
        if latest_job and latest_job.status == RunJob.STATUS_SUCCEEDED:
            last_run_reconciliation_warning = reconciliation_warning_by_job_id.get(latest_job.id)

        if status == "healthy":
            healthy_count += 1
        elif status == "warning":
            warning_count += 1
        elif status == "unknown":
            unknown_count += 1
        else:
            critical_count += 1

        companies_context.append(
            {
                "name": company.display_name,
                "company_key": company.company_key,
                "last_run": last_run_time,
                "status": status,
                "health": health,
                "run_activity": run_activity,
                "health_reason_labels": health_reason_labels,
                "token_info": token_info,
                "records_synced": latest_artifact.rows_kept if latest_artifact else 0,
                "summary": summary,
                "show_summary": show_summary,
                "last_run_reconciliation_warning": last_run_reconciliation_warning,
            }
        )

    # Check for active runs across all companies
    active_runs = RunJob.objects.filter(
        status__in=[RunJob.STATUS_RUNNING, RunJob.STATUS_QUEUED]
    ).order_by("-created_at")
    active_run_count = active_runs.count()

    system_health = _classify_system_health(healthy_count, warning_count, critical_count)
    system_health_breakdown = _format_system_health_breakdown(
        healthy_count,
        warning_count,
        critical_count,
        unknown_count,
    )

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

    # Latest completed run for overview freshness
    latest_completed_run = (
        RunJob.objects.filter(
            status__in=[RunJob.STATUS_SUCCEEDED, RunJob.STATUS_FAILED, RunJob.STATUS_CANCELLED],
            finished_at__isnull=False,
        )
        .order_by("-finished_at", "-created_at")
        .first()
    )
    latest_run_id = str(latest_completed_run.id) if latest_completed_run else ""

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
            "unknown_count": unknown_count,
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
        "latest_run_id": latest_run_id,
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
    response = render(request, "components/overview_refresh.html", context)
    response["Cache-Control"] = "no-store"
    response["Pragma"] = "no-cache"
    return response


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
        "reconcile_diff_warning": _dashboard_reconcile_diff_warning_threshold(),
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


def _schedule_default_timezone_name() -> str:
    return str(
        getattr(
            settings,
            "OIAT_BUSINESS_TIMEZONE",
            getattr(settings, "TIME_ZONE", "UTC"),
        )
    )


def _schedule_create_initial() -> dict:
    return {
        "enabled": True,
        "scope": RunJob.SCOPE_ALL,
        "cron_expr": "0 18 * * *",
        "timezone_name": _schedule_default_timezone_name(),
        "target_date_mode": RunSchedule.TARGET_DATE_MODE_TRADING_DATE,
        "parallel": _dashboard_default_parallel(),
        "stagger_seconds": _dashboard_default_stagger_seconds(),
        "continue_on_failure": False,
    }


def _form_error_text(form: RunScheduleForm) -> str:
    parts: list[str] = []
    for field_name, errors in form.errors.items():
        label = "General" if field_name == "__all__" else field_name
        joined = ", ".join([str(err) for err in errors])
        parts.append(f"{label}: {joined}")
    return "; ".join(parts)


@login_required
@permission_required("epos_qbo.can_manage_schedules", raise_exception=True)
@require_GET
def schedules_page(request):
    _ensure_company_records()
    schedules = list(RunSchedule.objects.order_by("-is_system_managed", "name", "created_at"))
    recent_events = list(
        RunScheduleEvent.objects.select_related("schedule", "run_job", "run_job__scheduled_by")
        .order_by("-created_at")[:60]
    )
    active_run_ids = list(
        RunJob.objects.filter(
            scheduled_by__isnull=False,
            status__in=[RunJob.STATUS_QUEUED, RunJob.STATUS_RUNNING],
        )
        .order_by("-created_at")
        .values_list("id", flat=True)[:20]
    )
    companies = CompanyConfigRecord.objects.filter(is_active=True).order_by("display_name")
    context = {
        "schedule_form": RunScheduleForm(initial=_schedule_create_initial()),
        "schedules": schedules,
        "recent_events": recent_events,
        "companies": companies,
        "active_run_ids_json": json.dumps([str(run_id) for run_id in active_run_ids]),
        "schedule_target_date_mode": RunSchedule.TARGET_DATE_MODE_TRADING_DATE,
        "single_scope": RunJob.SCOPE_SINGLE,
        "all_scope": RunJob.SCOPE_ALL,
        "scheduler_status": get_scheduler_status(),
    }
    context.update(_nav_context())
    context.update(
        _breadcrumb_context(
            [
                {"label": "Dashboard", "url": reverse("epos_qbo:overview")},
                {"label": "Schedules", "url": None},
            ],
            back_url=reverse("epos_qbo:overview"),
            back_label="Overview",
        )
    )
    return render(request, "epos_qbo/schedules.html", context)


@login_required
@require_GET
def schedule_status_api(request):
    """Return current scheduler status as JSON for live polling on the Schedules page."""
    status = get_scheduler_status()
    return JsonResponse({
        "running": status["running"],
        "message": status.get("message", ""),
    })


@login_required
@permission_required("epos_qbo.can_manage_schedules", raise_exception=True)
@require_POST
def schedule_create(request):
    form = RunScheduleForm(request.POST)
    if not form.is_valid():
        messages.error(request, f"Invalid schedule payload: {_form_error_text(form)}")
        return redirect("epos_qbo:schedules")

    schedule: RunSchedule = form.save(commit=False)
    schedule.created_by = request.user
    schedule.updated_by = request.user
    if schedule.enabled:
        try:
            schedule.next_fire_at = schedule.compute_next_fire_at(from_dt=timezone.now())
        except Exception as exc:
            messages.error(request, f"Unable to compute next fire time: {exc}")
            return redirect("epos_qbo:schedules")
    else:
        schedule.next_fire_at = None
    schedule.save()
    messages.success(request, f"Schedule created: {schedule.name}")
    return redirect("epos_qbo:schedules")


@login_required
@permission_required("epos_qbo.can_manage_schedules", raise_exception=True)
@require_POST
def schedule_update(request, schedule_id):
    schedule = get_object_or_404(RunSchedule, id=schedule_id)
    if schedule.is_system_managed:
        messages.error(request, "System-managed schedules cannot be edited.")
        return redirect("epos_qbo:schedules")

    form = RunScheduleForm(request.POST, instance=schedule)
    if not form.is_valid():
        messages.error(request, f"Invalid schedule payload: {_form_error_text(form)}")
        return redirect("epos_qbo:schedules")

    schedule = form.save(commit=False)
    schedule.updated_by = request.user
    if schedule.enabled:
        try:
            schedule.next_fire_at = schedule.compute_next_fire_at(from_dt=timezone.now())
        except Exception as exc:
            messages.error(request, f"Unable to compute next fire time: {exc}")
            return redirect("epos_qbo:schedules")
    else:
        schedule.next_fire_at = None
    schedule.save()
    messages.success(request, f"Schedule updated: {schedule.name}")
    return redirect("epos_qbo:schedules")


@login_required
@permission_required("epos_qbo.can_manage_schedules", raise_exception=True)
@require_POST
def schedule_toggle(request, schedule_id):
    schedule = get_object_or_404(RunSchedule, id=schedule_id)
    if schedule.is_system_managed:
        messages.error(request, "System-managed schedules cannot be toggled manually.")
        return redirect("epos_qbo:schedules")

    schedule.enabled = not schedule.enabled
    schedule.updated_by = request.user
    if schedule.enabled:
        try:
            schedule.next_fire_at = schedule.compute_next_fire_at(from_dt=timezone.now())
        except Exception as exc:
            messages.error(request, f"Could not enable schedule: {exc}")
            return redirect("epos_qbo:schedules")
        message = f"Schedule enabled: {schedule.name}"
    else:
        schedule.next_fire_at = None
        message = f"Schedule disabled: {schedule.name}"
    schedule.save(update_fields=["enabled", "next_fire_at", "updated_by", "updated_at"])
    messages.success(request, message)
    return redirect("epos_qbo:schedules")


@login_required
@permission_required("epos_qbo.can_manage_schedules", raise_exception=True)
@require_POST
def schedule_run_now(request, schedule_id):
    schedule = get_object_or_404(RunSchedule, id=schedule_id)
    if schedule.scope == RunJob.SCOPE_SINGLE and not (schedule.company_key or "").strip():
        messages.error(request, "Single-company schedule is missing company key.")
        return redirect("epos_qbo:schedules")

    job, result = enqueue_run_for_schedule(schedule, now=timezone.now(), source="manual")
    if job is None and result == RunScheduleEvent.TYPE_SKIPPED_OVERLAP:
        messages.warning(request, "Schedule already has a queued/running run. Manual enqueue skipped.")
        return redirect("epos_qbo:schedules")
    if job is None:
        messages.error(request, "Could not queue run for schedule.")
        return redirect("epos_qbo:schedules")

    dispatch_next_queued_job()
    job.refresh_from_db()
    if job.status == RunJob.STATUS_RUNNING:
        messages.success(request, f"Scheduled run started: {job.display_label}")
        return redirect("epos_qbo:run-detail", job_id=job.id)

    messages.success(request, f"Scheduled run queued: {job.display_label}")
    return redirect("epos_qbo:schedules")


@login_required
@permission_required("epos_qbo.can_manage_schedules", raise_exception=True)
@require_POST
def schedule_delete(request, schedule_id):
    schedule = get_object_or_404(RunSchedule, id=schedule_id)
    if schedule.is_system_managed:
        messages.error(request, "System-managed schedules cannot be deleted.")
        return redirect("epos_qbo:schedules")

    schedule_name = schedule.name
    schedule.delete()
    messages.success(request, f"Schedule deleted: {schedule_name}")
    return redirect("epos_qbo:schedules")


def _reconciliation_label_for_job(job_id: str, artifacts_by_job: dict) -> str:
    """Return 'Match', 'Mismatch', or 'Not reconciled' for a run from its artifacts' reconcile_status."""
    statuses = artifacts_by_job.get(job_id) or []
    if not statuses:
        return "Not reconciled"
    if any(s == "MISMATCH" for s in statuses):
        return "Mismatch"
    if all(s == "MATCH" for s in statuses):
        return "Match"
    return "Not reconciled"


def _run_attention_message(job: RunJob, artifacts: list) -> str | None:
    """Return a short message for run-detail banner when run succeeded but needs attention; else None."""
    if job.status != RunJob.STATUS_SUCCEEDED:
        return None
    if not artifacts:
        return (
            "Run succeeded but no artifacts were linked. "
            "Check pipeline logs and that metadata files exist under Uploaded/."
        )
    statuses = [a.reconcile_status for a in artifacts if getattr(a, "reconcile_status", None)]
    label = _reconciliation_label_for_job(str(job.id), {str(job.id): statuses})
    if label == "Mismatch":
        return "Reconciliation mismatch: EPOS and QBO totals differ. Verify in QuickBooks."
    if label == "Not reconciled":
        return "Reconciliation did not run or failed. Check pipeline logs for this run."
    return None


@login_required
def runs_list(request):
    _ensure_company_records()
    default_parallel = _dashboard_default_parallel()
    default_stagger_seconds = _dashboard_default_stagger_seconds()
    jobs = list(RunJob.objects.order_by("-created_at")[:100])
    job_ids = [j.id for j in jobs]
    # Reconcile status per run (from artifacts)
    artifacts_by_job = defaultdict(list)
    for run_job_id, status in RunArtifact.objects.filter(
        run_job_id__in=job_ids,
    ).exclude(reconcile_status="").values_list("run_job_id", "reconcile_status"):
        if run_job_id and status:
            artifacts_by_job[run_job_id].append(status)
    run_rows = [
        {"job": job, "reconciliation_label": _reconciliation_label_for_job(job.id, artifacts_by_job)}
        for job in jobs
    ]
    form = RunTriggerForm(initial={"scope": RunJob.SCOPE_ALL, "date_mode": "yesterday"})
    companies = CompanyConfigRecord.objects.filter(is_active=True).order_by("display_name")
    
    # Get active run IDs for polling
    active_runs = RunJob.objects.filter(
        status__in=[RunJob.STATUS_QUEUED, RunJob.STATUS_RUNNING]
    ).values_list('id', flat=True)[:10]  # Limit to 10 most recent
    
    active_run_ids_list = [str(id) for id in active_runs]
    context = {
        "run_rows": run_rows,
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
    total_7d = all_jobs_7d.count()
    total_30d = all_jobs_30d.count()
    succeeded_7d = all_jobs_7d.filter(status=RunJob.STATUS_SUCCEEDED).count()
    failed_7d = all_jobs_7d.filter(status=RunJob.STATUS_FAILED).count()
    active_runs_qs = RunJob.objects.filter(
        status__in=[RunJob.STATUS_QUEUED, RunJob.STATUS_RUNNING]
    )
    active_run_ids = list(active_runs_qs.values_list("id", flat=True)[:10])

    stats = {
        "total_runs_7d": total_7d,
        "total_runs_30d": total_30d,
        "success_rate_7d": round(
            (succeeded_7d * 100 / total_7d) if total_7d > 0 else 100.0,
            1,
        ),
        "error_count_7d": failed_7d,
        "active_runs": len(active_run_ids),
    }

    companies = CompanyConfigRecord.objects.filter(is_active=True).order_by("display_name")
    
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
        "active_run_ids": [str(i) for i in active_run_ids],
        "active_run_ids_json": json.dumps([str(i) for i in active_run_ids]),
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
    artifacts_list = list(artifacts)
    active_run_ids_list = [str(job.id)] if job.status in [RunJob.STATUS_QUEUED, RunJob.STATUS_RUNNING] else []
    run_upload_summary_message = _run_detail_upload_summary_message(artifacts_list)
    context = {
        "job": job,
        "artifacts": artifacts,
        "active_run_ids": active_run_ids_list,
        "active_run_ids_json": json.dumps(active_run_ids_list),
        "exit_code_info": _exit_code_info(job.exit_code),
        "exit_code_reference": EXIT_CODE_REFERENCE,
        "run_attention_message": _run_attention_message(job, artifacts_list),
        "run_upload_summary_message": run_upload_summary_message,
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


def _artifact_upload_stat(artifact: RunArtifact, key: str) -> int | None:
    """Return upload_stats_json[key] as int, or None if missing/invalid."""
    stats = artifact.upload_stats_json if isinstance(artifact.upload_stats_json, dict) else {}
    raw = stats.get(key)
    try:
        return int(raw)
    except (TypeError, ValueError):
        return None


def _run_detail_upload_summary_message(artifacts_list: list[RunArtifact]) -> str | None:
    """If run had 0 uploads but some skipped, return a short explanation; else None."""
    total_uploaded = 0
    total_skipped = 0
    for art in artifacts_list:
        u = _artifact_upload_stat(art, "uploaded")
        s = _artifact_upload_stat(art, "skipped")
        if u is not None:
            total_uploaded += u
        if s is not None:
            total_skipped += s
    if total_uploaded == 0 and total_skipped > 0:
        return RUN_DETAIL_ALL_SKIPPED_MESSAGE.format(skipped=total_skipped)
    return None


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


def _company_runs_queryset_ordered_by_latest(company_key: str):
    """Runs for this company (single or All Companies with artifact), ordered for 'latest run'.
    Ordering must match overview and companies list: -finished_at, -started_at, -created_at."""
    return _company_runs_queryset(company_key).order_by("-finished_at", "-started_at", "-created_at")


def _status_display_from_canonical(
    status_str: str,
    latest_run: RunJob | None,
    latest_artifact: RunArtifact | None,
) -> dict:
    """Map canonical status from _status_for_company to display dict (level, label, color, icon)."""
    if status_str == "critical":
        return {
            "level": "critical",
            "canonical_level": "critical",
            "label": "Critical",
            "color": "red",
            "icon": "solar:close-circle-linear",
        }
    if status_str == "healthy":
        return {
            "level": "healthy",
            "canonical_level": "healthy",
            "label": "Healthy",
            "color": "emerald",
            "icon": "solar:check-circle-linear",
        }
    if status_str == "unknown":
        return {
            "level": "unknown",
            "canonical_level": "unknown",
            "label": "Never Run",
            "color": "amber",
            "icon": "solar:question-circle-linear",
        }
    # warning: show "Never Run" when no run, else "Warning"
    if not latest_run and not latest_artifact:
        return {
            "level": "warning",
            "canonical_level": "warning",
            "label": "Never Run",
            "color": "amber",
            "icon": "solar:danger-triangle-linear",
        }
    return {
        "level": "warning",
        "canonical_level": "warning",
        "label": "Warning",
        "color": "amber",
        "icon": "solar:danger-triangle-linear",
    }


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


def _run_activity_display(run_activity: str) -> dict | None:
    if run_activity == "running":
        return {
            "state": "running",
            "label": "Sync running",
            "icon": "solar:refresh-linear",
            "color": "blue",
        }
    if run_activity == "queued":
        return {
            "state": "queued",
            "label": "Sync queued",
            "icon": "solar:clock-circle-linear",
            "color": "amber",
        }
    return None


def _enrich_company_data(
    company: CompanyConfigRecord,
    latest_run: RunJob | None,
    preloaded: dict | None = None,
) -> dict:
    """Build enriched company dict for list/detail templates. Uses same status logic as Overview.
    When preloaded is provided (e.g. from companies_list batch), use it to avoid N+1 queries."""
    if preloaded is not None:
        latest_artifact = preloaded.get("latest_artifact")
        artifacts_today = preloaded.get("artifacts_today") or []
        token_info = preloaded.get("token_info") or _get_token_info_for_display(company)
        latest_successful_artifact = preloaded.get("latest_successful_artifact")
    else:
        latest_artifact = (
            RunArtifact.objects.filter(company_key=company.company_key)
            .order_by("-processed_at", "-imported_at")
            .first()
        )
        bounds = get_dashboard_date_bounds()
        today_start_utc = bounds["today_start_utc"]
        now_utc = bounds["now_utc"]
        artifacts_today = list(
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
        token_info = _get_token_info_for_display(company)
        latest_successful_artifact = (
            RunArtifact.objects.filter(company_key=company.company_key)
            .filter(run_job__status=RunJob.STATUS_SUCCEEDED)
            .select_related("run_job")
            .order_by("-processed_at", "-imported_at", "-id")
            .first()
        )

    health = _company_health_snapshot(
        company,
        latest_artifact=latest_artifact,
        latest_job=latest_run,
        token_info=token_info,
    )
    status_str = health["level"]
    status = _status_display_from_canonical(
        status_str,
        latest_run,
        latest_artifact,
    )
    run_activity = _run_activity_display(health["run_activity"])
    health_reason_labels = _health_reason_labels(health.get("reason_codes"))
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

    if latest_successful_artifact:
        records_latest_sync = _artifact_uploaded_count(latest_successful_artifact)
        latest_sync_target_date = latest_successful_artifact.target_date
        upload_skipped_latest_sync = _artifact_upload_stat(latest_successful_artifact, "skipped")
        if upload_skipped_latest_sync is None:
            upload_skipped_latest_sync = 0
    else:
        records_latest_sync = 0
        latest_sync_target_date = None
        upload_skipped_latest_sync = 0

    # So templates can style the issue block by severity (amber/red), not overall status (which can be healthy)
    issues_highest_severity = None
    if issues:
        severities = {i.get("severity") for i in issues}
        if "red" in severities:
            issues_highest_severity = "critical"
        elif "amber" in severities:
            issues_highest_severity = "warning"

    return {
        "company": company,
        "status": status,
        "health": health,
        "health_reason_labels": health_reason_labels,
        "run_activity": run_activity,
        "latest_run": latest_run,
        "latest_artifact": latest_artifact,
        "token_info": token_info,
        "issues": issues,
        "issues_highest_severity": issues_highest_severity,
        "config_display": config_display,
        "records_24h": records_24h,
        "last_activity_at": last_activity_at,
        "last_run_display": _format_last_run_time(last_activity_at),
        "records_latest_sync": records_latest_sync,
        "latest_sync_target_date": latest_sync_target_date,
        "upload_skipped_latest_sync": upload_skipped_latest_sync,
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
        order = {"critical": 0, "warning": 1, "unknown": 2, "healthy": 3}
        return sorted(companies_data, key=lambda c: order.get(c["status"]["level"], 99))
    return companies_data


def _calculate_companies_summary(companies_data: list) -> dict:
    total = len(companies_data)
    healthy = sum(
        1
        for c in companies_data
        if c["status"].get("canonical_level", c["status"]["level"]) == "healthy"
    )
    warning = sum(
        1
        for c in companies_data
        if c["status"].get("canonical_level", c["status"]["level"]) == "warning"
    )
    critical = sum(
        1
        for c in companies_data
        if c["status"].get("canonical_level", c["status"]["level"]) == "critical"
    )
    unknown = sum(
        1
        for c in companies_data
        if c["status"].get("canonical_level", c["status"]["level"]) == "unknown"
    )
    return {"total": total, "healthy": healthy, "warning": warning, "critical": critical, "unknown": unknown}


def _batch_preload_companies_data(companies: list) -> tuple[dict[str, RunJob | None], dict[str, RunArtifact | None], dict[str, list], dict[str, RunArtifact | None], dict[str, dict]]:
    """Batch-fetch latest run, latest artifact, artifacts_today, latest_successful_artifact, and token_info per company_key.
    Latest run per company includes All Companies runs that have an artifact for that company (same logic as overview)."""
    company_keys = [c.company_key for c in companies]
    if not company_keys:
        return {}, {}, {}, {}, {}

    # Latest run per company (include All Companies runs that produced an artifact for this company)
    # Same logic as Overview page: include All Companies runs and prioritize running/queued jobs
    job_id_to_company_keys: dict = defaultdict(set)
    for run_job_id, ck in RunArtifact.objects.filter(company_key__in=company_keys).exclude(run_job_id__isnull=True).values_list("run_job_id", "company_key"):
        if run_job_id and ck:
            job_id_to_company_keys[run_job_id].add(ck)
    job_ids_with_artifacts = list(job_id_to_company_keys.keys())
    # Same ordering as _company_runs_queryset_ordered_by_latest so "latest run" is consistent app-wide
    # Include All Companies runs (same as Overview)
    all_relevant_jobs = list(RunJob.objects.filter(
        Q(company_key__in=company_keys) | Q(id__in=job_ids_with_artifacts) | Q(company_key__isnull=True, scope=RunJob.SCOPE_ALL)
    ).order_by("-finished_at", "-started_at", "-created_at"))
    latest_runs_map: dict[str, RunJob | None] = {}
    # First pass: prioritize running/queued jobs (same as Overview)
    for job in all_relevant_jobs:
        if job.status not in (RunJob.STATUS_RUNNING, RunJob.STATUS_QUEUED):
            continue
        candidates = []
        if job.company_key and job.company_key in company_keys:
            candidates.append(job.company_key)
        elif job.company_key is None and job.scope == RunJob.SCOPE_ALL:
            # All Companies run applies to all companies
            candidates.extend(company_keys)
        candidates.extend(job_id_to_company_keys.get(job.id, []))
        for ck in candidates:
            if ck not in latest_runs_map:
                latest_runs_map[ck] = job
    # Second pass: fill in completed jobs for companies without active runs
    for job in all_relevant_jobs:
        candidates = []
        if job.company_key and job.company_key in company_keys:
            candidates.append(job.company_key)
        elif job.company_key is None and job.scope == RunJob.SCOPE_ALL:
            # All Companies run applies to all companies
            candidates.extend(company_keys)
        candidates.extend(job_id_to_company_keys.get(job.id, []))
        for ck in candidates:
            if ck not in latest_runs_map:
                latest_runs_map[ck] = job

    # Latest artifact per company
    latest_artifacts_map: dict[str, RunArtifact | None] = {}
    for art in RunArtifact.objects.filter(company_key__in=company_keys).order_by("company_key", "-processed_at", "-imported_at"):
        if art.company_key not in latest_artifacts_map:
            latest_artifacts_map[art.company_key] = art

    bounds = get_dashboard_date_bounds()
    today_start_utc = bounds["today_start_utc"]
    now_utc = bounds["now_utc"]
    artifacts_today_all = list(
        RunArtifact.objects.filter(company_key__in=company_keys)
        .filter(
            Q(processed_at__gte=today_start_utc, processed_at__lt=now_utc)
            | Q(
                processed_at__isnull=True,
                imported_at__gte=today_start_utc,
                imported_at__lt=now_utc,
            )
        )
        .select_related("run_job")
        .order_by("company_key", "-processed_at", "-imported_at")
    )
    artifacts_today_by_key: dict[str, list] = {}
    for art in artifacts_today_all:
        artifacts_today_by_key.setdefault(art.company_key, []).append(art)

    # Latest successful artifact per company
    latest_successful_map: dict[str, RunArtifact | None] = {}
    for art in (
        RunArtifact.objects.filter(company_key__in=company_keys)
        .filter(run_job__status=RunJob.STATUS_SUCCEEDED)
        .select_related("run_job")
        .order_by("company_key", "-processed_at", "-imported_at", "-id")
    ):
        if art.company_key not in latest_successful_map:
            latest_successful_map[art.company_key] = art

    ensure_db_initialized()
    token_pairs = [
        (c.company_key, ((c.config_json or {}).get("qbo") or {}).get("realm_id"))
        for c in companies
    ]
    token_pairs = [(k, r) for k, r in token_pairs if r]
    token_batch = load_tokens_batch(token_pairs)

    token_info_by_key: dict[str, dict] = {}
    for company in companies:
        realm_id = ((company.config_json or {}).get("qbo") or {}).get("realm_id")
        preloaded_tokens = token_batch.get((company.company_key, realm_id)) if realm_id else None
        token_info_by_key[company.company_key] = _company_token_health(company, tokens=preloaded_tokens)

    return latest_runs_map, latest_artifacts_map, artifacts_today_by_key, latest_successful_map, token_info_by_key


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
    companies = list(companies)

    if not companies:
        companies_data = []
    else:
        latest_runs_map, latest_artifacts_map, artifacts_today_by_key, latest_successful_map, token_info_by_key = _batch_preload_companies_data(companies)
        companies_data = []
        for company in companies:
            preloaded = {
                "latest_artifact": latest_artifacts_map.get(company.company_key),
                "artifacts_today": artifacts_today_by_key.get(company.company_key, []),
                "latest_successful_artifact": latest_successful_map.get(company.company_key),
                "token_info": token_info_by_key.get(company.company_key),
            }
            company_data = _enrich_company_data(
                company,
                latest_runs_map.get(company.company_key),
                preloaded=preloaded,
            )
            companies_data.append(company_data)

    if filter_status != "all":
        companies_data = [
            c
            for c in companies_data
            if c["status"].get("canonical_level", c["status"]["level"]) == filter_status
        ]
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
    recent_runs = list(_company_runs_queryset_ordered_by_latest(company_key)[:30])
    latest_run = recent_runs[0] if recent_runs else None
    company_data = _enrich_company_data(company, latest_run)
    # Sales from last successful run (not 7D aggregate)
    latest_successful_artifact = (
        RunArtifact.objects.filter(company_key=company_key)
        .filter(run_job__status=RunJob.STATUS_SUCCEEDED)
        .select_related("run_job")
        .order_by("-processed_at", "-imported_at", "-id")
        .first()
    )
    if latest_successful_artifact:
        amount = extract_amount_hybrid(latest_successful_artifact, prefer_reconcile=True)
        company_data["sales_last_run_display"] = _metrics_format_currency(amount)
        company_data["sales_last_run_target_date"] = latest_successful_artifact.target_date
    else:
        company_data["sales_last_run_display"] = "—"
        company_data["sales_last_run_target_date"] = None
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
