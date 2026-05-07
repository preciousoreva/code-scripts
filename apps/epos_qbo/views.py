from __future__ import annotations

import json
import logging
import os
import subprocess
import sys

import requests
from collections import defaultdict
from datetime import datetime, timedelta, timezone as dt_timezone
from decimal import Decimal
from math import ceil
from pathlib import Path
from urllib.parse import urlencode
from zoneinfo import ZoneInfo

from django.conf import settings
from django.contrib import messages
from django.contrib.auth.decorators import login_required, permission_required
from django.db import DatabaseError
from django.db.models import Q
from django.http import FileResponse, Http404, HttpResponseForbidden, JsonResponse
from django.shortcuts import get_object_or_404, redirect, render
from django.urls import reverse
from django.utils import timezone
from django.views.decorators.http import require_GET, require_POST

from oiat_portal.paths import OPS_REPORTS_DIR
from code_scripts.token_manager import (
    ensure_db_initialized,
    get_access_token,
    load_tokens,
    load_tokens_batch,
    refresh_access_token,
)
from code_scripts.company_config import (
    get_qbo_api_base_url,
    normalize_qbo_environment,
)

from .forms import (
    CompanyAdvancedForm,
    CompanyBasicForm,
    InventoryTriggerForm,
    PortalSettingsForm,
    RunScheduleForm,
    RunTriggerForm,
    UserPreferencesForm,
)
from .models import (
    CompanyConfigRecord,
    DashboardUserPreference,
    PortalSettings,
    RunArtifact,
    RunJob,
    RunSchedule,
    RunScheduleEvent,
)
from . import portal_settings
from .services.config_sync import (
    apply_advanced_payload,
    build_basic_payload,
    import_all_company_json,
    sync_record_to_json,
    validate_company_config,
)
from .services.job_runner import dispatch_next_queued_job, read_log_chunk, resolve_python_executable
from .services.inventory_categories import load_inventory_categories_by_company
from .services.inventory_review_slack import send_inventory_review_action_queued
from .services.inventory_review import REASON_GROUPS, parse_inventory_review_csv
from .services.inventory_review_actions import (
    REASON_GROUP_MISSING,
    RETRY_INTENT_CATALOG,
    RETRY_INTENT_QUANTITY,
    REVIEW_CREATE_MISSING_INTENT,
    SNAPSHOT_PACK_GUARD_MESSAGE,
    build_missing_item_creation_preview,
    coalesce_picker_date_from_get,
    collect_category_options,
    filter_missing_preview_by_category,
    get_catalog_cleanup_rows,
    get_quantity_adjustment_rows,
    get_review_rows_by_reason,
    inv_start_date_floor_iso,
    load_review_context,
    queue_missing_item_creation_job,
    resolve_category_scope_labels,
    resolve_txn_date_for_review_missing_item_creation,
    retry_catalog_cleanup_for_review,
    retry_quantity_adjustments_for_review,
    validate_inventory_start_date_for_missing_queue,
)
from .services.schedule_worker import enqueue_run_for_schedule, get_scheduler_status
from .dashboard_timezone import get_dashboard_date_bounds, get_dashboard_timezone_display
from .business_date import (
    get_business_day_cutoff,
    get_business_timezone,
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
    "INVENTORY_FAILURE": "Latest inventory run failed",
    "INVENTORY_NEEDS_REVIEW": "Inventory needs review",
    "INVENTORY_NOT_CHECKED": "Inventory not checked",
    "NO_ARTIFACT_METADATA": "No successful sales sync recorded",
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
RUN_ARTIFACT_REPORT_LABELS = {
    "source": "Inventory Report",
    "summary_csv": "Summary CSV",
    "summary_json": "Summary JSON",
    "final_audit": "Final Audit",
    "initial_audit": "Initial Audit",
    "catalog_cleanup": "Catalog Cleanup",
    "quantity_preview_csv": "Quantity Preview",
    "quantity_preview_json": "Quantity Preview JSON",
    "post_catalog_audit": "Post Catalog Audit",
    "review_missing_create_report": "Missing item creation report",
}
RUN_ARTIFACT_REPORT_ORDER = [
    "summary_csv",
    "summary_json",
    "review_missing_create_report",
    "final_audit",
    "initial_audit",
    "catalog_cleanup",
    "post_catalog_audit",
    "source",
]
RUN_ARTIFACT_REPORT_SUFFIXES = {".csv", ".json"}
INVENTORY_MODE_LABELS = {
    "audit_only": "Audit only",
    "quantity_preview": "Preview only",
    "quantity_apply": "Applied quantity adjustments",
    "catalog_plan_only": "Catalog plan only",
    "catalog_apply_admin_only": "Catalog cleanup applied",
    "review_create_missing_items": "Missing item creation",
}
INVENTORY_MODE_WRITE_INTENT_LABELS = {
    "audit_only": "No QBO writes",
    "quantity_preview": "Preview quantity adjustments",
    "quantity_apply": "Apply quantity adjustments",
    "catalog_plan_only": "Plan catalog cleanup",
    "catalog_apply_admin_only": "Admin catalog apply",
    "review_create_missing_items": "Create missing inventory items",
}
INVENTORY_SAFE_APPLY_COPY = "Production inventory apply is blocked by default. Audit and preview are safe."


def _unique_existing_resolved_dirs(paths: list[Path]) -> list[Path]:
    roots: list[Path] = []
    for path in paths:
        try:
            resolved = path.expanduser().resolve(strict=False)
        except (OSError, RuntimeError):
            continue
        if not resolved.exists() or not resolved.is_dir():
            continue
        if resolved not in roots:
            roots.append(resolved)
    return roots


def _trusted_report_roots() -> list[Path]:
    roots = [
        Path(settings.BASE_DIR),
        OPS_REPORTS_DIR,
    ]
    state_root = os.getenv("STATE_ROOT") or os.getenv("OIAT_STATE_ROOT")
    if state_root:
        roots.append(Path(state_root) / "code_scripts" / "reports")
    roots.append(Path("/data/code_scripts/reports"))
    return _unique_existing_resolved_dirs(roots)


def _path_is_relative_to(path: Path, root: Path) -> bool:
    try:
        path.relative_to(root)
    except ValueError:
        return False
    return True


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


def _get_user_overview_defaults(request):
    """Return (default_company_key, default_revenue_period) for the current user."""
    try:
        pref = DashboardUserPreference.objects.get(user=request.user)
    except DashboardUserPreference.DoesNotExist:
        return (None, "7d")
    except DatabaseError:
        return (None, "7d")
    period = (pref.default_revenue_period or "").strip() or "7d"
    if period not in REVENUE_PERIOD_DAYS:
        period = "7d"
    company = (pref.default_overview_company_key or "").strip() or None
    return (company, period)


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
    guidance = portal_settings.get_reauth_guidance()
    cfg = company.config_json or {}
    realm_id = (cfg.get("qbo") or {}).get("realm_id")
    if not realm_id:
        return {
            "valid": False,
            "severity": "critical",
            "status_color": "red",
            "token_unknown": True,
            "connection_state": "missing_realm_id",
            "access_state": "unknown",
            "display_label": "Realm ID not configured",
            "display_subtext": "Add the Realm ID in company settings to connect QuickBooks.",
            "status_message": "Realm ID not configured",
            "days_remaining": None,
            "expiring_soon": False,
            "expires_at": None,
            "token_days": None,
            "reauth_guidance": guidance,
            "issues": [
                {
                    "severity": "red",
                    "icon": "solar:shield-warning-linear",
                    "message": "Realm ID not configured",
                    "action": "configure_realm_id",
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
        and refresh_seconds_left <= portal_settings.get_refresh_expiring_days() * 86400
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
    run_label = job.friendly_id
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


def _run_status_time(job: RunJob | None):
    if not job:
        return None
    return job.finished_at or job.started_at or job.created_at


def _artifact_status_time(artifact: RunArtifact | None):
    if not artifact:
        return None
    return artifact.processed_at or artifact.imported_at


def _coerce_config_bool(value) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value != 0
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "on"}
    return False


def _company_inventory_enabled(company: CompanyConfigRecord) -> bool:
    cfg = company.config_json if isinstance(company.config_json, dict) else {}
    inventory = cfg.get("inventory") if isinstance(cfg.get("inventory"), dict) else {}
    return _coerce_config_bool(inventory.get("enable_inventory_items"))


def _company_capabilities(company: CompanyConfigRecord) -> dict:
    return {
        "sales_sync": True,
        "inventory": _company_inventory_enabled(company),
    }


def _safe_int_stat(stats: dict, key: str, default: int = 0) -> int:
    try:
        return int(stats.get(key, default) or 0)
    except (TypeError, ValueError):
        return int(default)


def _is_inventory_pipeline_artifact(artifact: RunArtifact) -> bool:
    stats = artifact.upload_stats_json if isinstance(artifact.upload_stats_json, dict) else {}
    if stats.get("report_type") == RunJob.SCOPE_INVENTORY_PIPELINE:
        return True
    if artifact.run_job and artifact.run_job.scope == RunJob.SCOPE_INVENTORY_PIPELINE:
        return True
    source_name = os.path.basename(str(artifact.source_path or ""))
    return source_name.startswith("inventory_pipeline_") and source_name.endswith(".json")


def _is_inventory_artifact(artifact: RunArtifact) -> bool:
    if artifact.kind == RunArtifact.KIND_INVENTORY_AUDIT:
        return True
    if artifact.run_job and artifact.run_job.scope in {
        RunJob.SCOPE_INVENTORY_PIPELINE,
        RunJob.SCOPE_INVENTORY_SYNC,
    }:
        return True
    return _is_inventory_pipeline_artifact(artifact)


def _is_sales_artifact(artifact: RunArtifact) -> bool:
    if _is_inventory_artifact(artifact):
        return False
    return artifact.kind in {"", RunArtifact.KIND_SALES_UPLOAD, None}


def _inventory_summary_from_artifact(artifact: RunArtifact | None) -> dict:
    if artifact is None:
        return {}
    stats = artifact.upload_stats_json if isinstance(artifact.upload_stats_json, dict) else {}
    summary = dict(stats)
    path = str(summary.get("summary_json") or artifact.source_path or "").strip()
    if path and os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as handle:
                payload = json.load(handle)
        except (OSError, json.JSONDecodeError):
            payload = None
        if isinstance(payload, dict) and payload.get("run_type") == RunJob.SCOPE_INVENTORY_PIPELINE:
            for key, value in payload.items():
                summary.setdefault(key, value)
    return summary


def _coerce_bool_stat(value: object) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}


def _inventory_mode_label(mode: object, summary: dict | None = None) -> str:
    raw_mode = str(mode or "").strip()
    summary = summary if isinstance(summary, dict) else {}
    if raw_mode == "catalog_apply_admin_only" and _coerce_bool_stat(summary.get("qbo_write_blocked")):
        return "Catalog apply blocked"
    return INVENTORY_MODE_LABELS.get(raw_mode, raw_mode.replace("_", " ").title() if raw_mode else "")


def _inventory_mode_context(job: RunJob, artifacts_list: list[RunArtifact]) -> dict[str, object] | None:
    if job.scope not in {RunJob.SCOPE_INVENTORY_PIPELINE, RunJob.SCOPE_INVENTORY_SYNC}:
        return None
    opts = job.inventory_options_json if isinstance(job.inventory_options_json, dict) else {}
    summary: dict = {}
    for artifact in artifacts_list:
        if _is_inventory_artifact(artifact):
            summary = _inventory_summary_from_artifact(artifact)
            if summary:
                break
    mode = str(summary.get("inventory_mode") or opts.get("mode") or "").strip()
    if not mode:
        return None
    return {
        "inventory_mode": mode,
        "mode_label": _inventory_mode_label(mode, summary),
        "write_intent": summary.get("write_intent") or INVENTORY_MODE_WRITE_INTENT_LABELS.get(mode, ""),
        "qbo_write_attempted": _coerce_bool_stat(summary.get("qbo_write_attempted")),
        "qbo_write_blocked": _coerce_bool_stat(summary.get("qbo_write_blocked")),
        "catalog_apply_enabled": _coerce_bool_stat(summary.get("catalog_apply_enabled")),
        "quantity_apply_enabled": _coerce_bool_stat(summary.get("quantity_apply_enabled")),
        "missing_item_create_enabled": _coerce_bool_stat(summary.get("missing_item_create_enabled")),
    }


def _has_non_in_sync_inventory_rows(summary: dict) -> bool:
    counts = summary.get("final_status_counts") if isinstance(summary.get("final_status_counts"), dict) else {}
    for status, raw_count in counts.items():
        if str(status) == "in_sync":
            continue
        if _safe_int_stat({str(status): raw_count}, str(status)) > 0:
            return True
    return False


def _sales_status_for_company(
    *,
    latest_job: RunJob | None,
    latest_artifact: RunArtifact | None,
    reconcile_statuses_by_job: dict,
) -> dict:
    last_run = _run_status_time(latest_job) or _artifact_status_time(latest_artifact)
    if latest_job is None and latest_artifact is None:
        return {
            "label": "No successful sales sync recorded",
            "severity": "unknown",
            "last_run": None,
            "subtext": "",
        }
    if latest_job and latest_job.status == RunJob.STATUS_FAILED:
        return {
            "label": "Failed",
            "severity": "critical",
            "last_run": last_run,
            "subtext": latest_job.failure_reason or "Latest sales run failed.",
        }
    if latest_job and latest_job.status == RunJob.STATUS_RUNNING and latest_artifact is None:
        return {
            "label": "Running",
            "severity": "unknown",
            "last_run": last_run,
            "subtext": "Sales sync is running.",
        }
    if latest_job and latest_job.status == RunJob.STATUS_QUEUED and latest_artifact is None:
        return {
            "label": "Queued",
            "severity": "unknown",
            "last_run": last_run,
            "subtext": "Sales sync is queued.",
        }

    statuses = []
    if latest_job:
        statuses = reconcile_statuses_by_job.get(str(latest_job.id), [])
    elif latest_artifact:
        statuses = [latest_artifact.reconcile_status or ""]
    reconcile_label = _reconciliation_label_for_job("sales", {"sales": statuses})
    if reconcile_label == "Match":
        return {
            "label": "Reconciled",
            "severity": "healthy",
            "last_run": last_run,
            "subtext": "",
        }
    subtext = "Reconciliation mismatch." if reconcile_label == "Mismatch" else "No reconciliation artifact found."
    return {
        "label": "Not reconciled",
        "severity": "warning",
        "last_run": last_run,
        "subtext": subtext,
    }


def _inventory_status_for_company(
    *,
    latest_job: RunJob | None,
    latest_artifact: RunArtifact | None,
) -> dict:
    last_sync = _run_status_time(latest_job) or _artifact_status_time(latest_artifact)
    if latest_job is None and latest_artifact is None:
        return {
            "label": "Not checked",
            "severity": "unknown",
            "last_sync": None,
            "products_checked": 0,
            "blocked_items": 0,
            "updates_applied": 0,
            "subtext": "",
        }
    if latest_job and latest_job.status == RunJob.STATUS_FAILED:
        return {
            "label": "Failed",
            "severity": "critical",
            "last_sync": last_sync,
            "products_checked": 0,
            "blocked_items": 0,
            "updates_applied": 0,
            "subtext": latest_job.failure_reason or "Latest inventory sync failed.",
        }
    if latest_job and latest_job.status == RunJob.STATUS_RUNNING:
        return {
            "label": "Running",
            "severity": "warning",
            "last_sync": last_sync,
            "products_checked": 0,
            "blocked_items": 0,
            "updates_applied": 0,
            "subtext": "Inventory sync is running.",
        }
    if latest_job and latest_job.status == RunJob.STATUS_QUEUED:
        return {
            "label": "Queued",
            "severity": "warning",
            "last_sync": last_sync,
            "products_checked": 0,
            "blocked_items": 0,
            "updates_applied": 0,
            "subtext": "Inventory sync is queued.",
        }

    summary = _inventory_summary_from_artifact(latest_artifact)
    inventory_mode = str(summary.get("inventory_mode") or "").strip()
    mode_label = _inventory_mode_label(inventory_mode, summary)
    products_checked = _safe_int_stat(summary, "products_checked")
    in_sync = _safe_int_stat(summary, "in_sync", _safe_int_stat(summary, "already_correct"))
    blocked = _safe_int_stat(summary, "blocked_items")
    still_needs_review = _safe_int_stat(summary, "still_needs_review")
    updates = (
        _safe_int_stat(summary, "catalog_fixes_applied")
        + _safe_int_stat(summary, "base_items_created")
        + _safe_int_stat(summary, "duplicate_base_items_resolved")
        + _safe_int_stat(summary, "quantity_updates_applied")
    )
    needs_review = (
        blocked > 0
        or still_needs_review > 0
        or _has_non_in_sync_inventory_rows(summary)
        or (products_checked > 0 and in_sync < products_checked)
    )
    clean = products_checked > 0 and in_sync == products_checked and blocked == 0 and still_needs_review == 0
    if needs_review:
        return {
            "label": "Needs review",
            "severity": "warning",
            "last_sync": last_sync,
            "products_checked": products_checked,
            "blocked_items": blocked,
            "updates_applied": updates,
            "subtext": "",
        }
    if clean:
        return {
            "label": mode_label or "In sync",
            "severity": "healthy",
            "last_sync": last_sync,
            "products_checked": products_checked,
            "blocked_items": blocked,
            "updates_applied": updates,
            "subtext": f"{updates} updates applied" if updates > 0 else "",
            "inventory_mode": inventory_mode,
        }
    return {
        "label": "Not checked",
        "severity": "unknown",
        "last_sync": last_sync,
        "products_checked": products_checked,
        "blocked_items": blocked,
        "updates_applied": updates,
        "subtext": "No inventory summary found.",
    }


def _inventory_review_required(inventory_enabled: bool, inventory_status: dict) -> bool:
    return bool(inventory_enabled and str(inventory_status.get("label") or "") == "Needs review")


def _inventory_review_action_label(inventory_status: dict) -> str:
    blocked = _safe_int_stat(inventory_status, "blocked_items")
    if blocked > 0:
        return f"Review {blocked} item{'s' if blocked != 1 else ''}"
    return "Review inventory"


def _company_card_status(
    sales_status: dict,
    inventory_status: dict,
    token_info: dict,
    *,
    inventory_enabled: bool = True,
) -> str:
    sales_level = str(sales_status.get("severity") or "unknown")
    inventory_level = str(inventory_status.get("severity") or "unknown")
    token_level = str(token_info.get("severity") or "unknown")
    severities = [
        sales_level,
        token_level,
    ]
    if inventory_enabled:
        severities.append(inventory_level)
    if "critical" in severities:
        return "critical"
    if "warning" in severities:
        return "warning"
    if sales_level == "unknown" or (inventory_enabled and inventory_level == "unknown"):
        return "unknown"
    return "healthy"


def _company_health_snapshot(
    company: CompanyConfigRecord,
    latest_artifact: RunArtifact | None,
    latest_job: RunJob | None,
    token_info: dict | None = None,
    *,
    inventory_enabled: bool | None = None,
    inventory_status: dict | None = None,
) -> dict:
    """Canonical company health classification used by overview/list/detail views."""
    cfg = company.config_json or {}
    epos = cfg.get("epos") or {}
    token_info = token_info or _company_token_health(company)
    if inventory_enabled is None:
        inventory_enabled = _company_inventory_enabled(company)
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
            "summary": latest_job.failure_reason or "Latest sales sync failed.",
            "reason_codes": ["LATEST_RUN_FAILED"],
            "run_activity": run_activity,
        }

    if inventory_enabled and inventory_status:
        inventory_level = str(inventory_status.get("severity") or "")
        if inventory_level == "critical":
            return {
                "level": "critical",
                "summary": inventory_status.get("subtext") or "Latest inventory run failed.",
                "reason_codes": ["INVENTORY_FAILURE"],
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
        if reconcile_diff is not None and abs(reconcile_diff) > portal_settings.get_reconcile_diff_warning():
            return {
                "level": "warning",
                "summary": "Reconciliation mismatch above threshold.",
                "reason_codes": ["RECON_MISMATCH"],
                "run_activity": run_activity,
            }

    if inventory_enabled and inventory_status:
        inventory_level = str(inventory_status.get("severity") or "")
        if inventory_level == "warning":
            return {
                "level": "warning",
                "summary": inventory_status.get("subtext") or "Inventory needs review.",
                "reason_codes": ["INVENTORY_NEEDS_REVIEW"],
                "run_activity": run_activity,
            }
        if inventory_level == "unknown":
            return {
                "level": "unknown",
                "summary": inventory_status.get("subtext") or "Inventory not checked.",
                "reason_codes": ["INVENTORY_NOT_CHECKED"],
                "run_activity": run_activity,
            }

    if not latest_artifact:
        return {
            "level": "unknown",
            "summary": "No successful sales sync recorded.",
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


def _overview_context(revenue_period: str = "7d", company_key: str | None = None) -> dict:
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

    all_companies = list(CompanyConfigRecord.objects.filter(is_active=True).order_by("company_key"))
    selected_company = next(
        (company for company in all_companies if company_key and company.company_key == company_key),
        None,
    )
    companies = [selected_company] if selected_company else all_companies
    company_keys = [company.company_key for company in companies]
    revenue_companies = all_companies
    revenue_company_keys = [company.company_key for company in revenue_companies]
    date_resolution = resolve_overview_target_date(company_keys)
    has_overview_target_date = bool(date_resolution["has_data"])
    target_date = date_resolution["target_date"]
    prev_target_date = date_resolution["prev_target_date"]
    last_successful_sync_at = date_resolution["last_successful_at"]
    if target_date is not None:
        target_date_display = target_date.strftime("%b %d, %Y")
    if prev_target_date is not None:
        prev_target_date_display = prev_target_date.strftime("%b %d")

    latest_sales_artifacts: dict[str, RunArtifact] = {}
    latest_inventory_artifacts: dict[str, RunArtifact] = {}
    sales_job_id_to_company_keys: dict = defaultdict(set)
    inventory_job_id_to_company_keys: dict = defaultdict(set)
    sales_reconcile_statuses_by_company_job: dict = defaultdict(list)

    overview_artifacts = list(
        RunArtifact.objects.filter(company_key__in=company_keys)
        .select_related("run_job")
        .order_by("company_key", "-processed_at", "-imported_at")
    )
    for artifact in overview_artifacts:
        if _is_sales_artifact(artifact):
            if artifact.company_key not in latest_sales_artifacts:
                latest_sales_artifacts[artifact.company_key] = artifact
            if artifact.run_job_id and artifact.company_key:
                sales_job_id_to_company_keys[artifact.run_job_id].add(artifact.company_key)
                sales_reconcile_statuses_by_company_job[
                    (artifact.company_key, str(artifact.run_job_id))
                ].append(artifact.reconcile_status or "")
        elif _is_inventory_artifact(artifact):
            if artifact.company_key not in latest_inventory_artifacts:
                latest_inventory_artifacts[artifact.company_key] = artifact
            if (
                artifact.run_job_id
                and artifact.company_key
                and artifact.run_job
                and artifact.run_job.scope in {RunJob.SCOPE_INVENTORY_PIPELINE, RunJob.SCOPE_INVENTORY_SYNC}
            ):
                inventory_job_id_to_company_keys[artifact.run_job_id].add(artifact.company_key)

    latest_sales_jobs: dict[str, RunJob] = {}
    sales_job_ids_with_artifacts = list(sales_job_id_to_company_keys.keys())
    all_relevant_sales_jobs = list(
        RunJob.objects.filter(
            Q(company_key__in=company_keys, scope__in=[RunJob.SCOPE_SINGLE, RunJob.SCOPE_ALL])
            | Q(id__in=sales_job_ids_with_artifacts)
            | Q(company_key__isnull=True, scope=RunJob.SCOPE_ALL)
        ).order_by("-finished_at", "-started_at", "-created_at")
    )
    for active_only in (True, False):
        for job in all_relevant_sales_jobs:
            if active_only and job.status not in (RunJob.STATUS_RUNNING, RunJob.STATUS_QUEUED):
                continue
            candidates = []
            if job.company_key and job.company_key in company_keys:
                candidates.append(job.company_key)
            elif job.company_key is None and job.scope == RunJob.SCOPE_ALL:
                candidates.extend(company_keys)
            candidates.extend(sales_job_id_to_company_keys.get(job.id, []))
            for ck in set(candidates):
                if ck not in latest_sales_jobs:
                    latest_sales_jobs[ck] = job

    latest_inventory_jobs: dict[str, RunJob] = {}
    inventory_job_ids_with_artifacts = list(inventory_job_id_to_company_keys.keys())
    all_relevant_inventory_jobs = list(
        RunJob.objects.filter(
            Q(
                company_key__in=company_keys,
                scope__in=[RunJob.SCOPE_INVENTORY_PIPELINE, RunJob.SCOPE_INVENTORY_SYNC],
            )
            | Q(id__in=inventory_job_ids_with_artifacts)
        ).order_by("-finished_at", "-started_at", "-created_at")
    )
    for active_only in (True, False):
        for job in all_relevant_inventory_jobs:
            if active_only and job.status not in (RunJob.STATUS_RUNNING, RunJob.STATUS_QUEUED):
                continue
            candidates = []
            if job.company_key and job.company_key in company_keys:
                candidates.append(job.company_key)
            candidates.extend(inventory_job_id_to_company_keys.get(job.id, []))
            for ck in set(candidates):
                if ck not in latest_inventory_jobs:
                    latest_inventory_jobs[ck] = job

    latest_activity_jobs: dict[str, RunJob] = {}
    activity_job_ids_with_artifacts = list(
        set(sales_job_ids_with_artifacts + inventory_job_ids_with_artifacts)
    )
    all_relevant_activity_jobs = list(
        RunJob.objects.filter(
            Q(company_key__in=company_keys)
            | Q(id__in=activity_job_ids_with_artifacts)
            | Q(company_key__isnull=True, scope=RunJob.SCOPE_ALL)
        ).order_by("-finished_at", "-started_at", "-created_at")
    )
    for active_only in (True, False):
        for job in all_relevant_activity_jobs:
            if active_only and job.status not in (RunJob.STATUS_RUNNING, RunJob.STATUS_QUEUED):
                continue
            candidates = []
            if job.company_key and job.company_key in company_keys:
                candidates.append(job.company_key)
            elif job.company_key is None and job.scope == RunJob.SCOPE_ALL:
                candidates.extend(company_keys)
            candidates.extend(sales_job_id_to_company_keys.get(job.id, []))
            candidates.extend(inventory_job_id_to_company_keys.get(job.id, []))
            for ck in set(candidates):
                if ck not in latest_activity_jobs:
                    latest_activity_jobs[ck] = job

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
        latest_sales_artifact = latest_sales_artifacts.get(company.company_key)
        latest_sales_job = latest_sales_jobs.get(company.company_key)
        latest_inventory_artifact = latest_inventory_artifacts.get(company.company_key)
        latest_inventory_job = latest_inventory_jobs.get(company.company_key)
        latest_activity_job = latest_activity_jobs.get(company.company_key)
        capabilities = _company_capabilities(company)
        inventory_enabled = capabilities["inventory"]
        realm_id = ((company.config_json or {}).get("qbo") or {}).get("realm_id")
        preloaded_tokens = token_batch.get((company.company_key, realm_id)) if realm_id else None
        token_info = _company_token_health(company, tokens=preloaded_tokens)
        sales_status = _sales_status_for_company(
            latest_job=latest_sales_job,
            latest_artifact=latest_sales_artifact,
            reconcile_statuses_by_job={
                str(latest_sales_job.id): sales_reconcile_statuses_by_company_job.get(
                    (company.company_key, str(latest_sales_job.id))
                    if latest_sales_job
                    else (company.company_key, "")
                )
                or []
            } if latest_sales_job else {},
        )
        inventory_status = _inventory_status_for_company(
            latest_job=latest_inventory_job,
            latest_artifact=latest_inventory_artifact,
        )
        inventory_review_required = _inventory_review_required(inventory_enabled, inventory_status)
        health = _company_health_snapshot(
            company,
            latest_artifact=latest_sales_artifact,
            latest_job=latest_sales_job,
            token_info=token_info,
            inventory_enabled=inventory_enabled,
            inventory_status=inventory_status,
        )
        status = _company_card_status(
            sales_status,
            inventory_status,
            token_info,
            inventory_enabled=inventory_enabled,
        )
        summary = health["summary"]
        run_activity = _run_activity_display(_run_activity_status(latest_activity_job))
        health_reason_labels = _health_reason_labels(health.get("reason_codes"))
        show_summary = health["level"] == status and _should_show_company_summary(
            status,
            summary,
            health_reason_labels,
        )
        latest_activity = _latest_activity_snapshot(
            latest_activity_job=latest_activity_job,
            latest_sales_artifact=latest_sales_artifact,
            latest_inventory_artifact=latest_inventory_artifact,
        )
        last_run_time = latest_activity["at"]

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
                "latest_activity_job": latest_activity_job,
                "latest_activity_label": latest_activity["label"],
                "latest_activity_display": latest_activity["display"],
                "status": status,
                "health": health,
                "run_activity": run_activity,
                "health_reason_labels": health_reason_labels,
                "token_info": token_info,
                "token_status": token_info,
                "sales_status": sales_status,
                "latest_sales_job": latest_sales_job,
                "latest_sales_artifact": latest_sales_artifact,
                "latest_sales_sync_display": _sales_sync_display(latest_sales_artifact),
                "inventory_enabled": inventory_enabled,
                "capabilities": capabilities,
                "inventory_status": inventory_status,
                "inventory_review_required": inventory_review_required,
                "inventory_review_label": _inventory_review_action_label(inventory_status),
                "inventory_review_url": reverse(
                    "epos_qbo:company_inventory_review",
                    kwargs={"company_key": company.company_key},
                ) if inventory_enabled else "",
                "latest_inventory_job": latest_inventory_job,
                "latest_inventory_artifact": latest_inventory_artifact,
                "records_synced": latest_sales_artifact.rows_kept if latest_sales_artifact else 0,
                "summary": summary,
                "show_summary": show_summary,
                "last_run_reconciliation_warning": None,
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
    revenue_series_map = {company.company_key: [0.0] * len(revenue_dates) for company in revenue_companies}
    revenue_totals_by_company = {company.company_key: 0.0 for company in revenue_companies}
    latest_reconciled_artifacts: dict[tuple[str, object], RunArtifact] = {}

    if revenue_company_keys and revenue_days > 0 and revenue_start_date is not None and revenue_end_date is not None:
        reconciled_qs = RunArtifact.objects.filter(
            company_key__in=revenue_company_keys,
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
        for company in revenue_companies
    ]
    revenue_company_totals = sorted(
        [
            {
                "company_key": company.company_key,
                "name": company.display_name,
                "total": round(revenue_totals_by_company.get(company.company_key, 0.0), 2),
            }
            for company in revenue_companies
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
        "revenue_company_options": [
            {"company_key": company.company_key, "name": company.display_name}
            for company in revenue_companies
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
        "overview_company_options": [{"value": "", "label": "All companies"}] + [{"value": c.company_key, "label": c.display_name} for c in all_companies],
        "overview_selected_company": selected_company.company_key if selected_company else "",
    }


@login_required
def overview(request):
    _ensure_company_records()
    revenue_period_param = request.GET.get("revenue_period")
    company_param = request.GET.get("company")
    if (revenue_period_param or "").strip() or (company_param or "").strip():
        revenue_period = _normalize_revenue_period(revenue_period_param)
        company_key = (company_param or "").strip() or None
    else:
        company_key, revenue_period = _get_user_overview_defaults(request)
    context = _overview_context(revenue_period, company_key=company_key)
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
    revenue_period_param = request.GET.get("revenue_period")
    company_param = request.GET.get("company")
    if (revenue_period_param or "").strip() or (company_param or "").strip():
        revenue_period = _normalize_revenue_period(revenue_period_param)
        company_key = (company_param or "").strip() or None
    else:
        company_key, revenue_period = _get_user_overview_defaults(request)
    context = _overview_context(revenue_period, company_key=company_key)
    response = render(request, "components/overview_refresh.html", context)
    response["Cache-Control"] = "no-store"
    response["Pragma"] = "no-cache"
    return response


def _scheduler_env_for_display():
    """Build read-only scheduler/env key-value dict for Settings page."""
    return {
        "OIAT_SCHEDULER_POLL_SECONDS": os.environ.get("OIAT_SCHEDULER_POLL_SECONDS", "(default 15)"),
        "OIAT_SCHEDULER_ENABLE_ENV_FALLBACK": os.environ.get("OIAT_SCHEDULER_ENABLE_ENV_FALLBACK", "(default 1)"),
        "OIAT_BUSINESS_TIMEZONE": getattr(settings, "OIAT_BUSINESS_TIMEZONE", os.environ.get("OIAT_BUSINESS_TIMEZONE", "(default Africa/Lagos)")),
        "OIAT_BUSINESS_DAY_CUTOFF_HOUR": os.environ.get("OIAT_BUSINESS_DAY_CUTOFF_HOUR") or getattr(settings, "OIAT_BUSINESS_DAY_CUTOFF_HOUR", "(default 5)"),
        "OIAT_BUSINESS_DAY_CUTOFF_MINUTE": os.environ.get("OIAT_BUSINESS_DAY_CUTOFF_MINUTE") or getattr(settings, "OIAT_BUSINESS_DAY_CUTOFF_MINUTE", "(default 0)"),
        "SCHEDULE_CRON": os.environ.get("SCHEDULE_CRON", "(default 0 18 * * *)"),
        "SCHEDULE_TZ": os.environ.get("SCHEDULE_TZ", "(default from OIAT_BUSINESS_TIMEZONE)"),
    }


@login_required
def settings_page(request):
    """Settings page: portal defaults (editable with can_manage_portal_settings), my preferences, dashboard and scheduler read-only."""
    can_edit_portal = request.user.has_perm("epos_qbo.can_manage_portal_settings")
    portal_form = None
    user_prefs_form = None

    active_companies = list(CompanyConfigRecord.objects.filter(is_active=True).order_by("display_name"))
    if request.method == "POST":
        if request.POST.get("save_portal") and not can_edit_portal:
            return HttpResponseForbidden("You do not have permission to edit portal defaults.")
        if can_edit_portal and request.POST.get("save_portal"):
            portal_form = PortalSettingsForm(request.POST)
            if portal_form.is_valid():
                portal_form.save(request.user)
                messages.success(request, "Portal defaults saved.")
                return redirect("epos_qbo:settings")
        elif request.POST.get("save_preferences"):
            user_prefs_form = UserPreferencesForm(request.POST)
            user_prefs_form.fields["default_overview_company_key"].choices = [("", "All companies")] + [(c.company_key, c.display_name) for c in active_companies]
            if user_prefs_form.is_valid():
                user_prefs_form.save(request.user)
                messages.success(request, "Your preferences saved.")
                return redirect("epos_qbo:settings")

    if portal_form is None:
        row = PortalSettings.objects.filter(pk=1).first()
        initial_portal = {}
        if row:
            initial_portal = {
                "default_parallel": row.default_parallel,
                "default_stagger_seconds": row.default_stagger_seconds,
                "stale_hours_warning": row.stale_hours_warning,
                "refresh_expiring_days": row.refresh_expiring_days,
                "reconcile_diff_warning": row.reconcile_diff_warning,
                "reauth_guidance": row.reauth_guidance or "",
                "dashboard_timezone": row.dashboard_timezone or "",
            }
        portal_form = PortalSettingsForm(initial=initial_portal)

    if user_prefs_form is None:
        try:
            pref = DashboardUserPreference.objects.get(user=request.user)
            initial_prefs = {
                "default_revenue_period": pref.default_revenue_period or "7d",
                "default_overview_company_key": pref.default_overview_company_key or "",
            }
        except DashboardUserPreference.DoesNotExist:
            initial_prefs = {"default_revenue_period": "7d", "default_overview_company_key": ""}
        user_prefs_form = UserPreferencesForm(initial=initial_prefs)

    user_prefs_form.fields["default_overview_company_key"].choices = [("", "All companies")] + [(c.company_key, c.display_name) for c in active_companies]
    context = {
        "can_edit_portal": can_edit_portal,
        "portal_form": portal_form,
        "user_prefs_form": user_prefs_form,
        "dashboard_timezone_display": get_dashboard_timezone_display(),
        "dashboard_timezone_name": portal_settings.get_dashboard_timezone_name(),
        "default_parallel": portal_settings.get_default_parallel(),
        "default_stagger_seconds": portal_settings.get_default_stagger_seconds(),
        "stale_hours_warning": portal_settings.get_stale_hours_warning(),
        "refresh_expiring_days": portal_settings.get_refresh_expiring_days(),
        "reconcile_diff_warning": portal_settings.get_reconcile_diff_warning(),
        "reauth_guidance": portal_settings.get_reauth_guidance(),
        "scheduler_env": _scheduler_env_for_display(),
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


def _safe_zoneinfo(name: str | None) -> ZoneInfo:
    raw = (name or "").strip()
    if not raw:
        return ZoneInfo("UTC")
    try:
        return ZoneInfo(raw)
    except Exception:
        return ZoneInfo("UTC")


def _format_dt_fallback_utc(value: datetime) -> str:
    dt = value
    if timezone.is_naive(dt):
        dt = timezone.make_aware(dt, timezone.utc)
    dt_utc = dt.astimezone(timezone.utc)
    return dt_utc.strftime("%Y-%m-%d %H:%M UTC")


def _human_day_label(*, local_dt: datetime, now_local: datetime) -> str:
    d = local_dt.date()
    today = now_local.date()
    if d == today:
        return "Today"
    if d == today + timedelta(days=1):
        return "Tomorrow"
    if d == today - timedelta(days=1):
        return "Yesterday"
    if abs((d - today).days) <= 6:
        return local_dt.strftime("%A")
    return local_dt.strftime("%b %d, %Y")


def _format_local_datetime_label(
    value: datetime | None,
    *,
    tz_name: str,
    now_utc: datetime,
    include_tz_suffix: bool = True,
    prefer_tz_abbrev: bool = True,
) -> str:
    """Format a datetime in the schedule's timezone for operator display."""
    if value is None:
        return "—"
    try:
        dt = value
        if timezone.is_naive(dt):
            dt = timezone.make_aware(dt, timezone.utc)
        tz = _safe_zoneinfo(tz_name)
        local_dt = dt.astimezone(tz)
        now_local = now_utc.astimezone(tz)
        day = _human_day_label(local_dt=local_dt, now_local=now_local)
        time_part = local_dt.strftime("%H:%M")
        tz_abbrev = local_dt.strftime("%Z").strip()
        tz_suffix = ""
        if include_tz_suffix:
            if prefer_tz_abbrev and tz_abbrev:
                tz_suffix = f" {tz_abbrev}"
            else:
                tz_suffix = f" {tz_name}".strip() if tz_name else " UTC"
        return f"{day} at {time_part}{tz_suffix}".strip()
    except Exception:
        return _format_dt_fallback_utc(value)


def _format_schedule_next_run(schedule: RunSchedule, *, now_utc: datetime) -> str:
    if not schedule.enabled:
        return "—"
    if schedule.is_one_time and schedule.completed_at is not None:
        return "—"
    if schedule.next_fire_at is None:
        return "—"
    return _format_local_datetime_label(
        schedule.next_fire_at,
        tz_name=schedule.timezone_name,
        now_utc=now_utc,
        include_tz_suffix=True,
        prefer_tz_abbrev=True,
    )


def _format_one_time_timing(schedule: RunSchedule, *, now_utc: datetime) -> tuple[str, str, str]:
    """Return (primary, secondary, detail) for one-time schedule timing column."""
    tz_name = schedule.timezone_name or "UTC"
    if schedule.completed_at is not None:
        completed = _format_local_datetime_label(
            schedule.completed_at,
            tz_name=tz_name,
            now_utc=now_utc,
            include_tz_suffix=False,
        )
        primary = f"Completed {completed.lower()}"
        return primary, f"{tz_name} · Ran once", ""
    if not schedule.enabled:
        when = _format_local_datetime_label(
            schedule.run_once_at,
            tz_name=tz_name,
            now_utc=now_utc,
            include_tz_suffix=False,
        )
        return "Disabled one-time run", f"{tz_name} · Scheduled for {when}", ""
    when = _format_local_datetime_label(
        schedule.run_once_at or schedule.next_fire_at,
        tz_name=tz_name,
        now_utc=now_utc,
        include_tz_suffix=False,
    )
    return when, f"{tz_name} · Run once", ""


def _format_recurring_timing(schedule: RunSchedule) -> tuple[str, str, str]:
    primary = _friendly_cron_label(schedule)
    secondary = schedule.timezone_name or "UTC"
    detail = f"Cron: {schedule.cron_expr}" if schedule.cron_expr else "Cron: —"
    return primary, secondary, detail


def _local_input_date_time(
    value: datetime | None,
    *,
    tz_name: str,
) -> tuple[str, str]:
    """Return (YYYY-MM-DD, HH:MM) in schedule timezone for form input values."""
    if value is None:
        return "", ""
    try:
        dt = value
        if timezone.is_naive(dt):
            dt = timezone.make_aware(dt, timezone.utc)
        tz = _safe_zoneinfo(tz_name)
        local_dt = dt.astimezone(tz)
        return local_dt.strftime("%Y-%m-%d"), local_dt.strftime("%H:%M")
    except Exception:
        # Fallback to UTC representation (still safe for form inputs)
        dt = value
        if timezone.is_naive(dt):
            dt = timezone.make_aware(dt, timezone.utc)
        utc_dt = dt.astimezone(timezone.utc)
        return utc_dt.strftime("%Y-%m-%d"), utc_dt.strftime("%H:%M")


def _schedule_create_initial() -> dict:
    return {
        "enabled": True,
        "schedule_type": RunSchedule.SCHEDULE_TYPE_RECURRING,
        "workflow": RunScheduleForm.WORKFLOW_SALES,
        "company_target": RunScheduleForm.COMPANY_TARGET_ALL,
        "scope": RunJob.SCOPE_ALL,
        "cron_expr": "0 18 * * *",
        "timezone_name": _schedule_default_timezone_name(),
        "target_date_mode": RunSchedule.TARGET_DATE_MODE_TRADING_DATE,
        "parallel": portal_settings.get_default_parallel(),
        "stagger_seconds": portal_settings.get_default_stagger_seconds(),
        "continue_on_failure": False,
    }


def _operator_schedule_name(schedule: RunSchedule) -> str:
    if schedule.name == "All Companies Daily Run":
        return "Daily Sales Sync"
    if schedule.name == "Legacy Env Fallback":
        return "System Fallback Schedule"
    return schedule.name


def _inventory_options(schedule: RunSchedule) -> dict:
    return schedule.inventory_options_json if isinstance(schedule.inventory_options_json, dict) else {}


def _first_inventory_category(schedule: RunSchedule) -> str:
    categories = _inventory_options(schedule).get("categories") or []
    if isinstance(categories, str):
        return categories.strip()
    if isinstance(categories, list) and categories:
        return str(categories[0] or "").strip()
    return ""


def _inventory_product_filter(schedule: RunSchedule) -> str:
    return str(_inventory_options(schedule).get("product_filter") or "").strip()


def _company_display(company_map: dict[str, str], company_key: str | None) -> str:
    key = (company_key or "").strip()
    if not key:
        return ""
    return company_map.get(key) or key


def _schedule_subtitle(schedule: RunSchedule, company_map: dict[str, str]) -> str:
    if schedule.name == "Legacy Env Fallback" and schedule.is_system_managed:
        return "Legacy environment configuration"
    if schedule.scope == RunJob.SCOPE_ALL:
        return "Sales Sync · All eligible companies"
    if schedule.scope == RunJob.SCOPE_SINGLE:
        company = _company_display(company_map, schedule.company_key)
        return f"Sales Sync · {company}" if company else "Sales Sync · One company"
    if schedule.scope == RunJob.SCOPE_INVENTORY_PIPELINE:
        parts = ["Inventory Sync"]
        company = _company_display(company_map, schedule.company_key)
        if company:
            parts.append(company)
        category = _first_inventory_category(schedule)
        product = _inventory_product_filter(schedule)
        if category:
            parts.append(f"Category: {category}")
        if product:
            parts.append(f"Product: {product}")
        if not category and not product:
            parts.append("All products")
        return " · ".join(parts) if parts else "Inventory"
    return schedule.get_scope_display()


def _schedule_workflow(schedule: RunSchedule) -> str:
    if schedule.scope == RunJob.SCOPE_INVENTORY_PIPELINE:
        return RunScheduleForm.WORKFLOW_INVENTORY
    return RunScheduleForm.WORKFLOW_SALES


def _schedule_company_target(schedule: RunSchedule) -> str:
    if schedule.scope == RunJob.SCOPE_ALL:
        return RunScheduleForm.COMPANY_TARGET_ALL
    return RunScheduleForm.COMPANY_TARGET_ONE


def _schedule_workflow_label(schedule: RunSchedule) -> str:
    if schedule.scope == RunJob.SCOPE_INVENTORY_PIPELINE:
        return "Inventory Sync"
    return "Sales Sync"


def _friendly_cron_label(schedule: RunSchedule) -> str:
    if schedule.is_one_time:
        if schedule.completed_at:
            return "Completed"
        return "One-time run"
    parts = (schedule.cron_expr or "").split()
    if len(parts) != 5:
        return "Custom schedule"
    minute, hour, day, month, weekday = parts
    if not minute.isdigit() or not hour.isdigit():
        return "Custom schedule"
    minute_int = int(minute)
    hour_int = int(hour)
    if not (0 <= minute_int <= 59 and 0 <= hour_int <= 23):
        return "Custom schedule"
    time_label = f"{hour_int:02d}:{minute_int:02d}"
    weekday_labels = {
        "0": "Sunday",
        "7": "Sunday",
        "1": "Monday",
        "2": "Tuesday",
        "3": "Wednesday",
        "4": "Thursday",
        "5": "Friday",
        "6": "Saturday",
    }
    if day == "*" and month == "*" and weekday == "*":
        return f"Daily at {time_label}"
    if day == "*" and month == "*" and weekday in weekday_labels:
        return f"Weekly on {weekday_labels[weekday]} at {time_label}"
    return "Custom schedule"


def _last_result_label_from_event(event: RunScheduleEvent) -> str:
    if event.event_type == RunScheduleEvent.TYPE_RUN_SUCCEEDED:
        return "Succeeded"
    if event.event_type in {RunScheduleEvent.TYPE_RUN_FAILED, RunScheduleEvent.TYPE_ERROR}:
        return "Failed"
    if event.event_type in {
        RunScheduleEvent.TYPE_SKIPPED_OVERLAP,
        RunScheduleEvent.TYPE_SKIPPED_INVALID,
    }:
        return "Skipped"
    return "Queued"


def _last_result_label_from_value(value: str) -> str:
    labels = {
        RunSchedule.LAST_RESULT_QUEUED: "Queued",
        RunSchedule.LAST_RESULT_SUCCEEDED: "Succeeded",
        RunSchedule.LAST_RESULT_FAILED: "Failed",
        RunSchedule.LAST_RESULT_CANCELLED: "Skipped",
        RunSchedule.LAST_RESULT_SKIPPED_OVERLAP: "Skipped",
        RunSchedule.LAST_RESULT_SKIPPED_INVALID: "Skipped",
        RunSchedule.LAST_RESULT_ERROR: "Failed",
    }
    return labels.get(value or "", "Never run")


def _schedule_last_result(schedule: RunSchedule) -> dict:
    terminal_event = (
        schedule.events.select_related("run_job")
        .filter(
            event_type__in=[
                RunScheduleEvent.TYPE_RUN_SUCCEEDED,
                RunScheduleEvent.TYPE_RUN_FAILED,
                RunScheduleEvent.TYPE_SKIPPED_OVERLAP,
                RunScheduleEvent.TYPE_SKIPPED_INVALID,
                RunScheduleEvent.TYPE_ERROR,
            ]
        )
        .order_by("-created_at")
        .first()
    )
    active_job = (
        schedule.scheduled_jobs.filter(status__in=[RunJob.STATUS_QUEUED, RunJob.STATUS_RUNNING])
        .order_by("-created_at")
        .first()
    )
    if terminal_event is not None:
        run_job = terminal_event.run_job
        last_run_at = (
            run_job.finished_at
            if run_job is not None and run_job.finished_at is not None
            else terminal_event.created_at
        )
        return {
            "label": _last_result_label_from_event(terminal_event),
            "last_run_at": last_run_at,
            "current": active_job.status.capitalize() if active_job is not None else "",
        }
    if active_job is not None:
        return {
            "label": active_job.status.capitalize(),
            "last_run_at": None,
            "current": active_job.status.capitalize(),
        }
    return {
        "label": _last_result_label_from_value(schedule.last_result),
        "last_run_at": schedule.last_fired_at if schedule.last_result else None,
        "current": "",
    }


def _schedule_rows(schedules: list[RunSchedule], company_map: dict[str, str]) -> list[dict]:
    rows: list[dict] = []
    now_utc = timezone.now()
    for schedule in schedules:
        result = _schedule_last_result(schedule)
        one_time_completed = schedule.is_one_time and schedule.completed_at is not None
        status_subtext = ""
        if one_time_completed:
            status_subtext = "Ran once · Disabled automatically"
        elif not schedule.enabled and schedule.is_one_time:
            status_subtext = "One-time run not completed"
        if schedule.is_one_time:
            timing_primary, timing_secondary, timing_detail = _format_one_time_timing(
                schedule,
                now_utc=now_utc,
            )
        else:
            timing_primary, timing_secondary, timing_detail = _format_recurring_timing(schedule)
        run_once_date_value = ""
        run_once_time_value = ""
        if schedule.is_one_time:
            run_once_date_value, run_once_time_value = _local_input_date_time(
                schedule.run_once_at,
                tz_name=schedule.timezone_name,
            )
        rows.append(
            {
                "schedule": schedule,
                "display_name": _operator_schedule_name(schedule),
                "subtitle": _schedule_subtitle(schedule, company_map),
                "workflow": _schedule_workflow(schedule),
                "company_target": _schedule_company_target(schedule),
                "timing_primary": timing_primary,
                "timing_secondary": timing_secondary,
                "timing_detail": timing_detail,
                "next_run_label": _format_schedule_next_run(schedule, now_utc=now_utc),
                "last_result_label": result["label"],
                "last_run_at": result["last_run_at"],
                "current_status_label": result["current"],
                "one_time_completed": one_time_completed,
                "status_subtext": status_subtext,
                "category": _first_inventory_category(schedule),
                "product_filter": _inventory_product_filter(schedule),
                "run_once_date_value": run_once_date_value,
                "run_once_time_value": run_once_time_value,
            }
        )
    return rows


def _group_schedule_rows(rows: list[dict]) -> list[dict]:
    active: list[dict] = []
    disabled: list[dict] = []

    for row in rows:
        schedule: RunSchedule = row["schedule"]
        if schedule.enabled:
            active.append(row)
        else:
            disabled.append(row)

    return [
        {"title": "Enabled", "rows": active, "empty_message": "No enabled schedules.", "show_when_empty": True},
        {"title": "Disabled", "rows": disabled, "empty_message": "No disabled schedules.", "show_when_empty": False},
    ]


def _form_error_text(form: RunScheduleForm) -> str:
    parts: list[str] = []
    for field_name, errors in form.errors.items():
        label = "General" if field_name == "__all__" else field_name
        joined = ", ".join([str(err) for err in errors])
        parts.append(f"{label}: {joined}")
    return "; ".join(parts)


def _operator_schedule_form_error_message(form: RunScheduleForm) -> str:
    """Operator-friendly schedule form error banner message.

    Avoid raw field names like `company_target` / `workflow` in the top alert.
    """
    flat_errors = " ".join([" ".join([str(e) for e in errs]) for errs in form.errors.values()]).strip()
    if "Inventory all-companies schedules are not supported yet." in flat_errors:
        return (
            "Inventory Sync currently supports one inventory-enabled company. "
            'Select "One company" and choose a company.'
        )
    # Default: show only error text, without field names
    if not flat_errors:
        return "Invalid schedule. Please review the fields and try again."
    return f"Invalid schedule. {flat_errors}"


@login_required
@permission_required("epos_qbo.can_manage_schedules", raise_exception=True)
@require_GET
def schedules_page(request):
    _ensure_company_records()
    now_utc = timezone.now()
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
    companies = list(CompanyConfigRecord.objects.filter(is_active=True).order_by("display_name"))
    company_map = {company.company_key: company.display_name for company in companies}
    company_options = [
        {
            "company_key": company.company_key,
            "display_name": company.display_name,
            "inventory_enabled": _company_inventory_enabled(company),
        }
        for company in companies
    ]
    inventory_company_options = [c for c in company_options if c.get("inventory_enabled")]
    inventory_company_default_key = (
        str(inventory_company_options[0]["company_key"])
        if len(inventory_company_options) == 1
        else ""
    )
    default_tz_name = _schedule_default_timezone_name()
    default_tz = _safe_zoneinfo(default_tz_name)
    now_default_local = now_utc.astimezone(default_tz)
    now_default_time = now_default_local.strftime("%H:%M")
    now_default_abbrev = now_default_local.strftime("%Z").strip()
    schedule_rows = _schedule_rows(schedules, company_map)
    context = {
        "schedule_form": RunScheduleForm(initial=_schedule_create_initial()),
        "schedule_rows": schedule_rows,
        "schedule_sections": _group_schedule_rows(schedule_rows),
        "recent_events": recent_events,
        "companies": companies,
        "company_options": company_options,
        "inventory_company_options": inventory_company_options,
        "inventory_company_default_key": inventory_company_default_key,
        "default_schedule_timezone_name": default_tz_name,
        "default_schedule_timezone_now_label": f"{now_default_time} {now_default_abbrev}".strip(),
        "active_run_ids_json": json.dumps([str(run_id) for run_id in active_run_ids]),
        "schedule_target_date_mode": RunSchedule.TARGET_DATE_MODE_TRADING_DATE,
        "single_scope": RunJob.SCOPE_SINGLE,
        "all_scope": RunJob.SCOPE_ALL,
        "inventory_scope": RunJob.SCOPE_INVENTORY_PIPELINE,
        "schedule_type_recurring": RunSchedule.SCHEDULE_TYPE_RECURRING,
        "schedule_type_one_time": RunSchedule.SCHEDULE_TYPE_ONE_TIME,
        "workflow_sales": RunScheduleForm.WORKFLOW_SALES,
        "workflow_inventory": RunScheduleForm.WORKFLOW_INVENTORY,
        "company_target_all": RunScheduleForm.COMPANY_TARGET_ALL,
        "company_target_one": RunScheduleForm.COMPANY_TARGET_ONE,
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
        messages.error(request, _operator_schedule_form_error_message(form))
        return redirect("epos_qbo:schedules")

    schedule: RunSchedule = form.save(commit=False)
    schedule.created_by = request.user
    schedule.updated_by = request.user
    if not schedule.is_one_time:
        schedule.completed_at = None
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

    was_one_time_completed = schedule.is_one_time and schedule.completed_at is not None
    previous_run_once_at = schedule.run_once_at

    form = RunScheduleForm(request.POST, instance=schedule)
    if not form.is_valid():
        messages.error(request, _operator_schedule_form_error_message(form))
        return redirect("epos_qbo:schedules")

    schedule = form.save(commit=False)
    schedule.updated_by = request.user
    if was_one_time_completed and schedule.is_one_time and schedule.enabled:
        # Prevent accidental re-queue of a completed one-time schedule unless the operator
        # explicitly moves the run time forward.
        if schedule.run_once_at is None:
            messages.error(request, "Completed one-time schedules cannot be re-enabled without a new run time.")
            return redirect("epos_qbo:schedules")
        if previous_run_once_at == schedule.run_once_at:
            messages.error(request, "Completed one-time schedules cannot be re-enabled. Edit the run time first.")
            return redirect("epos_qbo:schedules")
        if schedule.run_once_at <= timezone.now():
            messages.error(request, "One-time schedules must be scheduled in the future when re-enabled.")
            return redirect("epos_qbo:schedules")
        schedule.completed_at = None
    if not schedule.is_one_time:
        schedule.completed_at = None
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
        if schedule.is_one_time and schedule.completed_at is not None:
            messages.error(request, "Completed one-time schedules cannot be re-enabled. Edit the run time first.")
            return redirect("epos_qbo:schedules")
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
    if schedule.is_system_managed:
        messages.error(request, "System-managed schedules cannot be run manually.")
        return redirect("epos_qbo:schedules")
    if schedule.scope in {RunJob.SCOPE_SINGLE, RunJob.SCOPE_INVENTORY_PIPELINE} and not (
        schedule.company_key or ""
    ).strip():
        messages.error(request, "Schedule is missing company key.")
        return redirect("epos_qbo:schedules")

    job, result = enqueue_run_for_schedule(schedule, now=timezone.now(), source="manual")
    if job is None and result == RunScheduleEvent.TYPE_SKIPPED_OVERLAP:
        messages.warning(request, "Skipped because another run is active.")
        return redirect("epos_qbo:schedules")
    if job is None:
        messages.error(request, "Could not queue run for schedule.")
        return redirect("epos_qbo:schedules")

    dispatch_next_queued_job()
    job.refresh_from_db()
    if job.status == RunJob.STATUS_RUNNING:
        messages.success(request, f"Scheduled run started: {job.friendly_id}")
        return redirect("epos_qbo:run-detail", job_id=job.id)

    messages.success(request, f"Scheduled run queued: {job.friendly_id}")
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
        if job.scope in {RunJob.SCOPE_INVENTORY_PIPELINE, RunJob.SCOPE_INVENTORY_SYNC}:
            return (
                "Run succeeded but no inventory reports were linked. "
                "Check pipeline logs and reports/inventory_pipeline/."
            )
        return (
            "Run succeeded but no artifacts were linked. "
            "Check pipeline logs and that metadata files exist under Uploaded/."
        )
    if job.scope in {RunJob.SCOPE_INVENTORY_PIPELINE, RunJob.SCOPE_INVENTORY_SYNC}:
        return None
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
    default_parallel = portal_settings.get_default_parallel()
    default_stagger_seconds = portal_settings.get_default_stagger_seconds()
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
    companies = list(CompanyConfigRecord.objects.filter(is_active=True).order_by("display_name"))
    inventory_companies = [company for company in companies if _company_inventory_enabled(company)]
    
    # Get active run IDs for polling
    active_runs = RunJob.objects.filter(
        status__in=[RunJob.STATUS_QUEUED, RunJob.STATUS_RUNNING]
    ).values_list('id', flat=True)[:10]  # Limit to 10 most recent
    
    active_run_ids_list = [str(id) for id in active_runs]

    categories_by_company = load_inventory_categories_by_company(inventory_companies)
    context = {
        "run_rows": run_rows,
        "form": form,
        "companies": companies,
        "inventory_companies": inventory_companies,
        "default_parallel": default_parallel,
        "default_stagger_seconds": default_stagger_seconds,
        "active_run_ids": active_run_ids_list,
        "categories_by_company": categories_by_company,
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
    company_display_name = ""
    if (job.company_key or "").strip():
        record = CompanyConfigRecord.objects.filter(company_key=job.company_key).only("display_name").first()
        company_display_name = record.display_name if record else ""
    artifacts = job.artifacts.order_by("-processed_at", "-imported_at")
    artifacts_list = list(artifacts)
    for artifact in artifacts_list:
        artifact.report_links = _artifact_report_links(job, artifact)
    active_run_ids_list = [str(job.id)] if job.status in [RunJob.STATUS_QUEUED, RunJob.STATUS_RUNNING] else []
    run_upload_summary_message = _run_detail_upload_summary_message(artifacts_list)
    context = {
        "job": job,
        "target_label": job.get_target_label(company_display_name=company_display_name),
        "artifacts": artifacts_list,
        "active_run_ids": active_run_ids_list,
        "active_run_ids_json": json.dumps(active_run_ids_list),
        "exit_code_info": _exit_code_info(job.exit_code),
        "exit_code_reference": EXIT_CODE_REFERENCE,
        "run_attention_message": _run_attention_message(job, artifacts_list),
        "run_upload_summary_message": run_upload_summary_message,
        "inventory_review_action": _run_detail_inventory_review_action_context(job),
        "inventory_mode_context": _inventory_mode_context(job, artifacts_list),
    }
    context.update(_nav_context())
    context.update(
        _breadcrumb_context(
            [
                {"label": "Dashboard", "url": reverse("epos_qbo:overview")},
                {"label": "Runs", "url": reverse("epos_qbo:runs")},
                {"label": f"Run {job.friendly_id}", "url": None},
            ],
            back_url=reverse("epos_qbo:runs"),
            back_label="Runs",
        )
    )
    return render(request, "epos_qbo/run_detail.html", context)


@login_required
@require_GET
def run_artifact_report(request, job_id, artifact_id: int, report_key: str):
    job = get_object_or_404(RunJob, id=job_id)
    artifact = get_object_or_404(RunArtifact, id=artifact_id, run_job=job)
    report_path = _resolve_artifact_report_path(artifact, report_key)
    filename = report_path.name or RUN_ARTIFACT_REPORT_LABELS.get(report_key, "report")
    try:
        handle = report_path.open("rb")
    except OSError as exc:
        raise Http404("Report not found.") from exc
    return FileResponse(handle, as_attachment=True, filename=filename)


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
        parallel=int(cleaned.get("parallel") or portal_settings.get_default_parallel()),
        stagger_seconds=int(cleaned.get("stagger_seconds") or portal_settings.get_default_stagger_seconds()),
        continue_on_failure=bool(cleaned.get("continue_on_failure")),
        requested_by=request.user,
        status=RunJob.STATUS_QUEUED,
    )
    dispatch_next_queued_job()

    job.refresh_from_db()
    if job.status == RunJob.STATUS_RUNNING:
        messages.success(request, f"Run started: {job.friendly_id}")
        return redirect("epos_qbo:run-detail", job_id=job.id)

    messages.info(request, f"Run queued: {job.friendly_id}. It will start automatically.")
    return redirect("epos_qbo:runs")


@login_required
@permission_required("epos_qbo.can_trigger_runs", raise_exception=True)
@require_POST
def trigger_inventory_run(request):
    """Queue the unified inventory pipeline in an explicit safe mode."""
    form = InventoryTriggerForm(request.POST)
    if not form.is_valid():
        messages.error(request, f"Invalid inventory trigger payload: {form.errors.get_json_data()}")
        return redirect("epos_qbo:runs")

    cleaned = form.cleaned_data
    company_key = (cleaned.get("company_key") or "").strip()
    if not CompanyConfigRecord.objects.filter(company_key=company_key).exists():
        messages.error(request, "Unknown company key for inventory.")
        return redirect("epos_qbo:runs")

    mode = (cleaned.get("mode") or "audit_only").strip() or "audit_only"
    inventory_options: dict = {"mode": mode}
    category = (cleaned.get("category") or "").strip()
    if category:
        inventory_options["categories"] = [category]
    product_filter = (cleaned.get("product_filter") or "").strip()
    if product_filter:
        inventory_options["product_filter"] = product_filter

    job = RunJob.objects.create(
        scope=RunJob.SCOPE_INVENTORY_PIPELINE,
        company_key=company_key,
        inventory_options_json=inventory_options,
        requested_by=request.user,
        status=RunJob.STATUS_QUEUED,
    )
    dispatch_next_queued_job()

    job.refresh_from_db()
    mode_label = _inventory_mode_label(mode) or "Inventory run"
    if job.status == RunJob.STATUS_RUNNING:
        messages.success(request, f"{mode_label} started: {job.friendly_id}")
        return redirect("epos_qbo:run-detail", job_id=job.id)

    messages.info(request, f"{mode_label} queued: {job.friendly_id}. It will start automatically.")
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
                "inventory_enabled": _company_inventory_enabled(record),
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
                "aggregate_products": (cfg.get("transform") or {}).get("aggregate_products", False),
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
        "inventory_enabled": _coerce_config_bool(inventory.get("enable_inventory_items")),
        "tax_rate": qbo.get("tax_rate"),
        "deposit_account": qbo.get("deposit_account", "Undeposited Funds"),
        "group_by": ", ".join(transform.get("group_by", ["date", "tender"])),
        "date_format": transform.get("date_format", "%Y-%m-%d"),
        "aggregate_products": transform.get("aggregate_products", False),
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


REVIEW_RETRY_INTENT_LABELS = {
    RETRY_INTENT_CATALOG: "Catalog cleanup retry",
    RETRY_INTENT_QUANTITY: "Quantity adjustment retry",
}


def _run_detail_inventory_review_action_context(job: RunJob) -> dict[str, object] | None:
    """Template context for Inventory Review–triggered runs (retry or missing-item creation)."""
    opts = job.inventory_options_json if isinstance(job.inventory_options_json, dict) else {}
    rcm = opts.get("review_create_missing_items")
    if isinstance(rcm, dict) and rcm:
        raw_audit = str(rcm.get("source_final_audit") or "").strip()
        source_final_audit_name = Path(raw_audit).name if raw_audit else ""
        affected_raw = rcm.get("affected_base_names")
        if not isinstance(affected_raw, list):
            affected_raw = opts.get("base_names") if isinstance(opts.get("base_names"), list) else []
        affected_base_names = [str(x).strip() for x in affected_raw if str(x).strip()]
        preview_limit = 10
        preview_base_names = affected_base_names[:preview_limit]
        has_more_base_names = len(affected_base_names) > preview_limit
        more_base_names_count = max(0, len(affected_base_names) - preview_limit)
        try:
            safe_count = int(rcm.get("safe_count"))
        except (TypeError, ValueError):
            safe_count = len(affected_base_names)
        try:
            blocked_count = int(rcm.get("blocked_count"))
        except (TypeError, ValueError):
            blocked_count = 0
        qty_policy = str(rcm.get("create_qty_policy") or "").strip() or "initial_qty_from_epos"
        mapping_source = str(rcm.get("mapping_source") or "").strip() or "Product.Mapping.csv"
        try:
            max_catalog_fixes = int(opts.get("max_catalog_fixes", 0))
        except (TypeError, ValueError):
            max_catalog_fixes = 0
        try:
            max_quantity_adjustments = int(opts.get("max_quantity_adjustments", 0))
        except (TypeError, ValueError):
            max_quantity_adjustments = 0
        txn_date = str(opts.get("txn_date") or rcm.get("item_inv_start_date") or "").strip()
        txn_date_source = str(rcm.get("txn_date_source") or "").strip()
        category_scope_label = str(rcm.get("category_label") or "").strip() or "All categories"
        try:
            total_in_scope = int(rcm.get("total_candidates_in_scope"))
        except (TypeError, ValueError):
            total_in_scope = safe_count + blocked_count
        missing_create_report_url = ""
        missing_create_report_label = ""
        for art in job.artifacts.order_by("-processed_at", "-imported_at"):
            for link in _artifact_report_links(job, art):
                if link.get("key") == "review_missing_create_report":
                    missing_create_report_url = str(link.get("url") or "")
                    missing_create_report_label = str(link.get("label") or "")
                    break
            if missing_create_report_url:
                break
        return {
            "action_type": "create_missing",
            "intent": str(rcm.get("intent") or REVIEW_CREATE_MISSING_INTENT),
            "intent_label": "Missing item creation",
            "source_artifact_id": rcm.get("source_artifact_id"),
            "source_final_audit": raw_audit,
            "source_final_audit_name": source_final_audit_name or "—",
            "safe_count": safe_count,
            "blocked_count": blocked_count,
            "affected_base_names": affected_base_names,
            "preview_base_names": preview_base_names,
            "has_more_base_names": has_more_base_names,
            "more_base_names_count": more_base_names_count,
            "scope_label": "Safe missing QBO candidates only",
            "category_scope_label": category_scope_label,
            "total_candidates_in_scope": total_in_scope,
            "mapping_source": mapping_source,
            "create_qty_policy": qty_policy,
            "create_qty_policy_label": "Initial QtyOnHand from EPOS expected (no separate adjustment in this run).",
            "max_catalog_fixes": max_catalog_fixes,
            "max_quantity_adjustments": max_quantity_adjustments,
            "item_inv_start_date": txn_date or "—",
            "txn_date_source": txn_date_source,
            "missing_create_report_url": missing_create_report_url,
            "missing_create_report_label": missing_create_report_label,
        }

    review_retry = opts.get("review_retry")
    if not isinstance(review_retry, dict) or not review_retry:
        return None
    intent = str(review_retry.get("intent") or "").strip()
    intent_label = REVIEW_RETRY_INTENT_LABELS.get(intent, intent.replace("_", " ").strip() or "Inventory review retry")
    raw_audit = str(review_retry.get("source_final_audit") or "").strip()
    source_final_audit_name = Path(raw_audit).name if raw_audit else ""
    affected_raw = review_retry.get("affected_base_names")
    if not isinstance(affected_raw, list):
        affected_raw = opts.get("base_names") if isinstance(opts.get("base_names"), list) else []
    affected_base_names = [str(x).strip() for x in affected_raw if str(x).strip()]
    preview_limit = 10
    preview_base_names = affected_base_names[:preview_limit]
    has_more_base_names = len(affected_base_names) > preview_limit
    more_base_names_count = max(0, len(affected_base_names) - preview_limit)
    try:
        affected_count = int(review_retry.get("row_count"))
    except (TypeError, ValueError):
        affected_count = len(affected_base_names)
    try:
        max_catalog_fixes = int(opts.get("max_catalog_fixes", 0))
    except (TypeError, ValueError):
        max_catalog_fixes = 0
    try:
        max_quantity_adjustments = int(opts.get("max_quantity_adjustments", 0))
    except (TypeError, ValueError):
        max_quantity_adjustments = 0
    return {
        "action_type": "retry",
        "intent": intent,
        "intent_label": intent_label,
        "source_artifact_id": review_retry.get("source_artifact_id"),
        "source_final_audit": raw_audit,
        "source_final_audit_name": source_final_audit_name or "—",
        "affected_count": affected_count,
        "affected_base_names": affected_base_names,
        "preview_base_names": preview_base_names,
        "has_more_base_names": has_more_base_names,
        "more_base_names_count": more_base_names_count,
        "max_catalog_fixes": max_catalog_fixes,
        "max_quantity_adjustments": max_quantity_adjustments,
        "scope_label": "Selected base names only",
    }


def _artifact_report_path_value(artifact: RunArtifact, report_key: str) -> str:
    stats = artifact.upload_stats_json if isinstance(artifact.upload_stats_json, dict) else {}
    if report_key == "source":
        return str(artifact.source_path or "").strip()
    if report_key in {"summary_json", "summary_csv"}:
        return str(stats.get(report_key) or "").strip()
    if report_key in {
        "final_audit",
        "initial_audit",
        "catalog_cleanup",
        "post_catalog_audit",
        "review_missing_create_report",
    }:
        child_reports = stats.get("child_reports") if isinstance(stats.get("child_reports"), dict) else {}
        value = str(child_reports.get(report_key) or "").strip()
        if value:
            return value
        summary = _inventory_summary_from_artifact(artifact)
        child_reports = summary.get("child_reports") if isinstance(summary.get("child_reports"), dict) else {}
        return str(child_reports.get(report_key) or "").strip()
    return ""


def _artifact_report_links(job: RunJob, artifact: RunArtifact) -> list[dict[str, str]]:
    if not _is_inventory_artifact(artifact):
        return []

    links: list[dict[str, str]] = []
    for key in RUN_ARTIFACT_REPORT_ORDER:
        raw_path = _artifact_report_path_value(artifact, key)
        if not raw_path:
            continue
        try:
            resolved = _resolve_artifact_report_path(artifact, key)
        except Http404:
            # Don't render broken download buttons for missing/invalid paths.
            continue
        if resolved.suffix.lower() != ".csv":
            continue
        links.append(
            {
                "key": key,
                "label": RUN_ARTIFACT_REPORT_LABELS[key],
                "path": raw_path,
                "filename": resolved.name or RUN_ARTIFACT_REPORT_LABELS[key],
                "url": reverse(
                    "epos_qbo:run-artifact-report",
                    kwargs={"job_id": job.id, "artifact_id": artifact.id, "report_key": key},
                ),
            }
        )
    return links


def _resolve_artifact_report_path(artifact: RunArtifact, report_key: str) -> Path:
    if report_key not in RUN_ARTIFACT_REPORT_LABELS:
        raise Http404("Unknown report.")

    raw_path = _artifact_report_path_value(artifact, report_key)
    if not raw_path or "\x00" in raw_path:
        raise Http404("Report not found.")

    base_dir = Path(settings.BASE_DIR).resolve(strict=False)
    candidate = Path(os.path.expandvars(raw_path)).expanduser()
    resolved = (
        candidate.resolve(strict=False)
        if candidate.is_absolute()
        else (base_dir / candidate).resolve(strict=False)
    )

    if resolved.suffix.lower() not in RUN_ARTIFACT_REPORT_SUFFIXES:
        raise Http404("Report not found.")
    if not any(_path_is_relative_to(resolved, root) for root in _trusted_report_roots()):
        raise Http404("Report not found.")
    if not resolved.exists() or not resolved.is_file():
        raise Http404("Report not found.")
    return resolved


def _latest_inventory_review_artifact(company_key: str) -> RunArtifact | None:
    artifacts = (
        RunArtifact.objects.filter(company_key=company_key)
        .select_related("run_job")
        .order_by("-processed_at", "-imported_at", "-id")
    )
    for artifact in artifacts:
        if not _is_inventory_artifact(artifact):
            continue
        summary = _inventory_summary_from_artifact(artifact)
        if (
            str(summary.get("report_type") or "") == RunJob.SCOPE_INVENTORY_PIPELINE
            or _artifact_report_path_value(artifact, "final_audit")
            or isinstance(summary.get("final_status_counts"), dict)
            or "products_checked" in summary
        ):
            return artifact
    return None


def _format_inventory_review_number(value) -> str:
    try:
        number = Decimal(str(value if value not in (None, "") else 0))
    except Exception:
        return str(value or "0")
    if number == number.to_integral_value():
        return f"{int(number):,}"
    return f"{number:,.2f}".rstrip("0").rstrip(".")


def _inventory_review_reason_counts(rows: list[dict]) -> list[dict]:
    counts: dict[str, int] = defaultdict(int)
    for row in rows:
        slug = str(row.get("reason_group_slug") or "other")
        counts[slug] += 1
    return [
        {"slug": slug, "label": label, "count": counts.get(slug, 0)}
        for slug, label in REASON_GROUPS.items()
        if counts.get(slug, 0) > 0
    ]


def _inventory_review_summary_cards(summary: dict, rows: list[dict], parsed_total_rows: int, parsed_healthy_rows: int) -> dict:
    products_checked = _safe_int_stat(summary, "products_checked")
    if products_checked == 0 and parsed_total_rows:
        products_checked = parsed_total_rows
    in_sync = _safe_int_stat(summary, "in_sync", _safe_int_stat(summary, "already_correct"))
    if in_sync == 0 and parsed_healthy_rows:
        in_sync = parsed_healthy_rows
    blocked = _safe_int_stat(summary, "blocked_items")
    if blocked == 0 and rows:
        blocked = len(rows)
    negative_rows = _safe_int_stat(summary, "epos_negative_rows_clamped")
    negative_units = summary.get("epos_negative_units_clamped", 0)
    return {
        "products_checked": products_checked,
        "products_checked_display": _format_inventory_review_number(products_checked),
        "in_sync": in_sync,
        "in_sync_display": _format_inventory_review_number(in_sync),
        "blocked_items": blocked,
        "blocked_items_display": _format_inventory_review_number(blocked),
        "epos_negative_rows_clamped": negative_rows,
        "epos_negative_rows_clamped_display": _format_inventory_review_number(negative_rows),
        "epos_negative_units_clamped": negative_units,
        "epos_negative_units_clamped_display": _format_inventory_review_number(negative_units),
    }


def _inventory_review_report_links(artifact: RunArtifact | None) -> dict[str, str]:
    if artifact is None or artifact.run_job_id is None or artifact.run_job is None:
        return {}
    return {
        link["key"]: link["url"]
        for link in _artifact_report_links(artifact.run_job, artifact)
    }


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


def _receipt_word(count: int) -> str:
    return "receipt" if count == 1 else "receipts"


def _sales_sync_display(artifact: RunArtifact | None) -> str:
    if artifact is None:
        return "No successful sales sync recorded"
    uploaded = _artifact_uploaded_count(artifact)
    skipped = _artifact_upload_stat(artifact, "skipped") or 0
    if uploaded == 0 and skipped > 0:
        return f"0 uploaded — {skipped} skipped (already in QBO)"
    date_part = (
        f" — {artifact.target_date.strftime('%b')} {artifact.target_date.day}, {artifact.target_date.year}"
        if artifact.target_date
        else ""
    )
    return f"{uploaded} {_receipt_word(uploaded)}{date_part}"


def _inventory_activity_label(job: RunJob | None, artifact: RunArtifact | None) -> str:
    summary = _inventory_summary_from_artifact(artifact)
    opts = job.inventory_options_json if job and isinstance(job.inventory_options_json, dict) else {}
    mode = str(summary.get("inventory_mode") or opts.get("mode") or "").strip()
    if mode:
        return _inventory_mode_label(mode, summary) or "Inventory run"
    apply_stats = summary.get("apply") if isinstance(summary.get("apply"), dict) else {}
    apply_mode = str(apply_stats.get("mode") or "").strip().lower()
    posted = _safe_int_stat(apply_stats, "posted") if apply_stats else 0
    updates = (
        _safe_int_stat(summary, "catalog_fixes_applied")
        + _safe_int_stat(summary, "base_items_created")
        + _safe_int_stat(summary, "duplicate_base_items_resolved")
        + _safe_int_stat(summary, "quantity_updates_applied")
    )
    if apply_mode == "apply" or posted > 0 or updates > 0:
        return "Inventory sync"
    return "Inventory audit"


def _activity_label_for(job: RunJob | None, artifact: RunArtifact | None = None) -> str:
    if job and job.scope in {RunJob.SCOPE_SINGLE, RunJob.SCOPE_ALL}:
        return "Sales sync"
    if job and job.scope in {RunJob.SCOPE_INVENTORY_PIPELINE, RunJob.SCOPE_INVENTORY_SYNC}:
        return _inventory_activity_label(job, artifact)
    if artifact:
        if _is_inventory_artifact(artifact):
            return _inventory_activity_label(job, artifact)
        return "Sales sync"
    return "Activity"


def _latest_activity_snapshot(
    *,
    latest_activity_job: RunJob | None,
    latest_sales_artifact: RunArtifact | None,
    latest_inventory_artifact: RunArtifact | None,
) -> dict:
    candidates: list[tuple[datetime, str, RunJob | None, RunArtifact | None]] = []
    if latest_activity_job:
        activity_artifact = None
        if latest_activity_job.scope in {RunJob.SCOPE_INVENTORY_PIPELINE, RunJob.SCOPE_INVENTORY_SYNC}:
            activity_artifact = latest_inventory_artifact
        elif latest_activity_job.scope in {RunJob.SCOPE_SINGLE, RunJob.SCOPE_ALL}:
            activity_artifact = latest_sales_artifact
        activity_time = _run_activity_time(latest_activity_job)
        if activity_time:
            candidates.append(
                (
                    activity_time,
                    _activity_label_for(latest_activity_job, activity_artifact),
                    latest_activity_job,
                    activity_artifact,
                )
            )
    for artifact in (latest_sales_artifact, latest_inventory_artifact):
        artifact_time = _artifact_activity_time(artifact)
        if artifact and artifact_time:
            candidates.append(
                (
                    artifact_time,
                    _activity_label_for(None, artifact),
                    artifact.run_job,
                    artifact,
                )
            )

    if not candidates:
        return {
            "at": None,
            "label": "",
            "display": "Last activity: None recorded",
            "relative": "None recorded",
            "job": None,
            "artifact": None,
        }

    at, label, job, artifact = max(candidates, key=lambda item: item[0])
    relative = _format_last_run_time(at)
    return {
        "at": at,
        "label": label,
        "display": f"Last activity: {label} {relative}",
        "relative": relative,
        "job": job,
        "artifact": artifact,
    }


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
            "message": "No successful sales sync recorded",
            "action": "trigger_sync",
        })
    if latest_run and latest_run.started_at:
        hours_since = (timezone.now() - latest_run.started_at).total_seconds() / 3600
        if hours_since > portal_settings.get_stale_hours_warning():
            issues.append({
                "severity": "amber",
                "icon": "solar:clock-circle-linear",
                "message": f"No sales sync in {int(hours_since)} hours",
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
    if preloaded is None:
        preloaded_maps = _batch_preload_companies_data([company])
        ck = company.company_key
        preloaded = {
            "latest_activity_job": preloaded_maps["latest_activity_jobs"].get(ck) or latest_run,
            "latest_sales_job": preloaded_maps["latest_sales_jobs"].get(ck),
            "latest_inventory_job": preloaded_maps["latest_inventory_jobs"].get(ck),
            "latest_sales_artifact": preloaded_maps["latest_sales_artifacts"].get(ck),
            "latest_inventory_artifact": preloaded_maps["latest_inventory_artifacts"].get(ck),
            "artifacts_today": preloaded_maps["artifacts_today_by_key"].get(ck, []),
            "latest_successful_sales_artifact": preloaded_maps[
                "latest_successful_sales_artifacts"
            ].get(ck),
            "token_info": preloaded_maps["token_info_by_key"].get(ck),
            "sales_reconcile_statuses_by_company_job": preloaded_maps[
                "sales_reconcile_statuses_by_company_job"
            ],
        }

    latest_activity_job = preloaded.get("latest_activity_job") or latest_run
    latest_sales_job = preloaded.get("latest_sales_job")
    latest_inventory_job = preloaded.get("latest_inventory_job")
    latest_sales_artifact = preloaded.get("latest_sales_artifact")
    latest_inventory_artifact = preloaded.get("latest_inventory_artifact")
    latest_artifact = latest_sales_artifact
    artifacts_today = preloaded.get("artifacts_today") or []
    token_info = preloaded.get("token_info") or _get_token_info_for_display(company)
    latest_successful_artifact = preloaded.get("latest_successful_sales_artifact")
    sales_reconcile_statuses_by_company_job = preloaded.get("sales_reconcile_statuses_by_company_job") or {}

    capabilities = _company_capabilities(company)
    inventory_enabled = capabilities["inventory"]
    sales_status = _sales_status_for_company(
        latest_job=latest_sales_job,
        latest_artifact=latest_sales_artifact,
        reconcile_statuses_by_job={
            str(latest_sales_job.id): sales_reconcile_statuses_by_company_job.get(
                (company.company_key, str(latest_sales_job.id))
            )
            or []
        } if latest_sales_job else {},
    )
    inventory_status = _inventory_status_for_company(
        latest_job=latest_inventory_job,
        latest_artifact=latest_inventory_artifact,
    )
    inventory_review_required = _inventory_review_required(inventory_enabled, inventory_status)

    health = _company_health_snapshot(
        company,
        latest_artifact=latest_sales_artifact,
        latest_job=latest_sales_job,
        token_info=token_info,
        inventory_enabled=inventory_enabled,
        inventory_status=inventory_status,
    )
    status_str = _company_card_status(
        sales_status,
        inventory_status,
        token_info,
        inventory_enabled=inventory_enabled,
    )
    status = _status_display_from_canonical(
        status_str,
        latest_sales_job,
        latest_sales_artifact,
    )
    run_activity = _run_activity_display(_run_activity_status(latest_activity_job))
    health_reason_labels = _health_reason_labels(health.get("reason_codes"))
    issues = _get_company_issues_for_list(company, latest_sales_job, latest_sales_artifact, token_info)
    config_display = _parse_config_for_display(company.config_json)
    artifacts_by_day: dict[object, list[RunArtifact]] = {}
    for artifact in artifacts_today:
        if not _is_sales_artifact(artifact):
            continue
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
    latest_activity = _latest_activity_snapshot(
        latest_activity_job=latest_activity_job,
        latest_sales_artifact=latest_sales_artifact,
        latest_inventory_artifact=latest_inventory_artifact,
    )
    last_activity_at = latest_activity["at"]

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
        "latest_run": latest_activity_job,
        "latest_activity_job": latest_activity_job,
        "latest_activity_label": latest_activity["label"],
        "latest_activity_display": latest_activity["display"],
        "latest_artifact": latest_artifact,
        "latest_sales_job": latest_sales_job,
        "latest_sales_artifact": latest_sales_artifact,
        "latest_inventory_job": latest_inventory_job,
        "latest_inventory_artifact": latest_inventory_artifact,
        "token_info": token_info,
        "issues": issues,
        "issues_highest_severity": issues_highest_severity,
        "config_display": config_display,
        "capabilities": capabilities,
        "inventory_enabled": inventory_enabled,
        "sales_status": sales_status,
        "inventory_status": inventory_status,
        "inventory_review_required": inventory_review_required,
        "inventory_review_label": _inventory_review_action_label(inventory_status),
        "inventory_review_url": reverse(
            "epos_qbo:company_inventory_review",
            kwargs={"company_key": company.company_key},
        ) if inventory_enabled else "",
        "records_24h": records_24h,
        "last_activity_at": last_activity_at,
        "last_run_display": _format_last_run_time(last_activity_at),
        "latest_sales_sync_display": _sales_sync_display(latest_successful_artifact),
        "records_latest_sync": records_latest_sync,
        "latest_sync_target_date": latest_sync_target_date,
        "upload_skipped_latest_sync": upload_skipped_latest_sync,
        "latest_inventory_audit": latest_inventory_artifact if inventory_enabled else None,
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


def _batch_preload_companies_data(companies: list) -> dict:
    """Batch-fetch explicit activity, sales, inventory, and token state per company."""
    company_keys = [c.company_key for c in companies]
    if not company_keys:
        return {
            "latest_activity_jobs": {},
            "latest_sales_jobs": {},
            "latest_inventory_jobs": {},
            "latest_sales_artifacts": {},
            "latest_inventory_artifacts": {},
            "artifacts_today_by_key": {},
            "latest_successful_sales_artifacts": {},
            "token_info_by_key": {},
            "sales_reconcile_statuses_by_company_job": {},
        }

    sales_job_id_to_company_keys: dict = defaultdict(set)
    inventory_job_id_to_company_keys: dict = defaultdict(set)
    sales_reconcile_statuses_by_company_job: dict = defaultdict(list)
    latest_sales_artifacts: dict[str, RunArtifact | None] = {}
    latest_inventory_artifacts: dict[str, RunArtifact | None] = {}

    artifacts_all = list(
        RunArtifact.objects.filter(company_key__in=company_keys)
        .select_related("run_job")
        .order_by("company_key", "-processed_at", "-imported_at")
    )
    for art in artifacts_all:
        if _is_sales_artifact(art):
            if art.company_key not in latest_sales_artifacts:
                latest_sales_artifacts[art.company_key] = art
            if art.run_job_id and art.company_key:
                sales_job_id_to_company_keys[art.run_job_id].add(art.company_key)
                sales_reconcile_statuses_by_company_job[
                    (art.company_key, str(art.run_job_id))
                ].append(art.reconcile_status or "")
        elif _is_inventory_artifact(art):
            if art.company_key not in latest_inventory_artifacts:
                latest_inventory_artifacts[art.company_key] = art
            if art.run_job_id and art.company_key:
                inventory_job_id_to_company_keys[art.run_job_id].add(art.company_key)

    def latest_jobs_for_scope(
        *,
        scopes: list[str] | None,
        job_id_to_company_keys: dict,
        include_all_companies: bool,
    ) -> dict[str, RunJob | None]:
        job_ids_with_artifacts = list(job_id_to_company_keys.keys())
        query = Q(id__in=job_ids_with_artifacts)
        if scopes:
            query |= Q(company_key__in=company_keys, scope__in=scopes)
            if include_all_companies and RunJob.SCOPE_ALL in scopes:
                query |= Q(company_key__isnull=True, scope=RunJob.SCOPE_ALL)
        else:
            query |= Q(company_key__in=company_keys)
            if include_all_companies:
                query |= Q(company_key__isnull=True, scope=RunJob.SCOPE_ALL)
        jobs = list(RunJob.objects.filter(query).order_by("-finished_at", "-started_at", "-created_at"))
        latest_map: dict[str, RunJob | None] = {}
        for active_only in (True, False):
            for job in jobs:
                if active_only and job.status not in (RunJob.STATUS_RUNNING, RunJob.STATUS_QUEUED):
                    continue
                candidates = []
                if job.company_key and job.company_key in company_keys:
                    candidates.append(job.company_key)
                elif include_all_companies and job.company_key is None and job.scope == RunJob.SCOPE_ALL:
                    candidates.extend(company_keys)
                candidates.extend(job_id_to_company_keys.get(job.id, []))
                for ck in set(candidates):
                    if ck not in latest_map:
                        latest_map[ck] = job
        return latest_map

    latest_sales_jobs = latest_jobs_for_scope(
        scopes=[RunJob.SCOPE_SINGLE, RunJob.SCOPE_ALL],
        job_id_to_company_keys=sales_job_id_to_company_keys,
        include_all_companies=True,
    )
    latest_inventory_jobs = latest_jobs_for_scope(
        scopes=[RunJob.SCOPE_INVENTORY_PIPELINE, RunJob.SCOPE_INVENTORY_SYNC],
        job_id_to_company_keys=inventory_job_id_to_company_keys,
        include_all_companies=False,
    )
    activity_job_to_company_keys: dict = defaultdict(set)
    for job_id, keys in sales_job_id_to_company_keys.items():
        activity_job_to_company_keys[job_id].update(keys)
    for job_id, keys in inventory_job_id_to_company_keys.items():
        activity_job_to_company_keys[job_id].update(keys)
    latest_activity_jobs = latest_jobs_for_scope(
        scopes=None,
        job_id_to_company_keys=activity_job_to_company_keys,
        include_all_companies=True,
    )

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

    latest_successful_sales_artifacts: dict[str, RunArtifact | None] = {}
    for art in (
        RunArtifact.objects.filter(company_key__in=company_keys)
        .filter(Q(run_job__status=RunJob.STATUS_SUCCEEDED) | Q(run_job__isnull=True))
        .select_related("run_job")
        .order_by("company_key", "-processed_at", "-imported_at", "-id")
    ):
        if _is_sales_artifact(art) and art.company_key not in latest_successful_sales_artifacts:
            latest_successful_sales_artifacts[art.company_key] = art

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

    return {
        "latest_activity_jobs": latest_activity_jobs,
        "latest_sales_jobs": latest_sales_jobs,
        "latest_inventory_jobs": latest_inventory_jobs,
        "latest_sales_artifacts": latest_sales_artifacts,
        "latest_inventory_artifacts": latest_inventory_artifacts,
        "artifacts_today_by_key": artifacts_today_by_key,
        "latest_successful_sales_artifacts": latest_successful_sales_artifacts,
        "token_info_by_key": token_info_by_key,
        "sales_reconcile_statuses_by_company_job": sales_reconcile_statuses_by_company_job,
    }


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
        preloaded_maps = _batch_preload_companies_data(companies)
        companies_data = []
        for company in companies:
            ck = company.company_key
            preloaded = {
                "latest_activity_job": preloaded_maps["latest_activity_jobs"].get(ck),
                "latest_sales_job": preloaded_maps["latest_sales_jobs"].get(ck),
                "latest_inventory_job": preloaded_maps["latest_inventory_jobs"].get(ck),
                "latest_sales_artifact": preloaded_maps["latest_sales_artifacts"].get(ck),
                "latest_inventory_artifact": preloaded_maps["latest_inventory_artifacts"].get(ck),
                "artifacts_today": preloaded_maps["artifacts_today_by_key"].get(ck, []),
                "latest_successful_sales_artifact": preloaded_maps[
                    "latest_successful_sales_artifacts"
                ].get(ck),
                "token_info": preloaded_maps["token_info_by_key"].get(ck),
                "sales_reconcile_statuses_by_company_job": preloaded_maps[
                    "sales_reconcile_statuses_by_company_job"
                ],
            }
            company_data = _enrich_company_data(
                company,
                preloaded_maps["latest_activity_jobs"].get(ck),
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
def company_inventory_review(request, company_key):
    company = get_object_or_404(CompanyConfigRecord, company_key=company_key)
    inventory_enabled = _company_inventory_enabled(company)

    artifact = None
    summary: dict = {}
    rows: list[dict] = []
    parsed_total_rows = 0
    parsed_healthy_rows = 0
    parse_error = ""
    empty_message = ""
    final_audit_raw = ""
    final_audit_filename = ""

    if not inventory_enabled:
        status_label = "No inventory review found"
        status_color = "slate"
        empty_message = "Inventory review is not enabled for this company."
    else:
        artifact = _latest_inventory_review_artifact(company.company_key)
        if artifact is None:
            status_label = "No inventory review found"
            status_color = "slate"
            empty_message = "No inventory review is currently required for this company."
        else:
            summary = _inventory_summary_from_artifact(artifact)
            final_audit_raw = _artifact_report_path_value(artifact, "final_audit")
            if not final_audit_raw:
                empty_message = "No final inventory audit was found for the latest inventory run."
            else:
                try:
                    final_audit_path = _resolve_artifact_report_path(artifact, "final_audit")
                except Http404:
                    empty_message = (
                        "The final audit artifact exists in the database but the source file could not be found."
                    )
                else:
                    final_audit_filename = final_audit_path.name
                    parsed = parse_inventory_review_csv(final_audit_path)
                    rows = parsed.rows
                    parsed_total_rows = parsed.total_rows
                    parsed_healthy_rows = parsed.healthy_rows
                    parse_error = parsed.error
                    if not rows and not parse_error:
                        empty_message = "No inventory review is currently required for this company."
                    elif not rows and parse_error:
                        empty_message = "The final audit CSV could not be parsed."

            summary_cards = _inventory_review_summary_cards(
                summary,
                rows,
                parsed_total_rows,
                parsed_healthy_rows,
            )
            if rows or summary_cards["blocked_items"] > 0 or _has_non_in_sync_inventory_rows(summary):
                status_label = "Needs review"
                status_color = "amber"
            else:
                status_label = "Healthy"
                status_color = "emerald"

    if not inventory_enabled or artifact is None:
        summary_cards = _inventory_review_summary_cards(summary, rows, parsed_total_rows, parsed_healthy_rows)

    report_links = _inventory_review_report_links(artifact)
    run = artifact.run_job if artifact and artifact.run_job_id else None
    latest_run_time = _run_status_time(run) or _artifact_status_time(artifact)
    run_label = run.friendly_id if run else (Path(str(artifact.source_path)).name if artifact else "")
    run_title = run.friendly_title if run else "Inventory report"
    has_negative_summary = bool(
        summary_cards["epos_negative_rows_clamped"] > 0
        or str(summary_cards["epos_negative_units_clamped_display"]) not in {"", "0"}
    )

    actions = {
        "available": False,
        "catalog_cleanup_count": 0,
        "quantity_adjustment_count": 0,
        "missing_count": 0,
        "retry_catalog_cleanup_url": "",
        "retry_catalog_cleanup_confirm_url": "",
        "retry_quantity_adjustments_url": "",
        "retry_quantity_adjustments_confirm_url": "",
        "missing_preview_url": "",
    }
    if inventory_enabled and rows:
        actions = {
            "available": True,
            "catalog_cleanup_count": len(get_catalog_cleanup_rows(rows)),
            "quantity_adjustment_count": len(get_quantity_adjustment_rows(rows)),
            "missing_count": len(get_review_rows_by_reason(rows, REASON_GROUP_MISSING)),
            "retry_catalog_cleanup_url": reverse(
                "epos_qbo:company_inventory_retry_catalog_cleanup",
                kwargs={"company_key": company.company_key},
            ),
            "retry_catalog_cleanup_confirm_url": reverse(
                "epos_qbo:company_inventory_retry_catalog_cleanup_confirm",
                kwargs={"company_key": company.company_key},
            ),
            "retry_quantity_adjustments_url": reverse(
                "epos_qbo:company_inventory_retry_quantity_adjustments",
                kwargs={"company_key": company.company_key},
            ),
            "retry_quantity_adjustments_confirm_url": reverse(
                "epos_qbo:company_inventory_retry_quantity_adjustments_confirm",
                kwargs={"company_key": company.company_key},
            ),
            "missing_preview_url": reverse(
                "epos_qbo:company_inventory_missing_preview",
                kwargs={"company_key": company.company_key},
            ),
        }

    context = {
        "company": company,
        "inventory_enabled": inventory_enabled,
        "review": {
            "artifact": artifact,
            "run": run,
            "run_label": run_label,
            "run_title": run_title,
            "run_detail_url": reverse("epos_qbo:run-detail", kwargs={"job_id": run.id}) if run else "",
            "final_audit_download_url": report_links.get("final_audit", ""),
            "final_audit_raw": final_audit_raw,
            "final_audit_filename": final_audit_filename,
            "latest_run_time": latest_run_time,
            "status_label": status_label,
            "status_color": status_color,
            "summary": summary_cards,
            "rows": rows,
            "row_count": len(rows),
            "reason_counts": _inventory_review_reason_counts(rows),
            "empty_message": empty_message,
            "parse_error": parse_error,
            "has_negative_summary": has_negative_summary,
            "actions": actions,
        },
    }
    context.update(_nav_context())
    context.update(
        _breadcrumb_context(
            [
                {"label": "Dashboard", "url": reverse("epos_qbo:overview")},
                {"label": "Companies", "url": reverse("epos_qbo:companies-list")},
                {
                    "label": company.display_name,
                    "url": reverse("epos_qbo:company-detail", kwargs={"company_key": company.company_key}),
                },
                {"label": "Inventory Review", "url": None},
            ],
            back_url=reverse("epos_qbo:company-detail", kwargs={"company_key": company.company_key}),
            back_label=company.display_name,
        )
    )
    return render(request, "epos_qbo/company_inventory_review.html", context)


def _inventory_review_action_context(request, company_key: str):
    company = get_object_or_404(CompanyConfigRecord, company_key=company_key)
    review_url = reverse(
        "epos_qbo:company_inventory_review",
        kwargs={"company_key": company.company_key},
    )
    if not _company_inventory_enabled(company):
        messages.error(request, "Inventory is not enabled for this company.")
        return company, None, redirect("epos_qbo:company-detail", company_key=company.company_key)

    artifact = _latest_inventory_review_artifact(company.company_key)
    if artifact is None:
        messages.error(request, "No inventory final audit is available for this company yet.")
        return company, None, redirect(review_url)

    context = load_review_context(
        company=company,
        artifact=artifact,
        final_audit_path_resolver=_resolve_artifact_report_path,
    )
    if context is None:
        messages.error(
            request,
            "The final audit artifact exists in the database but the source file could not be found.",
        )
        return company, None, redirect(review_url)
    if context.parse_result.error and not context.rows:
        messages.error(request, "The final audit CSV could not be parsed.")
        return company, None, redirect(review_url)
    return company, context, None


def _inventory_retry_confirm_context(
    *,
    company,
    context,
    action_title: str,
    action_label: str,
    inventory_mode: str,
    warning_text: str,
    rows: list[dict],
    preview_limit: int = 25,
) -> dict:
    run = context.artifact.run_job if context.artifact and context.artifact.run_job_id else None
    run_label = run.friendly_id if run else ""
    return {
        "company": company,
        "action_title": action_title,
        "action_label": action_label,
        "inventory_mode": inventory_mode,
        "inventory_mode_label": _inventory_mode_label(inventory_mode),
        "inventory_write_intent": INVENTORY_MODE_WRITE_INTENT_LABELS.get(inventory_mode, ""),
        "inventory_safe_apply_copy": INVENTORY_SAFE_APPLY_COPY,
        "warning_text": warning_text,
        "row_count": len(rows),
        "rows": rows,
        "preview_rows": rows[:preview_limit],
        "preview_limit": int(preview_limit),
        "final_audit_filename": context.final_audit_path.name,
        "source_run_label": run_label,
    }


@login_required
@permission_required("epos_qbo.can_trigger_runs", raise_exception=True)
@require_GET
def company_inventory_retry_catalog_cleanup_confirm(request, company_key):
    company, context, error_redirect = _inventory_review_action_context(request, company_key)
    if error_redirect is not None:
        return error_redirect
    review_url = reverse(
        "epos_qbo:company_inventory_review",
        kwargs={"company_key": company.company_key},
    )
    rows = get_catalog_cleanup_rows(context.rows)
    if not rows:
        messages.info(request, "No duplicate/base conflicts found in the latest final audit.")
        return redirect(review_url)

    template_context = _inventory_retry_confirm_context(
        company=company,
        context=context,
        action_title="Confirm Scoped Catalog Apply",
        action_label="Scoped catalog apply",
        inventory_mode="catalog_apply_admin_only",
        warning_text=(
            "This queues catalog_apply_admin_only for the reviewed rows only. When the job runs, it may update "
            "QuickBooks inventory by consolidating/inactivating duplicate or pack-variant items "
            "and adjusting base quantities. Production apply remains blocked unless explicitly unlocked."
        ),
        rows=rows,
    )
    template_context.update(
        {
            "review_url": review_url,
            "confirm_post_url": reverse(
                "epos_qbo:company_inventory_retry_catalog_cleanup",
                kwargs={"company_key": company.company_key},
            ),
            "confirm_button_text": "Confirm and queue",
        }
    )
    template_context.update(_nav_context())
    template_context.update(
        _breadcrumb_context(
            [
                {"label": "Dashboard", "url": reverse("epos_qbo:overview")},
                {"label": "Companies", "url": reverse("epos_qbo:companies-list")},
                {
                    "label": company.display_name,
                    "url": reverse(
                        "epos_qbo:company-detail",
                        kwargs={"company_key": company.company_key},
                    ),
                },
                {"label": "Inventory Review", "url": review_url},
                {"label": "Confirm retry", "url": None},
            ],
            back_url=review_url,
            back_label="Inventory Review",
        )
    )
    return render(request, "epos_qbo/company_inventory_retry_confirm.html", template_context)


@login_required
@permission_required("epos_qbo.can_trigger_runs", raise_exception=True)
@require_GET
def company_inventory_retry_quantity_adjustments_confirm(request, company_key):
    company, context, error_redirect = _inventory_review_action_context(request, company_key)
    if error_redirect is not None:
        return error_redirect
    review_url = reverse(
        "epos_qbo:company_inventory_review",
        kwargs={"company_key": company.company_key},
    )
    rows = get_quantity_adjustment_rows(context.rows)
    if not rows:
        messages.info(request, "No exact-match quantity adjustments needed in the latest final audit.")
        return redirect(review_url)

    template_context = _inventory_retry_confirm_context(
        company=company,
        context=context,
        action_title="Confirm Scoped Quantity Apply",
        action_label="Scoped quantity apply",
        inventory_mode="quantity_apply",
        warning_text=(
            "This queues quantity_apply for the reviewed rows only. When the job runs, it may post "
            "QuickBooks InventoryAdjustment entries so QBO QtyOnHand matches EPOS. EPOS is the "
            "source of truth. Production apply remains blocked unless explicitly unlocked."
        ),
        rows=rows,
    )
    template_context.update(
        {
            "review_url": review_url,
            "confirm_post_url": reverse(
                "epos_qbo:company_inventory_retry_quantity_adjustments",
                kwargs={"company_key": company.company_key},
            ),
            "confirm_button_text": "Confirm and queue",
        }
    )
    template_context.update(_nav_context())
    template_context.update(
        _breadcrumb_context(
            [
                {"label": "Dashboard", "url": reverse("epos_qbo:overview")},
                {"label": "Companies", "url": reverse("epos_qbo:companies-list")},
                {
                    "label": company.display_name,
                    "url": reverse(
                        "epos_qbo:company-detail",
                        kwargs={"company_key": company.company_key},
                    ),
                },
                {"label": "Inventory Review", "url": review_url},
                {"label": "Confirm retry", "url": None},
            ],
            back_url=review_url,
            back_label="Inventory Review",
        )
    )
    return render(request, "epos_qbo/company_inventory_retry_confirm.html", template_context)


@login_required
@permission_required("epos_qbo.can_trigger_runs", raise_exception=True)
@require_POST
def company_inventory_retry_catalog_cleanup(request, company_key):
    company, context, error_redirect = _inventory_review_action_context(request, company_key)
    if error_redirect is not None:
        return error_redirect
    review_url = reverse(
        "epos_qbo:company_inventory_review",
        kwargs={"company_key": company.company_key},
    )

    rows = get_catalog_cleanup_rows(context.rows)
    if not rows:
        messages.info(request, "No duplicate/base conflicts found in the latest final audit.")
        return redirect(review_url)

    result = retry_catalog_cleanup_for_review(
        context=context,
        requested_by=request.user,
    )
    job_id = result.get("job_id")
    if job_id is None:
        messages.info(request, "No catalog cleanup actions were queued.")
        return redirect(review_url)

    dispatch_next_queued_job()
    job = RunJob.objects.get(pk=job_id)
    send_inventory_review_action_queued(company=company, job=job, request=request)
    messages.success(
        request,
        f"Catalog cleanup retry queued for {result['row_count']} item(s).",
    )
    return redirect("epos_qbo:run-detail", job_id=job_id)


@login_required
@permission_required("epos_qbo.can_trigger_runs", raise_exception=True)
@require_POST
def company_inventory_retry_quantity_adjustments(request, company_key):
    company, context, error_redirect = _inventory_review_action_context(request, company_key)
    if error_redirect is not None:
        return error_redirect
    review_url = reverse(
        "epos_qbo:company_inventory_review",
        kwargs={"company_key": company.company_key},
    )

    rows = get_quantity_adjustment_rows(context.rows)
    if not rows:
        messages.info(request, "No exact-match quantity adjustments needed in the latest final audit.")
        return redirect(review_url)

    result = retry_quantity_adjustments_for_review(
        context=context,
        requested_by=request.user,
    )
    job_id = result.get("job_id")
    if job_id is None:
        messages.info(request, "No quantity adjustment actions were queued.")
        return redirect(review_url)

    dispatch_next_queued_job()
    job = RunJob.objects.get(pk=job_id)
    send_inventory_review_action_queued(company=company, job=job, request=request)
    messages.success(
        request,
        f"Quantity adjustment retry queued for {result['row_count']} item(s).",
    )
    return redirect("epos_qbo:run-detail", job_id=job_id)


def _inventory_missing_preview_url(
    company_key: str, *, category: str | None = None, txn_date: str | None = None
) -> str:
    base = reverse(
        "epos_qbo:company_inventory_missing_preview",
        kwargs={"company_key": company_key},
    )
    params: dict[str, str] = {}
    if category:
        params["category"] = category
    if txn_date:
        params["txn_date"] = txn_date
    if params:
        return f"{base}?{urlencode(params)}"
    return base


def _missing_preview_date_input_bounds(*, company_key: str) -> tuple[str, str | None]:
    """Return (max_date_iso_today_in_business_tz, min_date_iso_from_company_floor_or_None)."""

    tz = get_business_timezone()
    now = timezone.now()
    if timezone.is_naive(now):
        now = timezone.make_aware(now)
    today_iso = now.astimezone(tz).date().isoformat()
    return today_iso, inv_start_date_floor_iso(company_key)


@login_required
@require_GET
def company_inventory_missing_preview(request, company_key):
    company, context, error_redirect = _inventory_review_action_context(request, company_key)
    if error_redirect is not None:
        return error_redirect

    preview_full = build_missing_item_creation_preview(context=context)
    category_param = str(request.GET.get("category") or "").strip()
    preview = filter_missing_preview_by_category(preview_full, category_param)

    resolved_iso, txn_date_source = resolve_txn_date_for_review_missing_item_creation(
        company_key=company.company_key, artifact=context.artifact
    )
    picker_date = coalesce_picker_date_from_get(
        company_key=company.company_key,
        get_value=request.GET.get("txn_date"),
        resolved_iso=resolved_iso,
    )
    category_options = collect_category_options(preview_full.rows)
    _, queue_category_label = resolve_category_scope_labels(
        preview_full=preview_full,
        category_scope=category_param,
    )
    missing_item_queue_allowed = not str(preview.qbo_base_names_error or "").strip()
    date_max_iso, date_min_iso = _missing_preview_date_input_bounds(company_key=company.company_key)

    review_url = reverse(
        "epos_qbo:company_inventory_review",
        kwargs={"company_key": company.company_key},
    )
    confirm_post_url = reverse(
        "epos_qbo:company_inventory_missing_create",
        kwargs={"company_key": company.company_key},
    )
    category_filter_url = reverse(
        "epos_qbo:company_inventory_missing_preview",
        kwargs={"company_key": company.company_key},
    )

    template_context = {
        "company": company,
        "preview": preview,
        "preview_full": preview_full,
        "review_url": review_url,
        "final_audit_filename": context.final_audit_path.name,
        "resolved_item_inv_start_date": resolved_iso,
        "item_inv_start_date": picker_date,
        "picker_date": picker_date,
        "txn_date_source": txn_date_source,
        "missing_item_queue_allowed": missing_item_queue_allowed,
        "snapshot_pack_guard_message": SNAPSHOT_PACK_GUARD_MESSAGE,
        "confirm_post_url": confirm_post_url,
        "category_options": category_options,
        "selected_category": category_param,
        "queue_category_label": queue_category_label,
        "date_input_max": date_max_iso,
        "date_input_min": date_min_iso,
        "category_filter_url": category_filter_url,
    }
    template_context.update(_nav_context())
    template_context.update(
        _breadcrumb_context(
            [
                {"label": "Dashboard", "url": reverse("epos_qbo:overview")},
                {"label": "Companies", "url": reverse("epos_qbo:companies-list")},
                {
                    "label": company.display_name,
                    "url": reverse(
                        "epos_qbo:company-detail",
                        kwargs={"company_key": company.company_key},
                    ),
                },
                {"label": "Inventory Review", "url": review_url},
                {"label": "Missing QuickBooks Items", "url": None},
            ],
            back_url=review_url,
            back_label="Inventory Review",
        )
    )
    return render(
        request,
        "epos_qbo/company_inventory_missing_preview.html",
        template_context,
    )


@login_required
@permission_required("epos_qbo.can_trigger_runs", raise_exception=True)
@require_GET
def company_inventory_missing_create_confirm(request, company_key):
    """Compatibility URL: confirmation now happens on the Missing Preview page."""

    target = reverse(
        "epos_qbo:company_inventory_missing_preview",
        kwargs={"company_key": company_key},
    )
    if request.GET:
        return redirect(f"{target}?{request.GET.urlencode()}")
    return redirect(target)


@login_required
@permission_required("epos_qbo.can_trigger_runs", raise_exception=True)
@require_POST
def company_inventory_missing_create(request, company_key):
    company, context, error_redirect = _inventory_review_action_context(request, company_key)
    if error_redirect is not None:
        return error_redirect
    missing_preview_url = reverse(
        "epos_qbo:company_inventory_missing_preview",
        kwargs={"company_key": company.company_key},
    )

    preview_full = build_missing_item_creation_preview(context=context)
    category_param = str(request.POST.get("category_scope") or "").strip()
    preview_scoped = filter_missing_preview_by_category(preview_full, category_param)

    resolved_iso, resolved_src = resolve_txn_date_for_review_missing_item_creation(
        company_key=company.company_key, artifact=context.artifact
    )
    txn_date, date_err, txn_src = validate_inventory_start_date_for_missing_queue(
        company_key=company.company_key,
        posted=request.POST.get("inventory_start_date"),
        resolved_iso=resolved_iso,
        resolved_source=resolved_src,
    )

    posted_date_raw = str(request.POST.get("inventory_start_date") or "").strip()
    redirect_back = _inventory_missing_preview_url(
        company.company_key,
        category=category_param or None,
        txn_date=posted_date_raw or None,
    )

    if date_err:
        messages.error(request, date_err)
        return redirect(redirect_back)

    if str(preview_full.qbo_base_names_error or "").strip():
        messages.error(request, SNAPSHOT_PACK_GUARD_MESSAGE)
        return redirect(redirect_back)

    cat_key, cat_label = resolve_category_scope_labels(
        preview_full=preview_full,
        category_scope=category_param,
    )

    job = queue_missing_item_creation_job(
        company=company,
        artifact=context.artifact,
        final_audit_path=context.final_audit_path,
        preview=preview_scoped,
        requested_by=request.user,
        txn_date=txn_date,
        txn_date_source=txn_src,
        category_filter_key=cat_key,
        category_label=cat_label,
    )
    if job is None:
        messages.warning(
            request,
            "No safe missing-item candidates to queue in the selected scope. Refresh the preview and try again if the audit changed.",
        )
        return redirect(redirect_back)

    dispatch_next_queued_job()
    send_inventory_review_action_queued(company=company, job=job, request=request)
    scope_note = ""
    if cat_label != "All categories":
        scope_note = f" ({cat_label})"
    messages.success(
        request,
        f"Missing item creation queued for {preview_scoped.safe_count} safe candidate(s){scope_note} using InvStartDate {txn_date}.",
    )
    return redirect("epos_qbo:run-detail", job_id=job.id)


@login_required
def company_detail(request, company_key):
    """Detail view for a single company."""
    company = get_object_or_404(CompanyConfigRecord, company_key=company_key)
    recent_runs = list(_company_runs_queryset_ordered_by_latest(company_key)[:30])
    latest_run = recent_runs[0] if recent_runs else None
    company_data = _enrich_company_data(company, latest_run)
    # Sales from last successful run (not 7D aggregate)
    latest_successful_artifact = None
    for artifact in (
        RunArtifact.objects.filter(company_key=company_key)
        .filter(Q(run_job__status=RunJob.STATUS_SUCCEEDED) | Q(run_job__isnull=True))
        .select_related("run_job")
        .order_by("-processed_at", "-imported_at", "-id")
    ):
        if _is_sales_artifact(artifact):
            latest_successful_artifact = artifact
            break
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


# ---------------------------------------------------------------------------
# Tools page
# ---------------------------------------------------------------------------

def _tools_venv_python() -> str:
    """Resolve Python executable for tools-page subprocesses."""
    return resolve_python_executable()


def _tools_subprocess_env() -> dict:
    """Build env dict for tool subprocesses (mirrors job_runner)."""
    from oiat_portal.paths import BASE_DIR
    env = dict(os.environ)
    pythonpath = str(BASE_DIR)
    env["PYTHONPATH"] = pythonpath + os.pathsep + env.get("PYTHONPATH", "")
    return env


TOOLS_QUERY_MAX_LENGTH = 2000
TOOLS_SUBPROCESS_TIMEOUT = 30


@login_required
@permission_required("epos_qbo.can_trigger_runs", raise_exception=True)
def tools_page(request):
    """Tools page: QBO query, verify mappings, and other script tools."""
    companies = CompanyConfigRecord.objects.filter(is_active=True).order_by("display_name")
    company_options = [{"value": c.company_key, "label": c.display_name} for c in companies]
    context = {
        "company_options": company_options,
    }
    context.update(_nav_context())
    context.update(
        _breadcrumb_context(
            [
                {"label": "Dashboard", "url": reverse("epos_qbo:overview")},
                {"label": "Tools", "url": None},
            ],
            back_url=reverse("epos_qbo:overview"),
            back_label="Overview",
        )
    )
    return render(request, "epos_qbo/tools.html", context)


@login_required
@permission_required("epos_qbo.can_trigger_runs", raise_exception=True)
@require_POST
def tools_qbo_query_api(request):
    """Execute a QBO SQL-like query via subprocess and return JSON."""
    from oiat_portal.paths import BASE_DIR

    try:
        body = json.loads(request.body)
    except (json.JSONDecodeError, ValueError):
        return JsonResponse({"success": False, "error": "Invalid JSON body."}, status=400)

    company_key = (body.get("company_key") or "").strip()
    query = (body.get("query") or "").strip()

    if not company_key:
        return JsonResponse({"success": False, "error": "Company is required."}, status=400)
    if not CompanyConfigRecord.objects.filter(company_key=company_key, is_active=True).exists():
        return JsonResponse({"success": False, "error": f"Unknown or inactive company: {company_key}"}, status=400)
    if not query:
        return JsonResponse({"success": False, "error": "Query is required."}, status=400)
    if len(query) > TOOLS_QUERY_MAX_LENGTH:
        return JsonResponse({"success": False, "error": f"Query exceeds {TOOLS_QUERY_MAX_LENGTH} character limit."}, status=400)

    script_path = str(BASE_DIR / "code_scripts" / "scripts" / "qbo_queries" / "qbo_query.py")
    cmd = [_tools_venv_python(), script_path, "--company", company_key, "query", query, "--raw-json"]

    try:
        result = subprocess.run(
            cmd,
            cwd=str(BASE_DIR),
            env=_tools_subprocess_env(),
            capture_output=True,
            text=True,
            timeout=TOOLS_SUBPROCESS_TIMEOUT,
        )
    except subprocess.TimeoutExpired:
        return JsonResponse({"success": False, "error": "Query timed out (30s limit)."}, status=504)
    except Exception as exc:
        return JsonResponse({"success": False, "error": f"Failed to run query: {exc}"}, status=500)

    if result.returncode != 0:
        error_msg = (result.stderr or result.stdout or "Unknown error").strip()
        if len(error_msg) > 1000:
            error_msg = error_msg[:1000] + "..."
        return JsonResponse({"success": False, "error": error_msg}, status=502)

    try:
        data = json.loads(result.stdout)
    except json.JSONDecodeError:
        return JsonResponse({"success": False, "error": "Script returned non-JSON output.", "raw": result.stdout[:2000]}, status=502)

    return JsonResponse({"success": True, "data": data})


@login_required
@permission_required("epos_qbo.can_trigger_runs", raise_exception=True)
@require_POST
def tools_verify_mapping_api(request):
    """Run verify-mapping-accounts script for a company and return JSON."""
    from oiat_portal.paths import BASE_DIR

    try:
        body = json.loads(request.body)
    except (json.JSONDecodeError, ValueError):
        return JsonResponse({"success": False, "error": "Invalid JSON body."}, status=400)

    company_key = (body.get("company_key") or "").strip()
    if not company_key:
        return JsonResponse({"success": False, "error": "Company is required."}, status=400)
    if not CompanyConfigRecord.objects.filter(company_key=company_key, is_active=True).exists():
        return JsonResponse({"success": False, "error": f"Unknown or inactive company: {company_key}"}, status=400)

    script_path = str(BASE_DIR / "code_scripts" / "scripts" / "qbo_queries" / "qbo_verify_mapping_accounts.py")
    cmd = [_tools_venv_python(), script_path, "--company", company_key]

    try:
        result = subprocess.run(
            cmd,
            cwd=str(BASE_DIR),
            env=_tools_subprocess_env(),
            capture_output=True,
            text=True,
            timeout=60,
        )
    except subprocess.TimeoutExpired:
        return JsonResponse({"success": False, "error": "Verification timed out (60s limit)."}, status=504)
    except Exception as exc:
        return JsonResponse({"success": False, "error": f"Failed to run verification: {exc}"}, status=500)

    output = (result.stdout or "").strip()
    error_output = (result.stderr or "").strip()

    if result.returncode != 0:
        combined = (output + "\n" + error_output).strip()
        if len(combined) > 2000:
            combined = combined[:2000] + "..."
        return JsonResponse({"success": False, "error": combined or "Verification failed."}, status=502)

    return JsonResponse({"success": True, "output": output})


# ---------------------------------------------------------------------------
# API Tokens / QuickBooks Connections page
# ---------------------------------------------------------------------------

_TOKEN_PAGE_LOGGER = logging.getLogger("epos_qbo.api_tokens")
QBO_TEST_QUERY_TIMEOUT = 15

CONNECTION_STATE_LABELS = {
    "connected": "Connected",
    "refresh_expiring": "Refresh token expiring soon",
    "refresh_expired": "Refresh token expired",
    "missing_realm_id": "Realm ID not configured",
    "missing_refresh_token": "Missing refresh token",
    "missing_tokens": "Missing tokens",
}

CONNECTION_STATE_EXPLAIN = {
    "connected": "Safe to run sync. QuickBooks credentials are healthy.",
    "refresh_expiring": (
        "Sync still works, but the long-lived refresh token will expire soon. "
        "Re-authorize QuickBooks before it expires to avoid disruption."
    ),
    "refresh_expired": (
        "Refresh token has expired. Sync will fail until you re-authorize QuickBooks "
        "for this company."
    ),
    "missing_realm_id": "No Realm ID is configured for this company. Add the Realm ID in the company settings.",
    "missing_refresh_token": (
        "No refresh token is stored for this company. Re-authorize QuickBooks to "
        "establish a connection."
    ),
    "missing_tokens": (
        "No QuickBooks tokens are stored for this company yet. "
        "Run the OAuth flow to connect."
    ),
}

ACCESS_STATE_LABELS = {
    "active": "Active",
    "expired": "Expired (will refresh on next sync)",
    "unknown": "Unknown",
}


def _format_local_datetime(epoch_seconds: int | float | None) -> str | None:
    if not epoch_seconds:
        return None
    try:
        ts = float(epoch_seconds)
    except (TypeError, ValueError):
        return None
    try:
        dt = datetime.fromtimestamp(ts, tz=dt_timezone.utc)
    except (OverflowError, OSError, ValueError):
        return None
    try:
        local_dt = dt.astimezone(get_business_timezone())
    except Exception:
        local_dt = dt
    return local_dt.strftime("%Y-%m-%d %H:%M %Z").strip()


def _format_relative(epoch_seconds: int | float | None) -> str | None:
    if not epoch_seconds:
        return None
    try:
        target = int(epoch_seconds)
    except (TypeError, ValueError):
        return None
    now_ts = int(timezone.now().timestamp())
    delta = target - now_ts
    if delta == 0:
        return "now"
    if delta > 0:
        return f"in {_format_duration(delta)}"
    return f"{_format_duration(-delta)} ago"


def _safe_fingerprint(value: str | None) -> str | None:
    if not value:
        return None
    text = str(value)
    if len(text) <= 8:
        return text
    return f"{text[:6]}…"


def _build_token_page_context(company: CompanyConfigRecord, *, tokens: dict | None) -> dict:
    cfg = company.config_json or {}
    qbo = cfg.get("qbo") or {}
    realm_id = qbo.get("realm_id")
    raw_environment = qbo.get("environment") or "production"
    environment = normalize_qbo_environment(raw_environment, default="production")
    if not realm_id:
        tokens = None

    health = _company_token_health(company, tokens=tokens)

    access_expires_at = (tokens or {}).get("expires_at")
    refresh_expires_at = (tokens or {}).get("refresh_expires_at")
    updated_at = (tokens or {}).get("updated_at")
    fingerprint = _safe_fingerprint((tokens or {}).get("client_fingerprint"))
    token_environment = (tokens or {}).get("environment")

    state = health.get("connection_state") or "missing_tokens"
    access_state = health.get("access_state") or "unknown"

    explanation = CONNECTION_STATE_EXPLAIN.get(state, health.get("display_subtext") or "")

    environment_mismatch = bool(
        token_environment
        and normalize_qbo_environment(token_environment, default=environment) != environment
    )

    if environment_mismatch:
        state_label = "Environment mismatch"
        status_color = "red"
        explanation = (
            "Stored token environment does not match this company's configured environment. "
            "The connection must be re-authorized in the correct environment before running sync."
        )
    else:
        state_label = CONNECTION_STATE_LABELS.get(state, health.get("display_label") or "Unknown")
        status_color = health.get("status_color") or "slate"

    show_explanation = environment_mismatch or state in {
        "missing_tokens",
        "missing_refresh_token",
        "refresh_expired",
        "refresh_expiring",
    }

    return {
        "company_key": company.company_key,
        "display_name": company.display_name,
        "is_active": company.is_active,
        "realm_id": realm_id or "",
        "environment": environment,
        "environment_label": "Production" if environment == "production" else "Sandbox",
        "connection_state": state,
        "connection_state_label": state_label,
        "status_color": status_color,
        "access_state": access_state,
        "access_state_label": ACCESS_STATE_LABELS.get(access_state, "Unknown"),
        "access_expires_at_human": _format_local_datetime(access_expires_at),
        "access_expires_relative": _format_relative(access_expires_at),
        "refresh_expires_at_human": _format_local_datetime(refresh_expires_at),
        "refresh_expires_relative": _format_relative(refresh_expires_at),
        "updated_at_human": _format_local_datetime(updated_at),
        "updated_at_relative": _format_relative(updated_at),
        "client_fingerprint": fingerprint,
        "explanation": explanation,
        "show_explanation": show_explanation,
        "has_tokens": bool(tokens),
        "has_realm_id": bool(realm_id),
        "environment_mismatch": environment_mismatch,
        "needs_reauth": state in {"missing_tokens", "missing_refresh_token", "refresh_expired"} or environment_mismatch,
        "expiring_soon": state == "refresh_expiring",
    }


def _qbo_test_query(company_key: str, realm_id: str, environment: str) -> tuple[bool, str]:
    """Run a harmless CompanyInfo query and return (ok, message)."""
    try:
        access_token = get_access_token(company_key, realm_id)
    except RuntimeError as exc:
        return False, _humanize_token_error(str(exc))
    except Exception as exc:  # pragma: no cover - defensive
        return False, f"Failed to obtain access token: {exc}"

    base_url = get_qbo_api_base_url(environment)
    url = f"{base_url}/v3/company/{realm_id}/query"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
    }
    params = {"query": "select CompanyName from CompanyInfo", "minorversion": "70"}

    try:
        resp = requests.get(url, headers=headers, params=params, timeout=QBO_TEST_QUERY_TIMEOUT)
    except requests.Timeout:
        return False, "QuickBooks API call timed out."
    except requests.RequestException as exc:
        return False, f"Network error contacting QuickBooks: {exc}"

    if resp.status_code == 200:
        try:
            payload = resp.json()
            company_name = (
                payload.get("QueryResponse", {})
                .get("CompanyInfo", [{}])[0]
                .get("CompanyName")
            )
        except (ValueError, IndexError, AttributeError):
            company_name = None
        if company_name:
            return True, f"Connection OK — QuickBooks returned “{company_name}”."
        return True, "Connection OK — QuickBooks responded successfully."

    if resp.status_code == 401:
        return False, "QuickBooks rejected the access token (401). Try refreshing the token or re-authorize."
    if resp.status_code == 403:
        return False, "QuickBooks denied access (403). Check that this realm is authorized for the configured app."
    return False, f"QuickBooks returned HTTP {resp.status_code}."


def _humanize_token_error(error_text: str) -> str:
    text = (error_text or "").lower()
    if "invalid_grant" in text:
        return "Refresh token is invalid or expired. Re-authorize QuickBooks for this company."
    if "invalid_client" in text:
        return "QBO client ID/secret mismatch. Check the server environment configuration."
    if "no tokens found" in text:
        return "No tokens stored for this company. Run the OAuth flow to connect QuickBooks."
    if "no refresh_token" in text:
        return "No refresh token stored. Re-authorize QuickBooks for this company."
    if "realm id mismatch" in text:
        return "Realm ID mismatch — stored tokens belong to a different QuickBooks company. Re-authorize."
    if "qbo environment mismatch" in text or "different qbo environment" in text:
        return "QBO environment mismatch — stored tokens were created for a different environment."
    if "different intuit client" in text:
        return "Stored tokens were created with a different Intuit client ID. Re-run the OAuth flow."
    return error_text or "Token operation failed."


@login_required
def api_tokens_page(request):
    """QuickBooks Connections page: per-company token health and actions."""
    try:
        ensure_db_initialized()
    except Exception:  # pragma: no cover - defensive
        pass

    companies = list(CompanyConfigRecord.objects.filter(is_active=True).order_by("display_name"))
    pairs: list[tuple[str, str]] = []
    for c in companies:
        cfg = c.config_json or {}
        qbo = cfg.get("qbo") or {}
        realm_id = qbo.get("realm_id")
        if realm_id:
            pairs.append((c.company_key, realm_id))

    tokens_by_pair = {}
    if pairs:
        try:
            tokens_by_pair = load_tokens_batch(pairs)
        except Exception:  # pragma: no cover - defensive
            tokens_by_pair = {}

    company_views = []
    for c in companies:
        cfg = c.config_json or {}
        qbo = cfg.get("qbo") or {}
        realm_id = qbo.get("realm_id")
        tokens = tokens_by_pair.get((c.company_key, realm_id)) if realm_id else None
        company_views.append(_build_token_page_context(c, tokens=tokens))

    summary = {
        "total": len(company_views),
        "connected": sum(1 for c in company_views if c["connection_state"] == "connected" and not c["environment_mismatch"]),
        "expiring": sum(1 for c in company_views if c["expiring_soon"]),
        "needs_reauth": sum(1 for c in company_views if c["needs_reauth"]),
        "missing": sum(1 for c in company_views if c["connection_state"] in {"missing_tokens", "missing_refresh_token"}),
    }

    context = {
        "page_title": "QuickBooks Connections",
        "page_subtitle": "Monitor, refresh, and test QuickBooks Online API tokens for each configured company.",
        "company_views": company_views,
        "summary": summary,
        "has_companies": bool(company_views),
    }
    context.update(_nav_context())
    context.update(
        _breadcrumb_context(
            [
                {"label": "Dashboard", "url": reverse("epos_qbo:overview")},
                {"label": "QuickBooks Connections", "url": None},
            ],
            back_url=reverse("epos_qbo:overview"),
            back_label="Overview",
        )
    )
    return render(request, "epos_qbo/api_tokens.html", context)


@login_required
@permission_required("epos_qbo.can_trigger_runs", raise_exception=True)
@require_POST
def api_tokens_test(request, company_key: str):
    """Run a harmless QBO query to verify the connection."""
    company = get_object_or_404(CompanyConfigRecord, company_key=company_key, is_active=True)
    cfg = company.config_json or {}
    qbo = cfg.get("qbo") or {}
    realm_id = qbo.get("realm_id")
    environment = normalize_qbo_environment(qbo.get("environment"), default="production")

    if not realm_id:
        messages.error(request, f"{company.display_name}: realm ID is not configured.")
        return redirect("epos_qbo:api-tokens")

    tokens = load_tokens(company.company_key, realm_id)
    if not tokens:
        messages.error(
            request,
            f"{company.display_name}: no QuickBooks tokens stored. Run the OAuth flow to connect.",
        )
        return redirect("epos_qbo:api-tokens")

    ok, message = _qbo_test_query(company.company_key, realm_id, environment)
    if ok:
        messages.success(request, f"{company.display_name}: {message}")
    else:
        _TOKEN_PAGE_LOGGER.warning(
            "QBO test connection failed for %s: %s", company.company_key, message
        )
        messages.error(request, f"{company.display_name}: {message}")
    return redirect("epos_qbo:api-tokens")


@login_required
@permission_required("epos_qbo.can_trigger_runs", raise_exception=True)
@require_POST
def api_tokens_refresh(request, company_key: str):
    """Force a refresh of the access token and verify it."""
    company = get_object_or_404(CompanyConfigRecord, company_key=company_key, is_active=True)
    cfg = company.config_json or {}
    qbo = cfg.get("qbo") or {}
    realm_id = qbo.get("realm_id")
    environment = normalize_qbo_environment(qbo.get("environment"), default="production")

    if not realm_id:
        messages.error(request, f"{company.display_name}: realm ID is not configured.")
        return redirect("epos_qbo:api-tokens")

    try:
        refresh_access_token(company.company_key, realm_id)
    except RuntimeError as exc:
        friendly = _humanize_token_error(str(exc))
        _TOKEN_PAGE_LOGGER.warning(
            "QBO refresh failed for %s: %s", company.company_key, friendly
        )
        messages.error(request, f"{company.display_name}: {friendly}")
        return redirect("epos_qbo:api-tokens")
    except Exception as exc:  # pragma: no cover - defensive
        messages.error(request, f"{company.display_name}: refresh failed ({exc}).")
        return redirect("epos_qbo:api-tokens")

    ok, message = _qbo_test_query(company.company_key, realm_id, environment)
    if ok:
        messages.success(
            request,
            f"{company.display_name}: tokens refreshed. {message}",
        )
    else:
        messages.warning(
            request,
            f"{company.display_name}: tokens refreshed, but health check reported: {message}",
        )
    return redirect("epos_qbo:api-tokens")
