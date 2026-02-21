from __future__ import annotations

import logging
import os
from datetime import datetime
from typing import Any

from django.conf import settings
from django.db import transaction
from django.utils import timezone

from apps.epos_qbo.business_date import get_target_trading_date

from ..models import RunJob, RunSchedule, RunScheduleEvent, SchedulerWorkerHeartbeat
from .job_runner import dispatch_next_queued_job

logger = logging.getLogger(__name__)

DEFAULT_WORKER_POLL_SECONDS = 15
DEFAULT_FALLBACK_CRON = "0 18 * * *"  # 6pm daily
FALLBACK_SCHEDULE_NAME = "Legacy Env Fallback"


def _env_flag(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _env_int(name: str, default: int, *, minimum: int = 1) -> int:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        value = int(raw)
    except (TypeError, ValueError):
        return default
    if value < minimum:
        return default
    return value


def configured_poll_seconds() -> int:
    return _env_int(
        "OIAT_SCHEDULER_POLL_SECONDS",
        DEFAULT_WORKER_POLL_SECONDS,
        minimum=1,
    )


def env_fallback_enabled() -> bool:
    return _env_flag("OIAT_SCHEDULER_ENABLE_ENV_FALLBACK", True)


def _default_schedule_timezone() -> str:
    return str(
        getattr(
            settings,
            "OIAT_BUSINESS_TIMEZONE",
            getattr(settings, "TIME_ZONE", "UTC"),
        )
    )


def _fallback_cron_expr() -> str:
    value = (os.getenv("SCHEDULE_CRON") or DEFAULT_FALLBACK_CRON).strip()
    return value or DEFAULT_FALLBACK_CRON


def _fallback_timezone_name() -> str:
    value = (os.getenv("SCHEDULE_TZ") or "").strip()
    if value:
        return value
    return _default_schedule_timezone()


def _create_event(
    *,
    schedule: RunSchedule | None,
    event_type: str,
    message: str,
    run_job: RunJob | None = None,
    payload: dict[str, Any] | None = None,
) -> RunScheduleEvent:
    payload_json = dict(payload or {})
    if schedule is not None:
        payload_json.setdefault("schedule_id", str(schedule.id))
        payload_json.setdefault("schedule_name", schedule.name)
        payload_json.setdefault("schedule_scope", schedule.scope)
    return RunScheduleEvent.objects.create(
        schedule=schedule,
        run_job=run_job,
        event_type=event_type,
        message=message,
        payload_json=payload_json,
    )


def _active_scheduled_run_exists(schedule: RunSchedule) -> bool:
    return RunJob.objects.filter(
        scheduled_by=schedule,
        status__in=[RunJob.STATUS_QUEUED, RunJob.STATUS_RUNNING],
    ).exists()


def _job_payload_from_schedule(schedule: RunSchedule, *, now: datetime) -> dict[str, Any]:
    target_date = get_target_trading_date(now=now)
    if schedule.scope == RunJob.SCOPE_SINGLE:
        parallel = 1
        continue_on_failure = False
    else:
        parallel = max(1, int(schedule.parallel))
        continue_on_failure = bool(schedule.continue_on_failure)
    return {
        "scope": schedule.scope,
        "company_key": schedule.company_key or None,
        "target_date": target_date,
        "parallel": parallel,
        "stagger_seconds": max(0, int(schedule.stagger_seconds)),
        "continue_on_failure": continue_on_failure,
        "status": RunJob.STATUS_QUEUED,
        "scheduled_by": schedule,
        "command_display": f"schedule:{schedule.name}",
    }


def _initialize_missing_next_fire(schedule: RunSchedule, *, now: datetime) -> bool:
    if not schedule.enabled or schedule.next_fire_at is not None:
        return False
    try:
        schedule.next_fire_at = schedule.compute_next_fire_at(from_dt=now)
        schedule.last_error = ""
        schedule.save(update_fields=["next_fire_at", "last_error", "updated_at"])
    except Exception as exc:
        schedule.last_result = RunSchedule.LAST_RESULT_SKIPPED_INVALID
        schedule.last_error = str(exc)
        schedule.save(update_fields=["last_result", "last_error", "updated_at"])
        _create_event(
            schedule=schedule,
            event_type=RunScheduleEvent.TYPE_SKIPPED_INVALID,
            message=f"Schedule is invalid and cannot be initialized: {exc}",
        )
    return True


def _upsert_env_fallback_schedule(*, now: datetime) -> dict[str, int]:
    stats = {"fallback_enabled": 0, "fallback_disabled": 0}

    if not env_fallback_enabled():
        for schedule in RunSchedule.objects.filter(is_system_managed=True, enabled=True):
            schedule.enabled = False
            schedule.save(update_fields=["enabled", "updated_at"])
            _create_event(
                schedule=schedule,
                event_type=RunScheduleEvent.TYPE_FALLBACK_DISABLED,
                message="Environment fallback disabled by OIAT_SCHEDULER_ENABLE_ENV_FALLBACK=0.",
            )
            stats["fallback_disabled"] += 1
        return stats

    has_enabled_user_schedule = RunSchedule.objects.filter(
        enabled=True,
        is_system_managed=False,
    ).exists()

    if has_enabled_user_schedule:
        for schedule in RunSchedule.objects.filter(is_system_managed=True, enabled=True):
            schedule.enabled = False
            schedule.save(update_fields=["enabled", "updated_at"])
            _create_event(
                schedule=schedule,
                event_type=RunScheduleEvent.TYPE_FALLBACK_DISABLED,
                message="Disabled env fallback because at least one DB schedule is enabled.",
            )
            stats["fallback_disabled"] += 1
        return stats

    schedule, created = RunSchedule.objects.get_or_create(
        name=FALLBACK_SCHEDULE_NAME,
        is_system_managed=True,
        defaults={
            "enabled": True,
            "scope": RunJob.SCOPE_ALL,
            "company_key": None,
            "cron_expr": _fallback_cron_expr(),
            "timezone_name": _fallback_timezone_name(),
            "target_date_mode": RunSchedule.TARGET_DATE_MODE_TRADING_DATE,
            "parallel": 2,
            "stagger_seconds": 2,
            "continue_on_failure": False,
        },
    )

    changed = created
    cron_expr = _fallback_cron_expr()
    timezone_name = _fallback_timezone_name()
    if schedule.cron_expr != cron_expr:
        schedule.cron_expr = cron_expr
        changed = True
    if schedule.timezone_name != timezone_name:
        schedule.timezone_name = timezone_name
        changed = True
    if not schedule.enabled:
        schedule.enabled = True
        changed = True

    if schedule.next_fire_at is None:
        changed = True
    if changed:
        try:
            schedule.next_fire_at = schedule.compute_next_fire_at(from_dt=now)
            schedule.last_error = ""
        except Exception as exc:
            schedule.last_result = RunSchedule.LAST_RESULT_SKIPPED_INVALID
            schedule.last_error = str(exc)
        update_fields = [
            "enabled",
            "cron_expr",
            "timezone_name",
            "next_fire_at",
            "last_result",
            "last_error",
            "updated_at",
        ]
        schedule.save(update_fields=update_fields)
        _create_event(
            schedule=schedule,
            event_type=RunScheduleEvent.TYPE_FALLBACK_ENABLED,
            message="Env fallback schedule enabled from SCHEDULE_CRON/SCHEDULE_TZ.",
            payload={"cron_expr": schedule.cron_expr, "timezone_name": schedule.timezone_name},
        )
        stats["fallback_enabled"] += 1

    return stats


def enqueue_run_for_schedule(
    schedule: RunSchedule,
    *,
    now: datetime | None = None,
    source: str = "manual",
) -> tuple[RunJob | None, str]:
    current = now or timezone.now()
    if schedule.scope == RunJob.SCOPE_SINGLE and not (schedule.company_key or "").strip():
        _create_event(
            schedule=schedule,
            event_type=RunScheduleEvent.TYPE_SKIPPED_INVALID,
            message=f"Skipped {source} enqueue: single-company schedule is missing company key.",
        )
        return None, RunScheduleEvent.TYPE_SKIPPED_INVALID

    with transaction.atomic():
        schedule = RunSchedule.objects.select_for_update().get(pk=schedule.pk)
        if schedule.scope == RunJob.SCOPE_SINGLE and not (schedule.company_key or "").strip():
            schedule.last_result = RunSchedule.LAST_RESULT_SKIPPED_INVALID
            schedule.last_error = "Single-company schedule is missing company key."
            schedule.save(update_fields=["last_result", "last_error", "updated_at"])
            _create_event(
                schedule=schedule,
                event_type=RunScheduleEvent.TYPE_SKIPPED_INVALID,
                message=f"Skipped {source} enqueue: single-company schedule is missing company key.",
            )
            return None, RunScheduleEvent.TYPE_SKIPPED_INVALID
        if _active_scheduled_run_exists(schedule):
            schedule.last_result = RunSchedule.LAST_RESULT_SKIPPED_OVERLAP
            schedule.last_error = ""
            schedule.last_fired_at = current
            schedule.save(update_fields=["last_result", "last_error", "last_fired_at", "updated_at"])
            _create_event(
                schedule=schedule,
                event_type=RunScheduleEvent.TYPE_SKIPPED_OVERLAP,
                message=f"Skipped {source} enqueue because this schedule already has a queued/running run.",
            )
            return None, RunScheduleEvent.TYPE_SKIPPED_OVERLAP

        payload = _job_payload_from_schedule(schedule, now=current)
        job = RunJob.objects.create(**payload)
        schedule.last_result = RunSchedule.LAST_RESULT_QUEUED
        schedule.last_error = ""
        schedule.last_fired_at = current
        schedule.save(update_fields=["last_result", "last_error", "last_fired_at", "updated_at"])

        _create_event(
            schedule=schedule,
            run_job=job,
            event_type=RunScheduleEvent.TYPE_QUEUED,
            message=f"Run queued ({source}).",
            payload={
                "scope": job.scope,
                "company_key": job.company_key,
                "target_date": job.target_date.isoformat() if job.target_date else None,
            },
        )
        return job, RunScheduleEvent.TYPE_QUEUED


def _process_due_schedule(schedule: RunSchedule, *, now: datetime) -> tuple[RunJob | None, str]:
    if schedule.scope == RunJob.SCOPE_SINGLE and not (schedule.company_key or "").strip():
        schedule.last_result = RunSchedule.LAST_RESULT_SKIPPED_INVALID
        schedule.last_error = "Single-company schedule is missing company key."
        schedule.save(update_fields=["last_result", "last_error", "updated_at"])
        _create_event(
            schedule=schedule,
            event_type=RunScheduleEvent.TYPE_SKIPPED_INVALID,
            message="Skipping invalid schedule: single-company scope requires company key.",
        )
        return None, RunScheduleEvent.TYPE_SKIPPED_INVALID

    try:
        next_fire_at = schedule.compute_next_fire_at(from_dt=now)
    except Exception as exc:
        schedule.last_result = RunSchedule.LAST_RESULT_SKIPPED_INVALID
        schedule.last_error = str(exc)
        schedule.save(update_fields=["last_result", "last_error", "updated_at"])
        _create_event(
            schedule=schedule,
            event_type=RunScheduleEvent.TYPE_SKIPPED_INVALID,
            message=f"Skipping invalid schedule: {exc}",
        )
        return None, RunScheduleEvent.TYPE_SKIPPED_INVALID

    schedule.next_fire_at = next_fire_at
    schedule.save(update_fields=["next_fire_at", "updated_at"])
    return enqueue_run_for_schedule(schedule, now=now, source="worker")


def process_schedule_cycle(*, now: datetime | None = None, max_due: int = 25) -> dict[str, int]:
    current = now or timezone.now()
    stats = {
        "initialized": 0,
        "due": 0,
        "queued": 0,
        "skipped_overlap": 0,
        "skipped_invalid": 0,
        "errors": 0,
        "fallback_enabled": 0,
        "fallback_disabled": 0,
    }

    fallback_stats = _upsert_env_fallback_schedule(now=current)
    stats.update(fallback_stats)

    for schedule in RunSchedule.objects.filter(enabled=True, next_fire_at__isnull=True):
        if _initialize_missing_next_fire(schedule, now=current):
            stats["initialized"] += 1

    with transaction.atomic():
        due_schedules = list(
            RunSchedule.objects.select_for_update(skip_locked=True)
            .filter(enabled=True, next_fire_at__isnull=False, next_fire_at__lte=current)
            .order_by("next_fire_at", "created_at")[:max_due]
        )
        stats["due"] = len(due_schedules)

        for schedule in due_schedules:
            try:
                job, result = _process_due_schedule(schedule, now=current)
            except Exception as exc:
                stats["errors"] += 1
                logger.exception("Failed processing schedule %s", schedule.id)
                _create_event(
                    schedule=schedule,
                    event_type=RunScheduleEvent.TYPE_ERROR,
                    message=f"Unhandled worker error: {exc}",
                )
                continue

            if job is not None and result == RunScheduleEvent.TYPE_QUEUED:
                stats["queued"] += 1
            elif result == RunScheduleEvent.TYPE_SKIPPED_OVERLAP:
                stats["skipped_overlap"] += 1
            elif result == RunScheduleEvent.TYPE_SKIPPED_INVALID:
                stats["skipped_invalid"] += 1

    if stats["queued"] > 0:
        dispatch_next_queued_job()

    _record_heartbeat(current)
    return stats


def _record_heartbeat(now: datetime | None = None) -> None:
    """Update the scheduler worker heartbeat (single row id=1). Used by the Schedules page to show service status."""
    current = now or timezone.now()
    SchedulerWorkerHeartbeat.objects.update_or_create(
        id=1,
        defaults={"last_seen": current},
    )


# Staleness threshold: if last_seen is older than this many seconds, consider the worker "not running"
HEARTBEAT_STALE_MULTIPLIER = 3


def get_scheduler_status() -> dict:
    """
    Return scheduler worker status for the Schedules page.
    Keys: running (bool), last_seen (datetime | None), message (str).
    """
    poll_seconds = configured_poll_seconds()
    stale_seconds = poll_seconds * HEARTBEAT_STALE_MULTIPLIER
    now = timezone.now()

    try:
        hb = SchedulerWorkerHeartbeat.objects.filter(id=1).first()
    except Exception:
        return {"running": False, "last_seen": None, "message": "Scheduler status unavailable."}

    if hb is None:
        return {"running": False, "last_seen": None, "message": "Scheduler has not run yet."}

    age_seconds = (now - hb.last_seen).total_seconds()
    running = age_seconds <= stale_seconds
    if running:
        message = "Worker is polling; scheduled runs will run at their next fire time."
    else:
        message = f"Worker last seen {int(age_seconds)}s ago. Start the scheduler (e.g. docker compose up -d scheduler) for scheduled runs to execute."
    return {"running": running, "last_seen": hb.last_seen, "message": message}
