from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from zoneinfo import ZoneInfo

DEFAULT_SCHEDULE_TZ = "Africa/Lagos"
DEFAULT_RUN_CMD = "python manage.py run_scheduled_all_companies --parallel 2"
DEFAULT_MISFIRE_GRACE_SECONDS = 300
DEFAULT_LOG_LEVEL = "INFO"

_VALID_LOG_LEVELS = {"CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG"}


@dataclass(frozen=True)
class SchedulerConfig:
    schedule_cron: str
    schedule_tz: str
    run_cmd: str
    misfire_grace_seconds: int
    log_level: str


def _read_int_env(name: str, default: int, *, minimum: int = 0) -> int:
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


def _validate_timezone_name(name: str) -> str:
    try:
        ZoneInfo(name)
    except Exception as exc:  # pragma: no cover - defensive guard
        raise ValueError(f"Invalid SCHEDULE_TZ timezone: {name}") from exc
    return name


def _normalize_log_level(raw_level: str) -> str:
    level = (raw_level or DEFAULT_LOG_LEVEL).strip().upper()
    if level in _VALID_LOG_LEVELS:
        return level
    return DEFAULT_LOG_LEVEL


def load_scheduler_config() -> SchedulerConfig:
    schedule_cron = (os.getenv("SCHEDULE_CRON") or "").strip()
    if not schedule_cron:
        raise ValueError("SCHEDULE_CRON is required (cron syntax: '*/5 * * * *').")

    schedule_tz = _validate_timezone_name(
        (os.getenv("SCHEDULE_TZ") or DEFAULT_SCHEDULE_TZ).strip() or DEFAULT_SCHEDULE_TZ
    )
    run_cmd = (os.getenv("RUN_CMD") or DEFAULT_RUN_CMD).strip() or DEFAULT_RUN_CMD

    misfire_grace_seconds = _read_int_env(
        "SCHEDULER_MISFIRE_GRACE_SECONDS",
        DEFAULT_MISFIRE_GRACE_SECONDS,
        minimum=1,
    )
    log_level = _normalize_log_level(os.getenv("SCHEDULER_LOG_LEVEL", DEFAULT_LOG_LEVEL))

    return SchedulerConfig(
        schedule_cron=schedule_cron,
        schedule_tz=schedule_tz,
        run_cmd=run_cmd,
        misfire_grace_seconds=misfire_grace_seconds,
        log_level=log_level,
    )


def resolve_logging_level(name: str) -> int:
    return getattr(logging, _normalize_log_level(name), logging.INFO)
