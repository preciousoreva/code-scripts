"""
Dashboard timezone: single source for "today" and "yesterday" across the portal.

All date-based dashboard logic (overview KPIs, Run Success, receipts uploaded today,
Quick Sync default date, revenue chart) uses this module so "today" and "yesterday"
are consistent and match the scheduler timezone (e.g. America/New_York).
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone as dt_tz
from zoneinfo import ZoneInfo

from django.utils import timezone

from . import portal_settings


def get_dashboard_timezone_name() -> str:
    """Return the timezone name used for dashboard dates (e.g. 'America/New_York'). DB overrides env."""
    return portal_settings.get_dashboard_timezone_name()


def get_dashboard_timezone_display() -> str:
    """Return a short label for the dashboard timezone for UI (e.g. 'EST' or 'America/New_York')."""
    name = get_dashboard_timezone_name()
    try:
        tz = ZoneInfo(name)
        now_in_tz = timezone.now().astimezone(tz)
        abbr = now_in_tz.tzname()
        if abbr and abbr not in ("UTC", "GMT"):
            return abbr
    except Exception:
        pass
    return name


def get_dashboard_date_bounds(
    now: datetime | None = None,
) -> dict:
    """
    Return canonical "now", "today start", and target dates in the dashboard timezone.
    Use these for all dashboard date logic so "today" and "yesterday" are consistent.

    Returns a dict with:
        now_utc: current time (aware UTC)
        today_start_utc: midnight "today" in dashboard TZ, as aware UTC (for DB queries)
        today_end_utc: same as now_utc (today = [today_start_utc, now_utc))
        yesterday_start_utc: midnight "yesterday" in dashboard TZ, as aware UTC
        target_date: date "yesterday" in dashboard TZ (overview target date)
        prev_target_date: date "day before yesterday" in dashboard TZ
        revenue_end_date: same as target_date (for revenue chart)
    """
    tz_name = get_dashboard_timezone_name()
    try:
        tz = ZoneInfo(tz_name)
    except Exception:
        tz = ZoneInfo("UTC")
    now_utc = now if now is not None else timezone.now()
    if timezone.is_naive(now_utc):
        now_utc = timezone.make_aware(now_utc, dt_tz.utc)
    now_dashboard = now_utc.astimezone(tz)
    today_start_dashboard = now_dashboard.replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    today_start_utc = today_start_dashboard.astimezone(dt_tz.utc)
    yesterday_start_dashboard = today_start_dashboard - timedelta(days=1)
    yesterday_start_utc = yesterday_start_dashboard.astimezone(dt_tz.utc)
    target_date = now_dashboard.date() - timedelta(days=1)
    prev_target_date = target_date - timedelta(days=1)
    return {
        "now_utc": now_utc,
        "today_start_utc": today_start_utc,
        "today_end_utc": now_utc,
        "yesterday_start_utc": yesterday_start_utc,
        "target_date": target_date,
        "prev_target_date": prev_target_date,
        "revenue_end_date": target_date,
    }
