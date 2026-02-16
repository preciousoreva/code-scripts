from __future__ import annotations

from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

from django.conf import settings
from django.utils import timezone


def _int_setting(name: str, default: int, *, minimum: int = 0, maximum: int = 59) -> int:
    raw = getattr(settings, name, default)
    try:
        value = int(raw)
    except (TypeError, ValueError):
        return default
    if value < minimum or value > maximum:
        return default
    return value


def get_business_timezone_name() -> str:
    return getattr(settings, "OIAT_BUSINESS_TIMEZONE", "Africa/Lagos")


def get_business_timezone() -> ZoneInfo:
    name = get_business_timezone_name()
    try:
        return ZoneInfo(name)
    except Exception:
        return ZoneInfo("UTC")


def get_business_day_cutoff() -> tuple[int, int]:
    hour = _int_setting("OIAT_BUSINESS_DAY_CUTOFF_HOUR", 5, minimum=0, maximum=23)
    minute = _int_setting("OIAT_BUSINESS_DAY_CUTOFF_MINUTE", 0, minimum=0, maximum=59)
    return hour, minute


def get_business_timezone_display(now: datetime | None = None) -> str:
    tz = get_business_timezone()
    tz_name = get_business_timezone_name()
    current = now if now is not None else timezone.now()
    if timezone.is_naive(current):
        current = timezone.make_aware(current)
    abbr = current.astimezone(tz).tzname() or ""
    if abbr and abbr not in ("UTC", "GMT"):
        return f"{abbr} ({tz_name})"
    return tz_name


def get_target_trading_date(now: datetime | None = None) -> date:
    tz = get_business_timezone()
    cutoff_hour, cutoff_minute = get_business_day_cutoff()
    current = now if now is not None else timezone.now()
    if timezone.is_naive(current):
        current = timezone.make_aware(current)
    local_now = current.astimezone(tz)
    days_back = 2 if (local_now.hour, local_now.minute) < (cutoff_hour, cutoff_minute) else 1
    return local_now.date() - timedelta(days=days_back)


def get_prev_trading_date(target_date: date) -> date:
    return target_date - timedelta(days=1)
