"""
Portal settings: DB-backed dashboard defaults with env/settings fallback.

When PortalSettings row (id=1) has a non-null value for a field, that value is used.
Otherwise the value comes from Django settings / environment.
"""

from __future__ import annotations

import threading
import time
from decimal import Decimal

from django.conf import settings as django_settings
from django.db import DatabaseError

from .models import PortalSettings

# Default reauth guidance when neither DB nor env is set
_DEFAULT_REAUTH_GUIDANCE = (
    "QBO re-authentication required. Run OAuth flow and store tokens using code_scripts/store_tokens.py."
)
_CACHE_TTL_SECONDS = 30.0
_CACHE_LOCK = threading.Lock()
_CACHE_UNSET = object()
_singleton_cache: dict | None | object = _CACHE_UNSET
_singleton_cache_at: float = 0.0


def invalidate_cache() -> None:
    """Clear in-process singleton cache. Called after PortalSettings writes."""
    global _singleton_cache, _singleton_cache_at
    with _CACHE_LOCK:
        _singleton_cache = _CACHE_UNSET
        _singleton_cache_at = 0.0


def _load_singleton_snapshot() -> tuple[dict | None, bool]:
    """
    Load singleton row values once.

    Returns:
        (snapshot, cacheable)
        snapshot: dict of selected fields, or None when no row.
        cacheable: False when DB is unavailable (skip caching transient failure).
    """
    try:
        snapshot = PortalSettings.objects.filter(pk=1).values(
            "default_parallel",
            "default_stagger_seconds",
            "stale_hours_warning",
            "refresh_expiring_days",
            "reconcile_diff_warning",
            "reauth_guidance",
            "dashboard_timezone",
        ).first()
        return snapshot, True
    except DatabaseError:
        return None, False


def _get_singleton_snapshot() -> dict | None:
    """Return cached singleton values for fast repeated reads with DB fallback."""
    global _singleton_cache, _singleton_cache_at
    now = time.monotonic()
    with _CACHE_LOCK:
        if (
            _singleton_cache is not _CACHE_UNSET
            and (now - _singleton_cache_at) < _CACHE_TTL_SECONDS
        ):
            return _singleton_cache
    snapshot, cacheable = _load_singleton_snapshot()
    if cacheable:
        with _CACHE_LOCK:
            _singleton_cache = snapshot
            _singleton_cache_at = now
    return snapshot


def _int_from_settings(name: str, default: int, *, minimum: int = 0) -> int:
    raw = getattr(django_settings, name, default)
    try:
        value = int(raw)
    except (TypeError, ValueError):
        return default
    if value < minimum:
        return default
    return value


def _decimal_from_settings(name: str, default: Decimal, *, minimum: Decimal | None = None) -> Decimal:
    raw = getattr(django_settings, name, default)
    try:
        value = Decimal(str(raw))
    except Exception:
        return default
    if minimum is not None and value < minimum:
        return default
    return value


def get_default_parallel() -> int:
    row = _get_singleton_snapshot()
    if row is not None and row.get("default_parallel") is not None:
        return max(1, int(row["default_parallel"]))
    return _int_from_settings("OIAT_DASHBOARD_DEFAULT_PARALLEL", 2, minimum=1)


def get_default_stagger_seconds() -> int:
    row = _get_singleton_snapshot()
    if row is not None and row.get("default_stagger_seconds") is not None:
        return max(0, int(row["default_stagger_seconds"]))
    return _int_from_settings("OIAT_DASHBOARD_DEFAULT_STAGGER_SECONDS", 2, minimum=0)


def get_stale_hours_warning() -> int:
    row = _get_singleton_snapshot()
    if row is not None and row.get("stale_hours_warning") is not None:
        return max(1, int(row["stale_hours_warning"]))
    return _int_from_settings("OIAT_DASHBOARD_STALE_HOURS_WARNING", 48, minimum=1)


def get_refresh_expiring_days() -> int:
    row = _get_singleton_snapshot()
    if row is not None and row.get("refresh_expiring_days") is not None:
        return max(1, int(row["refresh_expiring_days"]))
    return _int_from_settings("OIAT_DASHBOARD_REFRESH_EXPIRING_DAYS", 7, minimum=1)


def get_reconcile_diff_warning() -> Decimal:
    row = _get_singleton_snapshot()
    if row is not None and row.get("reconcile_diff_warning") is not None:
        val = Decimal(str(row["reconcile_diff_warning"]))
        return max(Decimal("0"), val)
    return _decimal_from_settings(
        "OIAT_DASHBOARD_RECON_DIFF_WARNING", Decimal("1.0"), minimum=Decimal("0")
    )


def get_reauth_guidance() -> str:
    row = _get_singleton_snapshot()
    row_text = ((row or {}).get("reauth_guidance") or "").strip()
    if row_text:
        return row_text
    text = str(
        getattr(django_settings, "OIAT_DASHBOARD_REAUTH_GUIDANCE", _DEFAULT_REAUTH_GUIDANCE)
    ).strip()
    return text or _DEFAULT_REAUTH_GUIDANCE


def get_dashboard_timezone_name() -> str:
    """Return the timezone name used for dashboard dates. DB overrides env/settings."""
    row = _get_singleton_snapshot()
    row_tz = ((row or {}).get("dashboard_timezone") or "").strip()
    if row_tz:
        return row_tz
    return str(
        getattr(
            django_settings,
            "OIAT_DASHBOARD_TIMEZONE",
            getattr(django_settings, "TIME_ZONE", "UTC"),
        )
    )
