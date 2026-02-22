from __future__ import annotations

from datetime import datetime

from django import template
from django.utils import timezone

register = template.Library()


def _pluralize(value: int, unit: str) -> str:
    suffix = "" if value == 1 else "s"
    return f"{value} {unit}{suffix}"


@register.filter(name="relative_time")
def relative_time(value: datetime | None) -> str:
    if value is None:
        return "-"

    if timezone.is_naive(value):
        value = timezone.make_aware(value, timezone.get_current_timezone())

    now = timezone.now()
    delta = now - value

    if delta.total_seconds() < 0:
        future_seconds = int(abs(delta.total_seconds()))
        if future_seconds < 60:
            return "in under a minute"
        if future_seconds < 3600:
            minutes = max(1, future_seconds // 60)
            return f"in {_pluralize(minutes, 'minute')}"
        hours = max(1, future_seconds // 3600)
        return f"in {_pluralize(hours, 'hour')}"

    seconds = int(delta.total_seconds())
    if seconds < 60:
        return "just now"
    if seconds < 3600:
        minutes = max(1, seconds // 60)
        return f"{_pluralize(minutes, 'minute')} ago"

    days = delta.days
    if days == 0:
        hours = max(1, seconds // 3600)
        return f"{_pluralize(hours, 'hour')} ago"
    if days == 1:
        return "yesterday"
    return f"{_pluralize(days, 'day')} ago"
