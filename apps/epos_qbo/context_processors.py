"""Context processors for epos_qbo app."""

from django.conf import settings
from django.urls import reverse

from .dashboard_timezone import get_dashboard_timezone_display


def dashboard_timezone(request):
    """Add dashboard timezone display label so templates can show which TZ drives dates."""
    return {"dashboard_timezone_display": get_dashboard_timezone_display()}


def copilot(request):
    """Expose read-only copilot UI config to authenticated portal templates."""
    enabled = bool(getattr(settings, "OIAT_COPILOT_ENABLED", False))
    return {
        "copilot_enabled": enabled,
        "copilot_ask_url": reverse("epos_qbo:copilot-ask") if enabled else "",
        "copilot_max_question_chars": getattr(settings, "OIAT_COPILOT_MAX_QUESTION_CHARS", 1000),
    }
