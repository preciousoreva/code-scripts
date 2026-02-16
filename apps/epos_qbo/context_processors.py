"""Context processors for epos_qbo app."""

from .dashboard_timezone import get_dashboard_timezone_display


def dashboard_timezone(request):
    """Add dashboard timezone display label so templates can show which TZ drives dates."""
    return {"dashboard_timezone_display": get_dashboard_timezone_display()}
