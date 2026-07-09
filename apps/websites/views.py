from __future__ import annotations

from datetime import datetime, time, timedelta

from django.contrib.auth.decorators import login_required
from django.core.paginator import Paginator
from django.db.models import Count, Q
from django.http import JsonResponse
from django.shortcuts import get_object_or_404, render
from django.utils.dateparse import parse_date
from django.utils import timezone
from django.utils.timesince import timesince

from .models import Website, WebsiteLogEvent


@login_required
def index(request):
    websites = list(
        Website.objects.annotate(log_count=Count("log_events")).order_by("name")
    )
    since = timezone.now() - timedelta(days=7)
    total_logs_7d = WebsiteLogEvent.objects.filter(received_at__gte=since).count()
    error_logs_7d = WebsiteLogEvent.objects.filter(
        received_at__gte=since,
        severity__in=[WebsiteLogEvent.SEVERITY_ERROR, WebsiteLogEvent.SEVERITY_CRITICAL],
    ).count()
    context = {
        "websites": websites,
        "stats": {
            "website_count": len(websites),
            "active_count": sum(1 for site in websites if site.is_active),
            "logs_7d": total_logs_7d,
            "errors_7d": error_logs_7d,
        },
    }
    return render(request, "websites/index.html", context)


@login_required
def website_detail(request, site_slug: str):
    website = get_object_or_404(Website, slug=site_slug)
    recent_logs = website.log_events.all()[:25]
    since = timezone.now() - timedelta(days=7)
    severity_counts = {
        row["severity"]: row["total"]
        for row in website.log_events.filter(received_at__gte=since)
        .values("severity")
        .annotate(total=Count("id"))
    }
    severity_cards = [
        {
            "severity": severity,
            "label": label,
            "count": severity_counts.get(severity, 0),
        }
        for severity, label in WebsiteLogEvent.SEVERITY_CHOICES
    ]
    context = {
        "website": website,
        "recent_logs": recent_logs,
        "severity_cards": severity_cards,
        "last_log": recent_logs[0] if recent_logs else None,
    }
    return render(request, "websites/detail.html", context)


@login_required
def website_logs(request, site_slug: str):
    website = get_object_or_404(Website, slug=site_slug)
    logs, filters = _filtered_logs(request, website)
    paginator = Paginator(logs, 50)
    page_obj = paginator.get_page(request.GET.get("page"))
    context = {
        "website": website,
        "logs": page_obj,
        "page_obj": page_obj,
        "filters": filters,
        "severity_choices": WebsiteLogEvent.SEVERITY_CHOICES,
    }
    return render(request, "websites/logs.html", context)


@login_required
def website_logs_api(request, site_slug: str):
    website = get_object_or_404(Website, slug=site_slug)
    logs, filters = _filtered_logs(request, website)
    limit = _bounded_int(request.GET.get("limit"), default=25, minimum=1, maximum=100)
    rows = list(logs[:limit])
    since = timezone.now() - timedelta(days=7)
    severity_counts = {
        row["severity"]: row["total"]
        for row in website.log_events.filter(received_at__gte=since)
        .values("severity")
        .annotate(total=Count("id"))
    }
    return JsonResponse(
        {
            "logs": [_serialize_log_event(log) for log in rows],
            "total": logs.count(),
            "filters": filters,
            "last_received": _last_received_payload(rows[0] if rows else website.log_events.first()),
            "severity_counts": {
                severity: severity_counts.get(severity, 0)
                for severity, _label in WebsiteLogEvent.SEVERITY_CHOICES
            },
        }
    )


@login_required
def website_log_detail_api(request, site_slug: str, log_id: int):
    website = get_object_or_404(Website, slug=site_slug)
    log = get_object_or_404(WebsiteLogEvent, website=website, id=log_id)
    return JsonResponse(
        {
            "log": _serialize_log_event(log),
            "raw_payload": log.raw_payload,
            "request_headers": log.request_headers,
            "received_from": {
                "remote_addr": log.remote_addr or "",
                "user_agent": log.user_agent,
            },
        }
    )


def _filtered_logs(request, website: Website):
    logs = website.log_events.all()
    filters = {
        "severity": request.GET.get("severity", "").strip(),
        "q": request.GET.get("q", "").strip(),
        "trace": request.GET.get("trace", "").strip(),
        "date_from": request.GET.get("date_from", "").strip(),
        "date_to": request.GET.get("date_to", "").strip(),
    }
    if filters["severity"]:
        logs = logs.filter(severity=filters["severity"])
    if filters["q"]:
        q = filters["q"]
        logs = logs.filter(
            Q(message__icontains=q)
            | Q(source__icontains=q)
            | Q(event_type__icontains=q)
            | Q(request_id__icontains=q)
            | Q(trace_id__icontains=q)
            | Q(pathname__icontains=q)
            | Q(function_name__icontains=q)
            | Q(context_text__icontains=q)
        )
    if filters["trace"]:
        trace = filters["trace"]
        logs = logs.filter(Q(request_id__icontains=trace) | Q(trace_id__icontains=trace))
    date_from = parse_date(filters["date_from"]) if filters["date_from"] else None
    if date_from:
        start = timezone.make_aware(
            datetime.combine(date_from, time.min),
            timezone.get_current_timezone(),
        )
        logs = logs.filter(received_at__gte=start)
    date_to = parse_date(filters["date_to"]) if filters["date_to"] else None
    if date_to:
        end = timezone.make_aware(
            datetime.combine(date_to, time.max),
            timezone.get_current_timezone(),
        )
        logs = logs.filter(received_at__lte=end)
    return logs, filters


def _serialize_log_event(log: WebsiteLogEvent) -> dict[str, object]:
    received = timezone.localtime(log.received_at)
    occurred = timezone.localtime(log.occurred_at) if log.occurred_at else None
    return {
        "id": log.id,
        "received_at": received.isoformat(),
        "received_display": received.strftime("%b %-d, %Y, %-I:%M %p"),
        "occurred_at": occurred.isoformat() if occurred else "",
        "severity": log.severity,
        "severity_label": log.get_severity_display(),
        "source": log.source,
        "event_type": log.event_type,
        "message": log.message or log.event_type or "No message",
        "request_id": log.request_id,
        "trace_id": log.trace_id,
        "trace_display": log.request_id or log.trace_id or "",
        "pathname": log.pathname,
        "function_name": log.function_name,
        "context": log.context,
        "context_summary": _context_summary(log.context),
    }


def _last_received_payload(log: WebsiteLogEvent | None) -> dict[str, str]:
    if not log:
        return {"label": "No logs received yet", "iso": ""}
    return {
        "label": f"Last received {timesince(log.received_at)} ago",
        "iso": timezone.localtime(log.received_at).isoformat(),
    }


def _bounded_int(value: str | None, *, default: int, minimum: int, maximum: int) -> int:
    try:
        parsed = int(value or default)
    except (TypeError, ValueError):
        parsed = default
    return min(max(parsed, minimum), maximum)


def _context_summary(context: dict[str, object]) -> str:
    priority = [
        "membershipId",
        "emailError",
        "mirrorError",
        "legacyMirrorError",
        "primaryOk",
        "legacyMirrorOk",
        "mirrorOk",
        "emailSent",
        "attempt",
    ]
    parts = []
    for key in priority:
        value = context.get(key)
        if value in ("", None):
            continue
        if isinstance(value, bool):
            value = "true" if value else "false"
        parts.append(f"{key}={value}")
        if len(parts) == 3:
            break
    return " · ".join(parts)
