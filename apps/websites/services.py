from __future__ import annotations

from datetime import datetime, timezone as dt_timezone
from typing import Any

from django.http import HttpRequest
from django.utils import timezone
from django.utils.dateparse import parse_datetime

from .models import Website, WebsiteLogEvent


SEVERITY_ALIASES = {
    "debug": WebsiteLogEvent.SEVERITY_DEBUG,
    "trace": WebsiteLogEvent.SEVERITY_DEBUG,
    "info": WebsiteLogEvent.SEVERITY_INFO,
    "log": WebsiteLogEvent.SEVERITY_INFO,
    "notice": WebsiteLogEvent.SEVERITY_INFO,
    "warn": WebsiteLogEvent.SEVERITY_WARNING,
    "warning": WebsiteLogEvent.SEVERITY_WARNING,
    "error": WebsiteLogEvent.SEVERITY_ERROR,
    "err": WebsiteLogEvent.SEVERITY_ERROR,
    "critical": WebsiteLogEvent.SEVERITY_CRITICAL,
    "fatal": WebsiteLogEvent.SEVERITY_CRITICAL,
}


def create_wix_log_events(
    *,
    website: Website,
    payload: dict[str, Any] | list[Any],
    request: HttpRequest,
) -> list[WebsiteLogEvent]:
    entries = payload if isinstance(payload, list) else [payload]
    events = []
    headers = _safe_headers(request)
    remote_addr = _remote_addr(request)
    user_agent = request.META.get("HTTP_USER_AGENT", "")

    for entry in entries:
        raw_entry = entry if isinstance(entry, dict) else {"value": entry}
        events.append(
            WebsiteLogEvent.objects.create(
                website=website,
                occurred_at=_extract_timestamp(raw_entry),
                severity=_extract_severity(raw_entry),
                source=_extract_source(raw_entry),
                event_type=_extract_event_type(raw_entry),
                message=_extract_message(raw_entry),
                request_id=_extract_request_id(raw_entry),
                trace_id=_extract_trace_id(raw_entry),
                pathname=_extract_path(raw_entry),
                function_name=_extract_function_name(raw_entry),
                remote_addr=remote_addr,
                user_agent=user_agent,
                raw_payload=raw_entry,
                request_headers=headers,
            )
        )
    return events


def _extract_severity(payload: dict[str, Any]) -> str:
    raw = _first_string(payload, "severity", "level", "logLevel", "log_level", "status")
    return SEVERITY_ALIASES.get(raw.lower(), WebsiteLogEvent.SEVERITY_UNKNOWN) if raw else WebsiteLogEvent.SEVERITY_UNKNOWN


def _extract_message(payload: dict[str, Any]) -> str:
    message = _first_string(payload, "message", "msg", "text", "description", "errorMessage", "error_message")
    if message:
        return message
    json_payload = payload.get("jsonPayload")
    if isinstance(json_payload, dict):
        message = _first_string(json_payload, "message", "msg", "text", "description", "errorMessage", "error_message")
        if message:
            return message
    error = payload.get("error")
    if isinstance(error, dict):
        return _first_string(error, "message", "name", "code")
    if isinstance(error, str):
        return error
    return ""


def _extract_path(payload: dict[str, Any]) -> str:
    direct = _first_string(payload, "path", "pathname", "url", "pageUrl", "page_url")
    if direct:
        return _trim(direct, 512)
    labels = payload.get("labels")
    if isinstance(labels, dict):
        label_path = _first_string(labels, "pageName", "path", "pathname", "route", "url")
        if label_path:
            return _trim(label_path, 512)
    request = payload.get("request")
    if isinstance(request, dict):
        return _trim(_first_string(request, "path", "pathname", "url"), 512)
    return ""


def _extract_timestamp(payload: dict[str, Any]) -> datetime | None:
    raw = _first_string(payload, "timestamp", "time", "datetime", "date", "createdAt", "created_at")
    if not raw:
        return None
    parsed = parse_datetime(raw)
    if parsed is None:
        return None
    if timezone.is_naive(parsed):
        return timezone.make_aware(parsed, timezone=dt_timezone.utc)
    return parsed


def _extract_source(payload: dict[str, Any]) -> str:
    source = _first_string(payload, "source", "logger", "module", "origin", "namespace")
    if source:
        return _trim(source, 255)
    source_location = payload.get("sourceLocation")
    if isinstance(source_location, dict):
        return _trim(_first_string(source_location, "file", "line", "function"), 255)
    return ""


def _extract_event_type(payload: dict[str, Any]) -> str:
    event_type = _first_string(payload, "type", "eventType", "event_type", "level", "severity")
    if event_type:
        return _trim(event_type, 255)
    labels = payload.get("labels")
    if isinstance(labels, dict):
        return _trim(_first_string(labels, "namespace", "viewMode", "revision"), 255)
    return ""


def _extract_request_id(payload: dict[str, Any]) -> str:
    request_id = _first_string(payload, "requestId", "request_id", "correlationId", "correlation_id", "insertId", "id")
    if request_id:
        return _trim(request_id, 255)
    operation = payload.get("operation")
    if isinstance(operation, dict):
        return _trim(_first_string(operation, "id", "producer"), 255)
    return ""


def _extract_trace_id(payload: dict[str, Any]) -> str:
    return _trim(_first_string(payload, "traceId", "trace_id", "transactionId", "transaction_id", "trace", "spanId"), 255)


def _extract_function_name(payload: dict[str, Any]) -> str:
    function_name = _first_string(payload, "functionName", "function_name", "function")
    if function_name:
        return _trim(function_name, 255)
    source_location = payload.get("sourceLocation")
    if isinstance(source_location, dict):
        return _trim(_first_string(source_location, "function"), 255)
    operation = payload.get("operation")
    if isinstance(operation, dict):
        return _trim(_first_string(operation, "producer"), 255)
    return ""


def _first_string(payload: dict[str, Any], *keys: str) -> str:
    for key in keys:
        value = payload.get(key)
        if value is None:
            continue
        if isinstance(value, str):
            return value.strip()
        if isinstance(value, (int, float, bool)):
            return str(value)
    return ""


def _remote_addr(request: HttpRequest) -> str | None:
    forwarded = request.META.get("HTTP_X_FORWARDED_FOR", "")
    if forwarded:
        return forwarded.split(",", 1)[0].strip() or None
    return request.META.get("REMOTE_ADDR") or None


def _safe_headers(request: HttpRequest) -> dict[str, str]:
    allowed = {
        "CONTENT_TYPE",
        "CONTENT_LENGTH",
        "HTTP_USER_AGENT",
        "HTTP_X_FORWARDED_FOR",
        "HTTP_CF_CONNECTING_IP",
        "HTTP_CF_RAY",
    }
    return {key: str(value) for key, value in request.META.items() if key in allowed}


def _trim(value: str, max_length: int) -> str:
    return value[:max_length] if value else ""
