from __future__ import annotations

import json
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

CONTEXT_KEYS = {
    "attempt",
    "emailError",
    "emailSent",
    "finalized",
    "finalizePending",
    "hasFallbackSvgUrl",
    "legacyMirrorDuplicate",
    "legacyMirrorError",
    "legacyMirrorOk",
    "membershipId",
    "mirrorDuplicate",
    "mirrorError",
    "mirrorOk",
    "primaryError",
    "primaryOk",
    "primaryStore",
}
CONTEXT_KEY_LOOKUP = {key.lower(): key for key in CONTEXT_KEYS}
SENSITIVE_CONTEXT_FRAGMENTS = (
    "secret",
    "token",
    "password",
    "api_key",
    "apikey",
    "client_secret",
    "authorization",
)


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
        context = _extract_context(raw_entry)
        events.append(
            WebsiteLogEvent.objects.create(
                website=website,
                occurred_at=_extract_timestamp(raw_entry),
                severity=_extract_severity(raw_entry, context=context),
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
                context=context,
                context_text=_context_search_text(context),
            )
        )
    return events


def _extract_severity(payload: dict[str, Any], *, context: dict[str, Any] | None = None) -> str:
    raw = _first_string(
        payload,
        "severity",
        "level",
        "logLevel",
        "log_level",
        "status",
        "logging.googleapis.com/severity",
    )
    if not raw:
        raw = _nested_first_string(
            payload,
            ("jsonPayload", "severity"),
            ("jsonPayload", "level"),
            ("jsonPayload", "logLevel"),
            ("jsonPayload", "log_level"),
            ("jsonPayload", "severityText"),
            ("jsonPayload", "levelname"),
            ("labels", "severity"),
            ("labels", "level"),
            ("labels", "logLevel"),
            ("labels", "log_level"),
            ("logging.googleapis.com", "severity"),
        )
    if raw:
        normalized = SEVERITY_ALIASES.get(raw.lower())
        if normalized:
            return normalized
    structured_severity = _classify_structured_outcome(payload, context or _extract_context(payload))
    if structured_severity:
        return structured_severity
    if _looks_like_wix_runtime_info(payload):
        return WebsiteLogEvent.SEVERITY_INFO
    return WebsiteLogEvent.SEVERITY_UNKNOWN


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
    json_event_type = _nested_first_string(
        payload,
        ("jsonPayload", "type"),
        ("jsonPayload", "eventType"),
        ("jsonPayload", "level"),
        ("jsonPayload", "severity"),
    )
    if json_event_type:
        return _trim(json_event_type, 255)
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


def _nested_first_string(payload: dict[str, Any], *paths: tuple[str, ...]) -> str:
    for path in paths:
        value: Any = payload
        for key in path:
            if not isinstance(value, dict):
                value = None
                break
            value = value.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
        if isinstance(value, (int, float, bool)):
            return str(value)
    return ""


def _looks_like_wix_runtime_info(payload: dict[str, Any]) -> bool:
    message = _extract_message(payload)
    if not message.startswith("Running the code for "):
        return False
    labels = payload.get("labels")
    source_location = payload.get("sourceLocation")
    return (
        isinstance(source_location, dict)
        or (isinstance(labels, dict) and labels.get("namespace") == "Velo")
    )


def _classify_structured_outcome(payload: dict[str, Any], context: dict[str, Any]) -> str:
    message = _extract_message(payload)
    if not message.startswith("Registration downstream outcome:"):
        return ""
    failure_keys = ("primaryOk", "legacyMirrorOk", "mirrorOk")
    if any(context.get(key) is False for key in failure_keys):
        return WebsiteLogEvent.SEVERITY_ERROR
    if _has_context_error_text(context):
        return WebsiteLogEvent.SEVERITY_WARNING
    if context.get("emailSent") is False:
        return WebsiteLogEvent.SEVERITY_WARNING
    success_keys = ("primaryOk", "legacyMirrorOk", "mirrorOk", "emailSent", "finalized")
    if all(context.get(key) is True for key in success_keys):
        return WebsiteLogEvent.SEVERITY_INFO
    return ""


def _has_context_error_text(context: dict[str, Any]) -> bool:
    error_keys = ("primaryError", "legacyMirrorError", "mirrorError", "emailError")
    return any(bool(str(context.get(key) or "").strip()) for key in error_keys)


def _extract_context(payload: dict[str, Any]) -> dict[str, Any]:
    context: dict[str, Any] = {}
    _collect_context(payload, context, depth=0)
    return context


def _collect_context(value: Any, context: dict[str, Any], *, depth: int) -> None:
    if depth > 5:
        return
    if isinstance(value, dict):
        for key, item in value.items():
            if not isinstance(key, str):
                continue
            canonical = CONTEXT_KEY_LOOKUP.get(key.lower())
            if canonical and not _is_sensitive_context_key(key):
                stored = _safe_context_value(item)
                if stored is not None:
                    context[canonical] = stored
            _collect_context(item, context, depth=depth + 1)
        return
    if isinstance(value, list):
        for item in value:
            _collect_context(item, context, depth=depth + 1)
        return
    if isinstance(value, str):
        parsed = _parse_json_candidate(value)
        if parsed is not None:
            _collect_context(parsed, context, depth=depth + 1)


def _safe_context_value(value: Any) -> Any:
    if isinstance(value, str):
        return _trim(value.strip(), 300)
    if isinstance(value, bool) or isinstance(value, (int, float)) or value is None:
        return value
    return None


def _is_sensitive_context_key(key: str) -> bool:
    lowered = key.lower()
    return any(fragment in lowered for fragment in SENSITIVE_CONTEXT_FRAGMENTS)


def _parse_json_candidate(value: str) -> Any | None:
    text = value.strip()
    if not text:
        return None
    candidates = []
    if text[0] in "[{":
        candidates.append(text)
    start = text.find("{")
    end = text.rfind("}")
    if start != -1 and end > start:
        candidates.append(text[start : end + 1])
    for candidate in candidates:
        try:
            return json.loads(candidate)
        except (TypeError, ValueError):
            continue
    return None


def _context_search_text(context: dict[str, Any]) -> str:
    parts = []
    for key in sorted(context):
        value = context[key]
        if value is None:
            rendered = ""
        elif isinstance(value, bool):
            rendered = "true" if value else "false"
        else:
            rendered = str(value)
        parts.append(f"{key}:{rendered}")
    return " ".join(parts)


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
