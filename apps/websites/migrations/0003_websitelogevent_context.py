from django.db import migrations, models


SEVERITY_ALIASES = {
    "debug": "debug",
    "trace": "debug",
    "info": "info",
    "log": "info",
    "notice": "info",
    "warn": "warning",
    "warning": "warning",
    "error": "error",
    "err": "error",
    "critical": "critical",
    "fatal": "critical",
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


def backfill_existing_log_context(apps, schema_editor):
    WebsiteLogEvent = apps.get_model("websites", "WebsiteLogEvent")
    updates = []
    for event in WebsiteLogEvent.objects.exclude(raw_payload={}).iterator(chunk_size=500):
        raw_payload = event.raw_payload if isinstance(event.raw_payload, dict) else {}
        context = _extract_context(raw_payload)
        event.context = context
        event.context_text = _context_search_text(context)
        if event.severity == "unknown":
            event.severity = _extract_severity(raw_payload)
        updates.append(event)
        if len(updates) == 500:
            WebsiteLogEvent.objects.bulk_update(updates, ["context", "context_text", "severity"])
            updates.clear()
    if updates:
        WebsiteLogEvent.objects.bulk_update(updates, ["context", "context_text", "severity"])


def _extract_severity(payload):
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
        )
    if raw:
        normalized = SEVERITY_ALIASES.get(raw.lower())
        if normalized:
            return normalized
    if _looks_like_wix_runtime_info(payload):
        return "info"
    return "unknown"


def _extract_message(payload):
    message = _first_string(
        payload,
        "message",
        "msg",
        "text",
        "description",
        "errorMessage",
        "error_message",
    )
    if message:
        return message
    json_payload = payload.get("jsonPayload")
    if isinstance(json_payload, dict):
        return _first_string(
            json_payload,
            "message",
            "msg",
            "text",
            "description",
            "errorMessage",
            "error_message",
        )
    return ""


def _first_string(payload, *keys):
    for key in keys:
        value = payload.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
        if isinstance(value, (int, float, bool)):
            return str(value)
    return ""


def _nested_first_string(payload, *paths):
    for path in paths:
        value = payload
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


def _looks_like_wix_runtime_info(payload):
    message = _extract_message(payload)
    if not message.startswith("Running the code for "):
        return False
    labels = payload.get("labels")
    source_location = payload.get("sourceLocation")
    return isinstance(source_location, dict) or (
        isinstance(labels, dict) and labels.get("namespace") == "Velo"
    )


def _extract_context(payload):
    context = {}
    _collect_context(payload, context, depth=0)
    return context


def _collect_context(value, context, *, depth):
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


def _safe_context_value(value):
    if isinstance(value, str):
        return value.strip()[:300]
    if isinstance(value, bool) or isinstance(value, (int, float)) or value is None:
        return value
    return None


def _is_sensitive_context_key(key):
    lowered = key.lower()
    return any(fragment in lowered for fragment in SENSITIVE_CONTEXT_FRAGMENTS)


def _parse_json_candidate(value):
    import json

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


def _context_search_text(context):
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


class Migration(migrations.Migration):

    dependencies = [
        ("websites", "0002_seed_working_people_united"),
    ]

    operations = [
        migrations.AddField(
            model_name="websitelogevent",
            name="context",
            field=models.JSONField(blank=True, default=dict),
        ),
        migrations.AddField(
            model_name="websitelogevent",
            name="context_text",
            field=models.TextField(blank=True),
        ),
        migrations.RunPython(backfill_existing_log_context, migrations.RunPython.noop),
    ]
