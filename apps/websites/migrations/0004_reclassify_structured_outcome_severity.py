import json

from django.db import migrations


def reclassify_structured_outcomes(apps, schema_editor):
    WebsiteLogEvent = apps.get_model("websites", "WebsiteLogEvent")
    updates = []
    queryset = WebsiteLogEvent.objects.filter(severity="unknown").only("id", "severity", "message", "context", "raw_payload")
    for event in queryset.iterator(chunk_size=500):
        context = event.context if isinstance(event.context, dict) else {}
        if not context:
            context = _extract_context(event.raw_payload if isinstance(event.raw_payload, dict) else {})
        severity = _classify_structured_outcome(event.message, context)
        if not severity:
            continue
        event.severity = severity
        updates.append(event)
        if len(updates) == 500:
            WebsiteLogEvent.objects.bulk_update(updates, ["severity"])
            updates.clear()
    if updates:
        WebsiteLogEvent.objects.bulk_update(updates, ["severity"])


def _extract_context(payload):
    message = ""
    json_payload = payload.get("jsonPayload")
    if isinstance(json_payload, dict):
        message = str(json_payload.get("message") or "")
    parsed = _parse_json_candidate(message)
    return parsed if isinstance(parsed, dict) else {}


def _parse_json_candidate(value):
    text = value.strip()
    start = text.find("{")
    end = text.rfind("}")
    if start == -1 or end <= start:
        return None
    try:
        return json.loads(text[start : end + 1])
    except (TypeError, ValueError):
        return None


def _classify_structured_outcome(message, context):
    if not str(message or "").startswith("Registration downstream outcome:"):
        return ""
    if any(context.get(key) is False for key in ("primaryOk", "legacyMirrorOk", "mirrorOk")):
        return "error"
    if _has_context_error_text(context):
        return "warning"
    if context.get("emailSent") is False:
        return "warning"
    if all(context.get(key) is True for key in ("primaryOk", "legacyMirrorOk", "mirrorOk", "emailSent", "finalized")):
        return "info"
    return ""


def _has_context_error_text(context):
    return any(bool(str(context.get(key) or "").strip()) for key in ("primaryError", "legacyMirrorError", "mirrorError", "emailError"))


class Migration(migrations.Migration):

    dependencies = [
        ("websites", "0003_websitelogevent_context"),
    ]

    operations = [
        migrations.RunPython(reclassify_structured_outcomes, migrations.RunPython.noop),
    ]
