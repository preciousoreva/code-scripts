from __future__ import annotations

import base64
import hashlib
import hmac
import json
import logging
import os

from django.http import HttpRequest, HttpResponse, JsonResponse
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_POST

from .services.qbo_webhook_notifications import process_qbo_webhook_body, record_rejected_webhook

logger = logging.getLogger(__name__)

SIGNATURE_HEADER = "HTTP_INTUIT_SIGNATURE"
VERIFIER_TOKEN_ENV = "QBO_WEBHOOK_VERIFIER_TOKEN"


@csrf_exempt
@require_POST
def quickbooks_webhook(request: HttpRequest) -> HttpResponse:
    verifier_token = os.getenv(VERIFIER_TOKEN_ENV, "").strip()
    if not verifier_token:
        logger.error("QBO webhook rejected: %s is not configured", VERIFIER_TOKEN_ENV)
        record_rejected_webhook(reason=f"{VERIFIER_TOKEN_ENV} is not configured.", payload=_request_fingerprint(request))
        return JsonResponse({"error": "webhook verifier not configured"}, status=503)

    signature = request.META.get(SIGNATURE_HEADER, "")
    if not _valid_intuit_signature(request.body, signature, verifier_token):
        logger.warning("QBO webhook rejected: invalid Intuit signature")
        record_rejected_webhook(reason="Invalid Intuit signature.", payload=_request_fingerprint(request))
        return JsonResponse({"error": "invalid signature"}, status=401)

    try:
        payload = json.loads(request.body.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        record_rejected_webhook(reason="Invalid JSON payload.", payload=_request_fingerprint(request))
        return JsonResponse({"error": "invalid JSON"}, status=400)
    if not isinstance(payload, (dict, list)):
        record_rejected_webhook(
            reason="Payload was not a supported JSON object or array.",
            payload={"request": _request_fingerprint(request), "payload_type": type(payload).__name__},
        )
        return JsonResponse({"error": "payload must be a supported JSON object or array"}, status=400)

    sent_count = process_qbo_webhook_body(payload)
    return JsonResponse({"status": "ok", "notifications_sent": sent_count})


def _valid_intuit_signature(body: bytes, signature: str, verifier_token: str) -> bool:
    if not signature:
        return False
    digest = hmac.new(verifier_token.encode("utf-8"), body, hashlib.sha256).digest()
    expected = base64.b64encode(digest).decode("ascii")
    return hmac.compare_digest(expected, signature.strip())


def _request_fingerprint(request: HttpRequest) -> dict[str, object]:
    return {
        "path": request.path,
        "method": request.method,
        "content_type": request.META.get("CONTENT_TYPE", ""),
        "content_length": request.META.get("CONTENT_LENGTH", ""),
        "user_agent": request.META.get("HTTP_USER_AGENT", ""),
        "remote_addr": request.META.get("REMOTE_ADDR", ""),
        "has_intuit_signature": bool(request.META.get(SIGNATURE_HEADER)),
        "has_intuit_tid": bool(request.META.get("HTTP_INTUIT_T_ID")),
        "intuit_created_time": request.META.get("HTTP_INTUIT_CREATED_TIME", ""),
        "intuit_schema_version": request.META.get("HTTP_INTUIT_NOTIFICATION_SCHEMA_VERSION", ""),
    }
