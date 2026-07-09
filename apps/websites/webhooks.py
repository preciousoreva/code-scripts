from __future__ import annotations

import json
import logging

from django.http import HttpRequest, HttpResponse, JsonResponse
from django.shortcuts import get_object_or_404
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_POST

from .models import Website
from .services import create_wix_log_events

logger = logging.getLogger(__name__)


@csrf_exempt
@require_POST
def wix_log_ingest(request: HttpRequest, site_slug: str, secret: str) -> HttpResponse:
    website = get_object_or_404(
        Website,
        slug=site_slug,
        log_ingest_secret=secret,
        is_active=True,
        platform=Website.PLATFORM_WIX,
    )

    try:
        payload = json.loads(request.body.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        logger.warning("Wix log ingest rejected for %s: invalid JSON", site_slug)
        return JsonResponse({"error": "invalid JSON"}, status=400)

    if not isinstance(payload, (dict, list)):
        return JsonResponse({"error": "payload must be a JSON object or array"}, status=400)

    events = create_wix_log_events(website=website, payload=payload, request=request)
    return JsonResponse({"status": "ok", "events_created": len(events)})
