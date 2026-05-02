"""Slack notifications for manual Inventory Review pipeline actions (portal-only)."""

from __future__ import annotations

import logging

from django.conf import settings
from django.urls import reverse

from code_scripts.company_config import load_company_config
from code_scripts.slack_notify import (
    build_inventory_review_action_envelope,
    build_run_detail_url,
    format_inventory_review_action_failed,
    format_inventory_review_action_queued_message,
    send_slack_success,
)

from ..models import RunJob

logger = logging.getLogger(__name__)


def _fallback_run_detail_url(job_id: object) -> str:
    base = str(getattr(settings, "OIAT_PORTAL_BASE_URL", "") or "").strip().rstrip("/")
    if not base:
        return ""
    return f"{base}/epos-qbo/runs/{job_id}/"


def send_inventory_review_action_queued(*, company, job: RunJob, request) -> None:
    opts = job.inventory_options_json if isinstance(job.inventory_options_json, dict) else {}
    envelope = build_inventory_review_action_envelope(opts)
    if not envelope:
        return
    try:
        cfg = load_company_config(company.company_key)
    except Exception as exc:
        logger.warning("Inventory review queued Slack skipped (company config): %s", exc)
        return
    webhook = cfg.slack_webhook_url
    if not webhook:
        return
    try:
        run_url = request.build_absolute_uri(
            reverse("epos_qbo:run-detail", kwargs={"job_id": job.id})
        )
    except Exception:
        run_url = _fallback_run_detail_url(job.id)
    queued_by = ""
    user = getattr(request, "user", None)
    if user is not None and getattr(user, "is_authenticated", False):
        queued_by = user.get_username()
    text = format_inventory_review_action_queued_message(
        envelope=envelope,
        company_display_name=str(company.display_name or ""),
        run_job_id=str(job.id),
        run_url=run_url,
        queued_by=queued_by,
    )
    send_slack_success(text, webhook)


def send_inventory_review_action_failed_notification(job: RunJob) -> None:
    if job.scope != RunJob.SCOPE_INVENTORY_PIPELINE:
        return
    opts = job.inventory_options_json if isinstance(job.inventory_options_json, dict) else {}
    envelope = build_inventory_review_action_envelope(opts)
    if not envelope:
        return
    try:
        cfg = load_company_config(job.company_key or "")
    except Exception as exc:
        logger.warning("Inventory review failure Slack skipped (company config): %s", exc)
        return
    webhook = cfg.slack_webhook_url
    if not webhook:
        return
    run_url = build_run_detail_url(str(job.id))
    if not run_url:
        run_url = _fallback_run_detail_url(job.id)
    text = format_inventory_review_action_failed(
        envelope=envelope,
        company_display_name=str(cfg.display_name or ""),
        exit_code=int(job.exit_code or 0),
        failure_reason=str(job.failure_reason or ""),
        run_url=run_url,
        run_job_id=str(job.id),
    )
    send_slack_success(text, webhook)
