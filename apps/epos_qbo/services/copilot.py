from __future__ import annotations

import json
import logging
import re
import time
import uuid
from dataclasses import dataclass
from datetime import timedelta
from typing import Any

import requests
from django.conf import settings
from django.urls import reverse
from django.utils import timezone

from code_scripts.company_config import normalize_qbo_environment
from code_scripts.token_manager import load_tokens_batch
from apps.epos_qbo.models import CompanyConfigRecord, InventoryReviewAcknowledgement, RunArtifact, RunJob, RunSchedule

logger = logging.getLogger(__name__)

SECRET_KEY_PATTERNS = (
    "password",
    "secret",
    "token",
    "webhook",
    "api_key",
    "apikey",
    "client_secret",
    "access",
    "refresh",
)

SYSTEM_PROMPT = """You are the read-only OIAT Portal Copilot.
Answer only from the provided portal evidence. Do not claim you checked QuickBooks,
EPOS, the filesystem, environment variables, or logs beyond the evidence provided.
Never suggest that you performed a write action. If evidence is insufficient, say so.
Keep answers operational and concise. Include the most relevant source labels."""


@dataclass(frozen=True)
class CopilotSource:
    label: str
    url: str
    type: str


@dataclass(frozen=True)
class CopilotResult:
    success: bool
    answer: str
    sources: list[CopilotSource]
    warnings: list[str]
    request_id: str
    status_code: int = 200


class CopilotConfigError(RuntimeError):
    pass


def answer_question(*, question: str, context: dict[str, Any] | None, user: Any) -> CopilotResult:
    request_id = uuid.uuid4().hex[:12]
    cleaned_question = _clean_question(question)
    if not cleaned_question:
        return CopilotResult(False, "", [], ["Question is required."], request_id, status_code=400)

    max_chars = int(getattr(settings, "OIAT_COPILOT_MAX_QUESTION_CHARS", 1000))
    if len(cleaned_question) > max_chars:
        return CopilotResult(
            False,
            "",
            [],
            [f"Question exceeds {max_chars} character limit."],
            request_id,
            status_code=400,
        )

    evidence, sources, warnings = build_evidence_pack(cleaned_question, context or {})
    if not getattr(settings, "OIAT_COPILOT_ENABLED", False):
        return CopilotResult(
            False,
            "",
            sources,
            ["Copilot is disabled on this environment."],
            request_id,
            status_code=503,
        )

    try:
        started = time.monotonic()
        answer = _call_provider(cleaned_question, evidence)
        elapsed_ms = int((time.monotonic() - started) * 1000)
        logger.info(
            "copilot_answered request_id=%s user_id=%s elapsed_ms=%s sources=%s",
            request_id,
            getattr(user, "id", None),
            elapsed_ms,
            len(sources),
        )
    except CopilotConfigError as exc:
        logger.warning("copilot_config_error request_id=%s error=%s", request_id, exc)
        return CopilotResult(False, "", sources, [str(exc)], request_id, status_code=503)
    except requests.Timeout:
        logger.warning("copilot_timeout request_id=%s", request_id)
        return CopilotResult(False, "", sources, ["Copilot provider timed out."], request_id, status_code=504)
    except requests.RequestException as exc:
        logger.warning("copilot_provider_error request_id=%s error=%s", request_id, exc)
        return CopilotResult(False, "", sources, ["Copilot provider request failed."], request_id, status_code=502)
    except Exception:
        logger.exception("copilot_unexpected_error request_id=%s", request_id)
        return CopilotResult(False, "", sources, ["Copilot failed unexpectedly."], request_id, status_code=500)

    return CopilotResult(True, answer, sources, warnings, request_id)


def build_evidence_pack(question: str, context: dict[str, Any]) -> tuple[dict[str, Any], list[CopilotSource], list[str]]:
    sources: list[CopilotSource] = []
    warnings: list[str] = []
    now = timezone.now()
    company_key = _context_company_key(context) or _infer_company_key(question)
    run_id = _context_run_id(context)

    active_companies = list(CompanyConfigRecord.objects.filter(is_active=True).order_by("display_name"))
    company_lookup = {company.company_key: company for company in active_companies}
    token_health = _token_health_by_company(active_companies)

    recent_jobs = RunJob.objects.order_by("-created_at").select_related("requested_by")[:12]
    failed_jobs = RunJob.objects.filter(status=RunJob.STATUS_FAILED).order_by("-created_at")[:8]
    active_jobs = RunJob.objects.filter(status__in=[RunJob.STATUS_QUEUED, RunJob.STATUS_RUNNING]).order_by("-created_at")[:8]
    recent_artifacts = RunArtifact.objects.order_by("-processed_at", "-imported_at").select_related("run_job")[:12]
    schedules = RunSchedule.objects.order_by("name")[:12]

    evidence: dict[str, Any] = {
        "generated_at": now.isoformat(),
        "question": question,
        "current_page": _safe_context_value(context.get("path")),
        "portal_summary": {
            "active_company_count": len(active_companies),
            "recent_run_count": RunJob.objects.filter(created_at__gte=now - timedelta(days=7)).count(),
            "failed_run_count_7d": RunJob.objects.filter(
                status=RunJob.STATUS_FAILED,
                created_at__gte=now - timedelta(days=7),
            ).count(),
            "active_run_count": RunJob.objects.filter(
                status__in=[RunJob.STATUS_QUEUED, RunJob.STATUS_RUNNING],
            ).count(),
        },
        "companies": [_company_summary(company) for company in active_companies[:20]],
        "token_health": token_health,
        "recent_runs": [_run_summary(job) for job in recent_jobs],
        "failed_runs": [_run_summary(job) for job in failed_jobs],
        "active_runs": [_run_summary(job) for job in active_jobs],
        "recent_artifacts": [_artifact_summary(artifact) for artifact in recent_artifacts],
        "schedules": [_schedule_summary(schedule) for schedule in schedules],
    }

    sources.append(CopilotSource("Portal dashboard", reverse("epos_qbo:overview"), "dashboard"))
    sources.append(CopilotSource("Runs", reverse("epos_qbo:runs"), "runs"))
    sources.append(CopilotSource("Companies", reverse("epos_qbo:companies-list"), "companies"))

    if company_key:
        company = company_lookup.get(company_key) or CompanyConfigRecord.objects.filter(company_key=company_key).first()
        if company:
            latest_company_jobs = RunJob.objects.filter(company_key=company.company_key).order_by("-created_at")[:8]
            latest_company_artifacts = RunArtifact.objects.filter(company_key=company.company_key).order_by(
                "-processed_at",
                "-imported_at",
            )[:8]
            evidence["focused_company"] = {
                **_company_summary(company),
                "token_health": token_health.get(company.company_key),
                "recent_runs": [_run_summary(job) for job in latest_company_jobs],
                "recent_artifacts": [_artifact_summary(artifact) for artifact in latest_company_artifacts],
                "latest_inventory_acknowledgement": _inventory_ack_summary(company.company_key),
            }
            sources.append(
                CopilotSource(
                    company.display_name,
                    reverse("epos_qbo:company-detail", kwargs={"company_key": company.company_key}),
                    "company",
                )
            )
            sources.append(
                CopilotSource(
                    f"{company.display_name} inventory review",
                    reverse("epos_qbo:company_inventory_review", kwargs={"company_key": company.company_key}),
                    "inventory_review",
                )
            )
        else:
            warnings.append(f"Company context '{company_key}' was not found.")

    if run_id:
        run = RunJob.objects.filter(id=run_id).first()
        if run:
            evidence["focused_run"] = {
                **_run_summary(run),
                "artifacts": [_artifact_summary(artifact) for artifact in run.artifacts.order_by("-imported_at")[:8]],
            }
            sources.append(
                CopilotSource(
                    run.friendly_title,
                    reverse("epos_qbo:run-detail", kwargs={"job_id": run.id}),
                    "run",
                )
            )
        else:
            warnings.append(f"Run context '{run_id}' was not found.")

    return _redact(evidence), _dedupe_sources(sources), warnings


def result_to_json(result: CopilotResult) -> dict[str, Any]:
    return {
        "success": result.success,
        "answer": result.answer,
        "sources": [source.__dict__ for source in result.sources],
        "warnings": result.warnings,
        "request_id": result.request_id,
    }


def _call_provider(question: str, evidence: dict[str, Any]) -> str:
    provider = str(getattr(settings, "OIAT_COPILOT_PROVIDER", "") or "").strip().lower()
    api_key = str(getattr(settings, "OIAT_COPILOT_API_KEY", "") or "").strip()
    api_url = str(getattr(settings, "OIAT_COPILOT_API_URL", "") or "").strip()
    model = str(getattr(settings, "OIAT_COPILOT_MODEL", "") or "").strip()
    timeout = float(getattr(settings, "OIAT_COPILOT_TIMEOUT_SECONDS", 20.0))

    if not provider:
        raise CopilotConfigError("Copilot provider is not configured.")
    if provider != "openai":
        raise CopilotConfigError("Unsupported copilot provider.")
    if not api_key:
        raise CopilotConfigError("Copilot API key is not configured.")
    if not api_url:
        raise CopilotConfigError("Copilot API URL is not configured.")
    if not model:
        raise CopilotConfigError("Copilot model is not configured.")

    payload = {
        "model": model,
        "input": [
            {"role": "system", "content": SYSTEM_PROMPT},
            {
                "role": "user",
                "content": (
                    "Question:\n"
                    f"{question}\n\n"
                    "Portal evidence JSON:\n"
                    f"{json.dumps(evidence, default=str, ensure_ascii=True)}"
                ),
            },
        ],
    }
    response = requests.post(
        api_url,
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
        json=payload,
        timeout=timeout,
    )
    response.raise_for_status()
    data = response.json()
    answer = _extract_openai_text(data)
    if not answer:
        raise CopilotConfigError("Copilot provider returned no answer text.")
    return answer


def _extract_openai_text(data: dict[str, Any]) -> str:
    if isinstance(data.get("output_text"), str):
        return data["output_text"].strip()
    parts: list[str] = []
    for item in data.get("output") or []:
        if not isinstance(item, dict):
            continue
        for content in item.get("content") or []:
            if not isinstance(content, dict):
                continue
            text = content.get("text")
            if isinstance(text, str) and text.strip():
                parts.append(text.strip())
    return "\n\n".join(parts).strip()


def _company_summary(company: CompanyConfigRecord) -> dict[str, Any]:
    config = company.config_json if isinstance(company.config_json, dict) else {}
    qbo = config.get("qbo") if isinstance(config.get("qbo"), dict) else {}
    epos = config.get("epos") if isinstance(config.get("epos"), dict) else {}
    return {
        "company_key": company.company_key,
        "display_name": company.display_name,
        "is_active": company.is_active,
        "qbo_environment": qbo.get("environment") or "production",
        "qbo_realm_configured": bool(qbo.get("realm_id")),
        "epos_env_keys_configured": bool(epos.get("username_env_key") and epos.get("password_env_key")),
        "updated_at": company.updated_at.isoformat() if company.updated_at else None,
    }


def _token_health_by_company(companies: list[CompanyConfigRecord]) -> dict[str, dict[str, Any]]:
    pairs: list[tuple[str, str]] = []
    company_realms: dict[str, tuple[str, str]] = {}
    for company in companies:
        config = company.config_json if isinstance(company.config_json, dict) else {}
        qbo = config.get("qbo") if isinstance(config.get("qbo"), dict) else {}
        realm_id = str(qbo.get("realm_id") or "").strip()
        environment = normalize_qbo_environment(qbo.get("environment"), default="production")
        if not realm_id:
            company_realms[company.company_key] = ("", environment)
            continue
        pair = (company.company_key, realm_id)
        pairs.append(pair)
        company_realms[company.company_key] = (realm_id, environment)

    try:
        token_batch = load_tokens_batch(pairs)
    except Exception as exc:
        logger.warning("copilot_token_health_unavailable error=%s", exc)
        return {
            company.company_key: {
                "state": "unknown",
                "severity": "warning",
                "message": "Token health could not be read.",
            }
            for company in companies
        }

    now_ts = int(timezone.now().timestamp())
    expiring_seconds = int(getattr(settings, "OIAT_DASHBOARD_REFRESH_EXPIRING_DAYS", 7)) * 86400
    result: dict[str, dict[str, Any]] = {}
    for company in companies:
        realm_id, expected_environment = company_realms.get(company.company_key, ("", "production"))
        if not realm_id:
            result[company.company_key] = {
                "state": "missing_realm",
                "severity": "critical",
                "message": "QBO realm ID is not configured.",
            }
            continue

        tokens = token_batch.get((company.company_key, realm_id))
        if not tokens:
            result[company.company_key] = {
                "state": "missing_tokens",
                "severity": "critical",
                "message": "QBO tokens are missing.",
            }
            continue

        token_environment = normalize_qbo_environment(tokens.get("environment"), default=expected_environment)
        refresh_expires_at = tokens.get("refresh_expires_at")
        refresh_seconds_left = int(refresh_expires_at - now_ts) if refresh_expires_at else None
        if token_environment != expected_environment:
            state = "environment_mismatch"
            severity = "critical"
            message = "Stored token environment does not match company configuration."
        elif not tokens.get("refresh_token"):
            state = "missing_refresh_token"
            severity = "critical"
            message = "QBO refresh token is missing."
        elif refresh_seconds_left is not None and refresh_seconds_left <= 0:
            state = "refresh_expired"
            severity = "critical"
            message = "QBO refresh token has expired."
        elif refresh_seconds_left is not None and refresh_seconds_left <= expiring_seconds:
            state = "refresh_expiring"
            severity = "warning"
            message = f"QBO refresh token expires in {max(0, refresh_seconds_left // 86400)} day(s)."
        else:
            state = "connected"
            severity = "healthy"
            message = "QBO tokens are present."

        result[company.company_key] = {
            "state": state,
            "severity": severity,
            "message": message,
            "environment": token_environment,
            "refresh_expires_at": refresh_expires_at,
        }
    return result


def _run_summary(job: RunJob) -> dict[str, Any]:
    return {
        "id": str(job.id),
        "friendly_id": job.friendly_id,
        "title": job.friendly_title,
        "scope": job.scope,
        "scope_label": job.scope_label,
        "company_key": job.company_key,
        "status": job.status,
        "target_date": job.target_date.isoformat() if job.target_date else None,
        "from_date": job.from_date.isoformat() if job.from_date else None,
        "to_date": job.to_date.isoformat() if job.to_date else None,
        "exit_code": job.exit_code,
        "failure_reason": _safe_text(job.failure_reason, 700),
        "queued_at": job.queued_at.isoformat() if job.queued_at else None,
        "started_at": job.started_at.isoformat() if job.started_at else None,
        "finished_at": job.finished_at.isoformat() if job.finished_at else None,
        "url": reverse("epos_qbo:run-detail", kwargs={"job_id": job.id}),
    }


def _artifact_summary(artifact: RunArtifact) -> dict[str, Any]:
    stats = artifact.upload_stats_json if isinstance(artifact.upload_stats_json, dict) else {}
    return {
        "id": artifact.id,
        "kind": artifact.kind,
        "label": artifact.operator_label,
        "company_key": artifact.company_key,
        "run_job_id": str(artifact.run_job_id) if artifact.run_job_id else None,
        "target_date": artifact.target_date.isoformat() if artifact.target_date else None,
        "processed_at": artifact.processed_at.isoformat() if artifact.processed_at else None,
        "reliability_status": artifact.reliability_status,
        "rows_total": artifact.rows_total,
        "rows_kept": artifact.rows_kept,
        "reconcile_status": artifact.reconcile_status,
        "reconcile_difference": str(artifact.reconcile_difference) if artifact.reconcile_difference is not None else None,
        "upload_stats": _redact({key: stats.get(key) for key in sorted(stats.keys())[:20]}),
    }


def _schedule_summary(schedule: RunSchedule) -> dict[str, Any]:
    return {
        "id": str(schedule.id),
        "name": schedule.name,
        "enabled": schedule.enabled,
        "scope": schedule.scope,
        "company_key": schedule.company_key,
        "schedule_type": schedule.schedule_type,
        "cron_expr": schedule.cron_expr,
        "timezone_name": schedule.timezone_name,
        "next_fire_at": schedule.next_fire_at.isoformat() if schedule.next_fire_at else None,
        "last_result": schedule.last_result,
        "last_error": _safe_text(schedule.last_error, 500),
    }


def _inventory_ack_summary(company_key: str) -> dict[str, Any] | None:
    ack = (
        InventoryReviewAcknowledgement.objects.filter(company_key=company_key)
        .select_related("artifact", "run_job", "reviewed_by")
        .order_by("-reviewed_at")
        .first()
    )
    if not ack:
        return None
    return {
        "reviewed_at": ack.reviewed_at.isoformat() if ack.reviewed_at else None,
        "reviewed_by": getattr(ack.reviewed_by, "username", None),
        "run_job_id": str(ack.run_job_id) if ack.run_job_id else None,
        "artifact_id": ack.artifact_id,
        "summary": _redact(ack.summary_json if isinstance(ack.summary_json, dict) else {}),
    }


def _clean_question(question: str) -> str:
    return re.sub(r"\s+", " ", str(question or "")).strip()


def _safe_text(value: Any, limit: int) -> str:
    text = str(value or "").strip()
    if len(text) <= limit:
        return text
    return text[: limit - 1].rstrip() + "..."


def _safe_context_value(value: Any) -> str:
    return _safe_text(value, 300)


def _context_company_key(context: dict[str, Any]) -> str:
    raw = str(context.get("company_key") or "").strip()
    return raw if re.fullmatch(r"[-a-zA-Z0-9_]{1,64}", raw) else ""


def _context_run_id(context: dict[str, Any]) -> str:
    raw = str(context.get("run_id") or "").strip()
    return raw if re.fullmatch(r"[0-9a-fA-F-]{32,36}", raw) else ""


def _infer_company_key(question: str) -> str:
    lowered = question.lower()
    for company in CompanyConfigRecord.objects.filter(is_active=True).only("company_key", "display_name"):
        if company.company_key.lower() in lowered or company.display_name.lower() in lowered:
            return company.company_key
    return ""


def _redact(value: Any) -> Any:
    if isinstance(value, dict):
        redacted: dict[str, Any] = {}
        for key, item in value.items():
            key_text = str(key)
            if any(pattern in key_text.lower() for pattern in SECRET_KEY_PATTERNS):
                redacted[key_text] = "[redacted]"
            else:
                redacted[key_text] = _redact(item)
        return redacted
    if isinstance(value, list):
        return [_redact(item) for item in value]
    if isinstance(value, str):
        if len(value) > 1200:
            return value[:1199].rstrip() + "..."
        return value
    return value


def _dedupe_sources(sources: list[CopilotSource]) -> list[CopilotSource]:
    seen: set[tuple[str, str]] = set()
    deduped: list[CopilotSource] = []
    for source in sources:
        key = (source.label, source.url)
        if key in seen:
            continue
        seen.add(key)
        deduped.append(source)
    return deduped[:8]
