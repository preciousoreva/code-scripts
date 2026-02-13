from __future__ import annotations

from collections import Counter
from datetime import timedelta

from django.contrib import messages
from django.contrib.auth.decorators import login_required, permission_required
from django.http import Http404, JsonResponse
from django.shortcuts import get_object_or_404, redirect, render
from django.utils import timezone
from django.views.decorators.http import require_GET, require_POST

from code_scripts.token_manager import load_tokens

from .forms import CompanyAdvancedForm, CompanyBasicForm, RunTriggerForm
from .models import CompanyConfigRecord, RunArtifact, RunJob
from .services.config_sync import (
    apply_advanced_payload,
    build_basic_payload,
    import_all_company_json,
    sync_record_to_json,
    validate_company_config,
)
from .services.job_runner import build_command, read_log_chunk, start_run_job
from .services.locking import acquire_run_lock, release_run_lock


def _ensure_company_records() -> None:
    if CompanyConfigRecord.objects.exists():
        return
    import_all_company_json()


def _nav_context() -> dict:
    return {"company_count": CompanyConfigRecord.objects.filter(is_active=True).count()}


def _company_token_days(company: CompanyConfigRecord) -> int | None:
    cfg = company.config_json or {}
    realm_id = (cfg.get("qbo") or {}).get("realm_id")
    if not realm_id:
        return None
    tokens = load_tokens(company.company_key, realm_id)
    if not tokens:
        return None
    expires_at = tokens.get("expires_at")
    if not expires_at:
        return None
    return int((expires_at - timezone.now().timestamp()) / 86400)


def _job_failure_source(job: RunJob) -> str:
    text = (job.failure_reason or "").lower()
    if "mapping" in text:
        return "mapping"
    if "inventory" in text:
        return "inventory"
    if "token" in text or "auth" in text:
        return "auth"
    if "network" in text or "timeout" in text:
        return "network"
    return "other"


def _status_for_company(
    company: CompanyConfigRecord,
    latest_artifact: RunArtifact | None,
    latest_job: RunJob | None,
) -> tuple[str, str]:
    cfg = company.config_json or {}
    epos = cfg.get("epos") or {}
    token_days = _company_token_days(company)

    if not epos.get("username_env_key") or not epos.get("password_env_key"):
        return "warning", "Missing EPOS env key names in company config."
    if token_days is None:
        return "warning", "QBO token record missing."
    if token_days <= 1:
        return "critical", "QBO token expires in 1 day or less."
    if token_days <= 5:
        return "warning", f"QBO token expires soon ({token_days}d)."

    if latest_job and latest_job.status == RunJob.STATUS_FAILED:
        return "critical", latest_job.failure_reason or "Latest run failed."
    if latest_job and latest_job.status in (RunJob.STATUS_QUEUED, RunJob.STATUS_RUNNING):
        return "warning", "Run currently queued/running."

    if latest_artifact:
        failed_uploads = int((latest_artifact.upload_stats_json or {}).get("failed", 0))
        if failed_uploads > 0:
            return "critical", f"{failed_uploads} upload(s) failed in latest run."
        if latest_artifact.reconcile_difference and abs(latest_artifact.reconcile_difference) > 1.0:
            return "warning", "Reconciliation mismatch above threshold."
        if latest_artifact.reliability_status == RunArtifact.RELIABILITY_WARNING:
            return "warning", "Latest metadata source is mutable (last_*)."
    else:
        return "warning", "No artifact metadata ingested yet."

    return "healthy", "Last run succeeded."


def _overview_context() -> dict:
    now = timezone.now()
    since_24h = now - timedelta(hours=24)
    since_7d = now - timedelta(days=7)
    since_60d = now - timedelta(days=60)

    artifacts_24h = RunArtifact.objects.filter(imported_at__gte=since_24h)
    records_synced_24h = sum(int(a.rows_kept or 0) for a in artifacts_24h)

    companies_context = []
    healthy_count = warning_count = critical_count = 0

    for company in CompanyConfigRecord.objects.filter(is_active=True).order_by("company_key"):
        latest_artifact = (
            RunArtifact.objects.filter(company_key=company.company_key)
            .order_by("-processed_at", "-imported_at")
            .first()
        )
        latest_job = RunJob.objects.filter(company_key=company.company_key).order_by("-created_at").first()
        token_days = _company_token_days(company)
        status, summary = _status_for_company(company, latest_artifact, latest_job)

        if status == "healthy":
            healthy_count += 1
        elif status == "warning":
            warning_count += 1
        else:
            critical_count += 1

        companies_context.append(
            {
                "name": company.display_name,
                "company_key": company.company_key,
                "last_run": latest_job.finished_at if latest_job else None,
                "status": status,
                "token_days": token_days,
                "records_synced": latest_artifact.rows_kept if latest_artifact else 0,
                "summary": summary,
            }
        )

    recent_jobs = RunJob.objects.order_by("-created_at")[:20]
    live_log = []
    for job in recent_jobs:
        if job.status == RunJob.STATUS_SUCCEEDED:
            level = "success"
            message = f"{job.scope.replace('_', ' ')} run {job.id} succeeded"
        elif job.status == RunJob.STATUS_FAILED:
            level = "error"
            message = f"{job.scope.replace('_', ' ')} run {job.id} failed ({job.failure_reason or 'see logs'})"
        elif job.status == RunJob.STATUS_RUNNING:
            level = "info"
            message = f"{job.scope.replace('_', ' ')} run {job.id} is running"
        else:
            level = "warning"
            message = f"{job.scope.replace('_', ' ')} run {job.id} queued"
        live_log.append({"timestamp": job.created_at, "level": level, "message": message})

    daily_success = []
    for day_index in range(7):
        day = (now - timedelta(days=6 - day_index)).date()
        day_jobs = RunJob.objects.filter(created_at__date=day)
        total = day_jobs.count()
        succeeded = day_jobs.filter(status=RunJob.STATUS_SUCCEEDED).count()
        rate = round((succeeded * 100 / total), 1) if total else 100.0
        daily_success.append({"label": day.strftime("%a"), "rate": rate})

    failure_jobs = RunJob.objects.filter(status=RunJob.STATUS_FAILED, created_at__gte=since_60d)
    source_counts = Counter(_job_failure_source(job) for job in failure_jobs)
    total_sources = sum(source_counts.values())
    failure_sources = []
    for label, key in [
        ("Mapping", "mapping"),
        ("Inventory", "inventory"),
        ("Auth/Token", "auth"),
        ("Network", "network"),
        ("Other", "other"),
    ]:
        count = source_counts.get(key, 0)
        percentage = round((count * 100 / total_sources), 1) if total_sources else 0.0
        failure_sources.append({"label": label, "count": count, "percentage": percentage})

    return {
        "kpis": {
            "healthy_count": healthy_count,
            "warning_count": warning_count,
            "critical_count": critical_count,
            "records_synced_24h": records_synced_24h,
            "queued_or_running": RunJob.objects.filter(
                status__in=[RunJob.STATUS_QUEUED, RunJob.STATUS_RUNNING]
            ).count(),
            "runs_7d": RunJob.objects.filter(created_at__gte=since_7d).count(),
        },
        "companies": companies_context,
        "live_log": live_log,
        "company_count": len(companies_context),
        "daily_success": daily_success,
        "failure_sources": failure_sources,
    }


@login_required
def overview(request):
    _ensure_company_records()
    context = _overview_context()
    context.update(_nav_context())
    return render(request, "dashboard/overview.html", context)


@login_required
def runs_list(request):
    _ensure_company_records()
    jobs = RunJob.objects.order_by("-created_at")[:100]
    form = RunTriggerForm(initial={"scope": RunJob.SCOPE_ALL, "date_mode": "yesterday"})
    companies = CompanyConfigRecord.objects.filter(is_active=True).order_by("display_name")
    context = {"jobs": jobs, "form": form, "companies": companies}
    context.update(_nav_context())
    return render(request, "epos_qbo/runs.html", context)


@login_required
def run_detail(request, job_id):
    job = get_object_or_404(RunJob, id=job_id)
    artifacts = job.artifacts.order_by("-processed_at", "-imported_at")
    context = {"job": job, "artifacts": artifacts}
    context.update(_nav_context())
    return render(request, "epos_qbo/run_detail.html", context)


@login_required
@require_GET
def run_logs(request, job_id):
    job = get_object_or_404(RunJob, id=job_id)
    try:
        offset = int(request.GET.get("offset", "0"))
    except ValueError as exc:
        raise Http404("Invalid offset") from exc
    if offset < 0:
        raise Http404("Invalid offset")
    chunk, next_offset = read_log_chunk(job, offset)
    return JsonResponse({"chunk": chunk, "next_offset": next_offset, "status": job.status})


@login_required
@permission_required("epos_qbo.can_trigger_runs", raise_exception=True)
@require_POST
def trigger_run(request):
    form = RunTriggerForm(request.POST)
    if not form.is_valid():
        messages.error(request, f"Invalid trigger payload: {form.errors.as_text()}")
        return redirect("epos_qbo:runs")

    cleaned = form.cleaned_data
    if cleaned["scope"] == RunJob.SCOPE_SINGLE and not CompanyConfigRecord.objects.filter(
        company_key=cleaned.get("company_key") or ""
    ).exists():
        messages.error(request, "Unknown company key for single-company run.")
        return redirect("epos_qbo:runs")

    job = RunJob.objects.create(
        scope=cleaned["scope"],
        company_key=cleaned.get("company_key") or None,
        target_date=cleaned.get("target_date"),
        from_date=cleaned.get("from_date"),
        to_date=cleaned.get("to_date"),
        skip_download=bool(cleaned.get("skip_download")),
        requested_by=request.user,
        status=RunJob.STATUS_QUEUED,
    )

    ok, reason = acquire_run_lock(holder=f"dashboard:{job.id}", run_job=job)
    if not ok:
        job.status = RunJob.STATUS_FAILED
        job.failure_reason = f"Could not acquire dashboard lock: {reason}"
        job.finished_at = timezone.now()
        job.exit_code = 2
        job.save(update_fields=["status", "failure_reason", "finished_at", "exit_code"])
        messages.error(request, reason)
        return redirect("epos_qbo:runs")

    try:
        command = build_command(cleaned)
        start_run_job(job, command)
    except Exception as exc:
        release_run_lock(run_job=job, force=True)
        job.status = RunJob.STATUS_FAILED
        job.failure_reason = f"Failed to start subprocess: {exc}"
        job.finished_at = timezone.now()
        job.exit_code = 3
        job.save(update_fields=["status", "failure_reason", "finished_at", "exit_code"])
        messages.error(request, str(exc))
        return redirect("epos_qbo:runs")

    messages.success(request, f"Run started: {job.id}")
    return redirect("epos_qbo:run-detail", job_id=job.id)


@login_required
@permission_required("epos_qbo.can_edit_companies", raise_exception=True)
def company_new(request):
    if request.method == "POST":
        form = CompanyBasicForm(request.POST)
        if form.is_valid():
            payload = build_basic_payload(form)
            result = validate_company_config(payload)
            if not result.valid:
                messages.error(request, "; ".join(result.errors))
            else:
                record = CompanyConfigRecord.objects.create(
                    company_key=form.cleaned_data["company_key"],
                    display_name=form.cleaned_data["display_name"],
                    config_json=payload,
                    created_by=request.user,
                    updated_by=request.user,
                )
                sync_record_to_json(record)
                messages.success(request, "Company created. Continue with advanced settings.")
                return redirect("epos_qbo:company-advanced", company_key=record.company_key)
    else:
        form = CompanyBasicForm()
    context = {"form": form}
    context.update(_nav_context())
    return render(request, "epos_qbo/company_form_basic.html", context)


@login_required
@permission_required("epos_qbo.can_edit_companies", raise_exception=True)
def company_advanced(request, company_key):
    record = get_object_or_404(CompanyConfigRecord, company_key=company_key)

    if request.method == "POST":
        form = CompanyAdvancedForm(request.POST)
        if form.is_valid():
            payload = apply_advanced_payload(record.config_json, form)
            result = validate_company_config(payload)
            if not result.valid:
                messages.error(request, "; ".join(result.errors))
            else:
                record.config_json = payload
                record.display_name = payload.get("display_name", record.display_name)
                record.config_version += 1
                record.updated_by = request.user
                record.save(update_fields=["config_json", "display_name", "config_version", "updated_by", "updated_at"])
                sync_record_to_json(record)
                messages.success(request, "Advanced settings saved.")
                return redirect("epos_qbo:overview")
    else:
        cfg = record.config_json or {}
        form = CompanyAdvancedForm(
            initial={
                "trading_day_enabled": (cfg.get("trading_day") or {}).get("enabled", False),
                "trading_day_start_hour": (cfg.get("trading_day") or {}).get("start_hour", 5),
                "trading_day_start_minute": (cfg.get("trading_day") or {}).get("start_minute", 0),
                "inventory_enabled": (cfg.get("inventory") or {}).get("enable_inventory_items", False),
                "allow_negative_inventory": (cfg.get("inventory") or {}).get("allow_negative_inventory", False),
                "inventory_start_date": (cfg.get("inventory") or {}).get("inventory_start_date", "today"),
                "default_qty_on_hand": (cfg.get("inventory") or {}).get("default_qty_on_hand", 0),
                "tax_rate": (cfg.get("qbo") or {}).get("tax_rate"),
                "tax_code_id": (cfg.get("qbo") or {}).get("tax_code_id", ""),
                "tax_code_name": (cfg.get("qbo") or {}).get("tax_code_name", ""),
                "date_format": (cfg.get("transform") or {}).get("date_format", "%Y-%m-%d"),
                "receipt_prefix": (cfg.get("transform") or {}).get("receipt_prefix", "SR"),
                "receipt_number_format": (cfg.get("transform") or {}).get("receipt_number_format", "date_tender_sequence"),
                "group_by": ",".join((cfg.get("transform") or {}).get("group_by", ["date", "tender"])),
            }
        )

    context = {"form": form, "record": record}
    context.update(_nav_context())
    return render(request, "epos_qbo/company_form_advanced.html", context)


@login_required
@permission_required("epos_qbo.can_edit_companies", raise_exception=True)
@require_POST
def sync_company_json(request, company_key):
    record = get_object_or_404(CompanyConfigRecord, company_key=company_key)
    sync_record_to_json(record)
    messages.success(request, f"Synced {company_key} to JSON")
    return redirect("epos_qbo:company-advanced", company_key=company_key)
