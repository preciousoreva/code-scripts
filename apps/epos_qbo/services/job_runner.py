from __future__ import annotations

import logging
import os
import subprocess
import sys
import threading
import time
from pathlib import Path
from shlex import join as shlex_join

from django.conf import settings
from django.db import transaction
from django.utils import timezone

from oiat_portal.paths import BASE_DIR, OPS_RUN_LOGS_DIR

from .. import portal_settings
from ..models import RunJob, RunLock, RunSchedule, RunScheduleEvent
from .artifact_ingestion import attach_recent_artifacts_to_job
from .locking import release_run_lock

logger = logging.getLogger(__name__)


def resolve_python_executable() -> str:
    """Resolve Python executable for dashboard subprocesses.

    Precedence:
    1. Explicit sandbox/dev override via OIAT_VENV_PATH
    2. The interpreter running Django right now
    3. Repo-local .venv as a final fallback
    """
    configured_venv = os.environ.get("OIAT_VENV_PATH")
    if configured_venv:
        configured_python = Path(configured_venv).expanduser() / "bin" / "python"
        if configured_python.exists():
            return str(configured_python)

    if sys.executable:
        return sys.executable

    venv_python = BASE_DIR / ".venv" / "bin" / "python"
    if venv_python.exists():
        return str(venv_python)

    return "python3"


def build_command(cleaned: dict) -> list[str]:
    scope = cleaned["scope"]
    date_mode = cleaned["date_mode"]
    python_exe = resolve_python_executable()

    if scope == RunJob.SCOPE_INVENTORY_SYNC:
        return _build_inventory_command(python_exe, cleaned)

    if scope == RunJob.SCOPE_INVENTORY_PIPELINE:
        return _build_inventory_pipeline_command(python_exe, cleaned)

    if scope == RunJob.SCOPE_SINGLE:
        cmd = [python_exe, str(BASE_DIR / "code_scripts" / "run_pipeline.py"), "--company", cleaned["company_key"]]
    else:
        cmd = [python_exe, str(BASE_DIR / "code_scripts" / "run_all_companies.py")]
        cmd.extend(["--parallel", str(int(cleaned.get("parallel") or portal_settings.get_default_parallel()))])
        cmd.extend(["--stagger-seconds", str(int(cleaned.get("stagger_seconds") or portal_settings.get_default_stagger_seconds()))])
        if cleaned.get("continue_on_failure"):
            cmd.append("--continue-on-failure")

    if date_mode == "target_date" and cleaned.get("target_date"):
        cmd.extend(["--target-date", cleaned["target_date"].strftime("%Y-%m-%d")])
    elif date_mode == "range" and cleaned.get("from_date") and cleaned.get("to_date"):
        cmd.extend(["--from-date", cleaned["from_date"].strftime("%Y-%m-%d"), "--to-date", cleaned["to_date"].strftime("%Y-%m-%d")])
        if cleaned.get("skip_download"):
            cmd.append("--skip-download")

    return [str(part) for part in cmd]


def _build_inventory_pipeline_command(python_exe: str, cleaned: dict) -> list[str]:
    """Build the operator-facing unified inventory pipeline command."""
    opts = cleaned.get("inventory_options") or {}
    company = cleaned["company_key"]
    if not company:
        raise ValueError("inventory_pipeline requires company_key")
    product_filter = str(opts.get("product_filter") or "").strip()

    cmd: list[str] = [
        python_exe,
        "-m",
        "code_scripts.inventory_pipeline",
        "--company",
        str(company),
    ]
    stock_csv = (opts.get("stock_csv") or "").strip()
    if stock_csv:
        cmd.extend(["--stock-csv", stock_csv])
    else:
        cmd.append("--auto-download")

    if opts.get("qbo_csv"):
        cmd.extend(["--qbo-csv", str(opts["qbo_csv"])])
    else:
        cmd.extend(["--auto-fetch-qbo", "--qbo-force-refresh"])

    if product_filter:
        cmd.extend(["--product", product_filter])
    base_names = opts.get("base_names") or []
    if isinstance(base_names, str):
        base_names = [base_names]
    if isinstance(base_names, list):
        for base_name in base_names:
            value = str(base_name or "").strip()
            if value:
                cmd.extend(["--base-name", value])
    categories = opts.get("categories") or []
    if isinstance(categories, str):
        categories = [categories]
    if isinstance(categories, list):
        for category in categories:
            value = str(category or "").strip()
            if value:
                cmd.extend(["--category", value])

    max_catalog_fixes = _positive_inventory_limit(
        opts.get("max_catalog_fixes"),
        "max_catalog_fixes",
    )
    max_quantity_adjustments = _positive_inventory_limit(
        opts.get("max_quantity_adjustments"),
        "max_quantity_adjustments",
    )
    if max_catalog_fixes is not None:
        cmd.extend(["--max-catalog-fixes", str(max_catalog_fixes)])
    if max_quantity_adjustments is not None:
        cmd.extend(["--max-quantity-adjustments", str(max_quantity_adjustments)])

    if opts.get("max_qty_delta") is not None:
        cmd.extend(["--max-qty-delta", str(opts["max_qty_delta"])])
    if opts.get("adjust_account_id"):
        cmd.extend(["--adjust-account-id", str(opts["adjust_account_id"])])
    if opts.get("txn_date"):
        cmd.extend(["--txn-date", str(opts["txn_date"])])
    if opts.get("dry_run"):
        cmd.append("--dry-run")

    return [str(part) for part in cmd]


def _positive_inventory_limit(value: object, option_name: str) -> int | None:
    if value is None or str(value).strip() == "":
        return None
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"inventory_pipeline requires non-negative {option_name}") from exc
    if parsed < 0:
        raise ValueError(f"inventory_pipeline requires non-negative {option_name}")
    return parsed


def _build_inventory_command(python_exe: str, cleaned: dict) -> list[str]:
    """Build a `python -m code_scripts.inventory_sync ...` command.

    Portal-triggered inventory audits always auto-download a fresh EPOS Stock
    Report — operators don't supply a CSV path through the form. Advanced
    operators who want to point at an existing CSV can pre-populate
    inventory_options['stock_csv'] (e.g. via API) and we'll honor it instead;
    otherwise we emit `--auto-download`.

    Required keys in cleaned: company_key.
    Optional keys pulled from inventory_options (dict): stock_csv, qbo_csv,
    product_filter, categories, tolerance, apply, dry_run, allow_ambiguous,
    max_adjustments, max_qty_delta, adjust_account_id, txn_date.
    """
    opts = cleaned.get("inventory_options") or {}
    company = cleaned["company_key"]
    if not company:
        raise ValueError("inventory_sync requires company_key")

    cmd: list[str] = [
        python_exe, "-m", "code_scripts.inventory_sync",
        "--company", str(company),
    ]
    stock_csv = (opts.get("stock_csv") or "").strip()
    if stock_csv:
        cmd.extend(["--stock-csv", stock_csv])
    else:
        cmd.append("--auto-download")

    if opts.get("qbo_csv"):
        cmd.extend(["--qbo-csv", str(opts["qbo_csv"])])
    if opts.get("product_filter"):
        cmd.extend(["--product", str(opts["product_filter"])])
    categories = opts.get("categories") or []
    if isinstance(categories, str):
        categories = [categories]
    if isinstance(categories, list):
        for category in categories:
            value = str(category or "").strip()
            if value:
                cmd.extend(["--category", value])
    if opts.get("tolerance") is not None:
        cmd.extend(["--tolerance", str(opts["tolerance"])])
    if opts.get("apply"):
        cmd.append("--apply")
    if opts.get("dry_run"):
        cmd.append("--dry-run")
    if opts.get("allow_ambiguous"):
        cmd.append("--allow-ambiguous")
    if opts.get("max_adjustments") is not None:
        cmd.extend(["--max-adjustments", str(int(opts["max_adjustments"]))])
    if opts.get("max_qty_delta") is not None:
        cmd.extend(["--max-qty-delta", str(opts["max_qty_delta"])])
    if opts.get("adjust_account_id"):
        cmd.extend(["--adjust-account-id", str(opts["adjust_account_id"])])
    if opts.get("txn_date"):
        cmd.extend(["--txn-date", str(opts["txn_date"])])

    return [str(part) for part in cmd]


def build_command_for_job(job: RunJob) -> list[str]:
    if job.from_date and job.to_date:
        date_mode = "range"
    elif job.target_date:
        date_mode = "target_date"
    else:
        date_mode = "yesterday"
    cleaned = {
        "scope": job.scope,
        "company_key": job.company_key or "",
        "date_mode": date_mode,
        "target_date": job.target_date,
        "from_date": job.from_date,
        "to_date": job.to_date,
        "skip_download": job.skip_download,
        "parallel": job.parallel,
        "stagger_seconds": job.stagger_seconds,
        "continue_on_failure": job.continue_on_failure,
        "inventory_options": job.inventory_options_json or {},
    }
    return build_command(cleaned)


def _monitor_process(job_id, popen: subprocess.Popen, log_handle):
    exit_code = None
    try:
        exit_code = popen.wait()
    finally:
        # Close the log file handle that the subprocess was writing to.
        try:
            log_handle.close()
        except OSError:
            pass

    job = None
    try:
        job = RunJob.objects.get(id=job_id)
    except RunJob.DoesNotExist:
        pass

    if job is not None:
        try:
            attached_artifacts = 0
            attach_started = time.monotonic()
            # Link artifacts before flipping the run out of RUNNING so dashboard completion
            # events observe status only after overview data is ready to refresh.
            attached_artifacts = attach_recent_artifacts_to_job(job)
            attach_elapsed_ms = int((time.monotonic() - attach_started) * 1000)

            job.exit_code = exit_code
            job.finished_at = timezone.now()
            job.status = RunJob.STATUS_SUCCEEDED if exit_code == 0 else RunJob.STATUS_FAILED
            if exit_code != 0 and not job.failure_reason:
                job.failure_reason = f"Subprocess exited with code {exit_code}"
            job.save(update_fields=["exit_code", "finished_at", "status", "failure_reason"])
            if job.scheduled_by_id:
                schedule = job.scheduled_by
                event_type = (
                    RunScheduleEvent.TYPE_RUN_SUCCEEDED
                    if job.status == RunJob.STATUS_SUCCEEDED
                    else RunScheduleEvent.TYPE_RUN_FAILED
                )
                message = "Run completed successfully" if job.status == RunJob.STATUS_SUCCEEDED else "Run failed"
                payload_json = {
                    "status": job.status,
                    "exit_code": exit_code,
                }
                if schedule is not None:
                    if job.status == RunJob.STATUS_SUCCEEDED:
                        schedule.last_result = RunSchedule.LAST_RESULT_SUCCEEDED
                        schedule.last_error = ""
                    else:
                        schedule.last_result = RunSchedule.LAST_RESULT_FAILED
                        schedule.last_error = job.failure_reason or "Run failed."
                    schedule.save(update_fields=["last_result", "last_error", "updated_at"])
                    payload_json["schedule_id"] = str(schedule.id)
                    payload_json["schedule_name"] = schedule.name
                RunScheduleEvent.objects.create(
                    schedule=schedule,
                    run_job=job,
                    event_type=event_type,
                    message=message,
                    payload_json=payload_json,
                )
            logger.info(
                "RunJob %s finalized: status=%s exit_code=%s attached_artifacts=%s attach_elapsed_ms=%s",
                job_id,
                job.status,
                exit_code,
                attached_artifacts,
                attach_elapsed_ms,
            )
        except Exception as exc:
            # Log error but don't crash - try to mark job as failed if status update failed
            logger.error(f"Failed to update RunJob {job_id} status after process exit: {exc}", exc_info=True)
            try:
                # Attempt to mark as failed if we couldn't update status normally
                RunJob.objects.filter(id=job_id).update(
                    status=RunJob.STATUS_FAILED,
                    failure_reason=f"Status update failed: {exc}",
                    finished_at=timezone.now(),
                    exit_code=exit_code if exit_code is not None else -1,
                )
            except Exception:
                # If even this fails, log and give up
                logger.error(f"Failed to mark RunJob {job_id} as failed after status update error", exc_info=True)

    try:
        release_run_lock(run_job=job, force=True)
        dispatch_next_queued_job()
    except Exception as exc:
        logger.error(f"Error in post-completion cleanup for RunJob {job_id}: {exc}", exc_info=True)


def start_run_job(job: RunJob, command: list[str]) -> RunJob:
    OPS_RUN_LOGS_DIR.mkdir(parents=True, exist_ok=True)
    log_path = OPS_RUN_LOGS_DIR / f"{job.id}.log"

    env = dict(os.environ)
    env["OIAT_RUN_SOURCE"] = "dashboard"
    env["OIAT_RUN_JOB_ID"] = str(job.id)
    env["OIAT_RUN_SCOPE"] = str(job.scope)
    env["OIAT_RUN_STARTED_AT"] = timezone.now().isoformat()
    # Prefer explicit env override; otherwise fall back to Django settings.
    if not env.get("OIAT_PORTAL_BASE_URL"):
        base = str(getattr(settings, "OIAT_PORTAL_BASE_URL", "") or "").strip().rstrip("/")
        if base:
            env["OIAT_PORTAL_BASE_URL"] = base
    # Ensure code_scripts package is importable when running run_pipeline.py
    pythonpath = str(BASE_DIR)
    env["PYTHONPATH"] = pythonpath + os.pathsep + env.get("PYTHONPATH", "")

    # Keep the log file handle open for the lifetime of the subprocess.
    # The monitor thread closes it after the process exits.
    log_handle = open(log_path, "ab")  # noqa: SIM115
    try:
        popen = subprocess.Popen(
            command,
            cwd=str(BASE_DIR),
            stdout=log_handle,
            stderr=subprocess.STDOUT,
            env=env,
        )
    except Exception:
        log_handle.close()
        raise

    job.command_json = command
    job.command_display = shlex_join(command)
    job.status = RunJob.STATUS_RUNNING
    job.pid = popen.pid
    job.log_file_path = str(log_path)
    job.dispatched_at = timezone.now()
    job.started_at = timezone.now()
    job.save(update_fields=["command_json", "command_display", "status", "pid", "log_file_path", "dispatched_at", "started_at"])

    t = threading.Thread(target=_monitor_process, args=(job.id, popen, log_handle), daemon=True)
    t.start()
    return job


# Max consecutive start failures before giving up (avoids unbounded recursion / thrashing)
DISPATCH_START_FAILURE_LIMIT = 5


def dispatch_next_queued_job() -> tuple[RunJob | None, str]:
    failure_count = 0
    while failure_count < DISPATCH_START_FAILURE_LIMIT:
        with transaction.atomic():
            lock, _ = RunLock.objects.select_for_update().get_or_create(id=1)
            if lock.active:
                return None, "queued"

            job = (
                RunJob.objects.select_for_update()
                .filter(status=RunJob.STATUS_QUEUED)
                .order_by("created_at")
                .first()
            )
            if job is None:
                return None, "empty"

            lock.active = True
            lock.holder = f"dashboard:{job.id}"
            lock.owner_run_job = job
            lock.acquired_at = timezone.now()
            lock.save(update_fields=["active", "holder", "owner_run_job", "acquired_at", "updated_at"])

        try:
            command = build_command_for_job(job)
            started_job = start_run_job(job, command)
            return started_job, "started"
        except Exception as exc:
            failure_count += 1
            release_run_lock(run_job=job, force=True)
            RunJob.objects.filter(id=job.id).update(
                status=RunJob.STATUS_FAILED,
                failure_reason=f"Failed to start subprocess: {exc}",
                finished_at=timezone.now(),
                exit_code=3,
            )
            # Loop to try next queued job instead of recursing
    return None, "start_failed"


def read_log_chunk(job: RunJob, offset: int, max_bytes: int = 65536) -> tuple[str, int]:
    if not job.log_file_path:
        return "", offset
    path = Path(os.path.expandvars(job.log_file_path)).expanduser()
    if not path.is_absolute():
        path = BASE_DIR / path
    if not path.exists():
        return "", offset
    try:
        with open(path, "rb") as f:
            f.seek(offset)
            data = f.read(max_bytes)
            next_offset = f.tell()
    except OSError:
        return "", offset
    return data.decode("utf-8", errors="replace"), next_offset
