from __future__ import annotations

import os
import subprocess
import sys
import threading
from pathlib import Path
from shlex import join as shlex_join

from django.db import transaction
from django.utils import timezone

from oiat_portal.paths import BASE_DIR, OPS_RUN_LOGS_DIR

from ..models import RunJob, RunLock
from .artifact_ingestion import attach_recent_artifacts_to_job
from .locking import release_run_lock


def build_command(cleaned: dict) -> list[str]:
    scope = cleaned["scope"]
    date_mode = cleaned["date_mode"]
    
    # Prefer project's venv Python if it exists, otherwise use sys.executable
    venv_python = BASE_DIR / ".venv" / "bin" / "python"
    if venv_python.exists():
        python_exe = str(venv_python)
    else:
        python_exe = sys.executable

    if scope == RunJob.SCOPE_SINGLE:
        cmd = [python_exe, str(BASE_DIR / "code_scripts" / "run_pipeline.py"), "--company", cleaned["company_key"]]
    else:
        cmd = [python_exe, str(BASE_DIR / "code_scripts" / "run_all_companies.py")]
        cmd.extend(["--parallel", str(int(cleaned.get("parallel") or 2))])
        cmd.extend(["--stagger-seconds", str(int(cleaned.get("stagger_seconds") or 2))])
        if cleaned.get("continue_on_failure"):
            cmd.append("--continue-on-failure")

    if date_mode == "target_date" and cleaned.get("target_date"):
        cmd.extend(["--target-date", cleaned["target_date"].strftime("%Y-%m-%d")])
    elif date_mode == "range" and cleaned.get("from_date") and cleaned.get("to_date"):
        cmd.extend(["--from-date", cleaned["from_date"].strftime("%Y-%m-%d"), "--to-date", cleaned["to_date"].strftime("%Y-%m-%d")])
        if cleaned.get("skip_download"):
            cmd.append("--skip-download")

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
    }
    return build_command(cleaned)


def _monitor_process(job_id, popen: subprocess.Popen, log_handle):
    try:
        exit_code = popen.wait()
    finally:
        # Close the log file handle that the subprocess was writing to.
        try:
            log_handle.close()
        except OSError:
            pass

    try:
        job = RunJob.objects.get(id=job_id)
    except RunJob.DoesNotExist:
        return

    job.exit_code = exit_code
    job.finished_at = timezone.now()
    job.status = RunJob.STATUS_SUCCEEDED if exit_code == 0 else RunJob.STATUS_FAILED
    if exit_code != 0 and not job.failure_reason:
        job.failure_reason = f"Subprocess exited with code {exit_code}"
    # #region agent log
    try:
        import json
        with open("/mnt/c/Users/MARVIN-DEV/Documents/Developer Projects/Oreva Innovations & Tech/epos_to_qbo_automation/code-scripts/.cursor/debug.log", "a") as f:
            f.write(json.dumps({"location": "job_runner._monitor_process:exit", "message": "Subprocess exited", "data": {"job_id": str(job_id), "exit_code": exit_code, "status": job.status, "failure_reason": (job.failure_reason or "")[:200]}, "timestamp": __import__("time").time() * 1000, "hypothesisId": "H10"}) + "\n")
    except Exception:
        pass
    # #endregion
    job.save(update_fields=["exit_code", "finished_at", "status", "failure_reason"])

    attach_recent_artifacts_to_job(job)
    release_run_lock(run_job=job, force=True)
    dispatch_next_queued_job()


def start_run_job(job: RunJob, command: list[str]) -> RunJob:
    OPS_RUN_LOGS_DIR.mkdir(parents=True, exist_ok=True)
    log_path = OPS_RUN_LOGS_DIR / f"{job.id}.log"

    env = dict(os.environ)
    env["OIAT_RUN_SOURCE"] = "dashboard"
    # Ensure code_scripts package is importable when running run_pipeline.py
    pythonpath = str(BASE_DIR)
    env["PYTHONPATH"] = pythonpath + os.pathsep + env.get("PYTHONPATH", "")
    
    # #region agent log
    try:
        import json
        with open("/mnt/c/Users/MARVIN-DEV/Documents/Developer Projects/Oreva Innovations & Tech/epos_to_qbo_automation/code-scripts/.cursor/debug.log", "a") as f:
            f.write(json.dumps({"location": "job_runner.start_run_job:before_popen", "message": "Starting subprocess", "data": {"command": command, "sys_executable": sys.executable, "cwd": str(BASE_DIR), "pythonpath": pythonpath}, "timestamp": __import__("time").time() * 1000, "hypothesisId": "H9"}) + "\n")
    except Exception:
        pass
    # #endregion

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


def dispatch_next_queued_job() -> tuple[RunJob | None, str]:
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
        release_run_lock(run_job=job, force=True)
        RunJob.objects.filter(id=job.id).update(
            status=RunJob.STATUS_FAILED,
            failure_reason=f"Failed to start subprocess: {exc}",
            finished_at=timezone.now(),
            exit_code=3,
        )
        return dispatch_next_queued_job()


def read_log_chunk(job: RunJob, offset: int, max_bytes: int = 65536) -> tuple[str, int]:
    if not job.log_file_path:
        return "", offset
    path = Path(job.log_file_path)
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
