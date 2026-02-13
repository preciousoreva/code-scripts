from __future__ import annotations

import os
import subprocess
import sys
import threading
from pathlib import Path
from shlex import join as shlex_join

from django.utils import timezone

from oiat_portal.paths import BASE_DIR, OPS_RUN_LOGS_DIR

from ..models import RunJob
from .artifact_ingestion import attach_recent_artifacts_to_job
from .locking import release_run_lock


def build_command(cleaned: dict) -> list[str]:
    scope = cleaned["scope"]
    date_mode = cleaned["date_mode"]

    if scope == RunJob.SCOPE_SINGLE:
        cmd = [sys.executable, str(BASE_DIR / "code_scripts" / "run_pipeline.py"), "--company", cleaned["company_key"]]
    else:
        cmd = [sys.executable, str(BASE_DIR / "code_scripts" / "run_all_companies.py")]

    if date_mode == "target_date" and cleaned.get("target_date"):
        cmd.extend(["--target-date", cleaned["target_date"].strftime("%Y-%m-%d")])
    elif date_mode == "range" and cleaned.get("from_date") and cleaned.get("to_date"):
        cmd.extend(["--from-date", cleaned["from_date"].strftime("%Y-%m-%d"), "--to-date", cleaned["to_date"].strftime("%Y-%m-%d")])
        if cleaned.get("skip_download"):
            cmd.append("--skip-download")

    return [str(part) for part in cmd]


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
    job.save(update_fields=["exit_code", "finished_at", "status", "failure_reason"])

    attach_recent_artifacts_to_job(job)
    release_run_lock(run_job=job, force=True)


def start_run_job(job: RunJob, command: list[str]) -> RunJob:
    OPS_RUN_LOGS_DIR.mkdir(parents=True, exist_ok=True)
    log_path = OPS_RUN_LOGS_DIR / f"{job.id}.log"

    env = dict(os.environ)
    env["OIAT_RUN_SOURCE"] = "dashboard"

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
    job.started_at = timezone.now()
    job.save(update_fields=["command_json", "command_display", "status", "pid", "log_file_path", "started_at"])

    t = threading.Thread(target=_monitor_process, args=(job.id, popen, log_handle), daemon=True)
    t.start()
    return job


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
