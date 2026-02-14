"""
Scheduled all-companies run: acquires global lock, logs to TEMP, runs pipeline via subprocess.

Task Scheduler should call this via: python manage.py run_scheduled_all_companies --parallel 2
so runs are visible in the Django dashboard (RunJob record when available).
"""

from __future__ import annotations

import logging
import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

from django.core.management.base import BaseCommand

logger = logging.getLogger(__name__)


def get_repo_root() -> Path:
    """Project root: directory containing manage.py (and code_scripts project root)."""
    # This file: apps/dashboard/management/commands/run_scheduled_all_companies.py -> 5 parents to repo root
    return Path(__file__).resolve().parent.parent.parent.parent.parent


def get_global_lock_path(repo_root: Path) -> Path:
    """Path to the global run lock file (same as scheduler .cmd uses)."""
    return repo_root / "runtime" / "global_run.lock"


def get_scheduled_log_path() -> Path:
    """Log file path: %TEMP%\\epos_to_qbo_automation\\run_all_companies.log (append)."""
    temp = os.environ.get("TEMP") or os.environ.get("TMP") or os.path.expandvars("%TEMP%")
    if not temp or not os.path.isabs(temp):
        temp = os.path.join(os.path.expanduser("~"), "AppData", "Local", "Temp") if os.name == "nt" else "/tmp"
    log_dir = Path(temp) / "epos_to_qbo_automation"
    log_dir.mkdir(parents=True, exist_ok=True)
    return log_dir / "run_all_companies.log"


def run_pipeline_subprocess(
    repo_root: Path,
    log_path: Path,
    parallel: int,
    stagger_seconds: int,
    continue_on_failure: bool,
) -> int:
    """
    Run code_scripts.run_all_companies as subprocess; stdout+stderr go to log_path (append).
    Returns the subprocess exit code.
    """
    cmd = [
        sys.executable,
        "-m",
        "code_scripts.run_all_companies",
        "--parallel",
        str(parallel),
        "--stagger-seconds",
        str(stagger_seconds),
    ]
    if continue_on_failure:
        cmd.append("--continue-on-failure")

    with open(log_path, "a", encoding="utf-8") as log_file:
        proc = subprocess.Popen(
            cmd,
            cwd=str(repo_root),
            stdout=log_file,
            stderr=subprocess.STDOUT,
            env={**os.environ},
        )
        proc.wait()
    return proc.returncode


def acquire_lock(lock_path: Path) -> bool:
    """
    Create runtime dir and lock file with timestamp. Use exclusive create so only one holder.
    Returns True if lock was acquired, False if lock already exists (another run active).
    """
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        # On Windows, open in exclusive create; on Unix, O_CREAT|O_EXCL
        fd = os.open(
            str(lock_path),
            os.O_CREAT | os.O_EXCL | os.O_WRONLY,
            0o644,
        )
        try:
            content = (
                f'{{"holder":"run_scheduled_all_companies","pid":{os.getpid()},'
                f'"acquired_at":"{datetime.now(timezone.utc).isoformat()}"}}\n'
            )
            os.write(fd, content.encode("utf-8"))
        finally:
            os.close(fd)
        return True
    except FileExistsError:
        return False


def release_lock(lock_path: Path) -> None:
    """Remove the lock file. Safe to call if already removed."""
    try:
        lock_path.unlink(missing_ok=True)
    except OSError:
        pass


def _get_run_job_model():
    """Return RunJob model if available (e.g. apps.epos_qbo.models.RunJob). Else None."""
    try:
        from apps.epos_qbo.models import RunJob
        return RunJob
    except ImportError:
        return None


class Command(BaseCommand):
    """Django management command: run_scheduled_all_companies."""

    help = (
        "Run all companies pipeline under global lock and log to %TEMP%\\epos_to_qbo_automation\\run_all_companies.log. "
        "Creates/updates a RunJob record when the model is available so scheduled runs appear in the dashboard."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--parallel",
            type=int,
            default=2,
            help="Number of companies to run in parallel (default: 2).",
        )
        parser.add_argument(
            "--stagger-seconds",
            type=int,
            default=2,
            help="Seconds between starting parallel jobs (default: 2).",
        )
        parser.add_argument(
            "--continue-on-failure",
            action="store_true",
            help="Continue with remaining companies if one fails.",
        )

    def handle(self, *args, **options):
        parallel = options["parallel"]
        stagger_seconds = options["stagger_seconds"]
        continue_on_failure = options["continue_on_failure"]

        repo_root = get_repo_root()
        lock_path = get_global_lock_path(repo_root)
        log_path = get_scheduled_log_path()

        if not acquire_lock(lock_path):
            self.stderr.write("Another run is already active (lock file exists). Exiting.\n")
            sys.exit(2)

        run_job = None
        try:
            run_job = self._create_run_job_if_available(log_path, parallel, stagger_seconds, continue_on_failure)

            with open(log_path, "a", encoding="utf-8") as f:
                f.write(
                    f"\n--- run_scheduled_all_companies started at {datetime.now(timezone.utc).isoformat()} ---\n"
                )

            exit_code = run_pipeline_subprocess(
                repo_root=repo_root,
                log_path=log_path,
                parallel=parallel,
                stagger_seconds=stagger_seconds,
                continue_on_failure=continue_on_failure,
            )

            if run_job is not None:
                self._update_run_job(run_job, exit_code)
                self._attach_artifacts_to_run_job(run_job)

            sys.exit(exit_code)
        finally:
            release_lock(lock_path)

    def _create_run_job_if_available(self, log_path: Path, parallel: int, stagger_seconds: int, continue_on_failure: bool):
        """If RunJob model exists, create a running record. Else return None."""
        RunJob = _get_run_job_model()
        if RunJob is None:
            return None
        from django.utils import timezone

        command_display = (
            f"run_scheduled_all_companies --parallel {parallel} --stagger-seconds {stagger_seconds}"
            + (" --continue-on-failure" if continue_on_failure else "")
        )
        job = RunJob.objects.create(
            scope=RunJob.SCOPE_ALL,
            status=RunJob.STATUS_RUNNING,
            requested_by=None,
            command_display=command_display,
            started_at=timezone.now(),
            log_file_path=str(log_path),
            parallel=parallel,
            stagger_seconds=stagger_seconds,
            continue_on_failure=continue_on_failure,
        )
        return job

    def _update_run_job(self, run_job, exit_code: int) -> None:
        """Update RunJob status and finished_at, exit_code."""
        from django.utils import timezone

        run_job.status = run_job.STATUS_SUCCEEDED if exit_code == 0 else run_job.STATUS_FAILED
        run_job.finished_at = timezone.now()
        run_job.exit_code = exit_code
        run_job.save(update_fields=["status", "finished_at", "exit_code"])

    def _attach_artifacts_to_run_job(self, run_job) -> None:
        """
        Link recent metadata artifacts to this RunJob so the dashboard shows
        company artifacts, last-run times, and KPIs from this scheduled run.
        No-op if the artifact ingestion service is not available; logs and continues on error.
        """
        try:
            from apps.epos_qbo.services.artifact_ingestion import attach_recent_artifacts_to_job
            attach_recent_artifacts_to_job(run_job)
        except ImportError:
            logger.debug("Artifact ingestion not available; skipping artifact attach for run_job %s", run_job.id)
        except Exception as e:
            logger.warning("Could not attach artifacts to run_job %s: %s", run_job.id, e, exc_info=True)
