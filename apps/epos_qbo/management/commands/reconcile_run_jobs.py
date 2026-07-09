from __future__ import annotations

import os

from django.core.management.base import BaseCommand
from django.utils import timezone

from apps.epos_qbo.models import RunJob
from apps.epos_qbo.services.locking import release_run_lock


class Command(BaseCommand):
    help = "Mark stuck running jobs as failed when their PID no longer exists."

    def handle(self, *args, **options):
        fixed = 0
        for job in RunJob.objects.filter(status=RunJob.STATUS_RUNNING):
            alive = False
            if job.pid:
                try:
                    os.kill(job.pid, 0)
                    alive = True
                except OSError:
                    alive = False
            if alive:
                continue
            job.status = RunJob.STATUS_FAILED
            job.exit_code = -1
            job.finished_at = timezone.now()
            job.failure_reason = "Reconciled by reaper: PID not alive"
            job.save(update_fields=["status", "exit_code", "finished_at", "failure_reason"])
            release_run_lock(run_job=job, force=True)
            fixed += 1

        self.stdout.write(self.style.SUCCESS(f"Reconciled {fixed} run job(s)."))
