from __future__ import annotations

from django.core.management import call_command
from django.test import TestCase
from django.utils import timezone

from apps.epos_qbo.models import RunJob, RunLock
from apps.epos_qbo.services.locking import acquire_run_lock, release_run_lock


class LockingTests(TestCase):
    def test_lock_contention_and_release(self):
        job1 = RunJob.objects.create(scope=RunJob.SCOPE_SINGLE, company_key="company_a")
        job2 = RunJob.objects.create(scope=RunJob.SCOPE_SINGLE, company_key="company_b")

        acquired_1, _ = acquire_run_lock(holder="holder-1", run_job=job1)
        acquired_2, _ = acquire_run_lock(holder="holder-2", run_job=job2)
        self.assertTrue(acquired_1)
        self.assertFalse(acquired_2)

        release_run_lock(run_job=job1)
        acquired_3, _ = acquire_run_lock(holder="holder-3", run_job=job2)
        self.assertTrue(acquired_3)


class ReaperTests(TestCase):
    def test_reconcile_run_jobs_marks_dead_pid_as_failed(self):
        job = RunJob.objects.create(
            scope=RunJob.SCOPE_SINGLE,
            company_key="company_a",
            status=RunJob.STATUS_RUNNING,
            pid=999999,
            started_at=timezone.now(),
        )
        RunLock.objects.create(active=True, holder="dashboard", owner_run_job=job, acquired_at=timezone.now())

        call_command("reconcile_run_jobs")

        job.refresh_from_db()
        lock = RunLock.objects.get(pk=1)
        self.assertEqual(job.status, RunJob.STATUS_FAILED)
        self.assertEqual(job.exit_code, -1)
        self.assertFalse(lock.active)
