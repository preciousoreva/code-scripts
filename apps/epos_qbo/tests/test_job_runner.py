from __future__ import annotations

from datetime import date

from django.test import SimpleTestCase, TestCase, override_settings
from unittest.mock import Mock, patch

from apps.epos_qbo.models import RunJob, RunLock
from apps.epos_qbo.services.job_runner import _monitor_process, build_command, dispatch_next_queued_job


class BuildCommandTests(SimpleTestCase):
    def test_build_command_single_company_yesterday(self):
        command = build_command(
            {
                "scope": RunJob.SCOPE_SINGLE,
                "company_key": "company_a",
                "date_mode": "yesterday",
                "target_date": None,
                "from_date": None,
                "to_date": None,
                "skip_download": False,
            }
        )
        self.assertIn("run_pipeline.py", " ".join(command))
        self.assertIn("--company", command)
        self.assertIn("company_a", command)
        self.assertNotIn("--target-date", command)

    def test_build_command_single_company_target_date(self):
        command = build_command(
            {
                "scope": RunJob.SCOPE_SINGLE,
                "company_key": "company_b",
                "date_mode": "target_date",
                "target_date": date(2026, 2, 10),
                "from_date": None,
                "to_date": None,
                "skip_download": False,
            }
        )
        self.assertIn("--target-date", command)
        self.assertIn("2026-02-10", command)

    def test_build_command_all_companies_range_with_skip_download(self):
        command = build_command(
            {
                "scope": RunJob.SCOPE_ALL,
                "company_key": "",
                "date_mode": "range",
                "target_date": None,
                "from_date": date(2026, 2, 1),
                "to_date": date(2026, 2, 5),
                "skip_download": True,
                "parallel": 2,
                "stagger_seconds": 2,
                "continue_on_failure": True,
            }
        )
        self.assertIn("run_all_companies.py", " ".join(command))
        self.assertIn("--parallel", command)
        self.assertIn("2", command)
        self.assertIn("--stagger-seconds", command)
        self.assertIn("--from-date", command)
        self.assertIn("2026-02-01", command)
        self.assertIn("--to-date", command)
        self.assertIn("2026-02-05", command)
        self.assertIn("--skip-download", command)
        self.assertIn("--continue-on-failure", command)

    @override_settings(
        OIAT_DASHBOARD_DEFAULT_PARALLEL=4,
        OIAT_DASHBOARD_DEFAULT_STAGGER_SECONDS=8,
    )
    def test_build_command_uses_settings_defaults_when_missing(self):
        command = build_command(
            {
                "scope": RunJob.SCOPE_ALL,
                "company_key": "",
                "date_mode": "yesterday",
                "target_date": None,
                "from_date": None,
                "to_date": None,
                "skip_download": False,
            }
        )
        self.assertIn("--parallel", command)
        self.assertIn("4", command)
        self.assertIn("--stagger-seconds", command)
        self.assertIn("8", command)


class QueueDispatchTests(TestCase):
    @patch("apps.epos_qbo.services.job_runner.start_run_job")
    def test_dispatch_starts_oldest_queued_job(self, start_run_job_mock):
        job1 = RunJob.objects.create(scope=RunJob.SCOPE_SINGLE, company_key="company_a", status=RunJob.STATUS_QUEUED)
        job2 = RunJob.objects.create(scope=RunJob.SCOPE_SINGLE, company_key="company_b", status=RunJob.STATUS_QUEUED)
        start_run_job_mock.side_effect = lambda job, command: job

        dispatched, status = dispatch_next_queued_job()

        self.assertEqual(status, "started")
        self.assertIsNotNone(dispatched)
        self.assertEqual(dispatched.id, job1.id)
        lock = RunLock.objects.get(pk=1)
        self.assertTrue(lock.active)
        self.assertEqual(lock.owner_run_job_id, job1.id)
        start_run_job_mock.assert_called_once()
        args, _kwargs = start_run_job_mock.call_args
        self.assertEqual(args[0].id, job1.id)
        self.assertIn("run_pipeline.py", " ".join(args[1]))
        job2.refresh_from_db()
        self.assertEqual(job2.status, RunJob.STATUS_QUEUED)

    @patch("apps.epos_qbo.services.job_runner.start_run_job")
    def test_dispatch_returns_queued_when_lock_held(self, start_run_job_mock):
        lock_owner = RunJob.objects.create(scope=RunJob.SCOPE_SINGLE, company_key="company_a", status=RunJob.STATUS_RUNNING)
        RunLock.objects.create(active=True, holder="dashboard:test", owner_run_job=lock_owner)
        queued = RunJob.objects.create(scope=RunJob.SCOPE_SINGLE, company_key="company_b", status=RunJob.STATUS_QUEUED)

        dispatched, status = dispatch_next_queued_job()

        self.assertIsNone(dispatched)
        self.assertEqual(status, "queued")
        start_run_job_mock.assert_not_called()
        queued.refresh_from_db()
        self.assertEqual(queued.status, RunJob.STATUS_QUEUED)


class MonitorProcessTests(TestCase):
    @patch("apps.epos_qbo.services.job_runner.dispatch_next_queued_job")
    @patch("apps.epos_qbo.services.job_runner.release_run_lock")
    @patch("apps.epos_qbo.services.job_runner.attach_recent_artifacts_to_job")
    def test_monitor_links_artifacts_before_marking_succeeded(
        self,
        attach_recent_artifacts_to_job_mock,
        release_run_lock_mock,
        dispatch_next_queued_job_mock,
    ):
        job = RunJob.objects.create(
            scope=RunJob.SCOPE_SINGLE,
            company_key="company_a",
            status=RunJob.STATUS_RUNNING,
        )
        observed_status = {}

        def _attach_side_effect(run_job):
            latest = RunJob.objects.get(id=run_job.id)
            observed_status["during_attach"] = latest.status
            return 2

        attach_recent_artifacts_to_job_mock.side_effect = _attach_side_effect

        popen = Mock()
        popen.wait.return_value = 0
        log_handle = Mock()

        _monitor_process(job.id, popen, log_handle)

        job.refresh_from_db()
        self.assertEqual(observed_status.get("during_attach"), RunJob.STATUS_RUNNING)
        self.assertEqual(job.status, RunJob.STATUS_SUCCEEDED)
        self.assertEqual(job.exit_code, 0)
        attach_recent_artifacts_to_job_mock.assert_called_once()
        release_run_lock_mock.assert_called_once()
        release_kwargs = release_run_lock_mock.call_args.kwargs
        self.assertTrue(release_kwargs["force"])
        self.assertEqual(release_kwargs["run_job"].id, job.id)
        dispatch_next_queued_job_mock.assert_called_once_with()

    @patch("apps.epos_qbo.services.job_runner.dispatch_next_queued_job")
    @patch("apps.epos_qbo.services.job_runner.release_run_lock")
    @patch("apps.epos_qbo.services.job_runner.attach_recent_artifacts_to_job")
    def test_monitor_releases_lock_and_dispatches_when_job_missing(
        self,
        attach_recent_artifacts_to_job_mock,
        release_run_lock_mock,
        dispatch_next_queued_job_mock,
    ):
        job = RunJob.objects.create(
            scope=RunJob.SCOPE_SINGLE,
            company_key="company_a",
            status=RunJob.STATUS_RUNNING,
        )
        RunLock.objects.create(active=True, holder=f"dashboard:{job.id}", owner_run_job=job)
        job_id = job.id
        job.delete()

        popen = Mock()
        popen.wait.return_value = 0
        log_handle = Mock()

        _monitor_process(job_id, popen, log_handle)

        popen.wait.assert_called_once_with()
        log_handle.close.assert_called_once_with()
        attach_recent_artifacts_to_job_mock.assert_not_called()
        release_run_lock_mock.assert_called_once_with(run_job=None, force=True)
        dispatch_next_queued_job_mock.assert_called_once_with()
