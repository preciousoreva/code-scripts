from __future__ import annotations

from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import mock

from django.test import TestCase

from apps.dashboard.management.commands.run_scheduled_all_companies import (
    Command,
    run_pipeline_subprocess,
)
from apps.epos_qbo.models import RunJob


class ScheduledRunCommandTests(TestCase):
    def test_run_pipeline_subprocess_includes_target_date_arg(self):
        captured = {}

        class DummyProc:
            returncode = 0

            def wait(self):
                return 0

        def fake_popen(cmd, **kwargs):
            captured["cmd"] = cmd
            return DummyProc()

        with TemporaryDirectory() as temp_dir:
            with mock.patch(
                "apps.dashboard.management.commands.run_scheduled_all_companies.subprocess.Popen",
                side_effect=fake_popen,
            ):
                exit_code = run_pipeline_subprocess(
                    repo_root=Path(temp_dir),
                    log_path=Path(temp_dir) / "run.log",
                    parallel=2,
                    stagger_seconds=3,
                    continue_on_failure=False,
                    target_date="2026-02-15",
                )

        self.assertEqual(exit_code, 0)
        self.assertIn("--target-date", captured["cmd"])
        self.assertIn("2026-02-15", captured["cmd"])

    def test_create_run_job_sets_target_date(self):
        command = Command()
        with TemporaryDirectory() as temp_dir:
            job = command._create_run_job_if_available(  # noqa: SLF001
                log_path=Path(temp_dir) / "run.log",
                parallel=2,
                stagger_seconds=3,
                continue_on_failure=False,
                target_date="2026-02-15",
            )

        self.assertIsNotNone(job)
        assert job is not None
        self.assertEqual(job.target_date.isoformat(), "2026-02-15")
        self.assertEqual(job.scope, RunJob.SCOPE_ALL)
