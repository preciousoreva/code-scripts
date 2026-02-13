from __future__ import annotations

from datetime import date

from django.test import SimpleTestCase

from apps.epos_qbo.models import RunJob
from apps.epos_qbo.services.job_runner import build_command


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
            }
        )
        self.assertIn("run_all_companies.py", " ".join(command))
        self.assertIn("--from-date", command)
        self.assertIn("2026-02-01", command)
        self.assertIn("--to-date", command)
        self.assertIn("2026-02-05", command)
        self.assertIn("--skip-download", command)
