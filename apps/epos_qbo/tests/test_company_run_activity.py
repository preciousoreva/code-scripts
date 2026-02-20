from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta
from unittest import mock

from django.contrib.auth.models import User
from django.test import TestCase
from django.urls import reverse
from django.utils import timezone

from apps.epos_qbo.models import CompanyConfigRecord, RunArtifact, RunJob


class CompanyRunActivityTests(TestCase):
    def setUp(self):
        self.fixed_now = timezone.make_aware(datetime(2026, 2, 14, 12, 0, 0))
        self.user = User.objects.create_user(username="operator", password="pw12345")
        self.company = CompanyConfigRecord.objects.create(
            company_key="company_a",
            display_name="Company A",
            config_json={
                "company_key": "company_a",
                "display_name": "Company A",
                "qbo": {"realm_id": "123"},
                "epos": {"username_env_key": "EPOS_USERNAME_A", "password_env_key": "EPOS_PASSWORD_A"},
            },
        )
        self.client.login(username="operator", password="pw12345")

    def _token_payload(self) -> dict:
        now_ts = int(self.fixed_now.timestamp())
        return {
            "access_token": "access-token",
            "refresh_token": "refresh-token",
            "expires_at": now_ts + 3600,
            "refresh_expires_at": now_ts + (30 * 86400),
            "updated_at": now_ts,
            "environment": "production",
        }

    @contextmanager
    def _patch_time_and_tokens(self):
        with (
            mock.patch("apps.epos_qbo.business_date.timezone.now", return_value=self.fixed_now),
            mock.patch("apps.epos_qbo.views.timezone.now", return_value=self.fixed_now),
            mock.patch("apps.epos_qbo.views.load_tokens", return_value=self._token_payload()),
        ):
            yield

    def test_companies_list_uses_run_linked_via_artifact(self):
        run = RunJob.objects.create(
            scope=RunJob.SCOPE_ALL,
            status=RunJob.STATUS_SUCCEEDED,
            started_at=self.fixed_now - timedelta(hours=5),
            finished_at=self.fixed_now - timedelta(hours=4, minutes=45),
        )
        RunArtifact.objects.create(
            run_job=run,
            company_key=self.company.company_key,
            target_date=(self.fixed_now - timedelta(days=1)).date(),
            processed_at=self.fixed_now - timedelta(hours=4, minutes=44),
            source_path="/tmp/company_a_linked.json",
            source_hash="hash-company-a-linked",
            rows_kept=100,
        )

        with self._patch_time_and_tokens():
            response = self.client.get(reverse("epos_qbo:companies-list"))

        self.assertEqual(response.status_code, 200)
        company_data = next(
            item for item in response.context["companies_data"] if item["company"].company_key == self.company.company_key
        )
        self.assertIsNotNone(company_data["latest_run"])
        self.assertEqual(company_data["latest_run"].id, run.id)
        self.assertNotEqual(company_data["last_run_display"], "Never run")

    def test_companies_list_records_today_uses_uploaded_counts_with_day_dedupe(self):
        """Receipts uploaded (runs completed today) uses calendar day (midnight to now); only today's artifacts count."""
        same_day = self.fixed_now.date()
        prev_day = (self.fixed_now - timedelta(days=1)).date()
        old_day = (self.fixed_now - timedelta(days=2)).date()

        # Same day: keep latest succeeded artifact only.
        run_success_old = RunJob.objects.create(
            scope=RunJob.SCOPE_SINGLE,
            company_key=self.company.company_key,
            status=RunJob.STATUS_SUCCEEDED,
            started_at=self.fixed_now - timedelta(hours=4),
        )
        RunArtifact.objects.create(
            run_job=run_success_old,
            company_key=self.company.company_key,
            target_date=same_day,
            processed_at=self.fixed_now - timedelta(hours=4),
            source_path="/tmp/company_a_day_old_success.json",
            source_hash="hash-day-old-success",
            upload_stats_json={"uploaded": 8},
        )
        run_success_new = RunJob.objects.create(
            scope=RunJob.SCOPE_SINGLE,
            company_key=self.company.company_key,
            status=RunJob.STATUS_SUCCEEDED,
            started_at=self.fixed_now - timedelta(hours=2),
        )
        RunArtifact.objects.create(
            run_job=run_success_new,
            company_key=self.company.company_key,
            target_date=same_day,
            processed_at=self.fixed_now - timedelta(hours=2),
            source_path="/tmp/company_a_day_new_success.json",
            source_hash="hash-day-new-success",
            upload_stats_json={"uploaded": 10},
        )
        run_failed = RunJob.objects.create(
            scope=RunJob.SCOPE_SINGLE,
            company_key=self.company.company_key,
            status=RunJob.STATUS_FAILED,
            started_at=self.fixed_now - timedelta(hours=1),
        )
        RunArtifact.objects.create(
            run_job=run_failed,
            company_key=self.company.company_key,
            target_date=same_day,
            processed_at=self.fixed_now - timedelta(hours=1),
            source_path="/tmp/company_a_day_failed.json",
            source_hash="hash-day-failed",
            upload_stats_json={"uploaded": 999},
        )
        RunArtifact.objects.create(
            company_key=self.company.company_key,
            target_date=same_day,
            processed_at=self.fixed_now - timedelta(minutes=30),
            source_path="/tmp/company_a_day_unlinked.json",
            source_hash="hash-day-unlinked",
            upload_stats_json={"uploaded": 30},
        )

        # Previous day: no succeeded linked artifact; include latest unlinked fallback.
        run_prev_failed = RunJob.objects.create(
            scope=RunJob.SCOPE_SINGLE,
            company_key=self.company.company_key,
            status=RunJob.STATUS_FAILED,
            started_at=self.fixed_now - timedelta(hours=20),
        )
        RunArtifact.objects.create(
            run_job=run_prev_failed,
            company_key=self.company.company_key,
            target_date=prev_day,
            processed_at=self.fixed_now - timedelta(hours=20),
            source_path="/tmp/company_a_prev_failed.json",
            source_hash="hash-prev-failed",
            upload_stats_json={"uploaded": 50},
        )
        RunArtifact.objects.create(
            company_key=self.company.company_key,
            target_date=prev_day,
            processed_at=self.fixed_now - timedelta(hours=19),
            source_path="/tmp/company_a_prev_unlinked.json",
            source_hash="hash-prev-unlinked",
            upload_stats_json={"created": 5},
        )

        # Outside today: ignored.
        run_old_success = RunJob.objects.create(
            scope=RunJob.SCOPE_SINGLE,
            company_key=self.company.company_key,
            status=RunJob.STATUS_SUCCEEDED,
            started_at=self.fixed_now - timedelta(hours=30),
        )
        RunArtifact.objects.create(
            run_job=run_old_success,
            company_key=self.company.company_key,
            target_date=old_day,
            processed_at=self.fixed_now - timedelta(hours=30),
            source_path="/tmp/company_a_old_success.json",
            source_hash="hash-old-success",
            upload_stats_json={"uploaded": 500},
        )

        with self._patch_time_and_tokens():
            response = self.client.get(reverse("epos_qbo:companies-list"))

        self.assertEqual(response.status_code, 200)
        company_data = next(
            item for item in response.context["companies_data"] if item["company"].company_key == self.company.company_key
        )
        # Today only: same-day bucket; latest succeeded artifact has uploaded=10.
        self.assertEqual(company_data["records_24h"], 10)

    def test_company_detail_recent_runs_includes_all_companies_run_when_linked(self):
        run = RunJob.objects.create(
            scope=RunJob.SCOPE_ALL,
            status=RunJob.STATUS_SUCCEEDED,
            started_at=self.fixed_now - timedelta(hours=3),
            finished_at=self.fixed_now - timedelta(hours=2, minutes=30),
        )
        RunArtifact.objects.create(
            run_job=run,
            company_key=self.company.company_key,
            target_date=(self.fixed_now - timedelta(days=1)).date(),
            processed_at=self.fixed_now - timedelta(hours=2, minutes=29),
            source_path="/tmp/company_a_recent_run.json",
            source_hash="hash-company-a-recent-run",
            rows_kept=70,
        )

        with self._patch_time_and_tokens():
            response = self.client.get(
                reverse("epos_qbo:company-detail", kwargs={"company_key": self.company.company_key})
            )

        self.assertEqual(response.status_code, 200)
        recent_runs = list(response.context["recent_runs"])
        self.assertEqual(len(recent_runs), 1)
        self.assertEqual(recent_runs[0].id, run.id)
        self.assertNotEqual(response.context["company_data"]["last_run_display"], "Never run")

    def test_company_detail_latest_run_ordered_by_finished_at_not_started_at(self):
        """Company detail 'latest run' must match overview/companies list: order by finished_at then started_at."""
        # Run A: started earlier, finished at 18:00
        run_a = RunJob.objects.create(
            scope=RunJob.SCOPE_SINGLE,
            company_key=self.company.company_key,
            status=RunJob.STATUS_SUCCEEDED,
            started_at=self.fixed_now - timedelta(hours=4),
            finished_at=self.fixed_now - timedelta(hours=2),
        )
        RunArtifact.objects.create(
            run_job=run_a,
            company_key=self.company.company_key,
            target_date=(self.fixed_now - timedelta(days=1)).date(),
            processed_at=self.fixed_now - timedelta(hours=2),
            source_path="/tmp/company_a_run_a.json",
            source_hash="hash-run-a",
            rows_kept=10,
        )
        # Run B: started later (1h ago) but finished earlier (3h ago)
        run_b = RunJob.objects.create(
            scope=RunJob.SCOPE_SINGLE,
            company_key=self.company.company_key,
            status=RunJob.STATUS_SUCCEEDED,
            started_at=self.fixed_now - timedelta(hours=1),
            finished_at=self.fixed_now - timedelta(hours=3),
        )
        RunArtifact.objects.create(
            run_job=run_b,
            company_key=self.company.company_key,
            target_date=(self.fixed_now - timedelta(days=1)).date(),
            processed_at=self.fixed_now - timedelta(hours=3),
            source_path="/tmp/company_a_run_b.json",
            source_hash="hash-run-b",
            rows_kept=20,
        )

        with self._patch_time_and_tokens():
            response = self.client.get(
                reverse("epos_qbo:company-detail", kwargs={"company_key": self.company.company_key})
            )

        self.assertEqual(response.status_code, 200)
        # Latest by finished_at is run_a (finished 2h ago); run_b finished 3h ago. So run_a must be first.
        company_data = response.context["company_data"]
        self.assertIsNotNone(company_data["latest_run"])
        self.assertEqual(company_data["latest_run"].id, run_a.id)
        recent_runs = list(response.context["recent_runs"])
        self.assertEqual(len(recent_runs), 2)
        self.assertEqual(recent_runs[0].id, run_a.id)
        self.assertEqual(recent_runs[1].id, run_b.id)

    def test_company_detail_last_run_falls_back_to_artifact_time_without_runjob(self):
        RunArtifact.objects.create(
            company_key=self.company.company_key,
            target_date=(self.fixed_now - timedelta(days=1)).date(),
            processed_at=self.fixed_now - timedelta(hours=2),
            source_path="/tmp/company_a_artifact_only.json",
            source_hash="hash-company-a-artifact-only",
            rows_kept=55,
        )

        with self._patch_time_and_tokens():
            response = self.client.get(
                reverse("epos_qbo:company-detail", kwargs={"company_key": self.company.company_key})
            )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(len(response.context["recent_runs"]), 0)
        self.assertNotEqual(response.context["company_data"]["last_run_display"], "Never run")

    def test_company_detail_ignores_mismatched_single_company_run_link(self):
        foreign_run = RunJob.objects.create(
            scope=RunJob.SCOPE_SINGLE,
            company_key="company_b",
            status=RunJob.STATUS_FAILED,
            started_at=self.fixed_now - timedelta(hours=4),
        )
        # Corrupted legacy link: company_a artifact points to company_b single-company run.
        RunArtifact.objects.create(
            run_job=foreign_run,
            company_key=self.company.company_key,
            target_date=(self.fixed_now - timedelta(days=1)).date(),
            processed_at=self.fixed_now - timedelta(hours=3, minutes=59),
            source_path="/tmp/company_a_bad_link.json",
            source_hash="hash-company-a-bad-link",
            rows_kept=10,
        )

        with self._patch_time_and_tokens():
            response = self.client.get(
                reverse("epos_qbo:company-detail", kwargs={"company_key": self.company.company_key})
            )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(len(response.context["recent_runs"]), 0)

    def test_company_detail_replaces_records_kpi_with_sales_synced_trend(self):
        prev_run = RunJob.objects.create(
            scope=RunJob.SCOPE_SINGLE,
            company_key=self.company.company_key,
            status=RunJob.STATUS_SUCCEEDED,
            started_at=self.fixed_now - timedelta(days=10),
        )
        RunArtifact.objects.create(
            run_job=prev_run,
            company_key=self.company.company_key,
            target_date=(self.fixed_now - timedelta(days=10)).date(),
            processed_at=self.fixed_now - timedelta(days=10),
            source_path="/tmp/company_a_prev_sales.json",
            source_hash="hash-company-a-prev-sales",
            upload_stats_json={"total_amount": 50000},
        )
        this_run = RunJob.objects.create(
            scope=RunJob.SCOPE_SINGLE,
            company_key=self.company.company_key,
            status=RunJob.STATUS_SUCCEEDED,
            started_at=self.fixed_now - timedelta(days=1),
        )
        RunArtifact.objects.create(
            run_job=this_run,
            company_key=self.company.company_key,
            target_date=(self.fixed_now - timedelta(days=1)).date(),
            processed_at=self.fixed_now - timedelta(days=1),
            source_path="/tmp/company_a_this_sales.json",
            source_hash="hash-company-a-this-sales",
            upload_stats_json={"total_amount": 75000},
        )

        with self._patch_time_and_tokens():
            response = self.client.get(
                reverse("epos_qbo:company-detail", kwargs={"company_key": self.company.company_key})
            )

        self.assertEqual(response.status_code, 200)
        html = response.content.decode("utf-8")
        self.assertIn("Sales Synced (last run)", html)
        self.assertNotIn("Records (24h)", html)
        # Last successful run is this_run (target 1 day ago, total 75000)
        self.assertIn("75,000", html)
        self.assertIn("Target:", html)

    def test_warning_filter_excludes_running_when_health_is_otherwise_healthy(self):
        run = RunJob.objects.create(
            scope=RunJob.SCOPE_SINGLE,
            company_key=self.company.company_key,
            status=RunJob.STATUS_RUNNING,
            started_at=self.fixed_now - timedelta(minutes=30),
        )
        RunArtifact.objects.create(
            run_job=run,
            company_key=self.company.company_key,
            target_date=(self.fixed_now - timedelta(days=1)).date(),
            processed_at=self.fixed_now - timedelta(minutes=20),
            source_path="/tmp/company_a_running_filter.json",
            source_hash="hash-company-a-running-filter",
            rows_kept=1,
        )

        with self._patch_time_and_tokens():
            response = self.client.get(
                reverse("epos_qbo:companies-list"),
                {"filter": "warning"},
            )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(len(response.context["companies_data"]), 0)

        with self._patch_time_and_tokens():
            response_all = self.client.get(reverse("epos_qbo:companies-list"))
        company_data = response_all.context["companies_data"][0]
        self.assertEqual(company_data["status"]["level"], "healthy")
        self.assertEqual(company_data["status"]["canonical_level"], "healthy")
        self.assertEqual(company_data["health"]["run_activity"], "running")
        self.assertEqual(company_data["run_activity"]["state"], "running")

    def test_unknown_filter_includes_never_synced_companies(self):
        with self._patch_time_and_tokens():
            response = self.client.get(
                reverse("epos_qbo:companies-list"),
                {"filter": "unknown"},
            )
        self.assertEqual(response.status_code, 200)
        self.assertEqual(len(response.context["companies_data"]), 1)
        company_data = response.context["companies_data"][0]
        self.assertEqual(company_data["status"]["level"], "unknown")
        self.assertEqual(company_data["status"]["canonical_level"], "unknown")
