from __future__ import annotations

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

    def _patch_time_and_tokens(self):
        return (
            mock.patch("apps.epos_qbo.views.timezone.now", return_value=self.fixed_now),
            mock.patch("apps.epos_qbo.views.load_tokens", return_value=self._token_payload()),
        )

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
        self.assertIn("SALES SYNCED (7D)", html)
        self.assertNotIn("Records (24h)", html)
        self.assertIn("vs last week", html)
