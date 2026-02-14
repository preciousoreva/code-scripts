from __future__ import annotations

from datetime import datetime, timedelta
from unittest import mock

from django.contrib.auth.models import User
from django.test import TestCase
from django.urls import reverse
from django.utils import timezone

from apps.epos_qbo import views
from apps.epos_qbo.models import CompanyConfigRecord, RunJob


class OverviewUIContextTests(TestCase):
    def setUp(self):
        self.fixed_now = timezone.make_aware(datetime(2026, 2, 13, 12, 0, 0))
        self.company = CompanyConfigRecord.objects.create(
            company_key="company_a",
            display_name="Company A",
            config_json={
                "company_key": "company_a",
                "display_name": "Company A",
                "qbo": {"realm_id": "123456789"},
                "epos": {"username_env_key": "EPOS_USERNAME_A", "password_env_key": "EPOS_PASSWORD_A"},
            },
        )

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

    def test_daily_success_marks_no_run_days_as_none(self):
        run = RunJob.objects.create(
            scope=RunJob.SCOPE_SINGLE,
            company_key=self.company.company_key,
            status=RunJob.STATUS_SUCCEEDED,
        )
        RunJob.objects.filter(id=run.id).update(created_at=self.fixed_now - timedelta(days=1))

        with (
            mock.patch("apps.epos_qbo.views.timezone.now", return_value=self.fixed_now),
            mock.patch("apps.epos_qbo.views.load_tokens", return_value=self._token_payload()),
        ):
            context = views._overview_context()

        self.assertEqual(len(context["daily_success"]), 7)
        self.assertTrue(any(point["runs_total"] == 0 and point["rate"] is None for point in context["daily_success"]))

    def test_failure_sources_nonzero_excludes_zero_categories(self):
        run = RunJob.objects.create(
            scope=RunJob.SCOPE_SINGLE,
            company_key=self.company.company_key,
            status=RunJob.STATUS_FAILED,
            failure_reason="network timeout during request",
        )
        RunJob.objects.filter(id=run.id).update(created_at=self.fixed_now - timedelta(days=2))

        with (
            mock.patch("apps.epos_qbo.views.timezone.now", return_value=self.fixed_now),
            mock.patch("apps.epos_qbo.views.load_tokens", return_value=self._token_payload()),
        ):
            context = views._overview_context()

        self.assertGreater(context["failure_sources_total_60d"], 0)
        self.assertTrue(context["failure_sources_nonzero"])
        self.assertTrue(all(source["count"] > 0 for source in context["failure_sources_nonzero"]))
        self.assertNotIn("Mapping", [source["label"] for source in context["failure_sources_nonzero"]])

    def test_failure_sources_empty_when_no_failures(self):
        run = RunJob.objects.create(
            scope=RunJob.SCOPE_SINGLE,
            company_key=self.company.company_key,
            status=RunJob.STATUS_SUCCEEDED,
        )
        RunJob.objects.filter(id=run.id).update(created_at=self.fixed_now - timedelta(days=1))

        with (
            mock.patch("apps.epos_qbo.views.timezone.now", return_value=self.fixed_now),
            mock.patch("apps.epos_qbo.views.load_tokens", return_value=self._token_payload()),
        ):
            context = views._overview_context()

        self.assertEqual(context["failure_sources_total_60d"], 0)
        self.assertEqual(context["failure_sources_nonzero"], [])


class OverviewUITemplateTests(TestCase):
    def setUp(self):
        self.fixed_now = timezone.make_aware(datetime(2026, 2, 13, 12, 0, 0))
        self.user = User.objects.create_user(username="operator", password="pw12345")
        CompanyConfigRecord.objects.create(
            company_key="company_a",
            display_name="Company A",
            config_json={
                "company_key": "company_a",
                "display_name": "Company A",
                "qbo": {"realm_id": "123456789"},
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

    def test_overview_renders_search_and_overview_script(self):
        with (
            mock.patch("apps.epos_qbo.views.timezone.now", return_value=self.fixed_now),
            mock.patch("apps.epos_qbo.views.load_tokens", return_value=self._token_payload()),
        ):
            response = self.client.get(reverse("epos_qbo:overview"))

        self.assertEqual(response.status_code, 200)
        html = response.content.decode("utf-8")
        self.assertIn('id="overview-company-filter"', html)
        self.assertIn("js/overview.js", html)

    def test_live_log_appears_before_run_reliability(self):
        with (
            mock.patch("apps.epos_qbo.views.timezone.now", return_value=self.fixed_now),
            mock.patch("apps.epos_qbo.views.load_tokens", return_value=self._token_payload()),
        ):
            response = self.client.get(reverse("epos_qbo:overview"))

        html = response.content.decode("utf-8")
        self.assertLess(html.find("Live Log"), html.find("Run Reliability"))

    def test_overview_shows_no_runs_and_no_failures_copy(self):
        with (
            mock.patch("apps.epos_qbo.views.timezone.now", return_value=self.fixed_now),
            mock.patch("apps.epos_qbo.views.load_tokens", return_value=self._token_payload()),
        ):
            response = self.client.get(reverse("epos_qbo:overview"))

        html = response.content.decode("utf-8")
        self.assertIn("No runs", html)
        self.assertIn("No failures in last 60 days.", html)

    def test_live_log_uses_company_and_run_label_not_uuid(self):
        run = RunJob.objects.create(
            scope=RunJob.SCOPE_SINGLE,
            company_key="company_a",
            status=RunJob.STATUS_SUCCEEDED,
        )
        RunJob.objects.filter(id=run.id).update(created_at=self.fixed_now - timedelta(minutes=10))
        run.refresh_from_db()

        with (
            mock.patch("apps.epos_qbo.views.timezone.now", return_value=self.fixed_now),
            mock.patch("apps.epos_qbo.views.load_tokens", return_value=self._token_payload()),
        ):
            response = self.client.get(reverse("epos_qbo:overview"))

        html = response.content.decode("utf-8")
        self.assertIn(f"Company A: Run {run.display_label} succeeded", html)
        self.assertNotIn(str(run.id), html)
