from __future__ import annotations

from datetime import datetime, timedelta
from unittest import mock

from django.contrib.auth.models import Permission
from django.contrib.auth.models import User
from django.test import TestCase
from django.urls import reverse
from django.utils import timezone

from apps.epos_qbo import views
from apps.epos_qbo.models import CompanyConfigRecord, RunArtifact, RunJob


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

    def test_company_last_run_falls_back_to_latest_artifact_time(self):
        RunArtifact.objects.create(
            company_key=self.company.company_key,
            target_date=(self.fixed_now - timedelta(days=1)).date(),
            processed_at=self.fixed_now - timedelta(minutes=10),
            source_path="/tmp/company_a_last_transform.json",
            source_hash="artifact-hash-company-a",
            rows_kept=42,
        )

        with (
            mock.patch("apps.epos_qbo.views.timezone.now", return_value=self.fixed_now),
            mock.patch("apps.epos_qbo.views.load_tokens", return_value=self._token_payload()),
        ):
            context = views._overview_context()

        company_row = next(item for item in context["companies"] if item["company_key"] == self.company.company_key)
        self.assertIsNotNone(company_row["last_run"])

    def test_system_health_severity_classification(self):
        self.assertEqual(views._classify_system_health(2, 0, 0)["label"], "All Operational")
        self.assertEqual(views._classify_system_health(1, 1, 0)["label"], "Warning")
        self.assertEqual(views._classify_system_health(1, 0, 1)["label"], "Degraded")

    def test_overview_sales_24h_uses_reconcile_first_and_computes_trend(self):
        prev_run = RunJob.objects.create(
            scope=RunJob.SCOPE_SINGLE,
            company_key=self.company.company_key,
            status=RunJob.STATUS_SUCCEEDED,
        )
        RunArtifact.objects.create(
            run_job=prev_run,
            company_key=self.company.company_key,
            target_date=(self.fixed_now - timedelta(days=1)).date(),
            processed_at=self.fixed_now - timedelta(hours=30),
            source_path="/tmp/company_a_prev_24h.json",
            source_hash="hash-prev-24h",
            upload_stats_json={"total_amount": 999},
            reconcile_epos_total=100.0,
        )
        current_run = RunJob.objects.create(
            scope=RunJob.SCOPE_SINGLE,
            company_key=self.company.company_key,
            status=RunJob.STATUS_SUCCEEDED,
        )
        RunArtifact.objects.create(
            run_job=current_run,
            company_key=self.company.company_key,
            target_date=self.fixed_now.date(),
            processed_at=self.fixed_now - timedelta(hours=2),
            source_path="/tmp/company_a_curr_24h.json",
            source_hash="hash-curr-24h",
            upload_stats_json={"total_amount": 500},
            reconcile_epos_total=200.0,
        )

        with (
            mock.patch("apps.epos_qbo.views.timezone.now", return_value=self.fixed_now),
            mock.patch("apps.epos_qbo.views.load_tokens", return_value=self._token_payload()),
        ):
            context = views._overview_context()

        kpis = context["kpis"]
        self.assertEqual(str(kpis["sales_24h_total"]), "200.0")
        self.assertEqual(str(kpis["sales_prev_24h_total"]), "100.0")
        self.assertEqual(kpis["sales_24h_trend_dir"], "up")
        self.assertEqual(kpis["sales_24h_trend_text"], "↑ 100.0% increase vs yesterday")

    def test_overview_context_includes_avg_runtime_24h(self):
        completed = RunJob.objects.create(
            scope=RunJob.SCOPE_SINGLE,
            company_key=self.company.company_key,
            status=RunJob.STATUS_SUCCEEDED,
            started_at=self.fixed_now - timedelta(minutes=30),
            finished_at=self.fixed_now - timedelta(minutes=10),
        )
        RunJob.objects.filter(id=completed.id).update(created_at=self.fixed_now - timedelta(hours=1))
        with (
            mock.patch("apps.epos_qbo.views.timezone.now", return_value=self.fixed_now),
            mock.patch("apps.epos_qbo.views.load_tokens", return_value=self._token_payload()),
        ):
            context = views._overview_context()
        self.assertGreaterEqual(context["kpis"]["avg_runtime_24h_seconds"], 0)
        display = context["kpis"]["avg_runtime_24h_display"]
        self.assertTrue(any(unit in display for unit in ("s", "m", "h", "d")))
        self.assertIn("vs yesterday", context["kpis"]["avg_runtime_today_trend_text"])

    def test_overview_sales_24h_shows_no_monetary_totals_when_artifacts_have_no_amount(self):
        RunArtifact.objects.create(
            company_key=self.company.company_key,
            target_date=self.fixed_now.date(),
            processed_at=self.fixed_now - timedelta(hours=3),
            source_path="/tmp/company_a_no_amount.json",
            source_hash="hash-no-amount",
            upload_stats_json={"uploaded": 5},
        )
        with (
            mock.patch("apps.epos_qbo.views.timezone.now", return_value=self.fixed_now),
            mock.patch("apps.epos_qbo.views.load_tokens", return_value=self._token_payload()),
        ):
            context = views._overview_context()
        self.assertEqual(context["kpis"]["sales_24h_trend_text"], "No monetary totals found")

    def test_overview_avg_runtime_today_uses_faster_slower_wording(self):
        y_run = RunJob.objects.create(
            scope=RunJob.SCOPE_SINGLE,
            company_key=self.company.company_key,
            status=RunJob.STATUS_SUCCEEDED,
            started_at=self.fixed_now - timedelta(days=1, minutes=40),
            finished_at=self.fixed_now - timedelta(days=1, minutes=10),
        )
        RunJob.objects.filter(id=y_run.id).update(created_at=self.fixed_now - timedelta(days=1, minutes=41))
        t_run = RunJob.objects.create(
            scope=RunJob.SCOPE_SINGLE,
            company_key=self.company.company_key,
            status=RunJob.STATUS_SUCCEEDED,
            started_at=self.fixed_now - timedelta(minutes=20),
            finished_at=self.fixed_now - timedelta(minutes=10),
        )
        RunJob.objects.filter(id=t_run.id).update(created_at=self.fixed_now - timedelta(minutes=21))

        with (
            mock.patch("apps.epos_qbo.views.timezone.now", return_value=self.fixed_now),
            mock.patch("apps.epos_qbo.views.load_tokens", return_value=self._token_payload()),
        ):
            context = views._overview_context()
        self.assertIn("faster vs yesterday", context["kpis"]["avg_runtime_today_trend_text"])

    def test_overview_avg_runtime_today_uses_successful_runs_only(self):
        succeeded = RunJob.objects.create(
            scope=RunJob.SCOPE_SINGLE,
            company_key=self.company.company_key,
            status=RunJob.STATUS_SUCCEEDED,
            started_at=self.fixed_now - timedelta(minutes=20),
            finished_at=self.fixed_now - timedelta(minutes=10),
        )
        RunJob.objects.filter(id=succeeded.id).update(created_at=self.fixed_now - timedelta(minutes=21))
        failed = RunJob.objects.create(
            scope=RunJob.SCOPE_SINGLE,
            company_key=self.company.company_key,
            status=RunJob.STATUS_FAILED,
            started_at=self.fixed_now - timedelta(minutes=90),
            finished_at=self.fixed_now - timedelta(minutes=10),
        )
        RunJob.objects.filter(id=failed.id).update(created_at=self.fixed_now - timedelta(minutes=91))

        with (
            mock.patch("apps.epos_qbo.views.timezone.now", return_value=self.fixed_now),
            mock.patch("apps.epos_qbo.views.load_tokens", return_value=self._token_payload()),
        ):
            context = views._overview_context()
        # 10 minutes from the succeeded run only.
        self.assertEqual(context["kpis"]["avg_runtime_today_seconds"], 600)

    def test_overview_sales_24h_uses_decrease_wording_for_negative_delta(self):
        prev_run = RunJob.objects.create(
            scope=RunJob.SCOPE_SINGLE,
            company_key=self.company.company_key,
            status=RunJob.STATUS_SUCCEEDED,
        )
        RunArtifact.objects.create(
            run_job=prev_run,
            company_key=self.company.company_key,
            target_date=(self.fixed_now - timedelta(days=1)).date(),
            processed_at=self.fixed_now - timedelta(hours=30),
            source_path="/tmp/company_a_prev_drop.json",
            source_hash="hash-prev-drop",
            reconcile_epos_total=200.0,
        )
        current_run = RunJob.objects.create(
            scope=RunJob.SCOPE_SINGLE,
            company_key=self.company.company_key,
            status=RunJob.STATUS_SUCCEEDED,
        )
        RunArtifact.objects.create(
            run_job=current_run,
            company_key=self.company.company_key,
            target_date=self.fixed_now.date(),
            processed_at=self.fixed_now - timedelta(hours=2),
            source_path="/tmp/company_a_curr_drop.json",
            source_hash="hash-curr-drop",
            reconcile_epos_total=100.0,
        )
        with (
            mock.patch("apps.epos_qbo.views.timezone.now", return_value=self.fixed_now),
            mock.patch("apps.epos_qbo.views.load_tokens", return_value=self._token_payload()),
        ):
            context = views._overview_context()
        self.assertEqual(context["kpis"]["sales_24h_trend_text"], "↓ 50.0% decrease vs yesterday")


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

    def test_overview_does_not_render_run_reliability_panel(self):
        with (
            mock.patch("apps.epos_qbo.views.timezone.now", return_value=self.fixed_now),
            mock.patch("apps.epos_qbo.views.load_tokens", return_value=self._token_payload()),
        ):
            response = self.client.get(reverse("epos_qbo:overview"))

        html = response.content.decode("utf-8")
        self.assertNotIn("Run Reliability", html)
        self.assertNotIn("Failure Sources (Last 60 Days)", html)

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

    def test_overview_panels_endpoint_renders_fragment(self):
        with (
            mock.patch("apps.epos_qbo.views.timezone.now", return_value=self.fixed_now),
            mock.patch("apps.epos_qbo.views.load_tokens", return_value=self._token_payload()),
        ):
            response = self.client.get(reverse("epos_qbo:overview-panels"))

        self.assertEqual(response.status_code, 200)
        html = response.content.decode("utf-8")
        self.assertIn('id="overview-company-filter"', html)
        self.assertIn("Live Log", html)
        self.assertNotIn("Run Reliability", html)

    def test_overview_panels_respects_revenue_period_param(self):
        with (
            mock.patch("apps.epos_qbo.views.timezone.now", return_value=self.fixed_now),
            mock.patch("apps.epos_qbo.views.load_tokens", return_value=self._token_payload()),
        ):
            response = self.client.get(reverse("epos_qbo:overview-panels"), {"revenue_period": "90d"})

        self.assertEqual(response.status_code, 200)
        html = response.content.decode("utf-8")
        self.assertIn('<option value="90d" selected>', html)

    def test_overview_topbar_uses_quick_sync_label(self):
        perm = Permission.objects.get(codename="can_trigger_runs")
        self.user.user_permissions.add(perm)
        with (
            mock.patch("apps.epos_qbo.views.timezone.now", return_value=self.fixed_now),
            mock.patch("apps.epos_qbo.views.load_tokens", return_value=self._token_payload()),
        ):
            response = self.client.get(reverse("epos_qbo:overview"))

        self.assertEqual(response.status_code, 200)
        html = response.content.decode("utf-8")
        self.assertIn("Quick Sync", html)
        self.assertNotIn("Manual Sync", html)

    def test_overview_renders_consolidated_kpi_row(self):
        run_prev = RunJob.objects.create(
            scope=RunJob.SCOPE_SINGLE,
            company_key="company_a",
            status=RunJob.STATUS_SUCCEEDED,
        )
        RunArtifact.objects.create(
            run_job=run_prev,
            company_key="company_a",
            target_date=(self.fixed_now - timedelta(days=1)).date(),
            processed_at=self.fixed_now - timedelta(hours=30),
            source_path="/tmp/company_a_prev_kpi.json",
            source_hash="hash-company-a-prev-kpi",
            reconcile_epos_total=100.0,
        )
        run_now = RunJob.objects.create(
            scope=RunJob.SCOPE_SINGLE,
            company_key="company_a",
            status=RunJob.STATUS_SUCCEEDED,
        )
        RunArtifact.objects.create(
            run_job=run_now,
            company_key="company_a",
            target_date=self.fixed_now.date(),
            processed_at=self.fixed_now - timedelta(hours=4),
            source_path="/tmp/company_a_now_kpi.json",
            source_hash="hash-company-a-now-kpi",
            reconcile_epos_total=140.0,
        )

        with (
            mock.patch("apps.epos_qbo.views.timezone.now", return_value=self.fixed_now),
            mock.patch("apps.epos_qbo.views.load_tokens", return_value=self._token_payload()),
        ):
            response = self.client.get(reverse("epos_qbo:overview"))

        self.assertEqual(response.status_code, 200)
        html = response.content.decode("utf-8")
        self.assertIn("System Health", html)
        self.assertIn("Sales Synced (Today)", html)
        self.assertIn("Run Success (Today)", html)
        self.assertIn("Avg Runtime (Today)", html)
        self.assertNotIn("Healthy Companies", html)
        self.assertNotIn("Critical Errors", html)
        self.assertNotIn("Records Synced (24h)", html)
        self.assertNotIn("Active Runs", html)
