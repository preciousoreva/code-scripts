from __future__ import annotations

from datetime import datetime, timedelta
from decimal import Decimal
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
            mock.patch("apps.epos_qbo.business_date.timezone.now", return_value=self.fixed_now),
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

    def test_system_health_breakdown_compacts_zero_buckets(self):
        self.assertEqual(
            views._format_system_health_breakdown(2, 0, 0, 0),
            "2 healthy",
        )
        self.assertEqual(
            views._format_system_health_breakdown(1, 0, 1, 0),
            "1 healthy • 1 critical",
        )
        self.assertEqual(
            views._format_system_health_breakdown(1, 1, 0, 2),
            "1 healthy • 1 warning • 2 unknown",
        )

    def test_company_summary_visibility_rules(self):
        self.assertFalse(views._should_show_company_summary("healthy", "Last run succeeded.", []))
        self.assertFalse(views._should_show_company_summary("unknown", "No successful sync yet.", []))
        self.assertFalse(
            views._should_show_company_summary(
                "warning",
                "Reconciliation mismatch above threshold.",
                ["Reconciliation mismatch above threshold."],
            )
        )
        self.assertTrue(views._should_show_company_summary("critical", "Latest run failed.", ["Latest run failed"]))

    def test_overview_sales_24h_uses_reconcile_first_and_computes_trend(self):
        prev_run = RunJob.objects.create(
            scope=RunJob.SCOPE_SINGLE,
            company_key=self.company.company_key,
            status=RunJob.STATUS_SUCCEEDED,
        )
        # Prev target date (Feb 11): total 100
        RunArtifact.objects.create(
            run_job=prev_run,
            company_key=self.company.company_key,
            target_date=(self.fixed_now - timedelta(days=2)).date(),
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
        # This target date (Feb 12 / yesterday): total 200
        RunArtifact.objects.create(
            run_job=current_run,
            company_key=self.company.company_key,
            target_date=(self.fixed_now - timedelta(days=1)).date(),
            processed_at=self.fixed_now - timedelta(hours=2),
            source_path="/tmp/company_a_curr_24h.json",
            source_hash="hash-curr-24h",
            upload_stats_json={"total_amount": 500},
            reconcile_epos_total=200.0,
        )

        with (
            mock.patch("apps.epos_qbo.business_date.timezone.now", return_value=self.fixed_now),
            mock.patch("apps.epos_qbo.views.timezone.now", return_value=self.fixed_now),
            mock.patch("apps.epos_qbo.views.load_tokens", return_value=self._token_payload()),
        ):
            context = views._overview_context()

        kpis = context["kpis"]
        self.assertEqual(kpis["sales_24h_total"], Decimal("200.0000"))
        self.assertEqual(kpis["sales_prev_24h_total"], Decimal("100.0000"))
        self.assertEqual(kpis["sales_24h_trend_dir"], "up")
        self.assertEqual(kpis["sales_24h_trend_text"], "↑ 100.0% increase vs Feb 11")

    def test_overview_context_includes_avg_runtime_24h(self):
        completed = RunJob.objects.create(
            scope=RunJob.SCOPE_SINGLE,
            company_key=self.company.company_key,
            status=RunJob.STATUS_SUCCEEDED,
            started_at=self.fixed_now - timedelta(minutes=30),
            finished_at=self.fixed_now - timedelta(minutes=10),
        )
        RunJob.objects.filter(id=completed.id).update(created_at=self.fixed_now - timedelta(hours=1))
        RunArtifact.objects.create(
            run_job=completed,
            company_key=self.company.company_key,
            target_date=(self.fixed_now - timedelta(days=1)).date(),
            processed_at=self.fixed_now - timedelta(minutes=5),
            source_path="/tmp/company_a_runtime.json",
            source_hash="hash-runtime",
            reconcile_epos_total=100.0,
            upload_stats_json={"uploaded": 3, "skipped": 0, "failed": 0},
        )
        with (
            mock.patch("apps.epos_qbo.business_date.timezone.now", return_value=self.fixed_now),
            mock.patch("apps.epos_qbo.views.timezone.now", return_value=self.fixed_now),
            mock.patch("apps.epos_qbo.views.load_tokens", return_value=self._token_payload()),
        ):
            context = views._overview_context()
        self.assertGreaterEqual(context["kpis"]["avg_runtime_24h_seconds"], 0)
        display = context["kpis"]["avg_runtime_24h_display"]
        self.assertTrue(any(unit in display for unit in ("s", "m", "h", "d")))
        self.assertIn("vs Feb 11", context["kpis"]["avg_runtime_today_trend_text"])

    def test_overview_sales_24h_shows_no_monetary_totals_when_artifacts_have_no_amount(self):
        run = RunJob.objects.create(
            scope=RunJob.SCOPE_SINGLE,
            company_key=self.company.company_key,
            status=RunJob.STATUS_SUCCEEDED,
        )
        RunArtifact.objects.create(
            run_job=run,
            company_key=self.company.company_key,
            target_date=(self.fixed_now - timedelta(days=1)).date(),
            processed_at=self.fixed_now - timedelta(hours=3),
            source_path="/tmp/company_a_no_amount.json",
            source_hash="hash-no-amount",
            upload_stats_json={"uploaded": 5},
        )
        with (
            mock.patch("apps.epos_qbo.business_date.timezone.now", return_value=self.fixed_now),
            mock.patch("apps.epos_qbo.views.timezone.now", return_value=self.fixed_now),
            mock.patch("apps.epos_qbo.views.load_tokens", return_value=self._token_payload()),
        ):
            context = views._overview_context()
        self.assertEqual(context["kpis"]["sales_24h_trend_text"], "No monetary totals found")

    def test_overview_context_shows_no_data_basis_line_without_successful_run_artifacts(self):
        with (
            mock.patch("apps.epos_qbo.business_date.timezone.now", return_value=self.fixed_now),
            mock.patch("apps.epos_qbo.views.timezone.now", return_value=self.fixed_now),
            mock.patch("apps.epos_qbo.views.load_tokens", return_value=self._token_payload()),
        ):
            context = views._overview_context()
        self.assertFalse(context["overview_has_data"])
        self.assertEqual(context["metric_basis_line"], "No successful run data yet.")

    def test_overview_avg_runtime_today_uses_faster_slower_wording(self):
        yesterday_date = (self.fixed_now - timedelta(days=1)).date()
        prev_date = (self.fixed_now - timedelta(days=2)).date()
        y_run = RunJob.objects.create(
            scope=RunJob.SCOPE_SINGLE,
            company_key=self.company.company_key,
            status=RunJob.STATUS_SUCCEEDED,
            started_at=self.fixed_now - timedelta(days=1, minutes=40),
            finished_at=self.fixed_now - timedelta(days=1, minutes=10),
        )
        RunJob.objects.filter(id=y_run.id).update(created_at=self.fixed_now - timedelta(days=1, minutes=41))
        RunArtifact.objects.create(
            run_job=y_run,
            company_key=self.company.company_key,
            target_date=prev_date,
            processed_at=self.fixed_now - timedelta(days=1, minutes=5),
            source_path="/tmp/y_run.json",
            source_hash="hash-y",
            reconcile_epos_total=50.0,
            upload_stats_json={"uploaded": 4, "skipped": 0, "failed": 0},
        )
        t_run = RunJob.objects.create(
            scope=RunJob.SCOPE_SINGLE,
            company_key=self.company.company_key,
            status=RunJob.STATUS_SUCCEEDED,
            started_at=self.fixed_now - timedelta(minutes=20),
            finished_at=self.fixed_now - timedelta(minutes=10),
        )
        RunJob.objects.filter(id=t_run.id).update(created_at=self.fixed_now - timedelta(minutes=21))
        RunArtifact.objects.create(
            run_job=t_run,
            company_key=self.company.company_key,
            target_date=yesterday_date,
            processed_at=self.fixed_now - timedelta(minutes=5),
            source_path="/tmp/t_run.json",
            source_hash="hash-t",
            reconcile_epos_total=50.0,
            upload_stats_json={"uploaded": 4, "skipped": 0, "failed": 0},
        )

        with (
            mock.patch("apps.epos_qbo.business_date.timezone.now", return_value=self.fixed_now),
            mock.patch("apps.epos_qbo.views.timezone.now", return_value=self.fixed_now),
            mock.patch("apps.epos_qbo.views.load_tokens", return_value=self._token_payload()),
        ):
            context = views._overview_context()
        self.assertIn("faster vs Feb 11", context["kpis"]["avg_runtime_today_trend_text"])

    def test_overview_run_success_uses_target_date_artifact_linkage(self):
        """Run Success counts completed runs linked to artifacts for target trading date."""
        target_date = (self.fixed_now - timedelta(days=1)).date()
        other_date = (self.fixed_now - timedelta(days=2)).date()

        run_target_success = RunJob.objects.create(
            scope=RunJob.SCOPE_SINGLE,
            company_key=self.company.company_key,
            status=RunJob.STATUS_SUCCEEDED,
            finished_at=self.fixed_now - timedelta(hours=4),
        )
        RunArtifact.objects.create(
            run_job=run_target_success,
            company_key=self.company.company_key,
            target_date=target_date,
            processed_at=self.fixed_now - timedelta(hours=3),
            source_path="/tmp/target-success.json",
            source_hash="hash-target-success",
        )
        run_target_failed = RunJob.objects.create(
            scope=RunJob.SCOPE_SINGLE,
            company_key=self.company.company_key,
            status=RunJob.STATUS_FAILED,
            finished_at=self.fixed_now - timedelta(hours=2),
        )
        RunArtifact.objects.create(
            run_job=run_target_failed,
            company_key=self.company.company_key,
            target_date=target_date,
            processed_at=self.fixed_now - timedelta(hours=1),
            source_path="/tmp/target-failed.json",
            source_hash="hash-target-failed",
        )

        run_other_date = RunJob.objects.create(
            scope=RunJob.SCOPE_SINGLE,
            company_key=self.company.company_key,
            status=RunJob.STATUS_SUCCEEDED,
            finished_at=self.fixed_now - timedelta(hours=8),
        )
        RunArtifact.objects.create(
            run_job=run_other_date,
            company_key=self.company.company_key,
            target_date=other_date,
            processed_at=self.fixed_now - timedelta(hours=7),
            source_path="/tmp/other-date.json",
            source_hash="hash-other-date",
        )

        with (
            mock.patch("apps.epos_qbo.business_date.timezone.now", return_value=self.fixed_now),
            mock.patch("apps.epos_qbo.views.timezone.now", return_value=self.fixed_now),
            mock.patch("apps.epos_qbo.views.load_tokens", return_value=self._token_payload()),
        ):
            context = views._overview_context()
        # Only runs linked to artifacts for the target trading date should count.
        self.assertEqual(context["kpis"]["total_completed_runs_24h"], 2)
        self.assertEqual(context["kpis"]["successful_runs_24h"], 1)
        self.assertEqual(context["kpis"]["run_success_pct_24h"], 50.0)
        self.assertEqual(context["kpis"]["run_success_ratio_24h"], "1/2")

    def test_overview_avg_runtime_today_uses_successful_runs_only(self):
        yesterday_date = (self.fixed_now - timedelta(days=1)).date()
        succeeded = RunJob.objects.create(
            scope=RunJob.SCOPE_SINGLE,
            company_key=self.company.company_key,
            status=RunJob.STATUS_SUCCEEDED,
            started_at=self.fixed_now - timedelta(minutes=20),
            finished_at=self.fixed_now - timedelta(minutes=10),
        )
        RunJob.objects.filter(id=succeeded.id).update(created_at=self.fixed_now - timedelta(minutes=21))
        RunArtifact.objects.create(
            run_job=succeeded,
            company_key=self.company.company_key,
            target_date=yesterday_date,
            processed_at=self.fixed_now - timedelta(minutes=5),
            source_path="/tmp/succeeded.json",
            source_hash="hash-succeeded",
            reconcile_epos_total=100.0,
            upload_stats_json={"uploaded": 5, "skipped": 0, "failed": 0},
        )
        failed = RunJob.objects.create(
            scope=RunJob.SCOPE_SINGLE,
            company_key=self.company.company_key,
            status=RunJob.STATUS_FAILED,
            started_at=self.fixed_now - timedelta(minutes=90),
            finished_at=self.fixed_now - timedelta(minutes=10),
        )
        RunJob.objects.filter(id=failed.id).update(created_at=self.fixed_now - timedelta(minutes=91))

        with (
            mock.patch("apps.epos_qbo.business_date.timezone.now", return_value=self.fixed_now),
            mock.patch("apps.epos_qbo.views.timezone.now", return_value=self.fixed_now),
            mock.patch("apps.epos_qbo.views.load_tokens", return_value=self._token_payload()),
        ):
            context = views._overview_context()
        # 10 minutes from the succeeded run only (target-date logic).
        self.assertEqual(context["kpis"]["avg_runtime_today_seconds"], 600)

    def test_overview_sales_24h_uses_decrease_wording_for_negative_delta(self):
        prev_date = (self.fixed_now - timedelta(days=2)).date()
        this_date = (self.fixed_now - timedelta(days=1)).date()
        prev_run = RunJob.objects.create(
            scope=RunJob.SCOPE_SINGLE,
            company_key=self.company.company_key,
            status=RunJob.STATUS_SUCCEEDED,
        )
        RunArtifact.objects.create(
            run_job=prev_run,
            company_key=self.company.company_key,
            target_date=prev_date,
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
            target_date=this_date,
            processed_at=self.fixed_now - timedelta(hours=2),
            source_path="/tmp/company_a_curr_drop.json",
            source_hash="hash-curr-drop",
            reconcile_epos_total=100.0,
        )
        with (
            mock.patch("apps.epos_qbo.business_date.timezone.now", return_value=self.fixed_now),
            mock.patch("apps.epos_qbo.views.timezone.now", return_value=self.fixed_now),
            mock.patch("apps.epos_qbo.views.load_tokens", return_value=self._token_payload()),
        ):
            context = views._overview_context()
        self.assertEqual(context["kpis"]["sales_24h_trend_text"], "↓ 50.0% decrease vs Feb 11")

    def test_overview_sales_today_uses_latest_successful_artifact_per_company(self):
        CompanyConfigRecord.objects.create(
            company_key="company_b",
            display_name="Company B",
            config_json={
                "company_key": "company_b",
                "display_name": "Company B",
                "qbo": {"realm_id": "987654321"},
                "epos": {"username_env_key": "EPOS_USERNAME_B", "password_env_key": "EPOS_PASSWORD_B"},
            },
        )
        # Target date Feb 12 (yesterday): company_a 3,995,250, company_b 9,505,350.
        # Target date Feb 11 (prev): company_a 2,645,250, company_b 9,374,050.
        run_a_old = RunJob.objects.create(
            scope=RunJob.SCOPE_SINGLE,
            company_key="company_a",
            status=RunJob.STATUS_SUCCEEDED,
        )
        RunArtifact.objects.create(
            run_job=run_a_old,
            company_key="company_a",
            target_date=(self.fixed_now - timedelta(days=1)).date(),
            processed_at=self.fixed_now - timedelta(hours=10),
            source_path="/tmp/company_a_old.json",
            source_hash="company-a-old",
            reconcile_epos_total=3995250.0,
        )
        run_a_new = RunJob.objects.create(
            scope=RunJob.SCOPE_SINGLE,
            company_key="company_a",
            status=RunJob.STATUS_SUCCEEDED,
        )
        RunArtifact.objects.create(
            run_job=run_a_new,
            company_key="company_a",
            target_date=(self.fixed_now - timedelta(days=2)).date(),
            processed_at=self.fixed_now - timedelta(hours=6),
            source_path="/tmp/company_a_new.json",
            source_hash="company-a-new",
            reconcile_epos_total=2645250.0,
        )
        run_b_old = RunJob.objects.create(
            scope=RunJob.SCOPE_SINGLE,
            company_key="company_b",
            status=RunJob.STATUS_SUCCEEDED,
        )
        RunArtifact.objects.create(
            run_job=run_b_old,
            company_key="company_b",
            target_date=(self.fixed_now - timedelta(days=1)).date(),
            processed_at=self.fixed_now - timedelta(hours=9),
            source_path="/tmp/company_b_old.json",
            source_hash="company-b-old",
            reconcile_epos_total=9505350.0,
        )
        run_b_new = RunJob.objects.create(
            scope=RunJob.SCOPE_SINGLE,
            company_key="company_b",
            status=RunJob.STATUS_SUCCEEDED,
        )
        RunArtifact.objects.create(
            run_job=run_b_new,
            company_key="company_b",
            target_date=(self.fixed_now - timedelta(days=2)).date(),
            processed_at=self.fixed_now - timedelta(hours=5),
            source_path="/tmp/company_b_new.json",
            source_hash="company-b-new",
            reconcile_epos_total=9374050.0,
        )

        with (
            mock.patch("apps.epos_qbo.business_date.timezone.now", return_value=self.fixed_now),
            mock.patch("apps.epos_qbo.views.timezone.now", return_value=self.fixed_now),
            mock.patch("apps.epos_qbo.views.load_tokens", return_value=self._token_payload()),
        ):
            context = views._overview_context()

        # Resolver picks the latest succeeded artifact by processed_at; here that's Feb 11.
        self.assertEqual(context["target_date_iso"], (self.fixed_now - timedelta(days=2)).date().isoformat())
        # By target date: Feb 11 total = 2,645,250 + 9,374,050 = 12,019,300.
        self.assertEqual(context["kpis"]["sales_24h_total"], Decimal("12019300.0000"))
        self.assertEqual(context["kpis"]["sales_prev_24h_total"], Decimal("0.0000"))


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
            mock.patch("apps.epos_qbo.business_date.timezone.now", return_value=self.fixed_now),
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
            mock.patch("apps.epos_qbo.business_date.timezone.now", return_value=self.fixed_now),
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
            mock.patch("apps.epos_qbo.business_date.timezone.now", return_value=self.fixed_now),
            mock.patch("apps.epos_qbo.views.timezone.now", return_value=self.fixed_now),
            mock.patch("apps.epos_qbo.views.load_tokens", return_value=self._token_payload()),
        ):
            response = self.client.get(reverse("epos_qbo:overview"))

        html = response.content.decode("utf-8")
        self.assertIn(f"Company A: Run {run.display_label} succeeded", html)
        self.assertNotIn(str(run.id), html)

    def test_overview_panels_endpoint_renders_fragment(self):
        with (
            mock.patch("apps.epos_qbo.business_date.timezone.now", return_value=self.fixed_now),
            mock.patch("apps.epos_qbo.views.timezone.now", return_value=self.fixed_now),
            mock.patch("apps.epos_qbo.views.load_tokens", return_value=self._token_payload()),
        ):
            response = self.client.get(reverse("epos_qbo:overview-panels"))

        self.assertEqual(response.status_code, 200)
        html = response.content.decode("utf-8")
        self.assertIn("System Health", html)
        self.assertIn('id="overview-company-filter"', html)
        self.assertIn("Live Log", html)
        self.assertNotIn("Run Reliability", html)

    def test_overview_panels_respects_revenue_period_param(self):
        with (
            mock.patch("apps.epos_qbo.business_date.timezone.now", return_value=self.fixed_now),
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
            mock.patch("apps.epos_qbo.business_date.timezone.now", return_value=self.fixed_now),
            mock.patch("apps.epos_qbo.views.timezone.now", return_value=self.fixed_now),
            mock.patch("apps.epos_qbo.views.load_tokens", return_value=self._token_payload()),
        ):
            response = self.client.get(reverse("epos_qbo:overview"))

        self.assertEqual(response.status_code, 200)
        html = response.content.decode("utf-8")
        self.assertIn("Quick Sync", html)
        self.assertNotIn("Manual Sync", html)
        self.assertIn('name="date_mode" value="target_date"', html)
        self.assertIn('name="target_date"', html)

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
            mock.patch("apps.epos_qbo.business_date.timezone.now", return_value=self.fixed_now),
            mock.patch("apps.epos_qbo.views.timezone.now", return_value=self.fixed_now),
            mock.patch("apps.epos_qbo.views.load_tokens", return_value=self._token_payload()),
        ):
            response = self.client.get(reverse("epos_qbo:overview"))

        self.assertEqual(response.status_code, 200)
        html = response.content.decode("utf-8")
        self.assertIn("System Health", html)
        self.assertIn("Sales Synced", html)
        self.assertIn("Run Success", html)
        self.assertIn("Avg Runtime", html)
        self.assertIn("Metrics are based on Target Date:", html)
        self.assertIn("Last successful sync", html)
        self.assertNotIn("KPI basis: trading day cutoff", html)
        self.assertNotIn("Healthy Companies", html)
        self.assertNotIn("Critical Errors", html)
        self.assertNotIn("Records Synced (24h)", html)
        self.assertNotIn("Active Runs", html)
