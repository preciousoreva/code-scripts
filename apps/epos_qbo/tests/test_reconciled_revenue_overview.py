from __future__ import annotations

from datetime import date, datetime, timedelta
from unittest import mock

from django.contrib.auth.models import User
from django.test import TestCase
from django.urls import reverse
from django.utils import timezone

from apps.epos_qbo import views
from apps.epos_qbo.models import CompanyConfigRecord, RunArtifact


class ReconciledRevenueOverviewContextTests(TestCase):
    def setUp(self):
        self.fixed_now = timezone.make_aware(datetime(2026, 2, 14, 12, 0, 0))
        self.company_a = CompanyConfigRecord.objects.create(
            company_key="company_a",
            display_name="AKPONORA VENTURES LTD.",
            config_json={
                "company_key": "company_a",
                "display_name": "AKPONORA VENTURES LTD.",
                "qbo": {"realm_id": "111"},
                "epos": {"username_env_key": "EPOS_USERNAME_A", "password_env_key": "EPOS_PASSWORD_A"},
            },
        )
        self.company_b = CompanyConfigRecord.objects.create(
            company_key="company_b",
            display_name="GOLDPLATES FEASTHOUSE LTD.",
            config_json={
                "company_key": "company_b",
                "display_name": "GOLDPLATES FEASTHOUSE LTD.",
                "qbo": {"realm_id": "222"},
                "epos": {"username_env_key": "EPOS_USERNAME_B", "password_env_key": "EPOS_PASSWORD_B"},
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

    @staticmethod
    def _create_artifact(
        *,
        company_key: str,
        target_date: date,
        processed_at: datetime,
        status: str,
        epos_total: float | None,
        source_suffix: str,
    ) -> None:
        RunArtifact.objects.create(
            company_key=company_key,
            target_date=target_date,
            processed_at=processed_at,
            source_path=f"/tmp/{company_key}_{source_suffix}.json",
            source_hash=f"hash-{company_key}-{source_suffix}",
            reconcile_status=status,
            reconcile_epos_total=epos_total,
            reconcile_qbo_total=epos_total,
            reconcile_epos_count=10,
            reconcile_qbo_count=10,
        )

    def test_revenue_context_filters_match_and_deduplicates(self):
        self._create_artifact(
            company_key="company_a",
            target_date=date(2026, 2, 12),
            processed_at=timezone.make_aware(datetime(2026, 2, 12, 10, 0, 0)),
            status="MATCH",
            epos_total=100.0,
            source_suffix="old",
        )
        self._create_artifact(
            company_key="company_a",
            target_date=date(2026, 2, 12),
            processed_at=timezone.make_aware(datetime(2026, 2, 12, 11, 0, 0)),
            status="MATCH",
            epos_total=150.0,
            source_suffix="new",
        )
        self._create_artifact(
            company_key="company_b",
            target_date=date(2026, 2, 13),
            processed_at=timezone.make_aware(datetime(2026, 2, 13, 11, 0, 0)),
            status="MATCH",
            epos_total=250.0,
            source_suffix="b1",
        )
        self._create_artifact(
            company_key="company_b",
            target_date=date(2026, 2, 13),
            processed_at=timezone.make_aware(datetime(2026, 2, 13, 9, 0, 0)),
            status="MISMATCH",
            epos_total=999.0,
            source_suffix="mismatch",
        )
        self._create_artifact(
            company_key="company_a",
            target_date=date(2026, 2, 13),
            processed_at=timezone.make_aware(datetime(2026, 2, 13, 8, 0, 0)),
            status="MATCH",
            epos_total=None,
            source_suffix="null",
        )
        self._create_artifact(
            company_key="company_a",
            target_date=date(2026, 1, 10),
            processed_at=timezone.make_aware(datetime(2026, 1, 10, 8, 0, 0)),
            status="MATCH",
            epos_total=500.0,
            source_suffix="outside",
        )

        with (
            mock.patch("apps.epos_qbo.dashboard_timezone.timezone.now", return_value=self.fixed_now),
            mock.patch("apps.epos_qbo.views.timezone.now", return_value=self.fixed_now),
            mock.patch("apps.epos_qbo.views.load_tokens", return_value=self._token_payload()),
        ):
            context = views._overview_context("7d")

        self.assertEqual(context["revenue_period"], "7d")
        self.assertTrue(context["has_reconciled_revenue_data"])
        self.assertEqual(context["revenue_start_date_display"], "Feb 07")
        self.assertEqual(context["revenue_end_date_display"], "Feb 13")
        self.assertEqual(context["revenue_matched_days"], 2)

        series_map = {series["company_key"]: series for series in context["revenue_series"]}
        idx_feb12 = context["revenue_labels"].index("Feb 12")
        idx_feb13 = context["revenue_labels"].index("Feb 13")
        self.assertEqual(series_map["company_a"]["data"][idx_feb12], 150.0)
        self.assertEqual(series_map["company_b"]["data"][idx_feb13], 250.0)

    def test_revenue_period_invalid_falls_back_to_default(self):
        with (
            mock.patch("apps.epos_qbo.dashboard_timezone.timezone.now", return_value=self.fixed_now),
            mock.patch("apps.epos_qbo.views.timezone.now", return_value=self.fixed_now),
            mock.patch("apps.epos_qbo.views.load_tokens", return_value=self._token_payload()),
        ):
            context = views._overview_context("bad-value")

        self.assertEqual(context["revenue_period"], "7d")


class ReconciledRevenueOverviewTemplateTests(TestCase):
    def setUp(self):
        self.fixed_now = timezone.make_aware(datetime(2026, 2, 14, 12, 0, 0))
        self.user = User.objects.create_user(username="operator", password="pw12345")
        CompanyConfigRecord.objects.create(
            company_key="company_a",
            display_name="AKPONORA VENTURES LTD.",
            config_json={
                "company_key": "company_a",
                "display_name": "AKPONORA VENTURES LTD.",
                "qbo": {"realm_id": "111"},
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

    def test_overview_renders_revenue_chart_section(self):
        RunArtifact.objects.create(
            company_key="company_a",
            target_date=date(2026, 2, 13),
            processed_at=timezone.make_aware(datetime(2026, 2, 13, 20, 0, 0)),
            source_path="/tmp/match.json",
            source_hash="hash-match",
            reconcile_status="MATCH",
            reconcile_epos_total=5000.0,
            reconcile_qbo_total=5000.0,
            reconcile_epos_count=20,
            reconcile_qbo_count=20,
        )

        with (
            mock.patch("apps.epos_qbo.dashboard_timezone.timezone.now", return_value=self.fixed_now),
            mock.patch("apps.epos_qbo.views.timezone.now", return_value=self.fixed_now),
            mock.patch("apps.epos_qbo.views.load_tokens", return_value=self._token_payload()),
        ):
            response = self.client.get(reverse("epos_qbo:overview"), {"revenue_period": "90d"})

        self.assertEqual(response.status_code, 200)
        html = response.content.decode("utf-8")
        self.assertIn("Reconciled EPOS Revenue", html)
        self.assertIn("Last 90D", html)
        self.assertIn('id="overview-revenue-chart"', html)
        self.assertIn("overview-revenue-chart-data", html)

    def test_overview_renders_empty_state_for_no_reconciled_data(self):
        with (
            mock.patch("apps.epos_qbo.dashboard_timezone.timezone.now", return_value=self.fixed_now),
            mock.patch("apps.epos_qbo.views.timezone.now", return_value=self.fixed_now),
            mock.patch("apps.epos_qbo.views.load_tokens", return_value=self._token_payload()),
        ):
            response = self.client.get(reverse("epos_qbo:overview"))

        self.assertEqual(response.status_code, 200)
        html = response.content.decode("utf-8")
        self.assertIn("No reconciled revenue data yet for this period.", html)
