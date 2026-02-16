from __future__ import annotations

from datetime import datetime, timedelta
from unittest import mock

from django.contrib.auth.models import Permission, User
from django.test import TestCase, override_settings
from django.urls import reverse
from django.utils import timezone

from apps.epos_qbo import views
from apps.epos_qbo.forms import RunTriggerForm
from apps.epos_qbo.models import CompanyConfigRecord, RunJob


class DashboardSettingsTests(TestCase):
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

    @override_settings(
        OIAT_DASHBOARD_DEFAULT_PARALLEL=4,
        OIAT_DASHBOARD_DEFAULT_STAGGER_SECONDS=9,
    )
    def test_run_trigger_form_uses_settings_defaults(self):
        form = RunTriggerForm()
        self.assertEqual(form.fields["parallel"].initial, 4)
        self.assertEqual(form.fields["stagger_seconds"].initial, 9)

        posted = RunTriggerForm(data={"scope": "all_companies", "date_mode": "yesterday"})
        self.assertTrue(posted.is_valid())
        self.assertEqual(posted.cleaned_data["parallel"], 4)
        self.assertEqual(posted.cleaned_data["stagger_seconds"], 9)

    @override_settings(
        OIAT_DASHBOARD_DEFAULT_PARALLEL=5,
        OIAT_DASHBOARD_DEFAULT_STAGGER_SECONDS=6,
    )
    def test_runs_page_renders_settings_defaults(self):
        perm = Permission.objects.get(codename="can_trigger_runs")
        self.user.user_permissions.add(perm)

        response = self.client.get(reverse("epos_qbo:runs"))
        self.assertEqual(response.status_code, 200)
        html = response.content.decode("utf-8")
        self.assertIn('name="parallel" min="1" value="5"', html)
        self.assertIn('name="stagger_seconds" min="0" value="6"', html)

    @override_settings(OIAT_DASHBOARD_STALE_HOURS_WARNING=2)
    def test_stale_hours_warning_respects_setting(self):
        run = RunJob.objects.create(
            scope=RunJob.SCOPE_SINGLE,
            company_key=self.company.company_key,
            status=RunJob.STATUS_SUCCEEDED,
            started_at=self.fixed_now - timedelta(hours=3),
        )

        with (
            mock.patch("apps.epos_qbo.business_date.timezone.now", return_value=self.fixed_now),
            mock.patch("apps.epos_qbo.views.timezone.now", return_value=self.fixed_now),
            mock.patch("apps.epos_qbo.views.load_tokens", return_value=self._token_payload()),
        ):
            enriched = views._enrich_company_data(self.company, run)
        issue_messages = [item["message"] for item in enriched["issues"]]
        self.assertIn("No sync in 3 hours", issue_messages)

    @override_settings(
        OIAT_BUSINESS_TIMEZONE="Africa/Lagos",
        OIAT_BUSINESS_DAY_CUTOFF_HOUR=5,
        OIAT_BUSINESS_DAY_CUTOFF_MINUTE=0,
    )
    def test_quick_sync_default_target_date_uses_business_trading_date(self):
        with mock.patch("apps.epos_qbo.business_date.timezone.now", return_value=self.fixed_now):
            target_date = views._quick_sync_default_target_date()
        # fixed_now 2026-02-14 12:00 UTC -> 13:00 WAT, target trading date = previous day
        self.assertEqual(target_date, "2026-02-13")
