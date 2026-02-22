from __future__ import annotations

from datetime import datetime, timedelta
from decimal import Decimal
from unittest import mock

from django.contrib.auth.models import Permission, User
from django.db import DatabaseError, connection
from django.test import TestCase, override_settings
from django.test.utils import CaptureQueriesContext
from django.urls import reverse
from django.utils import timezone

from apps.epos_qbo import portal_settings, views
from apps.epos_qbo.forms import PortalSettingsForm, RunTriggerForm
from apps.epos_qbo.models import (
    CompanyConfigRecord,
    DashboardUserPreference,
    PortalSettings,
    RunArtifact,
    RunJob,
)


def _reset_portal_settings_defaults():
    PortalSettings.objects.update_or_create(
        pk=1,
        defaults={
            "default_parallel": None,
            "default_stagger_seconds": None,
            "stale_hours_warning": None,
            "refresh_expiring_days": None,
            "reconcile_diff_warning": None,
            "reauth_guidance": None,
            "dashboard_timezone": None,
            "updated_by": None,
        },
    )
    portal_settings.invalidate_cache()


class DashboardSettingsTests(TestCase):
    def setUp(self):
        _reset_portal_settings_defaults()
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

    @override_settings(OIAT_DASHBOARD_RECON_DIFF_WARNING="2.5")
    def test_reconcile_mismatch_threshold_respects_setting(self):
        artifact = RunArtifact.objects.create(
            company_key=self.company.company_key,
            target_date=(self.fixed_now - timedelta(days=1)).date(),
            processed_at=self.fixed_now - timedelta(hours=1),
            source_path="/tmp/company_a_recon_threshold.json",
            source_hash="hash-company-a-recon-threshold",
            reconcile_difference=Decimal("1.6"),
        )

        with (
            mock.patch("apps.epos_qbo.business_date.timezone.now", return_value=self.fixed_now),
            mock.patch("apps.epos_qbo.views.timezone.now", return_value=self.fixed_now),
            mock.patch("apps.epos_qbo.views.load_tokens", return_value=self._token_payload()),
        ):
            status, summary = views._status_for_company(self.company, artifact, latest_job=None)
        self.assertEqual(status, "healthy")
        self.assertEqual(summary, "Last run succeeded.")

    def test_running_activity_does_not_downgrade_health_status(self):
        run = RunJob.objects.create(
            scope=RunJob.SCOPE_SINGLE,
            company_key=self.company.company_key,
            status=RunJob.STATUS_RUNNING,
            started_at=self.fixed_now - timedelta(minutes=15),
        )
        artifact = RunArtifact.objects.create(
            run_job=run,
            company_key=self.company.company_key,
            target_date=(self.fixed_now - timedelta(days=1)).date(),
            processed_at=self.fixed_now - timedelta(minutes=10),
            source_path="/tmp/company_a_running_health.json",
            source_hash="hash-company-a-running-health",
            reconcile_difference=Decimal("0.0"),
        )
        with (
            mock.patch("apps.epos_qbo.business_date.timezone.now", return_value=self.fixed_now),
            mock.patch("apps.epos_qbo.views.timezone.now", return_value=self.fixed_now),
            mock.patch("apps.epos_qbo.views.load_tokens", return_value=self._token_payload()),
        ):
            status, summary = views._status_for_company(self.company, artifact, latest_job=run)
        self.assertEqual(status, "healthy")
        self.assertEqual(summary, "Last run succeeded.")

    def test_no_artifact_classifies_company_as_unknown(self):
        with (
            mock.patch("apps.epos_qbo.business_date.timezone.now", return_value=self.fixed_now),
            mock.patch("apps.epos_qbo.views.timezone.now", return_value=self.fixed_now),
            mock.patch("apps.epos_qbo.views.load_tokens", return_value=self._token_payload()),
        ):
            status, summary = views._status_for_company(self.company, latest_artifact=None, latest_job=None)
        self.assertEqual(status, "unknown")
        self.assertEqual(summary, "No successful sync yet.")

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


class PortalSettingsResolverTests(TestCase):
    def setUp(self):
        _reset_portal_settings_defaults()
        self.user = User.objects.create_user(username="portal-admin", password="pw12345")

    def test_resolver_reuses_cached_singleton_in_same_window(self):
        PortalSettings.objects.filter(pk=1).update(default_parallel=6)
        portal_settings.invalidate_cache()
        with CaptureQueriesContext(connection) as ctx:
            self.assertEqual(portal_settings.get_default_parallel(), 6)
            self.assertEqual(portal_settings.get_default_parallel(), 6)
            self.assertEqual(portal_settings.get_default_parallel(), 6)
        self.assertEqual(len(ctx), 1)

    @override_settings(OIAT_DASHBOARD_DEFAULT_PARALLEL=7)
    def test_resolver_falls_back_to_settings_when_db_unavailable(self):
        portal_settings.invalidate_cache()
        with mock.patch(
            "apps.epos_qbo.portal_settings.PortalSettings.objects.filter",
            side_effect=DatabaseError("db unavailable"),
        ):
            self.assertEqual(portal_settings.get_default_parallel(), 7)

    def test_portal_settings_form_save_recreates_singleton_if_missing(self):
        PortalSettings.objects.filter(pk=1).delete()
        portal_settings.invalidate_cache()
        form = PortalSettingsForm(
            data={
                "default_parallel": "3",
                "default_stagger_seconds": "5",
            }
        )
        self.assertTrue(form.is_valid(), form.errors.as_json())
        saved = form.save(self.user)
        self.assertEqual(saved.pk, 1)
        self.assertEqual(PortalSettings.objects.get(pk=1).default_parallel, 3)

    def test_portal_settings_cache_is_invalidated_after_save(self):
        PortalSettings.objects.filter(pk=1).update(default_parallel=2)
        portal_settings.invalidate_cache()
        self.assertEqual(portal_settings.get_default_parallel(), 2)
        form = PortalSettingsForm(data={"default_parallel": "9"})
        self.assertTrue(form.is_valid(), form.errors.as_json())
        form.save(self.user)
        self.assertEqual(portal_settings.get_default_parallel(), 9)


class SettingsPageTests(TestCase):
    def setUp(self):
        _reset_portal_settings_defaults()
        self.user = User.objects.create_user(username="operator", password="pw12345")
        self.staff = User.objects.create_superuser(username="staff", password="staff12345")
        self.portal_manager = User.objects.create_user(username="portal-manager", password="portal12345")
        portal_perm = Permission.objects.get(codename="can_manage_portal_settings")
        self.portal_manager.user_permissions.add(portal_perm)
        CompanyConfigRecord.objects.create(
            company_key="company_a",
            display_name="Company A",
            config_json={"qbo": {"realm_id": "123"}, "epos": {}},
        )
        self.client.login(username="operator", password="pw12345")

    def test_settings_page_get_returns_200(self):
        response = self.client.get(reverse("epos_qbo:settings"))
        self.assertEqual(response.status_code, 200)
        html = response.content.decode("utf-8")
        self.assertIn("My preferences", html)
        self.assertIn("Dashboard (effective values)", html)
        self.assertIn("Scheduler / environment", html)
        self.assertIn("save_preferences", html)

    def test_settings_page_superuser_sees_portal_form(self):
        self.client.login(username="staff", password="staff12345")
        response = self.client.get(reverse("epos_qbo:settings"))
        self.assertEqual(response.status_code, 200)
        html = response.content.decode("utf-8")
        self.assertIn("Portal defaults", html)
        self.assertIn("save_portal", html)

    def test_settings_page_user_with_portal_permission_sees_portal_form(self):
        self.client.login(username="portal-manager", password="portal12345")
        response = self.client.get(reverse("epos_qbo:settings"))
        self.assertEqual(response.status_code, 200)
        html = response.content.decode("utf-8")
        self.assertIn("Portal defaults", html)
        self.assertIn("save_portal", html)

    def test_settings_page_user_without_portal_permission_does_not_see_portal_form(self):
        response = self.client.get(reverse("epos_qbo:settings"))
        self.assertEqual(response.status_code, 200)
        html = response.content.decode("utf-8")
        self.assertNotIn("Save portal defaults", html)

    def test_user_with_permission_can_save_portal_defaults(self):
        self.client.login(username="portal-manager", password="portal12345")
        response = self.client.post(
            reverse("epos_qbo:settings"),
            {
                "csrfmiddlewaretoken": self.client.get(reverse("epos_qbo:settings")).cookies["csrftoken"].value,
                "save_portal": "1",
                "default_parallel": "3",
                "default_stagger_seconds": "5",
                "stale_hours_warning": "",
                "refresh_expiring_days": "",
                "reconcile_diff_warning": "",
                "reauth_guidance": "",
                "dashboard_timezone": "",
            },
            follow=False,
        )
        self.assertEqual(response.status_code, 302)
        self.assertEqual(response.url, reverse("epos_qbo:settings"))
        row = PortalSettings.objects.get(pk=1)
        self.assertEqual(row.default_parallel, 3)
        self.assertEqual(row.default_stagger_seconds, 5)

    def test_user_without_permission_gets_403_when_posting_portal_defaults(self):
        response = self.client.post(
            reverse("epos_qbo:settings"),
            {
                "csrfmiddlewaretoken": self.client.get(reverse("epos_qbo:settings")).cookies["csrftoken"].value,
                "save_portal": "1",
                "default_parallel": "3",
            },
            follow=False,
        )
        self.assertEqual(response.status_code, 403)

    def test_user_can_save_preferences(self):
        response = self.client.post(
            reverse("epos_qbo:settings"),
            {
                "csrfmiddlewaretoken": self.client.get(reverse("epos_qbo:settings")).cookies["csrftoken"].value,
                "save_preferences": "1",
                "default_revenue_period": "30d",
                "default_overview_company_key": "company_a",
            },
            follow=False,
        )
        self.assertEqual(response.status_code, 302)
        pref = DashboardUserPreference.objects.get(user=self.user)
        self.assertEqual(pref.default_revenue_period, "30d")
        self.assertEqual(pref.default_overview_company_key, "company_a")


class OverviewUserPrefsTests(TestCase):
    def setUp(self):
        _reset_portal_settings_defaults()
        self.user = User.objects.create_user(username="operator", password="pw12345")
        CompanyConfigRecord.objects.create(
            company_key="company_a",
            display_name="Company A",
            config_json={"qbo": {"realm_id": "123"}, "epos": {}},
        )
        CompanyConfigRecord.objects.create(
            company_key="company_b",
            display_name="Company B",
            config_json={"qbo": {"realm_id": "456"}, "epos": {}},
        )
        self.client.login(username="operator", password="pw12345")

    def test_overview_uses_user_prefs_when_no_get_params(self):
        DashboardUserPreference.objects.create(
            user=self.user,
            default_revenue_period="30d",
            default_overview_company_key="company_a",
        )
        response = self.client.get(reverse("epos_qbo:overview"))
        self.assertEqual(response.status_code, 200)
        # Context should have company filtered to company_a and revenue_period 30d
        self.assertIn(b"Company A", response.content)
        self.assertIn(b"30d", response.content)

    def test_overview_uses_get_params_when_provided(self):
        DashboardUserPreference.objects.create(
            user=self.user,
            default_revenue_period="30d",
            default_overview_company_key="company_a",
        )
        response = self.client.get(
            reverse("epos_qbo:overview") + "?revenue_period=7d&company=company_b"
        )
        self.assertEqual(response.status_code, 200)
        self.assertIn(b"company_b", response.content)
        self.assertIn(b"7d", response.content)

    def test_overview_defaults_fallback_when_user_prefs_lookup_errors(self):
        request = mock.Mock(user=self.user)
        with mock.patch(
            "apps.epos_qbo.views.DashboardUserPreference.objects.get",
            side_effect=DatabaseError("db unavailable"),
        ):
            company_key, period = views._get_user_overview_defaults(request)
        self.assertIsNone(company_key)
        self.assertEqual(period, "7d")
