from __future__ import annotations

from datetime import timedelta
from unittest import mock

from django.contrib.auth.models import User
from django.test import TestCase, override_settings
from django.urls import reverse
from django.utils import timezone

from apps.epos_qbo.models import CompanyConfigRecord
from apps.epos_qbo import views


class TokenHealthModelTests(TestCase):
    def setUp(self):
        self.company = CompanyConfigRecord.objects.create(
            company_key="company_b",
            display_name="Company B",
            config_json={
                "company_key": "company_b",
                "display_name": "Company B",
                "qbo": {"realm_id": "9130357766900456", "deposit_account": "Cash", "tax_mode": "tax_inclusive_composite"},
                "epos": {"username_env_key": "EPOS_USERNAME_B", "password_env_key": "EPOS_PASSWORD_B"},
            },
        )

    def _token_payload(
        self,
        *,
        access_seconds: int | None = 3600,
        refresh_seconds: int | None = 60 * 60 * 24 * 30,
        refresh_token: str | None = "refresh-token",
    ) -> dict:
        now_ts = int(timezone.now().timestamp())
        return {
            "access_token": "access-token",
            "refresh_token": refresh_token,
            "expires_at": now_ts + access_seconds if access_seconds is not None else None,
            "refresh_expires_at": now_ts + refresh_seconds if refresh_seconds is not None else None,
            "updated_at": now_ts,
            "environment": "production",
        }

    def test_missing_token_row(self):
        with mock.patch("apps.epos_qbo.views.load_tokens", return_value=None):
            info = views._company_token_health(self.company)
        self.assertEqual(info["connection_state"], "missing_tokens")
        self.assertEqual(info["severity"], "critical")

    def test_missing_refresh_token(self):
        tokens = self._token_payload(refresh_token=None)
        with mock.patch("apps.epos_qbo.views.load_tokens", return_value=tokens):
            info = views._company_token_health(self.company)
        self.assertEqual(info["connection_state"], "missing_refresh_token")
        self.assertEqual(info["severity"], "critical")

    def test_access_expired_refresh_valid(self):
        tokens = self._token_payload(access_seconds=-60, refresh_seconds=60 * 60 * 24 * 20)
        with mock.patch("apps.epos_qbo.views.load_tokens", return_value=tokens):
            info = views._company_token_health(self.company)
        self.assertEqual(info["connection_state"], "connected")
        self.assertEqual(info["access_state"], "expired")
        self.assertEqual(info["severity"], "healthy")
        self.assertIn("Access token expired (will refresh on next sync)", info["display_subtext"])

    def test_refresh_expiring_threshold(self):
        tokens = self._token_payload(access_seconds=3600, refresh_seconds=60 * 60 * 24 * 2)
        with mock.patch("apps.epos_qbo.views.load_tokens", return_value=tokens):
            info = views._company_token_health(self.company)
        self.assertEqual(info["connection_state"], "refresh_expiring")
        self.assertEqual(info["severity"], "warning")
        self.assertIn("Refresh token expires in", info["display_subtext"])

    def test_refresh_expired(self):
        tokens = self._token_payload(access_seconds=3600, refresh_seconds=-60)
        with mock.patch("apps.epos_qbo.views.load_tokens", return_value=tokens):
            info = views._company_token_health(self.company)
        self.assertEqual(info["connection_state"], "refresh_expired")
        self.assertEqual(info["severity"], "critical")

    @override_settings(OIAT_DASHBOARD_REFRESH_EXPIRING_DAYS=1)
    def test_refresh_expiring_threshold_is_settings_driven(self):
        tokens = self._token_payload(access_seconds=3600, refresh_seconds=60 * 60 * 24 * 2)
        with mock.patch("apps.epos_qbo.views.load_tokens", return_value=tokens):
            info = views._company_token_health(self.company)
        self.assertEqual(info["connection_state"], "connected")
        self.assertEqual(info["severity"], "healthy")

    @override_settings(OIAT_DASHBOARD_REAUTH_GUIDANCE="Use the shared OAuth runbook.")
    def test_reauth_guidance_is_configurable(self):
        with mock.patch("apps.epos_qbo.views.load_tokens", return_value=None):
            info = views._company_token_health(self.company)
        self.assertEqual(info["display_subtext"], "Use the shared OAuth runbook.")


class TokenHealthViewsTests(TestCase):
    def setUp(self):
        self.user = User.objects.create_user(username="operator", password="pw12345")
        self.company = CompanyConfigRecord.objects.create(
            company_key="company_b",
            display_name="Company B",
            config_json={
                "company_key": "company_b",
                "display_name": "Company B",
                "qbo": {"realm_id": "9130357766900456", "deposit_account": "Cash", "tax_mode": "tax_inclusive_composite"},
                "epos": {"username_env_key": "EPOS_USERNAME_B", "password_env_key": "EPOS_PASSWORD_B"},
            },
        )
        self.client.login(username="operator", password="pw12345")

    def _token_payload(
        self,
        *,
        access_delta: timedelta,
        refresh_delta: timedelta,
        refresh_token: str | None = "refresh-token",
    ) -> dict:
        now_ts = int(timezone.now().timestamp())
        return {
            "access_token": "access-token",
            "refresh_token": refresh_token,
            "expires_at": now_ts + int(access_delta.total_seconds()),
            "refresh_expires_at": now_ts + int(refresh_delta.total_seconds()),
            "updated_at": now_ts,
            "environment": "production",
        }

    def test_overview_never_shows_unknown_green(self):
        with (
            mock.patch("apps.epos_qbo.views.load_tokens_batch", return_value={}),
            mock.patch("apps.epos_qbo.views.load_tokens", return_value=None),
        ):
            response = self.client.get(reverse("epos_qbo:overview"))
        self.assertEqual(response.status_code, 200)
        html = response.content.decode("utf-8")
        self.assertNotRegex(html, r'text-emerald-600">\s*Unknown')
        self.assertIn("QBO re-authentication required", html)

    def test_companies_page_never_shows_valid_0d(self):
        tokens = self._token_payload(
            access_delta=timedelta(minutes=30),
            refresh_delta=timedelta(days=30),
        )
        with (
            mock.patch("apps.epos_qbo.views.load_tokens_batch", return_value={}),
            mock.patch("apps.epos_qbo.views.load_tokens", return_value=tokens),
        ):
            response = self.client.get(reverse("epos_qbo:companies-list"))
        self.assertEqual(response.status_code, 200)
        html = response.content.decode("utf-8")
        self.assertNotIn("Valid (0d)", html)
        self.assertIn("Connected", html)

    def test_invalid_or_missing_text_not_shown_for_valid_refresh(self):
        tokens = self._token_payload(
            access_delta=timedelta(minutes=-5),
            refresh_delta=timedelta(days=25),
        )
        with (
            mock.patch("apps.epos_qbo.views.load_tokens_batch", return_value={}),
            mock.patch("apps.epos_qbo.views.load_tokens", return_value=tokens),
        ):
            response = self.client.get(reverse("epos_qbo:companies-list"))
        self.assertEqual(response.status_code, 200)
        html = response.content.decode("utf-8")
        self.assertNotIn("QBO token is invalid or missing", html)
        self.assertNotIn("QBO re-authentication required", html)

    def test_reauth_text_shown_when_refresh_missing(self):
        tokens = self._token_payload(
            access_delta=timedelta(minutes=15),
            refresh_delta=timedelta(days=20),
            refresh_token=None,
        )
        with (
            mock.patch("apps.epos_qbo.views.load_tokens_batch", return_value={}),
            mock.patch("apps.epos_qbo.views.load_tokens", return_value=tokens),
        ):
            response = self.client.get(reverse("epos_qbo:companies-list"))
        self.assertEqual(response.status_code, 200)
        html = response.content.decode("utf-8")
        self.assertIn("QBO re-authentication required", html)
