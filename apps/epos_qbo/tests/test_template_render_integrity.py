from __future__ import annotations

from datetime import datetime
from unittest import mock

from django.contrib.auth.models import Permission, User
from django.test import TestCase
from django.urls import reverse
from django.utils import timezone

from apps.epos_qbo.models import CompanyConfigRecord


class TemplateRenderIntegrityTests(TestCase):
    def setUp(self):
        self.fixed_now = timezone.make_aware(datetime(2026, 2, 14, 12, 0, 0))
        self.user = User.objects.create_user(
            username="marvin",
            first_name="Marvin",
            last_name="Operator",
            password="pw12345",
        )
        self.client.login(username="marvin", password="pw12345")
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

    def _assert_no_unresolved_template_tokens(self, html: str):
        self.assertNotIn("{{", html)
        self.assertNotIn("{%", html)
        self.assertNotIn("{#", html)

    def test_primary_dashboard_pages_do_not_leak_template_tokens(self):
        perm = Permission.objects.get(codename="can_trigger_runs")
        self.user.user_permissions.add(perm)

        urls = [
            reverse("epos_qbo:overview"),
            reverse("epos_qbo:companies-list"),
            reverse("epos_qbo:runs"),
            reverse("epos_qbo:logs"),
            reverse("epos_qbo:company-detail", kwargs={"company_key": self.company.company_key}),
        ]

        with (
            mock.patch("apps.epos_qbo.views.timezone.now", return_value=self.fixed_now),
            mock.patch("apps.epos_qbo.views.load_tokens", return_value=self._token_payload()),
        ):
            for url in urls:
                response = self.client.get(url)
                self.assertEqual(response.status_code, 200, msg=f"unexpected status for {url}")
                self._assert_no_unresolved_template_tokens(response.content.decode("utf-8"))

    def test_sidebar_renders_companies_count_and_user_identity_values(self):
        with (
            mock.patch("apps.epos_qbo.views.timezone.now", return_value=self.fixed_now),
            mock.patch("apps.epos_qbo.views.load_tokens", return_value=self._token_payload()),
        ):
            response = self.client.get(reverse("epos_qbo:overview"))
        self.assertEqual(response.status_code, 200)
        html = response.content.decode("utf-8")

        self.assertIn(">1</span>", html)
        self.assertIn("Marvin Operator", html)
        self.assertNotIn("{{ company_count", html)
        self.assertNotIn("request.user.get_full_name", html)
