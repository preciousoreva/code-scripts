from __future__ import annotations

from unittest import mock

from django.contrib.auth.models import Permission, User
from django.test import TestCase
from django.urls import reverse
from django.utils import timezone

from apps.epos_qbo.models import CompanyConfigRecord
from apps.epos_qbo.tests.utils import suppress_expected_request_logs


class ApiTokensPageTests(TestCase):
    def setUp(self):
        self.user = User.objects.create_user(username="operator", password="pw12345")
        self.company = CompanyConfigRecord.objects.create(
            company_key="company_a",
            display_name="Company A",
            config_json={
                "company_key": "company_a",
                "display_name": "Company A",
                "qbo": {"realm_id": "111222333", "environment": "production"},
                "epos": {"username_env_key": "EPOS_USERNAME_A", "password_env_key": "EPOS_PASSWORD_A"},
            },
        )

    def _grant_trigger_perm(self):
        perm = Permission.objects.get(codename="can_trigger_runs")
        self.user.user_permissions.add(perm)

    def _login(self):
        self.client.login(username="operator", password="pw12345")

    def _healthy_tokens(self):
        now_ts = int(timezone.now().timestamp())
        return {
            "access_token": "access-token-secret",
            "refresh_token": "refresh-token-secret",
            "expires_at": now_ts + 3600,
            "refresh_expires_at": now_ts + 60 * 60 * 24 * 30,
            "updated_at": now_ts,
            "environment": "production",
            "client_fingerprint": "fp-test-fixture-x",
        }

    # --- Page load ---

    def test_requires_login(self):
        response = self.client.get(reverse("epos_qbo:api-tokens"))
        self.assertEqual(response.status_code, 302)
        self.assertIn("/login/", response.url)

    def test_page_loads_for_logged_in_user(self):
        self._login()
        with mock.patch("apps.epos_qbo.views.load_tokens", return_value=self._healthy_tokens()):
            response = self.client.get(reverse("epos_qbo:api-tokens"))
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "QuickBooks Connections")
        self.assertContains(response, "Company A")
        self.assertContains(response, "company_a")
        self.assertContains(response, "111222333")
        self.assertContains(response, "Connected")

    def test_page_renders_missing_tokens_state(self):
        self._login()
        with mock.patch("apps.epos_qbo.views.load_tokens", return_value=None):
            response = self.client.get(reverse("epos_qbo:api-tokens"))
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Missing tokens")

    def test_page_does_not_leak_tokens(self):
        self._login()
        with mock.patch("apps.epos_qbo.views.load_tokens", return_value=self._healthy_tokens()):
            response = self.client.get(reverse("epos_qbo:api-tokens"))
        html = response.content.decode("utf-8")
        self.assertNotIn("access-token-secret", html)
        self.assertNotIn("refresh-token-secret", html)
        self.assertNotIn("fp-test-fixture-x", html)  # full fingerprint

    def test_sidebar_links_to_api_tokens_page(self):
        self._login()
        response = self.client.get(reverse("epos_qbo:overview"))
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, reverse("epos_qbo:api-tokens"))

    # --- URL wiring ---

    def test_url_resolves(self):
        self.assertEqual(reverse("epos_qbo:api-tokens"), "/epos-qbo/api-tokens/")
        self.assertTrue(
            reverse("epos_qbo:api-tokens-test", kwargs={"company_key": "company_a"}).endswith(
                "/api-tokens/company_a/test/"
            )
        )
        self.assertTrue(
            reverse("epos_qbo:api-tokens-refresh", kwargs={"company_key": "company_a"}).endswith(
                "/api-tokens/company_a/refresh/"
            )
        )

    # --- Test connection ---

    def test_test_connection_requires_post(self):
        self._grant_trigger_perm()
        self._login()
        with suppress_expected_request_logs():
            response = self.client.get(
                reverse("epos_qbo:api-tokens-test", kwargs={"company_key": "company_a"})
            )
        self.assertEqual(response.status_code, 405)

    def test_test_connection_handles_no_tokens(self):
        self._grant_trigger_perm()
        self._login()
        with mock.patch("apps.epos_qbo.views.load_tokens", return_value=None):
            response = self.client.post(
                reverse("epos_qbo:api-tokens-test", kwargs={"company_key": "company_a"}),
                follow=True,
            )
        self.assertEqual(response.status_code, 200)
        msgs = [str(m) for m in response.context["messages"]]
        self.assertTrue(any("no QuickBooks tokens stored" in m for m in msgs))

    def test_test_connection_success(self):
        self._grant_trigger_perm()
        self._login()
        fake_response = mock.Mock(status_code=200)
        fake_response.json.return_value = {
            "QueryResponse": {"CompanyInfo": [{"CompanyName": "Acme Co"}]}
        }
        with (
            mock.patch("apps.epos_qbo.views.load_tokens", return_value=self._healthy_tokens()),
            mock.patch("apps.epos_qbo.views.get_access_token", return_value="abc"),
            mock.patch("apps.epos_qbo.views.requests.get", return_value=fake_response),
        ):
            response = self.client.post(
                reverse("epos_qbo:api-tokens-test", kwargs={"company_key": "company_a"}),
                follow=True,
            )
        self.assertEqual(response.status_code, 200)
        msgs = [str(m) for m in response.context["messages"]]
        self.assertTrue(any("Acme Co" in m for m in msgs))

    def test_test_connection_401_message(self):
        self._grant_trigger_perm()
        self._login()
        fake_response = mock.Mock(status_code=401)
        with (
            mock.patch("apps.epos_qbo.views.load_tokens", return_value=self._healthy_tokens()),
            mock.patch("apps.epos_qbo.views.get_access_token", return_value="abc"),
            mock.patch("apps.epos_qbo.views.requests.get", return_value=fake_response),
        ):
            response = self.client.post(
                reverse("epos_qbo:api-tokens-test", kwargs={"company_key": "company_a"}),
                follow=True,
            )
        msgs = [str(m) for m in response.context["messages"]]
        self.assertTrue(any("rejected" in m.lower() for m in msgs))

    # --- Refresh ---

    def test_refresh_handles_invalid_grant(self):
        self._grant_trigger_perm()
        self._login()
        with (
            mock.patch("apps.epos_qbo.views.load_tokens", return_value=self._healthy_tokens()),
            mock.patch(
                "apps.epos_qbo.views.refresh_access_token",
                side_effect=RuntimeError(
                    "Refresh token is invalid or expired (400 invalid_grant)."
                ),
            ),
        ):
            response = self.client.post(
                reverse("epos_qbo:api-tokens-refresh", kwargs={"company_key": "company_a"}),
                follow=True,
            )
        self.assertEqual(response.status_code, 200)
        msgs = [str(m) for m in response.context["messages"]]
        self.assertTrue(any("Re-authorize QuickBooks" in m for m in msgs))

    def test_refresh_no_tokens(self):
        self._grant_trigger_perm()
        self._login()
        with (
            mock.patch(
                "apps.epos_qbo.views.refresh_access_token",
                side_effect=RuntimeError("No tokens found for company_a"),
            ),
        ):
            response = self.client.post(
                reverse("epos_qbo:api-tokens-refresh", kwargs={"company_key": "company_a"}),
                follow=True,
            )
        msgs = [str(m) for m in response.context["messages"]]
        self.assertTrue(any("No tokens stored" in m for m in msgs))

    def test_refresh_success(self):
        self._grant_trigger_perm()
        self._login()
        fake_response = mock.Mock(status_code=200)
        fake_response.json.return_value = {
            "QueryResponse": {"CompanyInfo": [{"CompanyName": "Acme Co"}]}
        }
        with (
            mock.patch("apps.epos_qbo.views.refresh_access_token", return_value={}),
            mock.patch("apps.epos_qbo.views.get_access_token", return_value="abc"),
            mock.patch("apps.epos_qbo.views.requests.get", return_value=fake_response),
        ):
            response = self.client.post(
                reverse("epos_qbo:api-tokens-refresh", kwargs={"company_key": "company_a"}),
                follow=True,
            )
        msgs = [str(m) for m in response.context["messages"]]
        self.assertTrue(any("tokens refreshed" in m for m in msgs))

    def test_page_renders_with_multiple_companies_in_mixed_states(self):
        """Smoke check: page renders without crashing when companies have varied token states."""
        CompanyConfigRecord.objects.create(
            company_key="company_b",
            display_name="Company B",
            config_json={
                "company_key": "company_b",
                "display_name": "Company B",
                "qbo": {"realm_id": "999888777", "environment": "production"},
                "epos": {"username_env_key": "EPOS_USERNAME_B", "password_env_key": "EPOS_PASSWORD_B"},
            },
        )
        # company_a healthy, company_b missing tokens
        healthy = self._healthy_tokens()

        def fake_load(company_key, realm_id):
            if company_key == "company_a":
                return healthy
            return None

        self._login()
        with mock.patch("apps.epos_qbo.views.load_tokens", side_effect=fake_load):
            response = self.client.get(reverse("epos_qbo:api-tokens"))
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Company A")
        self.assertContains(response, "Company B")
        self.assertContains(response, "Connected")
        self.assertContains(response, "Missing tokens")

    def test_refresh_requires_perm(self):
        self._login()
        with suppress_expected_request_logs():
            response = self.client.post(
                reverse("epos_qbo:api-tokens-refresh", kwargs={"company_key": "company_a"})
            )
        self.assertEqual(response.status_code, 403)
