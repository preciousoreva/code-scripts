from __future__ import annotations

from unittest import mock

from django.contrib.auth.models import User
from django.test import TestCase, override_settings
from django.urls import reverse
from django.utils import timezone

from apps.epos_qbo.models import CompanyConfigRecord, RunArtifact, RunJob
from apps.epos_qbo.services.copilot import build_evidence_pack
from apps.epos_qbo.tests.utils import suppress_expected_request_logs


class CopilotTests(TestCase):
    def setUp(self):
        self.user = User.objects.create_user(username="operator", password="pw12345")
        self.company = CompanyConfigRecord.objects.create(
            company_key="company_a",
            display_name="Company A",
            config_json={
                "qbo": {
                    "realm_id": "123",
                    "environment": "production",
                    "client_secret": "do-not-leak",
                },
                "epos": {
                    "username_env_key": "EPOS_USERNAME_A",
                    "password_env_key": "EPOS_PASSWORD_A",
                },
            },
        )
        self.failed_run = RunJob.objects.create(
            scope=RunJob.SCOPE_SINGLE,
            company_key="company_a",
            status=RunJob.STATUS_FAILED,
            target_date=timezone.localdate(),
            failure_reason="QBO token expired while uploading sales.",
            exit_code=1,
        )
        RunArtifact.objects.create(
            run_job=self.failed_run,
            company_key="company_a",
            target_date=timezone.localdate(),
            processed_at=timezone.now(),
            source_path="/tmp/report.csv",
            source_hash="abc123",
            upload_stats_json={"total_amount": "1000", "access_token": "do-not-leak"},
        )

    def test_endpoint_requires_login(self):
        response = self.client.post(
            reverse("epos_qbo:copilot-ask"),
            data={"question": "What failed?"},
            content_type="application/json",
        )
        self.assertEqual(response.status_code, 302)
        self.assertIn("/login/", response.url)

    @override_settings(OIAT_COPILOT_ENABLED=False)
    def test_disabled_endpoint_returns_controlled_error(self):
        self.client.login(username="operator", password="pw12345")
        with suppress_expected_request_logs(extra_loggers=["apps.epos_qbo.services.copilot"]):
            response = self.client.post(
                reverse("epos_qbo:copilot-ask"),
                data={"question": "What failed?", "context": {"company_key": "company_a"}},
                content_type="application/json",
            )
        self.assertEqual(response.status_code, 503)
        data = response.json()
        self.assertFalse(data["success"])
        self.assertIn("disabled", data["warnings"][0].lower())

    @override_settings(
        OIAT_COPILOT_ENABLED=True,
        OIAT_COPILOT_PROVIDER="openai",
        OIAT_COPILOT_API_KEY="test-key",
        OIAT_COPILOT_API_URL="https://example.test/responses",
        OIAT_COPILOT_MODEL="test-model",
    )
    def test_endpoint_calls_provider_and_returns_sources(self):
        self.client.login(username="operator", password="pw12345")
        fake_response = mock.Mock()
        fake_response.json.return_value = {"output_text": "Company A failed because the QBO token expired."}
        fake_response.raise_for_status.return_value = None

        with mock.patch("apps.epos_qbo.services.copilot.requests.post", return_value=fake_response) as mocked_post:
            response = self.client.post(
                reverse("epos_qbo:copilot-ask"),
                data={"question": "Why did Company A fail?", "context": {"company_key": "company_a"}},
                content_type="application/json",
            )

        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertTrue(data["success"])
        self.assertIn("QBO token", data["answer"])
        self.assertTrue(data["sources"])
        payload = mocked_post.call_args.kwargs["json"]
        self.assertEqual(payload["model"], "test-model")
        self.assertNotIn("do-not-leak", str(payload))

    def test_evidence_pack_redacts_secret_like_fields(self):
        evidence, sources, warnings = build_evidence_pack(
            "Why did Company A fail?",
            {"company_key": "company_a", "path": "/epos-qbo/companies/company_a/"},
        )
        rendered = str(evidence)
        self.assertIn("company_a", rendered)
        self.assertIn("[redacted]", rendered)
        self.assertNotIn("do-not-leak", rendered)
        self.assertTrue(sources)
        self.assertEqual(warnings, [])

    @override_settings(
        OIAT_COPILOT_ENABLED=True,
        OIAT_COPILOT_PROVIDER="openai",
        OIAT_COPILOT_API_KEY="test-key",
        OIAT_COPILOT_API_URL="https://example.test/responses",
        OIAT_COPILOT_MODEL="test-model",
        OIAT_COPILOT_RATE_LIMIT_PER_MINUTE=1,
    )
    def test_endpoint_rate_limits_per_session(self):
        self.client.login(username="operator", password="pw12345")
        fake_response = mock.Mock()
        fake_response.json.return_value = {"output_text": "Company A failed because the QBO token expired."}
        fake_response.raise_for_status.return_value = None
        with mock.patch("apps.epos_qbo.services.copilot.requests.post", return_value=fake_response):
            first = self.client.post(
                reverse("epos_qbo:copilot-ask"),
                data={"question": "What failed?"},
                content_type="application/json",
            )
            second = self.client.post(
                reverse("epos_qbo:copilot-ask"),
                data={"question": "What failed again?"},
                content_type="application/json",
            )
        self.assertEqual(first.status_code, 200)
        self.assertEqual(second.status_code, 429)
        self.assertIn("rate limit", second.json()["warnings"][0].lower())
