from __future__ import annotations

from unittest import mock

from django.contrib.auth.models import Permission, User
from django.test import TestCase
from django.urls import reverse

from apps.epos_qbo.models import CompanyConfigRecord, RunJob


class AuthAndPermissionsTests(TestCase):
    def setUp(self):
        self.user = User.objects.create_user(username="operator", password="pw12345")
        self.company = CompanyConfigRecord.objects.create(
            company_key="company_a",
            display_name="Company A",
            config_json={
                "company_key": "company_a",
                "display_name": "Company A",
                "qbo": {"realm_id": "123", "deposit_account": "Cash", "tax_mode": "vat_inclusive_7_5"},
                "epos": {"username_env_key": "EPOS_USERNAME_A", "password_env_key": "EPOS_PASSWORD_A"},
                "transform": {
                    "group_by": ["date", "tender"],
                    "date_format": "%Y-%m-%d",
                    "receipt_prefix": "SR",
                    "receipt_number_format": "date_tender_sequence",
                },
                "output": {
                    "csv_prefix": "sales",
                    "metadata_file": "last_transform.json",
                    "uploaded_docnumbers_file": "uploaded_docnumbers.json",
                },
            },
        )

    def test_overview_requires_login(self):
        response = self.client.get(reverse("epos_qbo:overview"))
        self.assertEqual(response.status_code, 302)
        self.assertIn("/login/", response.url)

    def test_new_overview_and_run_status_endpoints_require_login(self):
        response_panels = self.client.get(reverse("epos_qbo:overview-panels"))
        self.assertEqual(response_panels.status_code, 302)
        self.assertIn("/login/", response_panels.url)

        response_active = self.client.get(reverse("epos_qbo:run-active-ids"))
        self.assertEqual(response_active.status_code, 302)
        self.assertIn("/login/", response_active.url)

    def test_trigger_requires_permission(self):
        self.client.login(username="operator", password="pw12345")
        response = self.client.post(
            reverse("epos_qbo:run-trigger"),
            {"scope": "all_companies", "date_mode": "yesterday"},
        )
        self.assertEqual(response.status_code, 403)

    def test_company_create_requires_edit_permission(self):
        self.client.login(username="operator", password="pw12345")
        response = self.client.get(reverse("epos_qbo:company-new"))
        self.assertEqual(response.status_code, 403)

    def test_trigger_with_permission_creates_job(self):
        perm = Permission.objects.get(codename="can_trigger_runs")
        self.user.user_permissions.add(perm)
        self.client.login(username="operator", password="pw12345")

        with mock.patch("apps.epos_qbo.services.job_runner.start_run_job") as mocked_start:
            mocked_start.return_value = mock.Mock()
            response = self.client.post(
                reverse("epos_qbo:run-trigger"),
                {"scope": "single_company", "company_key": "company_a", "date_mode": "yesterday"},
            )
        self.assertEqual(response.status_code, 302)
        self.assertEqual(RunJob.objects.count(), 1)
