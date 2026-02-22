from __future__ import annotations

import json
from unittest import mock

from django.contrib.auth.models import Permission, User
from django.test import TestCase
from django.urls import reverse

from apps.epos_qbo.models import CompanyConfigRecord


class ToolsPageTests(TestCase):
    def setUp(self):
        self.user = User.objects.create_user(username="operator", password="pw12345")
        self.company = CompanyConfigRecord.objects.create(
            company_key="company_a",
            display_name="Company A",
            config_json={
                "company_key": "company_a",
                "display_name": "Company A",
                "qbo": {"realm_id": "123"},
                "epos": {"username_env_key": "EPOS_USERNAME_A", "password_env_key": "EPOS_PASSWORD_A"},
                "transform": {"group_by": ["date", "tender"], "date_format": "%Y-%m-%d", "receipt_prefix": "SR", "receipt_number_format": "date_tender_sequence"},
                "output": {"csv_prefix": "sales", "metadata_file": "last.json", "uploaded_docnumbers_file": "uploaded.json"},
            },
        )

    def _grant_trigger_perm(self):
        perm = Permission.objects.get(codename="can_trigger_runs")
        self.user.user_permissions.add(perm)

    def test_tools_requires_login(self):
        response = self.client.get(reverse("epos_qbo:tools"))
        self.assertEqual(response.status_code, 302)
        self.assertIn("/login/", response.url)

    def test_tools_requires_permission(self):
        self.client.login(username="operator", password="pw12345")
        response = self.client.get(reverse("epos_qbo:tools"))
        self.assertEqual(response.status_code, 403)

    def test_tools_renders_for_authorized_user(self):
        self._grant_trigger_perm()
        self.client.login(username="operator", password="pw12345")
        response = self.client.get(reverse("epos_qbo:tools"))
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Tools")
        self.assertContains(response, "QBO Query")
        self.assertContains(response, "Verify Mapping Accounts")
        self.assertContains(response, "Company A")

    def test_tools_no_template_token_leak(self):
        self._grant_trigger_perm()
        self.client.login(username="operator", password="pw12345")
        response = self.client.get(reverse("epos_qbo:tools"))
        html = response.content.decode("utf-8")
        self.assertNotIn("{{", html)
        self.assertNotIn("{%", html)


class QBOQueryAPITests(TestCase):
    def setUp(self):
        self.user = User.objects.create_user(username="operator", password="pw12345")
        self.company = CompanyConfigRecord.objects.create(
            company_key="company_a",
            display_name="Company A",
            config_json={
                "company_key": "company_a",
                "display_name": "Company A",
                "qbo": {"realm_id": "123"},
                "epos": {"username_env_key": "EPOS_USERNAME_A", "password_env_key": "EPOS_PASSWORD_A"},
                "transform": {"group_by": ["date", "tender"], "date_format": "%Y-%m-%d", "receipt_prefix": "SR", "receipt_number_format": "date_tender_sequence"},
                "output": {"csv_prefix": "sales", "metadata_file": "last.json", "uploaded_docnumbers_file": "uploaded.json"},
            },
        )
        perm = Permission.objects.get(codename="can_trigger_runs")
        self.user.user_permissions.add(perm)

    def test_requires_post(self):
        self.client.login(username="operator", password="pw12345")
        response = self.client.get(reverse("epos_qbo:tools-qbo-query"))
        self.assertEqual(response.status_code, 405)

    def test_missing_company(self):
        self.client.login(username="operator", password="pw12345")
        response = self.client.post(
            reverse("epos_qbo:tools-qbo-query"),
            data=json.dumps({"query": "select Id from Item"}),
            content_type="application/json",
        )
        self.assertEqual(response.status_code, 400)
        data = response.json()
        self.assertFalse(data["success"])
        self.assertIn("Company is required", data["error"])

    def test_empty_query(self):
        self.client.login(username="operator", password="pw12345")
        response = self.client.post(
            reverse("epos_qbo:tools-qbo-query"),
            data=json.dumps({"company_key": "company_a", "query": ""}),
            content_type="application/json",
        )
        self.assertEqual(response.status_code, 400)
        data = response.json()
        self.assertFalse(data["success"])
        self.assertIn("Query is required", data["error"])

    def test_unknown_company(self):
        self.client.login(username="operator", password="pw12345")
        response = self.client.post(
            reverse("epos_qbo:tools-qbo-query"),
            data=json.dumps({"company_key": "nonexistent", "query": "select Id from Item"}),
            content_type="application/json",
        )
        self.assertEqual(response.status_code, 400)
        data = response.json()
        self.assertIn("Unknown or inactive", data["error"])

    @mock.patch("apps.epos_qbo.views.subprocess.run")
    def test_successful_query(self, mock_run):
        mock_run.return_value = mock.Mock(
            returncode=0,
            stdout='{"QueryResponse": {"Item": [{"Id": "1", "Name": "Test"}]}}',
            stderr="",
        )
        self.client.login(username="operator", password="pw12345")
        response = self.client.post(
            reverse("epos_qbo:tools-qbo-query"),
            data=json.dumps({"company_key": "company_a", "query": "select Id, Name from Item maxresults 1"}),
            content_type="application/json",
        )
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertTrue(data["success"])
        self.assertIn("QueryResponse", data["data"])

    @mock.patch("apps.epos_qbo.views.subprocess.run")
    def test_script_failure(self, mock_run):
        mock_run.return_value = mock.Mock(returncode=1, stdout="", stderr="Error: token expired")
        self.client.login(username="operator", password="pw12345")
        response = self.client.post(
            reverse("epos_qbo:tools-qbo-query"),
            data=json.dumps({"company_key": "company_a", "query": "select Id from Item"}),
            content_type="application/json",
        )
        self.assertEqual(response.status_code, 502)
        data = response.json()
        self.assertFalse(data["success"])
        self.assertIn("token expired", data["error"])


class VerifyMappingAPITests(TestCase):
    def setUp(self):
        self.user = User.objects.create_user(username="operator", password="pw12345")
        self.company = CompanyConfigRecord.objects.create(
            company_key="company_a",
            display_name="Company A",
            config_json={
                "company_key": "company_a",
                "display_name": "Company A",
                "qbo": {"realm_id": "123"},
                "epos": {"username_env_key": "EPOS_USERNAME_A", "password_env_key": "EPOS_PASSWORD_A"},
                "transform": {"group_by": ["date", "tender"], "date_format": "%Y-%m-%d", "receipt_prefix": "SR", "receipt_number_format": "date_tender_sequence"},
                "output": {"csv_prefix": "sales", "metadata_file": "last.json", "uploaded_docnumbers_file": "uploaded.json"},
            },
        )
        perm = Permission.objects.get(codename="can_trigger_runs")
        self.user.user_permissions.add(perm)

    def test_requires_post(self):
        self.client.login(username="operator", password="pw12345")
        response = self.client.get(reverse("epos_qbo:tools-verify-mapping"))
        self.assertEqual(response.status_code, 405)

    def test_missing_company(self):
        self.client.login(username="operator", password="pw12345")
        response = self.client.post(
            reverse("epos_qbo:tools-verify-mapping"),
            data=json.dumps({}),
            content_type="application/json",
        )
        self.assertEqual(response.status_code, 400)
        data = response.json()
        self.assertFalse(data["success"])

    @mock.patch("apps.epos_qbo.views.subprocess.run")
    def test_successful_verify(self, mock_run):
        mock_run.return_value = mock.Mock(
            returncode=0,
            stdout="All 15 accounts verified OK.\n",
            stderr="",
        )
        self.client.login(username="operator", password="pw12345")
        response = self.client.post(
            reverse("epos_qbo:tools-verify-mapping"),
            data=json.dumps({"company_key": "company_a"}),
            content_type="application/json",
        )
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertTrue(data["success"])
        self.assertIn("All 15 accounts", data["output"])
