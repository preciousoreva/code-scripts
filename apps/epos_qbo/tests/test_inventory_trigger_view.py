from __future__ import annotations

from unittest import mock

from django.contrib.auth.models import Permission, User
from django.test import TestCase
from django.urls import reverse

from apps.epos_qbo.models import CompanyConfigRecord, RunJob


class InventoryTriggerViewTests(TestCase):
    def setUp(self):
        self.user = User.objects.create_user(username="op", password="pw")
        self.user.user_permissions.add(
            Permission.objects.get(codename="can_trigger_runs")
        )
        CompanyConfigRecord.objects.create(
            company_key="company_a",
            display_name="Company A",
            config_json={"company_key": "company_a", "display_name": "Company A"},
        )

    def test_requires_login(self):
        response = self.client.post(reverse("epos_qbo:run-trigger-inventory"))
        self.assertEqual(response.status_code, 302)

    def test_requires_permission(self):
        no_perm = User.objects.create_user(username="np", password="pw")
        self.client.login(username="np", password="pw")
        response = self.client.post(
            reverse("epos_qbo:run-trigger-inventory"),
            {"company_key": "company_a"},
        )
        self.assertEqual(response.status_code, 403)

    def test_rejects_unknown_company(self):
        self.client.login(username="op", password="pw")
        with mock.patch(
            "apps.epos_qbo.views.dispatch_next_queued_job", return_value=(None, "queued")
        ):
            response = self.client.post(
                reverse("epos_qbo:run-trigger-inventory"),
                {"company_key": "ghost"},
                follow=False,
            )
        self.assertEqual(response.status_code, 302)
        self.assertEqual(RunJob.objects.count(), 0)

    def test_creates_queued_inventory_job_with_options(self):
        self.client.login(username="op", password="pw")
        with mock.patch(
            "apps.epos_qbo.views.dispatch_next_queued_job", return_value=(None, "queued")
        ):
            response = self.client.post(
                reverse("epos_qbo:run-trigger-inventory"),
                {
                    "company_key": "company_a",
                    "category": "Beverages",
                    "product_filter": "Widget",
                    "tolerance": "0.0",
                    "mode": "dry_run",
                    "max_adjustments": "3",
                },
            )
        self.assertEqual(response.status_code, 302)
        job = RunJob.objects.get()
        self.assertEqual(job.scope, RunJob.SCOPE_INVENTORY_SYNC)
        self.assertEqual(job.company_key, "company_a")
        self.assertTrue(job.inventory_options_json.get("dry_run"))
        self.assertNotIn("stock_csv", job.inventory_options_json)
        self.assertEqual(job.inventory_options_json.get("categories"), ["Beverages"])
        self.assertEqual(job.inventory_options_json.get("product_filter"), "Widget")
        self.assertEqual(job.inventory_options_json.get("max_adjustments"), 3)

    def test_form_rejects_apply_without_scope(self):
        self.client.login(username="op", password="pw")
        with mock.patch(
            "apps.epos_qbo.views.dispatch_next_queued_job", return_value=(None, "queued")
        ):
            response = self.client.post(
                reverse("epos_qbo:run-trigger-inventory"),
                {
                    "company_key": "company_a",
                    "mode": "apply",
                    "max_adjustments": "3",
                },
            )
        self.assertEqual(response.status_code, 302)
        self.assertEqual(RunJob.objects.count(), 0)

    def test_runs_context_includes_inventory_categories_by_company(self):
        self.client.login(username="op", password="pw")
        with mock.patch(
            "apps.epos_qbo.views.load_inventory_categories_by_company",
            return_value={"company_a": ["ALCOHOLS & SPIRITS"]},
        ):
            response = self.client.get(reverse("epos_qbo:runs"))

        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            response.context["categories_by_company"],
            {"company_a": ["ALCOHOLS & SPIRITS"]},
        )
