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
                    "max_catalog_fixes": "3",
                    "max_quantity_adjustments": "8",
                },
            )
        self.assertEqual(response.status_code, 302)
        job = RunJob.objects.get()
        self.assertEqual(job.scope, RunJob.SCOPE_INVENTORY_PIPELINE)
        self.assertEqual(job.company_key, "company_a")
        self.assertNotIn("stock_csv", job.inventory_options_json)
        self.assertEqual(job.inventory_options_json.get("categories"), ["Beverages"])
        self.assertEqual(job.inventory_options_json.get("product_filter"), "Widget")
        self.assertEqual(job.inventory_options_json.get("max_catalog_fixes"), 3)
        self.assertEqual(job.inventory_options_json.get("max_quantity_adjustments"), 8)

    def test_inventory_scope_filters_are_optional(self):
        self.client.login(username="op", password="pw")
        with mock.patch(
            "apps.epos_qbo.views.dispatch_next_queued_job", return_value=(None, "queued")
        ):
            response = self.client.post(
                reverse("epos_qbo:run-trigger-inventory"),
                {
                    "company_key": "company_a",
                },
            )
        self.assertEqual(response.status_code, 302)
        job = RunJob.objects.get()
        self.assertEqual(job.scope, RunJob.SCOPE_INVENTORY_PIPELINE)
        self.assertEqual(job.inventory_options_json.get("max_catalog_fixes"), 5)
        self.assertEqual(job.inventory_options_json.get("max_quantity_adjustments"), 10)

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

    def test_runs_page_shows_simple_inventory_workflow(self):
        self.client.login(username="op", password="pw")
        response = self.client.get(reverse("epos_qbo:runs"))
        self.assertEqual(response.status_code, 200)
        html = response.content.decode("utf-8")
        self.assertIn(">Sales", html)
        self.assertIn(">Inventory", html)
        self.assertEqual(html.count('role="tab"'), 2)
        self.assertIn(f'action="{reverse("epos_qbo:run-trigger-inventory")}"', html)
        self.assertIn("Sync Inventory", html)
        self.assertIn(
            "Downloads EPOS stock, checks QuickBooks inventory, fixes safe pack-variant catalog issues, and syncs quantities.",
            html,
        )
        self.assertIn("Max catalog fixes per run", html)
        self.assertIn("Max quantity adjustments per run", html)
        self.assertNotIn("Catalog Cleanup", html)
        self.assertNotIn("Inventory Audit", html)
