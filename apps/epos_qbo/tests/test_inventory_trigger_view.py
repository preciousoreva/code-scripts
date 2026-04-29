from __future__ import annotations

from unittest import mock

from django.contrib.auth.models import Permission, User
from django.test import TestCase
from django.urls import reverse
from django.utils import timezone

from apps.epos_qbo.models import CompanyConfigRecord, RunArtifact, RunJob
from apps.epos_qbo.tests.utils import suppress_expected_request_logs


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
        with suppress_expected_request_logs():
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
                },
            )
        self.assertEqual(response.status_code, 302)
        job = RunJob.objects.get()
        self.assertEqual(job.scope, RunJob.SCOPE_INVENTORY_PIPELINE)
        self.assertEqual(job.company_key, "company_a")
        self.assertNotIn("stock_csv", job.inventory_options_json)
        self.assertEqual(job.inventory_options_json.get("categories"), ["Beverages"])
        self.assertEqual(job.inventory_options_json.get("product_filter"), "Widget")
        self.assertNotIn("max_catalog_fixes", job.inventory_options_json)
        self.assertNotIn("max_quantity_adjustments", job.inventory_options_json)

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
        self.assertEqual(job.inventory_options_json, {})

    def test_product_filter_stores_scope_without_caps(self):
        self.client.login(username="op", password="pw")
        with mock.patch(
            "apps.epos_qbo.views.dispatch_next_queued_job", return_value=(None, "queued")
        ):
            response = self.client.post(
                reverse("epos_qbo:run-trigger-inventory"),
                {
                    "company_key": "company_a",
                    "product_filter": "Widget",
                },
            )
        self.assertEqual(response.status_code, 302)
        job = RunJob.objects.get()
        self.assertEqual(job.inventory_options_json.get("product_filter"), "Widget")
        self.assertNotIn("max_catalog_fixes", job.inventory_options_json)
        self.assertNotIn("max_quantity_adjustments", job.inventory_options_json)

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
            "Downloads EPOS stock, fixes supported QBO catalog issues, and syncs quantities.",
            html,
        )
        self.assertIn("aria-controls=\"sales-run-panel\"", html)
        self.assertIn("aria-controls=\"inventory-run-panel\"", html)
        self.assertNotIn("Catalog fixes limit", html)
        self.assertNotIn("Quantity updates limit", html)
        self.assertNotIn("max_catalog_fixes", html)
        self.assertNotIn("max_quantity_adjustments", html)
        self.assertNotIn("These safety limits cap how many QuickBooks changes can happen", html)
        self.assertNotIn("Max catalog fixes per run", html)
        self.assertNotIn("Max quantity adjustments per run", html)
        self.assertNotIn("Catalog Cleanup", html)
        self.assertNotIn("Inventory Audit", html)

    def test_run_detail_shows_inventory_pipeline_report_artifact(self):
        self.client.login(username="op", password="pw")
        job = RunJob.objects.create(
            scope=RunJob.SCOPE_INVENTORY_PIPELINE,
            company_key="company_a",
            status=RunJob.STATUS_SUCCEEDED,
            exit_code=0,
        )
        RunArtifact.objects.create(
            kind=RunArtifact.KIND_INVENTORY_AUDIT,
            run_job=job,
            company_key="company_a",
            processed_at=timezone.now(),
            source_path="/tmp/inventory_pipeline_company_a_120000.json",
            source_hash="a" * 64,
            reliability_status=RunArtifact.RELIABILITY_HIGH,
            rows_total=1,
            rows_kept=1,
            upload_stats_json={
                "report_type": "inventory_pipeline",
                "products_checked": 147,
                "in_sync": 147,
                "catalog_fixes_applied": 0,
                "base_items_created": 0,
                "duplicate_base_items_resolved": 0,
                "quantity_updates_applied": 0,
                "blocked_items": 0,
            },
        )

        response = self.client.get(reverse("epos_qbo:run-detail", kwargs={"job_id": job.id}))

        self.assertEqual(response.status_code, 200)
        html = response.content.decode("utf-8")
        self.assertIn("Inventory pipeline report", html)
        self.assertIn("Products checked:", html)
        self.assertIn("147", html)
        self.assertIn("Catalog fixes:", html)
        self.assertIn("Blocked items:", html)
        self.assertNotIn("Run succeeded but no artifacts were linked", html)
        self.assertNotIn("Reconciliation did not run or failed", html)

    def test_run_detail_sales_artifact_still_shows_sales_metrics(self):
        self.client.login(username="op", password="pw")
        job = RunJob.objects.create(
            scope=RunJob.SCOPE_SINGLE,
            company_key="company_a",
            status=RunJob.STATUS_SUCCEEDED,
            exit_code=0,
        )
        RunArtifact.objects.create(
            run_job=job,
            company_key="company_a",
            processed_at=timezone.now(),
            source_path="/tmp/sales.json",
            source_hash="b" * 64,
            reliability_status=RunArtifact.RELIABILITY_HIGH,
            rows_kept=12,
            upload_stats_json={"attempted": 12, "uploaded": 10, "skipped": 2, "failed": 0},
        )

        response = self.client.get(reverse("epos_qbo:run-detail", kwargs={"job_id": job.id}))

        self.assertEqual(response.status_code, 200)
        html = response.content.decode("utf-8")
        self.assertIn("Rows kept:", html)
        self.assertIn("QBO uploaded:", html)
        self.assertIn("10", html)
