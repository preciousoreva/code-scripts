from __future__ import annotations

import re
from datetime import datetime, timezone as dt_timezone
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import mock

from django.conf import settings
from django.contrib.auth import get_user_model
from django.test import TestCase
from django.urls import reverse
from django.utils import timezone

from apps.epos_qbo.models import CompanyConfigRecord, RunArtifact, RunJob
from apps.epos_qbo.services.inventory_review_actions import (
    RETRY_INTENT_CATALOG,
    RETRY_INTENT_QUANTITY,
    REVIEW_CREATE_MISSING_INTENT,
)


class RunJobFriendlyIdTests(TestCase):
    def test_friendly_id_inventory_pipeline(self):
        started = datetime(2026, 4, 29, 14, 52, tzinfo=dt_timezone.utc)
        job = RunJob.objects.create(
            scope=RunJob.SCOPE_INVENTORY_PIPELINE,
            company_key="company_a",
            started_at=started,
        )
        # UUID suffix depends on generated id; validate structure + prefix + timestamp parts.
        self.assertTrue(job.friendly_id.startswith("INV-0429-1452-"))
        self.assertEqual(len(job.friendly_id.split("-")[-1]), 4)

    def test_friendly_id_sales_all_companies(self):
        started = datetime(2026, 4, 28, 18, 0, tzinfo=dt_timezone.utc)
        job = RunJob.objects.create(scope=RunJob.SCOPE_ALL, started_at=started)
        self.assertTrue(job.friendly_id.startswith("SAL-0428-1800-"))
        self.assertEqual(len(job.friendly_id.split("-")[-1]), 4)

    def test_friendly_title_and_internal_id_separation(self):
        started = datetime(2026, 4, 28, 18, 0, tzinfo=dt_timezone.utc)
        job = RunJob.objects.create(scope=RunJob.SCOPE_ALL, started_at=started)
        self.assertIn("Sales Run SAL-0428-1800-", job.friendly_title)
        self.assertIn(str(job.id).split("-", 1)[0][:4].upper(), job.friendly_title)


class RunsAndRunDetailRenderingTests(TestCase):
    def setUp(self):
        User = get_user_model()
        self.user = User.objects.create_user(username="operator", password="pw12345")
        self.client.login(username="operator", password="pw12345")

    def test_runs_table_shows_workflow_and_friendly_id(self):
        started = datetime(2026, 4, 28, 18, 0, tzinfo=dt_timezone.utc)
        job = RunJob.objects.create(scope=RunJob.SCOPE_ALL, started_at=started, status=RunJob.STATUS_SUCCEEDED)
        response = self.client.get(reverse("epos_qbo:runs"))
        self.assertEqual(response.status_code, 200)
        html = response.content.decode("utf-8")
        self.assertIn(job.friendly_id, html)
        self.assertIn("Workflow", html)
        self.assertIn("Sales", html)

    def test_run_detail_shows_friendly_title_and_internal_id(self):
        started = datetime(2026, 4, 29, 14, 52, tzinfo=dt_timezone.utc)
        job = RunJob.objects.create(
            scope=RunJob.SCOPE_INVENTORY_PIPELINE,
            company_key="company_a",
            started_at=started,
            status=RunJob.STATUS_SUCCEEDED,
        )
        response = self.client.get(reverse("epos_qbo:run-detail", kwargs={"job_id": job.id}))
        self.assertEqual(response.status_code, 200)
        html = response.content.decode("utf-8")
        self.assertIn(job.friendly_title, html)
        self.assertIn("Internal ID:", html)
        self.assertIn(str(job.id), html)
        self.assertIn("Target", html)

    def test_run_detail_uses_operator_friendly_artifact_labels(self):
        started = datetime(2026, 4, 28, 18, 0, tzinfo=dt_timezone.utc)
        job = RunJob.objects.create(scope=RunJob.SCOPE_ALL, started_at=started, status=RunJob.STATUS_SUCCEEDED)
        RunArtifact.objects.create(
            run_job=job,
            company_key="company_a",
            kind=RunArtifact.KIND_SALES_UPLOAD,
            processed_at=started,
            source_path="/tmp/last_company_a_transform.json",
            source_hash="hash-1",
            reliability_status=RunArtifact.RELIABILITY_WARNING,
            upload_stats_json={},
        )
        response = self.client.get(reverse("epos_qbo:run-detail", kwargs={"job_id": job.id}))
        html = response.content.decode("utf-8")
        self.assertIn("Sales metadata", html)
        self.assertNotIn("Rolling (last_*)", html)
        self.assertNotIn("last_", html)

    def test_artifact_kind_inventory_audit_renders_inventory_audit_label(self):
        job = RunJob.objects.create(scope=RunJob.SCOPE_INVENTORY_PIPELINE, company_key="company_a", status=RunJob.STATUS_SUCCEEDED)
        RunArtifact.objects.create(
            run_job=job,
            company_key="company_a",
            kind=RunArtifact.KIND_INVENTORY_AUDIT,
            processed_at=datetime(2026, 4, 29, 1, 39, tzinfo=dt_timezone.utc),
            source_path="/data/code_scripts/reports/inventory_sync/2026-04-29/inventory_audit_company_a_initial_014004.json",
            source_hash="a" * 64,
            reliability_status=RunArtifact.RELIABILITY_HIGH,
            upload_stats_json={},
        )
        html = self.client.get(reverse("epos_qbo:run-detail", kwargs={"job_id": job.id})).content.decode("utf-8")
        self.assertIn("Inventory audit", html)
        self.assertNotIn("Sales metadata", html)

    def test_artifact_inventory_pipeline_report_type_renders_inventory_report_label(self):
        job = RunJob.objects.create(scope=RunJob.SCOPE_INVENTORY_PIPELINE, company_key="company_a", status=RunJob.STATUS_SUCCEEDED)
        RunArtifact.objects.create(
            run_job=job,
            company_key="company_a",
            kind=RunArtifact.KIND_INVENTORY_AUDIT,
            processed_at=datetime(2026, 4, 29, 1, 39, tzinfo=dt_timezone.utc),
            source_path="/data/code_scripts/reports/inventory_pipeline/2026-04-29/inventory_pipeline_company_a_014346.json",
            source_hash="b" * 64,
            reliability_status=RunArtifact.RELIABILITY_HIGH,
            upload_stats_json={"report_type": "inventory_pipeline"},
        )
        html = self.client.get(reverse("epos_qbo:run-detail", kwargs={"job_id": job.id})).content.decode("utf-8")
        self.assertIn("Inventory report", html)

    def test_artifact_catalog_cleanup_path_renders_catalog_cleanup_label(self):
        job = RunJob.objects.create(scope=RunJob.SCOPE_INVENTORY_PIPELINE, company_key="company_a", status=RunJob.STATUS_SUCCEEDED)
        RunArtifact.objects.create(
            run_job=job,
            company_key="company_a",
            kind=RunArtifact.KIND_INVENTORY_AUDIT,
            processed_at=datetime(2026, 4, 29, 1, 39, tzinfo=dt_timezone.utc),
            source_path="/data/code_scripts/reports/inventory_sync/2026-04-29/inventory_catalog_cleanup_company_a_014308.csv",
            source_hash="c" * 64,
            reliability_status=RunArtifact.RELIABILITY_HIGH,
            upload_stats_json={},
        )
        html = self.client.get(reverse("epos_qbo:run-detail", kwargs={"job_id": job.id})).content.decode("utf-8")
        self.assertIn("Catalog cleanup report", html)

    def test_unknown_artifact_does_not_render_sales_metadata(self):
        job = RunJob.objects.create(scope=RunJob.SCOPE_INVENTORY_PIPELINE, company_key="company_a", status=RunJob.STATUS_SUCCEEDED)
        RunArtifact.objects.create(
            run_job=job,
            company_key="company_a",
            kind="other",
            processed_at=datetime(2026, 4, 29, 1, 39, tzinfo=dt_timezone.utc),
            source_path="/tmp/unknown_artifact.json",
            source_hash="e" * 64,
            reliability_status=RunArtifact.RELIABILITY_HIGH,
            upload_stats_json={"report_type": "unknown"},
        )
        html = self.client.get(reverse("epos_qbo:run-detail", kwargs={"job_id": job.id})).content.decode("utf-8")
        self.assertIn("Artifact", html)
        self.assertNotIn("Sales metadata", html)

    def test_inventory_pipeline_run_with_audit_sidecars_does_not_label_them_as_sales_metadata(self):
        job = RunJob.objects.create(scope=RunJob.SCOPE_INVENTORY_PIPELINE, company_key="company_a", status=RunJob.STATUS_SUCCEEDED)
        for phase in ("initial", "post_catalog", "final"):
            RunArtifact.objects.create(
                run_job=job,
                company_key="company_a",
                kind=RunArtifact.KIND_INVENTORY_AUDIT,
                processed_at=datetime(2026, 4, 29, 1, 39, tzinfo=dt_timezone.utc),
                source_path=f"/data/code_scripts/reports/inventory_sync/2026-04-29/inventory_audit_company_a_{phase}_014004.json",
                source_hash=(phase[0] * 64),
                reliability_status=RunArtifact.RELIABILITY_HIGH,
                upload_stats_json={},
            )
        RunArtifact.objects.create(
            run_job=job,
            company_key="company_a",
            kind=RunArtifact.KIND_INVENTORY_AUDIT,
            processed_at=datetime(2026, 4, 29, 1, 40, tzinfo=dt_timezone.utc),
            source_path="/data/code_scripts/reports/inventory_pipeline/2026-04-29/inventory_pipeline_company_a_014346.json",
            source_hash="p" * 64,
            reliability_status=RunArtifact.RELIABILITY_HIGH,
            upload_stats_json={"report_type": "inventory_pipeline"},
        )
        html = self.client.get(reverse("epos_qbo:run-detail", kwargs={"job_id": job.id})).content.decode("utf-8")
        self.assertIn("Inventory report", html)
        self.assertIn("Inventory audit", html)
        self.assertNotIn("Sales metadata", html)

    def test_inventory_product_run_detail_shows_target_product(self):
        job = RunJob.objects.create(
            scope=RunJob.SCOPE_INVENTORY_PIPELINE,
            company_key="company_a",
            status=RunJob.STATUS_SUCCEEDED,
            inventory_options_json={"product_filter": "MALTONIC MALT DRINK CAN33cl*24"},
        )
        response = self.client.get(reverse("epos_qbo:run-detail", kwargs={"job_id": job.id}))
        html = response.content.decode("utf-8")
        self.assertIn("Workflow", html)
        self.assertIn("Inventory", html)
        self.assertIn("Target", html)
        self.assertIn("Product: MALTONIC MALT DRINK CAN33cl*24", html)
        self.assertNotIn(">Scope<", html)

    def test_inventory_category_run_detail_shows_target_category(self):
        job = RunJob.objects.create(
            scope=RunJob.SCOPE_INVENTORY_PIPELINE,
            company_key="company_a",
            status=RunJob.STATUS_SUCCEEDED,
            inventory_options_json={"categories": ["ALCOHOLS & SPIRITS"]},
        )
        html = self.client.get(reverse("epos_qbo:run-detail", kwargs={"job_id": job.id})).content.decode("utf-8")
        self.assertIn("Category: ALCOHOLS &amp; SPIRITS", html)

    def test_inventory_category_and_product_run_detail_shows_both(self):
        job = RunJob.objects.create(
            scope=RunJob.SCOPE_INVENTORY_PIPELINE,
            company_key="company_a",
            status=RunJob.STATUS_SUCCEEDED,
            inventory_options_json={"categories": ["ALCOHOLS & SPIRITS"], "product_filter": "ACTION BITTERS50ml"},
        )
        html = self.client.get(reverse("epos_qbo:run-detail", kwargs={"job_id": job.id})).content.decode("utf-8")
        self.assertIn("Category: ALCOHOLS &amp; SPIRITS; Product: ACTION BITTERS50ml", html)

    def test_inventory_no_filter_run_detail_shows_all_products(self):
        job = RunJob.objects.create(
            scope=RunJob.SCOPE_INVENTORY_PIPELINE,
            company_key="company_a",
            status=RunJob.STATUS_SUCCEEDED,
            inventory_options_json={},
        )
        html = self.client.get(reverse("epos_qbo:run-detail", kwargs={"job_id": job.id})).content.decode("utf-8")
        self.assertIn("All products", html)

    def test_sales_all_companies_run_detail_shows_target_all_companies(self):
        job = RunJob.objects.create(scope=RunJob.SCOPE_ALL, status=RunJob.STATUS_SUCCEEDED)
        html = self.client.get(reverse("epos_qbo:run-detail", kwargs={"job_id": job.id})).content.decode("utf-8")
        self.assertIn("Workflow", html)
        self.assertIn("Sales", html)
        self.assertIn("Target", html)
        self.assertIn("All companies", html)

    def test_sales_single_company_run_detail_prefers_company_display_name(self):
        CompanyConfigRecord.objects.create(company_key="company_a", display_name="ACME LTD.", config_json={})
        job = RunJob.objects.create(scope=RunJob.SCOPE_SINGLE, company_key="company_a", status=RunJob.STATUS_SUCCEEDED)
        html = self.client.get(reverse("epos_qbo:run-detail", kwargs={"job_id": job.id})).content.decode("utf-8")
        self.assertIn("Company: ACME LTD.", html)

    def test_run_detail_without_review_retry_does_not_show_inventory_review_action_card(self):
        job = RunJob.objects.create(
            scope=RunJob.SCOPE_INVENTORY_PIPELINE,
            company_key="company_a",
            status=RunJob.STATUS_SUCCEEDED,
            inventory_options_json={"product_filter": "SOME PRODUCT"},
        )
        html = self.client.get(reverse("epos_qbo:run-detail", kwargs={"job_id": job.id})).content.decode("utf-8")
        self.assertNotIn("Inventory Review Action", html)

    def test_run_detail_review_retry_shows_catalog_cleanup_metadata(self):
        affected = [f"CHEESE ITEM {i:02d}" for i in range(12)]
        job = RunJob.objects.create(
            scope=RunJob.SCOPE_INVENTORY_PIPELINE,
            company_key="company_a",
            status=RunJob.STATUS_QUEUED,
            inventory_options_json={
                "mode": "catalog_plan_only",
                "base_names": affected,
                "max_catalog_fixes": 12,
                "max_quantity_adjustments": 0,
                "review_retry": {
                    "intent": RETRY_INTENT_CATALOG,
                    "source_artifact_id": 42,
                    "source_final_audit": "/data/reports/inventory_pipeline/2026-04-29/final_audit_company_a.csv",
                    "affected_base_names": affected,
                    "row_count": 12,
                },
            },
        )
        html = self.client.get(reverse("epos_qbo:run-detail", kwargs={"job_id": job.id})).content.decode("utf-8")
        self.assertIn("Inventory Review Action", html)
        self.assertIn("Catalog cleanup retry", html)
        self.assertIn("Inventory Mode", html)
        self.assertIn("Catalog plan only", html)
        self.assertIn("Plan catalog cleanup", html)
        self.assertIn("This run was queued from the Inventory Review page", html)
        self.assertIn("Selected base names only", html)
        self.assertIn("final_audit_company_a.csv", html)
        self.assertIn("CHEESE ITEM 00", html)
        self.assertIn("and 2 more", html)
        self.assertIn("Catalog fixes: 12", html)
        self.assertIn("Quantity adjustments: 0", html)
        self.assertRegex(
            html,
            re.compile(
                r"Affected items</dt>\s*<dd class=\"font-medium text-slate-900 dark:text-slate-100\">12</dd>"
            ),
        )

    def test_run_detail_review_retry_shows_quantity_adjustment_metadata(self):
        affected = ["WIDGET A", "WIDGET B"]
        job = RunJob.objects.create(
            scope=RunJob.SCOPE_INVENTORY_PIPELINE,
            company_key="company_a",
            status=RunJob.STATUS_QUEUED,
            inventory_options_json={
                "mode": "opening_balance_correction_preview",
                "base_names": affected,
                "max_catalog_fixes": 0,
                "max_quantity_adjustments": 3,
                "review_retry": {
                    "intent": RETRY_INTENT_QUANTITY,
                    "source_artifact_id": 7,
                    "source_final_audit": "/tmp/other_final.csv",
                    "affected_base_names": affected,
                    "row_count": 2,
                },
            },
        )
        html = self.client.get(reverse("epos_qbo:run-detail", kwargs={"job_id": job.id})).content.decode("utf-8")
        self.assertIn("Quantity adjustment retry", html)
        self.assertIn("Inventory Mode", html)
        self.assertIn("Opening balance correction preview", html)
        self.assertIn("Preview opening balance correction", html)
        self.assertIn("other_final.csv", html)
        self.assertIn("WIDGET A", html)
        self.assertIn("Catalog fixes: 0", html)
        self.assertIn("Quantity adjustments: 3", html)
        self.assertIn("WIDGET B", html)
        self.assertRegex(
            html,
            re.compile(
                r"Affected items</dt>\s*<dd class=\"font-medium text-slate-900 dark:text-slate-100\">2</dd>"
            ),
        )

    def test_run_detail_review_create_missing_shows_item_creation_metadata(self):
        affected = [f"SAFE ITEM {i:02d}" for i in range(12)]
        job = RunJob.objects.create(
            scope=RunJob.SCOPE_INVENTORY_PIPELINE,
            company_key="company_a",
            status=RunJob.STATUS_QUEUED,
            inventory_options_json={
                "base_names": affected,
                "max_catalog_fixes": 0,
                "max_quantity_adjustments": 0,
                "txn_date": "2026-04-27",
                "review_create_missing_items": {
                    "intent": REVIEW_CREATE_MISSING_INTENT,
                    "source_artifact_id": 9,
                    "source_final_audit": "/data/reports/final_audit_company_a.csv",
                    "affected_base_names": affected,
                    "row_count": 12,
                    "safe_count": 12,
                    "blocked_count": 3,
                    "total_candidates_in_scope": 15,
                    "category_filter": None,
                    "category_label": "All categories",
                    "create_qty_policy": "initial_qty_from_epos",
                    "mapping_source": "Product.Mapping.csv",
                    "item_inv_start_date": "2026-04-27",
                    "txn_date_source": "test.fixture",
                },
            },
        )
        html = self.client.get(reverse("epos_qbo:run-detail", kwargs={"job_id": job.id})).content.decode("utf-8")
        self.assertIn("Inventory Review Action", html)
        self.assertIn("Missing item creation", html)
        self.assertIn("Safe missing QBO candidates only", html)
        self.assertIn("final_audit_company_a.csv", html)
        self.assertIn("Product.Mapping.csv", html)
        self.assertIn("SAFE ITEM 00", html)
        self.assertIn("and 2 more", html)
        self.assertIn("Catalog fixes: 0", html)
        self.assertIn("Quantity adjustments: 0", html)
        self.assertRegex(
            html,
            re.compile(
                r"Safe candidates</dt>\s*<dd class=\"font-medium text-slate-900 dark:text-slate-100\">12</dd>"
            ),
        )
        self.assertRegex(
            html,
            re.compile(
                r"Blocked candidates</dt>\s*<dd class=\"font-medium text-slate-900 dark:text-slate-100\">3</dd>"
            ),
        )
        self.assertIn("InvStartDate", html)
        self.assertIn("2026-04-27", html)
        self.assertIn("test.fixture", html)
        self.assertIn("Category scope", html)
        self.assertIn("All categories", html)
        self.assertIn("Candidates in scope", html)

    def test_run_detail_review_create_missing_shows_per_item_report_link(self):
        with TemporaryDirectory(dir=str(settings.BASE_DIR)) as td:
            td_path = Path(td)
            report_csv = td_path / "inventory_review_missing_create_company_a_120000.csv"
            report_csv.write_text("suggested_qbo_name,outcome\nX,created\n", encoding="utf-8")
            summary_json = td_path / "inventory_pipeline_company_a_120000.json"
            summary_json.write_text("{}", encoding="utf-8")
            job = RunJob.objects.create(
                scope=RunJob.SCOPE_INVENTORY_PIPELINE,
                company_key="company_a",
                status=RunJob.STATUS_SUCCEEDED,
                inventory_options_json={
                    "txn_date": "2026-04-26",
                    "review_create_missing_items": {
                        "intent": REVIEW_CREATE_MISSING_INTENT,
                        "item_inv_start_date": "2026-04-26",
                        "txn_date_source": "test",
                    },
                },
            )
            RunArtifact.objects.create(
                kind=RunArtifact.KIND_INVENTORY_AUDIT,
                run_job=job,
                company_key="company_a",
                processed_at=timezone.now(),
                source_path=str(summary_json),
                source_hash="x" * 64,
                reliability_status=RunArtifact.RELIABILITY_HIGH,
                upload_stats_json={
                    "report_type": "inventory_pipeline",
                    "summary_json": str(summary_json),
                    "child_reports": {"review_missing_create_report": str(report_csv)},
                },
            )
            with mock.patch(
                "apps.epos_qbo.views._trusted_report_roots",
                return_value=[td_path.resolve()],
            ):
                html = self.client.get(
                    reverse("epos_qbo:run-detail", kwargs={"job_id": job.id})
                ).content.decode("utf-8")

        self.assertIn("Per-item report", html)
        self.assertIn("Missing item creation report", html)
