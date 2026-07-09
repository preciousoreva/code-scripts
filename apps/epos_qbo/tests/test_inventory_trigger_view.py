from __future__ import annotations

from pathlib import Path
from tempfile import TemporaryDirectory
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
            config_json={
                "company_key": "company_a",
                "display_name": "Company A",
                "inventory": {"enable_inventory_items": True},
            },
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
        self.assertEqual(job.inventory_options_json.get("mode"), "audit_only")
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
        self.assertEqual(job.inventory_options_json, {"mode": "audit_only"})

    def test_quantity_preview_action_queues_preview_mode(self):
        self.client.login(username="op", password="pw")
        with mock.patch(
            "apps.epos_qbo.views.dispatch_next_queued_job", return_value=(None, "queued")
        ):
            response = self.client.post(
                reverse("epos_qbo:run-trigger-inventory"),
                {
                    "company_key": "company_a",
                    "mode": "quantity_preview",
                },
            )
        self.assertEqual(response.status_code, 302)
        job = RunJob.objects.get()
        self.assertEqual(job.inventory_options_json, {"mode": "quantity_preview"})

    def test_catalog_plan_action_queues_plan_mode(self):
        self.client.login(username="op", password="pw")
        with mock.patch(
            "apps.epos_qbo.views.dispatch_next_queued_job", return_value=(None, "queued")
        ):
            response = self.client.post(
                reverse("epos_qbo:run-trigger-inventory"),
                {
                    "company_key": "company_a",
                    "mode": "catalog_plan_only",
                },
            )
        self.assertEqual(response.status_code, 302)
        job = RunJob.objects.get()
        self.assertEqual(job.inventory_options_json, {"mode": "catalog_plan_only"})

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
        self.assertEqual(job.inventory_options_json.get("mode"), "audit_only")
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
        self.assertIn("Run Inventory Review", html)
        self.assertNotIn("Run Inventory Audit", html)
        self.assertNotIn("Preview Quantity Adjustments", html)
        self.assertNotIn("Catalog Cleanup Plan", html)
        self.assertIn(
            "Run one read-only inventory review.",
            html,
        )
        self.assertIn("no QBO inventory writes are made", html)
        self.assertIn("aria-controls=\"sales-run-panel\"", html)
        self.assertIn("aria-controls=\"inventory-run-panel\"", html)
        self.assertNotIn("Catalog fixes limit", html)
        self.assertNotIn("Quantity updates limit", html)
        self.assertNotIn("max_catalog_fixes", html)
        self.assertNotIn("max_quantity_adjustments", html)
        self.assertNotIn("These safety limits cap how many QuickBooks changes can happen", html)
        self.assertNotIn("Max catalog fixes per run", html)
        self.assertNotIn("Max quantity adjustments per run", html)
        self.assertNotIn("Catalog Apply", html)

    def test_company_detail_consolidates_inventory_actions(self):
        self.client.login(username="op", password="pw")
        response = self.client.get(
            reverse("epos_qbo:company-detail", kwargs={"company_key": "company_a"})
        )

        self.assertEqual(response.status_code, 200)
        html = response.content.decode("utf-8")
        self.assertIn("Run Inventory Review", html)
        self.assertIn("Run Sales Sync", html)
        self.assertNotIn("Run Inventory Audit", html)
        self.assertNotIn("Preview Quantity Adjustments", html)
        self.assertNotIn("Preview Opening Balance Correction", html)
        self.assertNotIn("Catalog Cleanup Plan", html)

    def test_runs_page_hides_inventory_trigger_when_no_company_enabled(self):
        CompanyConfigRecord.objects.update(
            config_json={"company_key": "company_a", "display_name": "Company A"}
        )
        self.client.login(username="op", password="pw")
        response = self.client.get(reverse("epos_qbo:runs"))

        self.assertEqual(response.status_code, 200)
        html = response.content.decode("utf-8")
        self.assertNotIn(f'action="{reverse("epos_qbo:run-trigger-inventory")}"', html)
        self.assertIn("No companies currently have inventory enabled.", html)

    def test_run_detail_shows_inventory_pipeline_report_artifact(self):
        self.client.login(username="op", password="pw")
        job = RunJob.objects.create(
            scope=RunJob.SCOPE_INVENTORY_PIPELINE,
            company_key="company_a",
            status=RunJob.STATUS_SUCCEEDED,
            exit_code=0,
        )
        # Reports must be under settings.BASE_DIR to be eligible for download links.
        from django.conf import settings

        with TemporaryDirectory(dir=str(settings.BASE_DIR)) as td:
            base = Path(td)
            summary_json = base / "inventory_pipeline_company_a_120000.json"
            summary_json.write_text("{}", encoding="utf-8")
            summary_csv = base / "inventory_pipeline_company_a_120000.csv"
            summary_csv.write_text("sku,status\nABC,in_sync\n", encoding="utf-8")
            final_audit = base / "inventory_audit_company_a_final_120000.csv"
            final_audit.write_text("base_name,status\nWidget,in_sync\n", encoding="utf-8")
            initial_audit = base / "inventory_audit_company_a_initial_120000.csv"
            initial_audit.write_text("base_name,status\nWidget,in_sync\n", encoding="utf-8")
            catalog_cleanup = base / "inventory_catalog_cleanup_company_a_120000.csv"
            catalog_cleanup.write_text("base_name,planned_action\nWidget,noop\n", encoding="utf-8")

            artifact = RunArtifact.objects.create(
                kind=RunArtifact.KIND_INVENTORY_AUDIT,
                run_job=job,
                company_key="company_a",
                processed_at=timezone.now(),
                source_path=str(summary_json),
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
                    "summary_json": str(summary_json),
                    "summary_csv": str(summary_csv),
                    "child_reports": {
                        "final_audit": str(final_audit),
                        "initial_audit": str(initial_audit),
                        "catalog_cleanup": str(catalog_cleanup),
                    },
                },
            )

            response = self.client.get(reverse("epos_qbo:run-detail", kwargs={"job_id": job.id}))

            self.assertEqual(response.status_code, 200)
            html = response.content.decode("utf-8")
            self.assertIn("Inventory report", html)
            self.assertIn("Products:", html)
            self.assertIn("147", html)
            self.assertIn("catalog", html)
            self.assertIn("blocked", html)
            self.assertIn("Summary CSV", html)
            self.assertNotIn("Summary JSON", html)
            self.assertIn("Final Audit", html)
            self.assertIn("Initial Audit", html)
            self.assertIn("Catalog Cleanup", html)
            self.assertIn('<select x-model="selectedReport"', html)
            summary_csv_url = reverse(
                "epos_qbo:run-artifact-report",
                kwargs={"job_id": job.id, "artifact_id": artifact.id, "report_key": "summary_csv"},
            )
            summary_json_url = reverse(
                "epos_qbo:run-artifact-report",
                kwargs={"job_id": job.id, "artifact_id": artifact.id, "report_key": "summary_json"},
            )
            self.assertIn(f'<option value="{summary_csv_url}">Summary CSV</option>', html)
            self.assertNotIn(summary_json_url, html)
            self.assertIn('x-model="selectedReport"', html)
            self.assertIn(':href="selectedReport"', html)
            self.assertNotIn("Run succeeded but no artifacts were linked", html)
            self.assertNotIn("Reconciliation did not run or failed", html)

    def test_authenticated_user_can_download_inventory_report_file(self):
        self.client.login(username="op", password="pw")
        from django.conf import settings

        with TemporaryDirectory(dir=str(settings.BASE_DIR)) as td:
            summary_csv = Path(td) / "inventory_pipeline_company_a_120000.csv"
            summary_csv.write_text("sku,status\nABC,in_sync\n", encoding="utf-8")
            summary_json = Path(td) / "inventory_pipeline_company_a_120000.json"
            summary_json.write_text("{}", encoding="utf-8")
            job = RunJob.objects.create(
                scope=RunJob.SCOPE_INVENTORY_PIPELINE,
                company_key="company_a",
                status=RunJob.STATUS_SUCCEEDED,
                exit_code=0,
            )
            artifact = RunArtifact.objects.create(
                kind=RunArtifact.KIND_INVENTORY_AUDIT,
                run_job=job,
                company_key="company_a",
                processed_at=timezone.now(),
                source_path=str(summary_json),
                source_hash="c" * 64,
                reliability_status=RunArtifact.RELIABILITY_HIGH,
                upload_stats_json={
                    "report_type": "inventory_pipeline",
                    "summary_csv": str(summary_csv),
                    "summary_json": str(summary_json),
                },
            )

            response = self.client.get(
                reverse(
                    "epos_qbo:run-artifact-report",
                    kwargs={"job_id": job.id, "artifact_id": artifact.id, "report_key": "summary_csv"},
                )
            )

            self.assertEqual(response.status_code, 200)
            self.assertEqual(b"".join(response.streaming_content), b"sku,status\nABC,in_sync\n")
            self.assertIn("inventory_pipeline_company_a_120000.csv", response.headers["Content-Disposition"])

    def test_operational_report_root_paths_render_and_download(self):
        self.client.login(username="op", password="pw")
        with TemporaryDirectory() as td:
            ops_reports_root = Path(td) / "code_scripts" / "reports"
            pipeline_dir = ops_reports_root / "inventory_pipeline" / "2026-04-30"
            sync_dir = ops_reports_root / "inventory_sync" / "2026-04-30"
            cleanup_dir = ops_reports_root / "inventory_catalog_cleanup" / "2026-04-30"
            pipeline_dir.mkdir(parents=True)
            sync_dir.mkdir(parents=True)
            cleanup_dir.mkdir(parents=True)
            summary_json = pipeline_dir / "inventory_pipeline_company_a_191504.json"
            summary_csv = pipeline_dir / "inventory_pipeline_company_a_191504.csv"
            final_audit = sync_dir / "inventory_audit_company_a_final_191504.csv"
            initial_audit = sync_dir / "inventory_audit_company_a_initial_180545.csv"
            catalog_cleanup = cleanup_dir / "inventory_catalog_cleanup_company_a_pipeline_180558.csv"
            post_catalog = sync_dir / "inventory_audit_company_a_post_catalog_184100.csv"
            missing_create = pipeline_dir / "inventory_review_missing_create_company_a_120001.csv"
            summary_json.write_text("{}", encoding="utf-8")
            summary_csv.write_text("sku,status\nABC,in_sync\n", encoding="utf-8")
            missing_create.write_text("suggested_qbo_name,outcome\nFOO,created\n", encoding="utf-8")
            final_audit.write_text("base_name,status\nABC,in_sync\n", encoding="utf-8")
            initial_audit.write_text("base_name,status\nABC,needs_review\n", encoding="utf-8")
            catalog_cleanup.write_text("base_name,planned_action\nABC,noop\n", encoding="utf-8")
            post_catalog.write_text("base_name,status\nABC,in_sync\n", encoding="utf-8")
            job = RunJob.objects.create(
                scope=RunJob.SCOPE_INVENTORY_PIPELINE,
                company_key="company_a",
                status=RunJob.STATUS_SUCCEEDED,
                exit_code=0,
            )
            artifact = RunArtifact.objects.create(
                kind=RunArtifact.KIND_INVENTORY_AUDIT,
                run_job=job,
                company_key="company_a",
                processed_at=timezone.now(),
                source_path=str(summary_json),
                source_hash="g" * 64,
                reliability_status=RunArtifact.RELIABILITY_HIGH,
                upload_stats_json={
                    "report_type": "inventory_pipeline",
                    "products_checked": 3345,
                    "in_sync": 3206,
                    "blocked_items": 139,
                    "summary_json": str(summary_json),
                    "summary_csv": str(summary_csv),
                    "child_reports": {
                        "final_audit": str(final_audit),
                        "initial_audit": str(initial_audit),
                        "catalog_cleanup": str(catalog_cleanup),
                        "post_catalog_audit": str(post_catalog),
                        "review_missing_create_report": str(missing_create),
                    },
                },
            )

            with mock.patch(
                "apps.epos_qbo.views._trusted_report_roots",
                return_value=[ops_reports_root.resolve()],
            ):
                detail = self.client.get(reverse("epos_qbo:run-detail", kwargs={"job_id": job.id}))
                download = self.client.get(
                    reverse(
                        "epos_qbo:run-artifact-report",
                        kwargs={"job_id": job.id, "artifact_id": artifact.id, "report_key": "summary_csv"},
                    )
                )
                download_missing = self.client.get(
                    reverse(
                        "epos_qbo:run-artifact-report",
                        kwargs={
                            "job_id": job.id,
                            "artifact_id": artifact.id,
                            "report_key": "review_missing_create_report",
                        },
                    )
                )

        self.assertEqual(detail.status_code, 200)
        html = detail.content.decode("utf-8")
        self.assertIn("Missing item creation report", html)
        self.assertEqual(download_missing.status_code, 200)
        self.assertIn(b"suggested_qbo_name", b"".join(download_missing.streaming_content))
        self.assertIn("Summary CSV", html)
        self.assertNotIn("Summary JSON", html)
        self.assertIn("Final Audit", html)
        self.assertIn("Initial Audit", html)
        self.assertIn("Catalog Cleanup", html)
        self.assertIn("Post Catalog Audit", html)
        self.assertIn('<select x-model="selectedReport"', html)
        self.assertEqual(download.status_code, 200)
        self.assertEqual(b"".join(download.streaming_content), b"sku,status\nABC,in_sync\n")

    def test_run_detail_shows_inventory_mode_summary_metadata(self):
        self.client.login(username="op", password="pw")
        job = RunJob.objects.create(
            scope=RunJob.SCOPE_INVENTORY_PIPELINE,
            company_key="company_a",
            status=RunJob.STATUS_SUCCEEDED,
            inventory_options_json={"mode": "quantity_preview"},
        )
        RunArtifact.objects.create(
            run_job=job,
            company_key="company_a",
            kind=RunArtifact.KIND_INVENTORY_AUDIT,
            source_path="/tmp/inventory_pipeline_company_a.json",
            source_hash="inventory-summary-mode",
            upload_stats_json={
                "report_type": "inventory_pipeline",
                "inventory_mode": "quantity_preview",
                "write_intent": "preview_quantity_adjustments",
                "qbo_write_attempted": False,
                "qbo_write_blocked": False,
                "catalog_apply_enabled": False,
                "quantity_apply_enabled": False,
                "missing_item_create_enabled": False,
            },
        )

        html = self.client.get(reverse("epos_qbo:run-detail", kwargs={"job_id": job.id})).content.decode("utf-8")

        self.assertIn("Inventory Mode", html)
        self.assertIn("Preview only", html)
        self.assertIn("preview_quantity_adjustments", html)
        self.assertIn("QBO write attempted", html)
        self.assertIn("Missing item create enabled", html)
        self.assertNotIn("Quantity apply enabled", html)

    def test_report_download_rejects_existing_file_outside_trusted_roots(self):
        self.client.login(username="op", password="pw")
        with TemporaryDirectory() as trusted_td, TemporaryDirectory() as outside_td:
            trusted_root = Path(trusted_td) / "reports"
            trusted_root.mkdir()
            outside_file = Path(outside_td) / "summary.csv"
            outside_file.write_text("ok\n", encoding="utf-8")
            job = RunJob.objects.create(
                scope=RunJob.SCOPE_INVENTORY_PIPELINE,
                company_key="company_a",
                status=RunJob.STATUS_SUCCEEDED,
                exit_code=0,
            )
            artifact = RunArtifact.objects.create(
                kind=RunArtifact.KIND_INVENTORY_AUDIT,
                run_job=job,
                company_key="company_a",
                processed_at=timezone.now(),
                source_path=str(outside_file),
                source_hash="h" * 64,
                reliability_status=RunArtifact.RELIABILITY_HIGH,
                upload_stats_json={"report_type": "inventory_pipeline", "summary_csv": str(outside_file)},
            )

            with mock.patch(
                "apps.epos_qbo.views._trusted_report_roots",
                return_value=[trusted_root.resolve()],
            ), suppress_expected_request_logs():
                response = self.client.get(
                    reverse(
                        "epos_qbo:run-artifact-report",
                        kwargs={"job_id": job.id, "artifact_id": artifact.id, "report_key": "summary_csv"},
                    )
                )

        self.assertEqual(response.status_code, 404)

    def test_missing_report_file_does_not_render_button(self):
        self.client.login(username="op", password="pw")
        with TemporaryDirectory() as td:
            trusted_root = Path(td) / "reports"
            trusted_root.mkdir()
            missing_csv = trusted_root / "missing_summary.csv"
            job = RunJob.objects.create(
                scope=RunJob.SCOPE_INVENTORY_PIPELINE,
                company_key="company_a",
                status=RunJob.STATUS_SUCCEEDED,
                exit_code=0,
            )
            artifact = RunArtifact.objects.create(
                kind=RunArtifact.KIND_INVENTORY_AUDIT,
                run_job=job,
                company_key="company_a",
                processed_at=timezone.now(),
                source_path=str(missing_csv.with_suffix(".json")),
                source_hash="i" * 64,
                reliability_status=RunArtifact.RELIABILITY_HIGH,
                upload_stats_json={"report_type": "inventory_pipeline", "summary_csv": str(missing_csv)},
            )

            with mock.patch(
                "apps.epos_qbo.views._trusted_report_roots",
                return_value=[trusted_root.resolve()],
            ):
                detail = self.client.get(reverse("epos_qbo:run-detail", kwargs={"job_id": job.id}))
                with suppress_expected_request_logs():
                    download = self.client.get(
                        reverse(
                            "epos_qbo:run-artifact-report",
                            kwargs={"job_id": job.id, "artifact_id": artifact.id, "report_key": "summary_csv"},
                        )
                    )

        self.assertEqual(detail.status_code, 200)
        html = detail.content.decode("utf-8")
        self.assertNotIn("Summary CSV", html)
        self.assertIn("<span class=\"text-slate-400 dark:text-slate-500\">—</span>", html)
        self.assertEqual(download.status_code, 404)

    def test_report_download_rejects_unknown_or_missing_report(self):
        self.client.login(username="op", password="pw")
        job = RunJob.objects.create(
            scope=RunJob.SCOPE_INVENTORY_PIPELINE,
            company_key="company_a",
            status=RunJob.STATUS_SUCCEEDED,
            exit_code=0,
        )
        artifact = RunArtifact.objects.create(
            kind=RunArtifact.KIND_INVENTORY_AUDIT,
            run_job=job,
            company_key="company_a",
            processed_at=timezone.now(),
            source_path="/tmp/missing_inventory_pipeline.json",
            source_hash="d" * 64,
            reliability_status=RunArtifact.RELIABILITY_HIGH,
            upload_stats_json={
                "report_type": "inventory_pipeline",
                "summary_csv": "/tmp/missing_inventory_pipeline.csv",
            },
        )

        with suppress_expected_request_logs():
            unknown = self.client.get(
                reverse(
                    "epos_qbo:run-artifact-report",
                    kwargs={"job_id": job.id, "artifact_id": artifact.id, "report_key": "unknown"},
                )
            )
            missing = self.client.get(
                reverse(
                    "epos_qbo:run-artifact-report",
                    kwargs={"job_id": job.id, "artifact_id": artifact.id, "report_key": "summary_csv"},
                )
            )

        self.assertEqual(unknown.status_code, 404)
        self.assertEqual(missing.status_code, 404)

    def test_report_download_rejects_artifact_from_another_run(self):
        self.client.login(username="op", password="pw")
        with TemporaryDirectory() as td:
            summary_csv = Path(td) / "summary.csv"
            summary_csv.write_text("ok\n", encoding="utf-8")
            job = RunJob.objects.create(scope=RunJob.SCOPE_INVENTORY_PIPELINE, company_key="company_a")
            other_job = RunJob.objects.create(scope=RunJob.SCOPE_INVENTORY_PIPELINE, company_key="company_a")
            artifact = RunArtifact.objects.create(
                kind=RunArtifact.KIND_INVENTORY_AUDIT,
                run_job=other_job,
                company_key="company_a",
                processed_at=timezone.now(),
                source_path=str(summary_csv),
                source_hash="e" * 64,
                reliability_status=RunArtifact.RELIABILITY_HIGH,
                upload_stats_json={"report_type": "inventory_pipeline", "summary_csv": str(summary_csv)},
            )

            with suppress_expected_request_logs():
                response = self.client.get(
                    reverse(
                        "epos_qbo:run-artifact-report",
                        kwargs={"job_id": job.id, "artifact_id": artifact.id, "report_key": "summary_csv"},
                    )
                )

        self.assertEqual(response.status_code, 404)

    def test_report_download_rejects_relative_traversal_path(self):
        self.client.login(username="op", password="pw")
        job = RunJob.objects.create(
            scope=RunJob.SCOPE_INVENTORY_PIPELINE,
            company_key="company_a",
            status=RunJob.STATUS_SUCCEEDED,
            exit_code=0,
        )
        artifact = RunArtifact.objects.create(
            kind=RunArtifact.KIND_INVENTORY_AUDIT,
            run_job=job,
            company_key="company_a",
            processed_at=timezone.now(),
            source_path="../outside.json",
            source_hash="f" * 64,
            reliability_status=RunArtifact.RELIABILITY_HIGH,
            upload_stats_json={"report_type": "inventory_pipeline", "summary_csv": "../outside.csv"},
        )

        with suppress_expected_request_logs():
            response = self.client.get(
                reverse(
                    "epos_qbo:run-artifact-report",
                    kwargs={"job_id": job.id, "artifact_id": artifact.id, "report_key": "summary_csv"},
                )
            )

        self.assertEqual(response.status_code, 404)

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
