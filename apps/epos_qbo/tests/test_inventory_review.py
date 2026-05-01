from __future__ import annotations

from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import mock

from django.conf import settings
from django.contrib.auth.models import User
from django.template.loader import render_to_string
from django.test import TestCase
from django.urls import reverse
from django.utils import timezone

from apps.epos_qbo import views
from apps.epos_qbo.models import CompanyConfigRecord, InventoryReviewItem, RunArtifact, RunJob
from apps.epos_qbo.services.inventory_review import ingest_inventory_review_items, parse_inventory_review_csv


class InventoryReviewParserTests(TestCase):
    def test_parser_filters_healthy_rows_and_keeps_blocked_rows(self):
        with TemporaryDirectory() as td:
            final_audit = Path(td) / "final.csv"
            final_audit.write_text(
                "\n".join(
                    [
                        "product_name,final_status,blocking_reason,epos_expected_qty,qbo_qty,delta,category",
                        "Synced Widget,in_sync,,5,5,0,General",
                        "31N1 CHILDREN BAND,missing_in_qbo,product not found in QuickBooks,6,0,6,Children",
                    ]
                ),
                encoding="utf-8",
            )

            result = parse_inventory_review_csv(final_audit)

        self.assertEqual(result.total_rows, 2)
        self.assertEqual(result.healthy_rows, 1)
        self.assertEqual(len(result.rows), 1)
        self.assertEqual(result.rows[0]["product"], "31N1 CHILDREN BAND")
        self.assertEqual(result.rows[0]["reason_group_slug"], "missing_from_quickbooks")
        self.assertIn("Create the QBO inventory item", result.rows[0]["suggested_next_step"])

    def test_parser_treats_missing_from_qbo_reason_as_blocked(self):
        with TemporaryDirectory() as td:
            final_audit = Path(td) / "final.csv"
            final_audit.write_text(
                "\n".join(
                    [
                        "product,status,catalog_issue_type,catalog_issue_detail",
                        "Base Item,in_sync,exact_name_match,",
                        "Missing Item,in_sync,missing_from_qbo,product not found in QuickBooks",
                    ]
                ),
                encoding="utf-8",
            )

            result = parse_inventory_review_csv(final_audit)

        self.assertEqual(len(result.rows), 1)
        self.assertEqual(result.rows[0]["product"], "Missing Item")
        self.assertEqual(result.rows[0]["reason_label"], "product not found in QuickBooks")
        self.assertEqual(result.rows[0]["reason_group"], "Missing from QuickBooks")


class InventoryReviewViewTests(TestCase):
    def setUp(self):
        self.user = User.objects.create_user(username="op", password="pw")
        self.enabled_company = CompanyConfigRecord.objects.create(
            company_key="company_a",
            display_name="Company A",
            config_json={
                "company_key": "company_a",
                "display_name": "Company A",
                "inventory": {"enable_inventory_items": True},
            },
        )
        self.disabled_company = CompanyConfigRecord.objects.create(
            company_key="company_b",
            display_name="Company B",
            config_json={
                "company_key": "company_b",
                "display_name": "Company B",
                "inventory": {"enable_inventory_items": False},
            },
        )
        self._source_hash_counter = 0

    def _login(self):
        self.client.login(username="op", password="pw")

    def _source_hash(self) -> str:
        self._source_hash_counter += 1
        return f"{self._source_hash_counter:064d}"

    def _create_inventory_artifact(
        self,
        *,
        company_key: str = "company_a",
        final_audit: Path,
        products_checked: int = 3345,
        in_sync: int = 3206,
        blocked_items: int = 139,
        negative_rows: int = 475,
        negative_units: float = 23267.0,
    ) -> tuple[RunJob, RunArtifact]:
        now = timezone.now()
        job = RunJob.objects.create(
            scope=RunJob.SCOPE_INVENTORY_PIPELINE,
            company_key=company_key,
            status=RunJob.STATUS_SUCCEEDED,
            started_at=now,
            finished_at=now,
            exit_code=0,
        )
        summary_json = final_audit.parent / f"inventory_pipeline_{company_key}_{self._source_hash_counter}.json"
        summary_json.write_text("{}", encoding="utf-8")
        artifact = RunArtifact.objects.create(
            kind=RunArtifact.KIND_INVENTORY_AUDIT,
            run_job=job,
            company_key=company_key,
            processed_at=now,
            source_path=str(summary_json),
            source_hash=self._source_hash(),
            reliability_status=RunArtifact.RELIABILITY_HIGH,
            rows_total=products_checked,
            rows_kept=in_sync,
            rows_non_target=blocked_items,
            upload_stats_json={
                "report_type": "inventory_pipeline",
                "products_checked": products_checked,
                "in_sync": in_sync,
                "blocked_items": blocked_items,
                "still_needs_review": blocked_items,
                "epos_negative_rows_clamped": negative_rows,
                "epos_negative_units_clamped": negative_units,
                "final_status_counts": {"in_sync": in_sync, "missing_in_qbo": blocked_items},
                "child_reports": {"final_audit": str(final_audit)},
            },
        )
        return job, artifact

    def _overview_company_rows(self):
        with (
            mock.patch("apps.epos_qbo.views.ensure_db_initialized"),
            mock.patch("apps.epos_qbo.views.load_tokens_batch", return_value={}),
            mock.patch("apps.epos_qbo.views.load_tokens", return_value={}),
        ):
            return views._overview_context()["companies"]

    def test_inventory_review_route_renders_latest_blocked_items(self):
        self._login()
        with TemporaryDirectory(dir=str(settings.BASE_DIR)) as td:
            final_audit = Path(td) / "inventory_audit_company_a_final.csv"
            final_audit.write_text(
                "\n".join(
                    [
                        "base_name,status,catalog_issue_type,catalog_issue_detail,epos_single_units,qbo_qty_on_hand,delta,epos_categories",
                        "Synced Widget,in_sync,exact_name_match,,5,5,0,General",
                        "31N1 CHILDREN BAND,missing_in_qbo,missing_from_qbo,product not found in QuickBooks,6,0,6,Children",
                        "Pack Conflict,needs_adjustment,base_with_pack_variants,base item and pack variants both exist,25,20,5,Beer",
                    ]
                ),
                encoding="utf-8",
            )
            self._create_inventory_artifact(final_audit=final_audit)

            response = self.client.get(
                reverse("epos_qbo:company_inventory_review", kwargs={"company_key": "company_a"})
            )

        self.assertEqual(response.status_code, 200)
        html = response.content.decode("utf-8")
        self.assertIn("Inventory Review", html)
        self.assertIn("Company A", html)
        self.assertIn("3,345", html)
        self.assertIn("3,206", html)
        self.assertIn("139", html)
        self.assertIn("31N1 CHILDREN BAND", html)
        self.assertIn("product not found in QuickBooks", html)
        self.assertIn("Missing from QuickBooks", html)
        self.assertIn("Create the QBO inventory item", html)
        self.assertIn("475 negative EPOS rows were clamped to 0 by policy.", html)
        self.assertIn("23,267 units were ignored/clamped before grouping.", html)
        self.assertIn("Run Detail", html)
        self.assertIn("Final Audit", html)
        self.assertNotIn("Synced Widget", html)

    def test_inventory_disabled_company_gets_polite_message(self):
        self._login()
        response = self.client.get(
            reverse("epos_qbo:company_inventory_review", kwargs={"company_key": "company_b"})
        )

        self.assertEqual(response.status_code, 200)
        html = response.content.decode("utf-8")
        self.assertIn("Inventory review is not enabled for this company.", html)
        self.assertNotIn("Items Needing Review", html)

    def test_overview_shows_review_link_when_inventory_needs_review(self):
        with TemporaryDirectory(dir=str(settings.BASE_DIR)) as td:
            final_audit = Path(td) / "inventory_audit_company_a_final.csv"
            final_audit.write_text("base_name,status\nBlocked,missing_in_qbo\n", encoding="utf-8")
            self._create_inventory_artifact(final_audit=final_audit)

            company_row = next(row for row in self._overview_company_rows() if row["company_key"] == "company_a")
            html = render_to_string(
                "components/company_list.html",
                {
                    "companies": [company_row],
                    "revenue_company_options": [],
                    "revenue_period_options": [],
                    "revenue_chart_payload": {},
                },
            )

        self.assertIn("Review 139 items", html)
        self.assertIn(
            reverse("epos_qbo:company_inventory_review", kwargs={"company_key": "company_a"}),
            html,
        )

    def test_overview_omits_review_link_for_inventory_disabled_company(self):
        with TemporaryDirectory(dir=str(settings.BASE_DIR)) as td:
            final_audit = Path(td) / "inventory_audit_company_b_final.csv"
            final_audit.write_text("base_name,status\nBlocked,missing_in_qbo\n", encoding="utf-8")
            self._create_inventory_artifact(company_key="company_b", final_audit=final_audit)

            company_row = next(row for row in self._overview_company_rows() if row["company_key"] == "company_b")
            html = render_to_string(
                "components/company_list.html",
                {
                    "companies": [company_row],
                    "revenue_company_options": [],
                    "revenue_period_options": [],
                    "revenue_chart_payload": {},
                },
            )

        self.assertFalse(company_row["inventory_enabled"])
        self.assertNotIn("Review inventory", html)
        self.assertNotIn("Review 139 items", html)
        self.assertNotIn(
            reverse("epos_qbo:company_inventory_review", kwargs={"company_key": "company_b"}),
            html,
        )

    def test_missing_final_audit_file_does_not_crash(self):
        self._login()
        with TemporaryDirectory(dir=str(settings.BASE_DIR)) as td:
            missing_final_audit = Path(td) / "missing_final.csv"
            self._create_inventory_artifact(final_audit=missing_final_audit, blocked_items=1)

            response = self.client.get(
                reverse("epos_qbo:company_inventory_review", kwargs={"company_key": "company_a"})
            )

        self.assertEqual(response.status_code, 200)
        self.assertContains(
            response,
            "The final audit artifact exists in the database but the source file could not be found.",
        )
        self.assertNotContains(response, "Traceback")

    def test_ingestion_persists_review_items_and_dedupes(self):
        self._login()
        with TemporaryDirectory(dir=str(settings.BASE_DIR)) as td:
            final_audit = Path(td) / "inventory_audit_company_a_final.csv"
            final_audit.write_text(
                "\n".join(
                    [
                        "base_name,status,catalog_issue_type,catalog_issue_detail,epos_single_units,qbo_qty_on_hand,delta,epos_categories",
                        "Synced Widget,in_sync,exact_name_match,,5,5,0,General",
                        "31N1 CHILDREN BAND,missing_in_qbo,missing_from_qbo,product not found in QuickBooks,6,0,6,Children",
                    ]
                ),
                encoding="utf-8",
            )
            job, artifact = self._create_inventory_artifact(final_audit=final_audit, blocked_items=1)

            result1 = ingest_inventory_review_items(artifact=artifact, final_audit_path=final_audit)
            self.assertEqual(result1["created"], 1)
            self.assertEqual(result1["total_blocked_from_artifact"], 1)

            # Re-ingest the same artifact is idempotent for rows (updates occurrence).
            result2 = ingest_inventory_review_items(artifact=artifact, final_audit_path=final_audit)
            self.assertEqual(result2["created"], 0)
            self.assertGreaterEqual(result2["updated"], 1)

            item = InventoryReviewItem.objects.get(company_key="company_a")
            self.assertEqual(item.product_name, "31N1 CHILDREN BAND")
            self.assertEqual(item.reason_group, "missing_from_qbo")
            self.assertTrue(item.is_active)
            self.assertGreaterEqual(item.occurrence_count, 2)
            self.assertEqual(item.run_job_id, job.id)

    def test_ingestion_marks_disappeared_blockers_resolved_by_rerun(self):
        self._login()
        with TemporaryDirectory(dir=str(settings.BASE_DIR)) as td:
            final_audit_1 = Path(td) / "final1.csv"
            final_audit_1.write_text(
                "\n".join(
                    [
                        "base_name,status,catalog_issue_type,catalog_issue_detail,epos_single_units,qbo_qty_on_hand,delta,epos_categories",
                        "Blocked Item,missing_in_qbo,missing_from_qbo,product not found in QuickBooks,6,0,6,Children",
                    ]
                ),
                encoding="utf-8",
            )
            _, artifact = self._create_inventory_artifact(final_audit=final_audit_1, blocked_items=1)
            ingest_inventory_review_items(artifact=artifact, final_audit_path=final_audit_1)

            self.assertEqual(InventoryReviewItem.objects.filter(company_key="company_a", is_active=True).count(), 1)

            final_audit_2 = Path(td) / "final2.csv"
            final_audit_2.write_text(
                "\n".join(
                    [
                        "base_name,status,catalog_issue_type,catalog_issue_detail,epos_single_units,qbo_qty_on_hand,delta,epos_categories",
                        "Blocked Item,in_sync,exact_name_match,,6,6,0,Children",
                    ]
                ),
                encoding="utf-8",
            )
            ingest_inventory_review_items(artifact=artifact, final_audit_path=final_audit_2)

            item = InventoryReviewItem.objects.get(company_key="company_a")
            self.assertFalse(item.is_active)
            self.assertEqual(item.review_status, InventoryReviewItem.REVIEW_RESOLVED_BY_RERUN)
            self.assertEqual(item.resolution_type, "resolved_by_future_sync")

    def test_review_item_actions_ack_ignore_resolve_reopen(self):
        self._login()
        with TemporaryDirectory(dir=str(settings.BASE_DIR)) as td:
            final_audit = Path(td) / "final.csv"
            final_audit.write_text(
                "\n".join(
                    [
                        "base_name,status,catalog_issue_type,catalog_issue_detail,epos_single_units,qbo_qty_on_hand,delta,epos_categories",
                        "Blocked Item,missing_in_qbo,missing_from_qbo,product not found in QuickBooks,6,0,6,Children",
                    ]
                ),
                encoding="utf-8",
            )
            _, artifact = self._create_inventory_artifact(final_audit=final_audit, blocked_items=1)
            ingest_inventory_review_items(artifact=artifact, final_audit_path=final_audit)
            item = InventoryReviewItem.objects.get(company_key="company_a")

            ack_url = reverse(
                "epos_qbo:inventory-review-item-acknowledge",
                kwargs={"company_key": "company_a", "item_id": item.id},
            )
            self.client.post(ack_url)
            item.refresh_from_db()
            self.assertEqual(item.review_status, InventoryReviewItem.REVIEW_ACKNOWLEDGED)

            ignore_url = reverse(
                "epos_qbo:inventory-review-item-ignore",
                kwargs={"company_key": "company_a", "item_id": item.id},
            )
            self.client.post(ignore_url)
            item.refresh_from_db()
            self.assertEqual(item.review_status, InventoryReviewItem.REVIEW_IGNORED)
            self.assertEqual(item.resolution_type, "intentional_ignore")

            resolve_url = reverse(
                "epos_qbo:inventory-review-item-mark-resolved",
                kwargs={"company_key": "company_a", "item_id": item.id},
            )
            self.client.post(resolve_url, data={"resolution_type": "manual_qbo_fix"})
            item.refresh_from_db()
            self.assertEqual(item.review_status, InventoryReviewItem.REVIEW_MANUALLY_RESOLVED)
            self.assertFalse(item.is_active)
            self.assertIsNotNone(item.resolved_at)

            reopen_url = reverse(
                "epos_qbo:inventory-review-item-reopen",
                kwargs={"company_key": "company_a", "item_id": item.id},
            )
            self.client.post(reopen_url)
            item.refresh_from_db()
            self.assertEqual(item.review_status, InventoryReviewItem.REVIEW_OPEN)
            self.assertTrue(item.is_active)
