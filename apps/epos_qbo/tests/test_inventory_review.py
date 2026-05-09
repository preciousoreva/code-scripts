from __future__ import annotations

from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import mock

from django.conf import settings
from django.contrib.auth.models import Permission
from django.contrib.auth.models import User
from django.template.loader import render_to_string
from django.test import TestCase
from django.urls import reverse
from django.utils import timezone

from apps.epos_qbo import views
from apps.epos_qbo.models import (
    CompanyConfigRecord,
    InventoryReviewAcknowledgement,
    RunArtifact,
    RunJob,
)
from apps.epos_qbo.services.inventory_review import parse_inventory_review_csv


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

    def test_parser_tolerates_quantity_preview_risk_columns(self):
        with TemporaryDirectory() as td:
            final_audit = Path(td) / "final.csv"
            final_audit.write_text(
                "\n".join(
                    [
                        "epos_product_name,status,catalog_issue_type,qbo_item_id,qbo_item_name,qbo_qty,epos_expected_qty,qty_delta,qbo_cost,estimated_value_impact,risk_flags,risk_level,recommended_action,apply_eligible",
                        "Widget,needs_adjustment,exact_name_match,1,Widget,2,5,3,10,30,,low,eligible_for_apply,True",
                        "Risky,needs_adjustment,exact_name_match,2,Risky,-2,5,7,10,70,negative_qbo_qty,medium,manual_review_required,False",
                    ]
                ),
                encoding="utf-8",
            )

            result = parse_inventory_review_csv(final_audit)

        self.assertEqual(result.total_rows, 2)
        self.assertEqual(len(result.rows), 2)
        self.assertEqual(result.rows[0]["product"], "Widget")
        self.assertEqual(result.rows[0]["delta"], "3")
        self.assertEqual(result.rows[1]["raw"]["risk_flags"], "negative_qbo_qty")


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

    def _grant_trigger_permission(self):
        permission = Permission.objects.get(codename="can_trigger_runs", content_type__app_label="epos_qbo")
        self.user.user_permissions.add(permission)

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
        self.assertIn("Missing from QuickBooks", html)
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

    def test_mark_reviewed_clears_overview_warning_for_current_inventory_artifact(self):
        self._login()
        self._grant_trigger_permission()
        with TemporaryDirectory(dir=str(settings.BASE_DIR)) as td:
            final_audit = Path(td) / "inventory_audit_company_a_final.csv"
            final_audit.write_text("base_name,status\nBlocked,missing_in_qbo\n", encoding="utf-8")
            _job, artifact = self._create_inventory_artifact(final_audit=final_audit)

            response = self.client.post(
                reverse("epos_qbo:company_inventory_review_mark_reviewed", kwargs={"company_key": "company_a"}),
                {"artifact_id": str(artifact.id)},
            )

            self.assertEqual(response.status_code, 302)
            acknowledgement = InventoryReviewAcknowledgement.objects.get(artifact=artifact)
            self.assertEqual(acknowledgement.company_key, "company_a")
            self.assertEqual(acknowledgement.reviewed_by, self.user)

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

        self.assertEqual(company_row["inventory_status"]["label"], "Reviewed")
        self.assertEqual(company_row["inventory_status"]["severity"], "healthy")
        self.assertFalse(company_row["inventory_review_required"])
        self.assertIn("Inventory: Reviewed", html)
        self.assertNotIn("Review 139 items", html)

    def test_new_inventory_artifact_reopens_review_after_previous_artifact_was_reviewed(self):
        self._login()
        self._grant_trigger_permission()
        with TemporaryDirectory(dir=str(settings.BASE_DIR)) as td:
            first_audit = Path(td) / "inventory_audit_company_a_first.csv"
            first_audit.write_text("base_name,status\nBlocked,missing_in_qbo\n", encoding="utf-8")
            _job, first_artifact = self._create_inventory_artifact(final_audit=first_audit)
            self.client.post(
                reverse("epos_qbo:company_inventory_review_mark_reviewed", kwargs={"company_key": "company_a"}),
                {"artifact_id": str(first_artifact.id)},
            )

            second_audit = Path(td) / "inventory_audit_company_a_second.csv"
            second_audit.write_text("base_name,status\nStill Blocked,missing_in_qbo\n", encoding="utf-8")
            self._create_inventory_artifact(final_audit=second_audit, blocked_items=7, in_sync=3338)

            company_row = next(row for row in self._overview_company_rows() if row["company_key"] == "company_a")

        self.assertEqual(company_row["inventory_status"]["label"], "Needs review")
        self.assertTrue(company_row["inventory_review_required"])
        self.assertEqual(company_row["inventory_status"]["blocked_items"], 7)

    def test_review_page_shows_acknowledged_state_after_marking_reviewed(self):
        self._login()
        self._grant_trigger_permission()
        with TemporaryDirectory(dir=str(settings.BASE_DIR)) as td:
            final_audit = Path(td) / "inventory_audit_company_a_final.csv"
            final_audit.write_text("base_name,status\nBlocked,missing_in_qbo\n", encoding="utf-8")
            _job, artifact = self._create_inventory_artifact(final_audit=final_audit)
            self.client.post(
                reverse("epos_qbo:company_inventory_review_mark_reviewed", kwargs={"company_key": "company_a"}),
                {"artifact_id": str(artifact.id)},
            )

            response = self.client.get(
                reverse("epos_qbo:company_inventory_review", kwargs={"company_key": "company_a"})
            )

        self.assertEqual(response.status_code, 200)
        html = response.content.decode("utf-8")
        self.assertIn("Reviewed", html)
        self.assertIn("Dashboard warning is cleared for this audit only.", html)
        self.assertNotIn("Mark Reviewed", html)

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
