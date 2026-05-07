from __future__ import annotations

from pathlib import Path
from tempfile import TemporaryDirectory
from urllib.parse import parse_qs, urlparse
from unittest import mock

from django.conf import settings
from django.contrib.auth.models import Permission, User
from django.test import TestCase
from django.urls import reverse
from django.utils import timezone

from apps.epos_qbo.models import CompanyConfigRecord, RunArtifact, RunJob
from apps.epos_qbo.services import inventory_review_actions as actions
from apps.epos_qbo.services.inventory_review import parse_inventory_review_csv
from apps.epos_qbo.services.inventory_review_actions import (
    REASON_GROUP_MISSING,
    REVIEW_CREATE_MISSING_INTENT,
    MissingPreview,
    build_missing_item_creation_preview,
    get_catalog_cleanup_rows,
    get_quantity_adjustment_rows,
    get_review_rows_by_reason,
)
from code_scripts.inventory_review_missing_candidates import _normalize_base_name


FINAL_AUDIT_ROWS = [
    "base_name,status,catalog_issue_type,catalog_issue_detail,epos_single_units,qbo_qty_on_hand,delta,epos_categories",
    # In sync — should not appear in any action set.
    "Synced Widget,in_sync,exact_name_match,,5,5,0,General",
    # Missing from QBO — typical, no pack suffix, in mapping.
    "31N1 CHILDREN BAND,missing_in_qbo,missing_from_qbo,product not found in QuickBooks,6,0,6,Children",
    "SECOND CHILD ITEM,missing_in_qbo,missing_from_qbo,product not found in QuickBooks,2,0,2,Children",
    # Missing from QBO — pack variant of an existing base; must be blocked.
    "AQUAFINA 50CL*12,missing_in_qbo,missing_from_qbo,product not found in QuickBooks,3,0,3,Drinks",
    # Missing from QBO — invalid CSV summary row.
    "Total:,missing_in_qbo,missing_from_qbo,product not found in QuickBooks,,,,",
    # Missing from QBO — category not in mapping.
    "MYSTERY CRISP,missing_in_qbo,missing_from_qbo,product not found in QuickBooks,4,0,4,Unmapped Category",
    # Duplicate/base conflict — should drive Retry catalog cleanup.
    "Pack Conflict,needs_adjustment,base_with_pack_variants,base item and pack variants both exist,25,20,5,Beer",
    # Exact-match adjustment — should drive Retry quantity adjustments.
    "BENSON & HEDGES CIGARETTES,needs_adjustment,exact_name_match,quantity differs,26,46,-20,Cigarettes",
]

PRODUCT_MAPPING = {
    "Children": {
        "asset": "Inventory Asset",
        "income": "Sales of Product Income",
        "expense": "Cost of Goods Sold",
    },
    "Drinks": {
        "asset": "Inventory Asset",
        "income": "Sales of Product Income",
        "expense": "Cost of Goods Sold",
    },
    "Beer": {
        "asset": "Inventory Asset",
        "income": "Sales of Product Income",
        "expense": "Cost of Goods Sold",
    },
    "Cigarettes": {
        "asset": "Inventory Asset",
        "income": "Sales of Product Income",
        "expense": "Cost of Goods Sold",
    },
}


def _write_final_audit(target_dir: Path) -> Path:
    final_audit = target_dir / "inventory_audit_company_a_final.csv"
    final_audit.write_text("\n".join(FINAL_AUDIT_ROWS), encoding="utf-8")
    return final_audit


class ServiceLevelTests(TestCase):
    def test_row_filtering_separates_action_groups(self):
        with TemporaryDirectory() as td:
            final_audit = _write_final_audit(Path(td))
            parsed = parse_inventory_review_csv(final_audit)
        rows = parsed.rows

        catalog_rows = get_catalog_cleanup_rows(rows)
        quantity_rows = get_quantity_adjustment_rows(rows)
        missing_rows = get_review_rows_by_reason(rows, REASON_GROUP_MISSING)

        catalog_products = {r["product"] for r in catalog_rows}
        quantity_products = {r["product"] for r in quantity_rows}
        missing_products = {r["product"] for r in missing_rows}

        self.assertIn("Pack Conflict", catalog_products)
        self.assertNotIn("BENSON & HEDGES CIGARETTES", catalog_products)
        self.assertIn("BENSON & HEDGES CIGARETTES", quantity_products)
        self.assertNotIn("Pack Conflict", quantity_products)
        self.assertIn("31N1 CHILDREN BAND", missing_products)
        self.assertIn("AQUAFINA 50CL*12", missing_products)
        self.assertNotIn("Total:", missing_products)
        self.assertIn("MYSTERY CRISP", missing_products)
        # Synced rows never reach the parser output.
        self.assertNotIn("Synced Widget", catalog_products | quantity_products | missing_products)

    def test_normalize_base_name_strips_pack_suffix(self):
        self.assertEqual(_normalize_base_name("AQUAFINA 50CL*12"), "AQUAFINA 50CL")
        self.assertEqual(_normalize_base_name(" Cheese  Balls 13g "), "Cheese Balls 13g")
        self.assertEqual(_normalize_base_name(""), "")

    def test_missing_preview_classifies_invalid_pack_and_unmapped(self):
        with TemporaryDirectory() as td:
            final_audit = _write_final_audit(Path(td))
            parsed = parse_inventory_review_csv(final_audit)

            company = CompanyConfigRecord(
                company_key="company_a",
                display_name="Company A",
                config_json={"inventory": {"enable_inventory_items": True}},
            )
            artifact = RunArtifact(id=1, source_path=str(final_audit))
            context = actions.ReviewContext(
                company=company,
                artifact=artifact,
                final_audit_path=final_audit,
                parse_result=parsed,
            )

            with mock.patch(
                "code_scripts.inventory_review_missing_candidates.load_category_mapping_for_company_key",
                return_value=(PRODUCT_MAPPING, ""),
            ), mock.patch(
                "code_scripts.inventory_review_missing_candidates.load_qbo_base_name_keys_for_company_key",
                return_value=({"aquafina 50cl"}, ""),
            ):
                preview = build_missing_item_creation_preview(context=context)

        self.assertIsInstance(preview, MissingPreview)
        by_product = {row.product: row for row in preview.rows}
        self.assertNotIn("Total:", by_product)

        pack_row = by_product["AQUAFINA 50CL*12"]
        self.assertEqual(pack_row.safety_status, "Pack variant of existing base")
        self.assertIn("base item exists", pack_row.block_reason)
        self.assertFalse(pack_row.is_safe)

        unmapped_row = by_product["MYSTERY CRISP"]
        self.assertEqual(unmapped_row.safety_status, "Category not in mapping")
        self.assertFalse(unmapped_row.is_safe)

        safe_row = by_product["31N1 CHILDREN BAND"]
        self.assertEqual(safe_row.safety_status, "Safe candidate")
        self.assertTrue(safe_row.is_safe)
        self.assertEqual(safe_row.suggested_qbo_name, "31N1 CHILDREN BAND")
        self.assertEqual(safe_row.inventory_account, "Inventory Asset")

        self.assertEqual(preview.safe_count, 2)
        self.assertEqual(preview.blocked_count, 2)


class InventoryReviewActionViewTests(TestCase):
    def setUp(self):
        self.user = User.objects.create_user(username="op", password="pw")
        self.user.user_permissions.add(Permission.objects.get(codename="can_trigger_runs"))
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

    def _create_inventory_artifact(self, *, company_key: str, final_audit: Path):
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
            rows_total=10,
            rows_kept=3,
            rows_non_target=7,
            upload_stats_json={
                "report_type": "inventory_pipeline",
                "products_checked": 10,
                "in_sync": 3,
                "blocked_items": 7,
                "still_needs_review": 7,
                "epos_negative_rows_clamped": 0,
                "epos_negative_units_clamped": 0,
                "final_status_counts": {"in_sync": 3, "missing_in_qbo": 4, "needs_adjustment": 2},
                "child_reports": {"final_audit": str(final_audit)},
                "inv_txn_date": "2026-04-29",
            },
        )
        return job, artifact

    def test_review_page_renders_resolve_review_items_section(self):
        self._login()
        with TemporaryDirectory(dir=str(settings.BASE_DIR)) as td:
            final_audit = _write_final_audit(Path(td))
            self._create_inventory_artifact(company_key="company_a", final_audit=final_audit)

            response = self.client.get(
                reverse("epos_qbo:company_inventory_review", kwargs={"company_key": "company_a"})
            )

        self.assertEqual(response.status_code, 200)
        html = response.content.decode("utf-8")
        self.assertIn("Resolve Review Items", html)
        self.assertIn("Review scoped cleanup apply", html)
        self.assertIn("Review scoped quantity apply", html)
        self.assertIn("Preview items", html)
        self.assertIn("Retry actions are scoped to reviewed rows and capped.", html)
        self.assertIn("Production inventory apply is blocked by default.", html)
        self.assertIn(
            reverse(
                "epos_qbo:company_inventory_retry_catalog_cleanup_confirm",
                kwargs={"company_key": "company_a"},
            ),
            html,
        )
        self.assertIn(
            reverse(
                "epos_qbo:company_inventory_retry_quantity_adjustments_confirm",
                kwargs={"company_key": "company_a"},
            ),
            html,
        )

    def test_catalog_cleanup_confirm_page_renders_preview_and_confirm_post(self):
        self._login()
        with TemporaryDirectory(dir=str(settings.BASE_DIR)) as td:
            final_audit = _write_final_audit(Path(td))
            self._create_inventory_artifact(company_key="company_a", final_audit=final_audit)

            response = self.client.get(
                reverse(
                    "epos_qbo:company_inventory_retry_catalog_cleanup_confirm",
                    kwargs={"company_key": "company_a"},
                )
            )

        self.assertEqual(response.status_code, 200)
        html = response.content.decode("utf-8")
        self.assertIn("Confirm Scoped Catalog Apply", html)
        self.assertIn("Catalog cleanup applied", html)
        self.assertIn("Admin catalog apply", html)
        self.assertIn("This queues catalog_apply_admin_only for the reviewed rows only.", html)
        self.assertIn("Production apply remains blocked unless explicitly unlocked.", html)
        self.assertIn("Affected items", html)
        self.assertIn("Pack Conflict", html)
        self.assertIn(
            reverse(
                "epos_qbo:company_inventory_retry_catalog_cleanup",
                kwargs={"company_key": "company_a"},
            ),
            html,
        )
        self.assertIn("Confirm and queue", html)

    def test_quantity_adjustments_confirm_page_renders_preview_and_confirm_post(self):
        self._login()
        with TemporaryDirectory(dir=str(settings.BASE_DIR)) as td:
            final_audit = _write_final_audit(Path(td))
            self._create_inventory_artifact(company_key="company_a", final_audit=final_audit)

            response = self.client.get(
                reverse(
                    "epos_qbo:company_inventory_retry_quantity_adjustments_confirm",
                    kwargs={"company_key": "company_a"},
                )
            )

        self.assertEqual(response.status_code, 200)
        html = response.content.decode("utf-8")
        self.assertIn("Confirm Scoped Quantity Apply", html)
        self.assertIn("Applied quantity adjustments", html)
        self.assertIn("Apply quantity adjustments", html)
        self.assertIn("This queues quantity_apply for the reviewed rows only.", html)
        self.assertIn("Production apply remains blocked unless explicitly unlocked.", html)
        self.assertIn("Affected items", html)
        self.assertIn("BENSON &amp; HEDGES CIGARETTES", html)
        self.assertIn(
            reverse(
                "epos_qbo:company_inventory_retry_quantity_adjustments",
                kwargs={"company_key": "company_a"},
            ),
            html,
        )
        self.assertIn("Confirm and queue", html)

    def test_retry_catalog_cleanup_rejects_inventory_disabled_company(self):
        self._login()
        response = self.client.post(
            reverse(
                "epos_qbo:company_inventory_retry_catalog_cleanup",
                kwargs={"company_key": "company_b"},
            )
        )
        self.assertEqual(response.status_code, 302)
        self.assertEqual(RunJob.objects.filter(company_key="company_b").count(), 0)

    def test_retry_quantity_adjustments_rejects_inventory_disabled_company(self):
        self._login()
        response = self.client.post(
            reverse(
                "epos_qbo:company_inventory_retry_quantity_adjustments",
                kwargs={"company_key": "company_b"},
            )
        )
        self.assertEqual(response.status_code, 302)
        self.assertEqual(RunJob.objects.filter(company_key="company_b").count(), 0)

    def test_retry_catalog_cleanup_queues_inventory_pipeline_job(self):
        self._login()
        with TemporaryDirectory(dir=str(settings.BASE_DIR)) as td:
            final_audit = _write_final_audit(Path(td))
            self._create_inventory_artifact(company_key="company_a", final_audit=final_audit)

            with mock.patch(
                "apps.epos_qbo.views.dispatch_next_queued_job",
                return_value=(None, "queued"),
            ):
                response = self.client.post(
                    reverse(
                        "epos_qbo:company_inventory_retry_catalog_cleanup",
                        kwargs={"company_key": "company_a"},
                    )
                )

        self.assertEqual(response.status_code, 302)
        # Original artifact RunJob plus the retry RunJob.
        retry_jobs = RunJob.objects.filter(
            company_key="company_a", status=RunJob.STATUS_QUEUED
        )
        self.assertEqual(retry_jobs.count(), 1)
        retry_job = retry_jobs.first()
        self.assertEqual(retry_job.scope, RunJob.SCOPE_INVENTORY_PIPELINE)
        review_retry = (retry_job.inventory_options_json or {}).get("review_retry", {})
        self.assertEqual(review_retry.get("intent"), actions.RETRY_INTENT_CATALOG)
        self.assertEqual(review_retry.get("row_count"), 1)
        self.assertIn("Pack Conflict", review_retry.get("affected_base_names", []))
        opts = retry_job.inventory_options_json or {}
        self.assertEqual(opts.get("mode"), "catalog_apply_admin_only")
        self.assertIn("Pack Conflict", opts.get("base_names", []))
        self.assertEqual(opts.get("max_catalog_fixes"), 1)
        self.assertEqual(opts.get("max_quantity_adjustments"), 0)

    def test_retry_quantity_adjustments_uses_only_trusted_audit_rows(self):
        """The view must ignore POST product names and only use parsed audit rows."""

        self._login()
        with TemporaryDirectory(dir=str(settings.BASE_DIR)) as td:
            final_audit = _write_final_audit(Path(td))
            self._create_inventory_artifact(company_key="company_a", final_audit=final_audit)

            with mock.patch(
                "apps.epos_qbo.views.dispatch_next_queued_job",
                return_value=(None, "queued"),
            ):
                response = self.client.post(
                    reverse(
                        "epos_qbo:company_inventory_retry_quantity_adjustments",
                        kwargs={"company_key": "company_a"},
                    ),
                    # Attempt to inject untrusted product names — they must be ignored.
                    {"product": "ATTACKER PRODUCT", "products[]": "MALICIOUS"},
                )

        self.assertEqual(response.status_code, 302)
        retry_jobs = RunJob.objects.filter(
            company_key="company_a", status=RunJob.STATUS_QUEUED
        )
        self.assertEqual(retry_jobs.count(), 1)
        retry_job = retry_jobs.first()
        review_retry = (retry_job.inventory_options_json or {}).get("review_retry", {})
        affected = review_retry.get("affected_base_names", [])
        self.assertIn("BENSON & HEDGES CIGARETTES", affected)
        self.assertNotIn("ATTACKER PRODUCT", affected)
        self.assertNotIn("MALICIOUS", affected)
        opts = retry_job.inventory_options_json or {}
        self.assertEqual(opts.get("mode"), "quantity_apply")
        self.assertIn("BENSON & HEDGES CIGARETTES", opts.get("base_names", []))
        self.assertEqual(opts.get("max_catalog_fixes"), 0)
        self.assertEqual(opts.get("max_quantity_adjustments"), 1)

    def test_missing_preview_renders_and_writes_no_jobs(self):
        self._login()
        baseline_jobs = RunJob.objects.count()
        with TemporaryDirectory(dir=str(settings.BASE_DIR)) as td:
            final_audit = _write_final_audit(Path(td))
            self._create_inventory_artifact(company_key="company_a", final_audit=final_audit)

            with mock.patch(
                "code_scripts.inventory_review_missing_candidates.load_category_mapping_for_company_key",
                return_value=(PRODUCT_MAPPING, ""),
            ), mock.patch(
                "code_scripts.inventory_review_missing_candidates.load_qbo_base_name_keys_for_company_key",
                return_value=({"aquafina 50cl"}, ""),
            ):
                response = self.client.get(
                    reverse(
                        "epos_qbo:company_inventory_missing_preview",
                        kwargs={"company_key": "company_a"},
                    )
                )

        self.assertEqual(response.status_code, 200)
        html = response.content.decode("utf-8")
        self.assertIn("Missing QuickBooks Inventory Items", html)
        self.assertIn("Preview only. No QuickBooks items are created", html)
        self.assertIn("31N1 CHILDREN BAND", html)
        self.assertIn("SECOND CHILD ITEM", html)
        self.assertIn("AQUAFINA 50CL*12", html)
        self.assertNotIn("Total:", html)
        self.assertIn("Confirm and queue", html)
        self.assertNotIn("Review item creation", html)
        self.assertIn(
            reverse(
                "epos_qbo:company_inventory_missing_create",
                kwargs={"company_key": "company_a"},
            ),
            html,
        )
        # No new RunJobs must be created from a preview GET.
        self.assertEqual(RunJob.objects.count(), baseline_jobs + 1)

    def test_missing_preview_hides_review_item_creation_when_no_safe_candidates(self):
        self._login()
        with TemporaryDirectory(dir=str(settings.BASE_DIR)) as td:
            final_audit = _write_final_audit(Path(td))
            self._create_inventory_artifact(company_key="company_a", final_audit=final_audit)

            def _empty_safe(*_args, **_kwargs):
                return {
                    "rows": [],
                    "safe_count": 0,
                    "blocked_count": 0,
                    "mapping_loaded": True,
                    "mapping_error": "",
                    "qbo_base_names_loaded": True,
                    "qbo_base_names_error": "",
                }

            with mock.patch(
                "apps.epos_qbo.services.inventory_review_actions.classify_missing_items_for_audit_file",
                side_effect=_empty_safe,
            ):
                response = self.client.get(
                    reverse(
                        "epos_qbo:company_inventory_missing_preview",
                        kwargs={"company_key": "company_a"},
                    )
                )

        self.assertEqual(response.status_code, 200)
        html = response.content.decode("utf-8")
        self.assertIn("No safe candidates in this scope", html)

    def test_missing_create_confirm_redirects_to_missing_preview(self):
        self._login()
        preview_url = reverse(
            "epos_qbo:company_inventory_missing_preview",
            kwargs={"company_key": "company_a"},
        )
        response = self.client.get(
            reverse(
                "epos_qbo:company_inventory_missing_create_confirm",
                kwargs={"company_key": "company_a"},
            )
        )
        self.assertEqual(response.status_code, 302)
        self.assertEqual(response.url, preview_url)

        response_q = self.client.get(
            reverse(
                "epos_qbo:company_inventory_missing_create_confirm",
                kwargs={"company_key": "company_a"},
            ),
            {"category": "Children", "txn_date": "2026-04-29"},
        )
        self.assertEqual(response_q.status_code, 302)
        qs = parse_qs(urlparse(response_q.url).query)
        self.assertEqual(qs.get("category"), ["Children"])
        self.assertEqual(qs.get("txn_date"), ["2026-04-29"])

    def test_missing_create_post_queues_job_with_review_create_metadata(self):
        self._login()
        with TemporaryDirectory(dir=str(settings.BASE_DIR)) as td:
            final_audit = _write_final_audit(Path(td))
            self._create_inventory_artifact(company_key="company_a", final_audit=final_audit)

            with mock.patch(
                "code_scripts.inventory_review_missing_candidates.load_category_mapping_for_company_key",
                return_value=(PRODUCT_MAPPING, ""),
            ), mock.patch(
                "code_scripts.inventory_review_missing_candidates.load_qbo_base_name_keys_for_company_key",
                return_value=({"aquafina 50cl"}, ""),
            ), mock.patch(
                "apps.epos_qbo.views.dispatch_next_queued_job",
                return_value=(None, "queued"),
            ):
                response = self.client.post(
                    reverse(
                        "epos_qbo:company_inventory_missing_create",
                        kwargs={"company_key": "company_a"},
                    ),
                    {
                        "product": "FAKE",
                        "products[]": "EVIL",
                        "inventory_start_date": "2026-04-29",
                    },
                )

        self.assertEqual(response.status_code, 302)
        queued = RunJob.objects.filter(company_key="company_a", status=RunJob.STATUS_QUEUED).first()
        self.assertIsNotNone(queued)
        opts = queued.inventory_options_json or {}
        rcm = opts.get("review_create_missing_items") or {}
        self.assertEqual(opts.get("mode"), REVIEW_CREATE_MISSING_INTENT)
        self.assertEqual(rcm.get("intent"), REVIEW_CREATE_MISSING_INTENT)
        self.assertEqual(rcm.get("safe_count"), 2)
        self.assertEqual(rcm.get("blocked_count"), 2)
        self.assertEqual(rcm.get("row_count"), 2)
        self.assertEqual(rcm.get("total_candidates_in_scope"), 4)
        self.assertEqual(rcm.get("category_label"), "All categories")
        self.assertIsNone(rcm.get("category_filter"))
        self.assertEqual(rcm.get("mapping_source"), "Product.Mapping.csv")
        self.assertEqual(rcm.get("create_qty_policy"), "initial_qty_from_epos")
        bases = sorted(opts.get("base_names") or [])
        self.assertEqual(bases, ["31N1 CHILDREN BAND", "SECOND CHILD ITEM"])
        self.assertNotIn("FAKE", bases)
        self.assertEqual(opts.get("max_catalog_fixes"), 0)
        self.assertEqual(opts.get("max_quantity_adjustments"), 0)
        self.assertEqual(opts.get("txn_date"), "2026-04-29")
        self.assertEqual(rcm.get("item_inv_start_date"), "2026-04-29")
        self.assertEqual(rcm.get("txn_date_source"), "summary.inv_txn_date")

    def test_missing_preview_category_filter_scopes_rows_and_counts(self):
        self._login()
        with TemporaryDirectory(dir=str(settings.BASE_DIR)) as td:
            final_audit = _write_final_audit(Path(td))
            self._create_inventory_artifact(company_key="company_a", final_audit=final_audit)

            with mock.patch(
                "code_scripts.inventory_review_missing_candidates.load_category_mapping_for_company_key",
                return_value=(PRODUCT_MAPPING, ""),
            ), mock.patch(
                "code_scripts.inventory_review_missing_candidates.load_qbo_base_name_keys_for_company_key",
                return_value=({"aquafina 50cl"}, ""),
            ):
                url = reverse(
                    "epos_qbo:company_inventory_missing_preview",
                    kwargs={"company_key": "company_a"},
                )
                html_all = self.client.get(url).content.decode("utf-8")
                html_children = self.client.get(url, {"category": "Children"}).content.decode("utf-8")
                html_drinks = self.client.get(url, {"category": "Drinks"}).content.decode("utf-8")

        self.assertIn("Overall (all categories): 2 safe of 4 candidate rows.", html_all)
        self.assertIn("31N1 CHILDREN BAND", html_children)
        self.assertIn("SECOND CHILD ITEM", html_children)
        self.assertIn("Will queue 2 safe", html_children)
        self.assertIn("AQUAFINA 50CL*12", html_drinks)
        self.assertNotIn("Will queue", html_drinks)

    def test_missing_create_post_category_scope_queues_only_that_category(self):
        self._login()
        with TemporaryDirectory(dir=str(settings.BASE_DIR)) as td:
            final_audit = _write_final_audit(Path(td))
            self._create_inventory_artifact(company_key="company_a", final_audit=final_audit)

            with mock.patch(
                "code_scripts.inventory_review_missing_candidates.load_category_mapping_for_company_key",
                return_value=(PRODUCT_MAPPING, ""),
            ), mock.patch(
                "code_scripts.inventory_review_missing_candidates.load_qbo_base_name_keys_for_company_key",
                return_value=({"aquafina 50cl"}, ""),
            ), mock.patch(
                "apps.epos_qbo.views.dispatch_next_queued_job",
                return_value=(None, "queued"),
            ):
                response = self.client.post(
                    reverse(
                        "epos_qbo:company_inventory_missing_create",
                        kwargs={"company_key": "company_a"},
                    ),
                    {
                        "category_scope": "Children",
                        "inventory_start_date": "2026-04-29",
                        "suggested_qbo_name": "INJECTED",
                    },
                )

        self.assertEqual(response.status_code, 302)
        queued = RunJob.objects.filter(company_key="company_a", status=RunJob.STATUS_QUEUED).first()
        opts = queued.inventory_options_json or {}
        rcm = opts.get("review_create_missing_items") or {}
        bases = sorted(opts.get("base_names") or [])
        self.assertEqual(bases, ["31N1 CHILDREN BAND", "SECOND CHILD ITEM"])
        self.assertNotIn("INJECTED", bases)
        self.assertEqual(rcm.get("category_label"), "Children")
        self.assertEqual(rcm.get("total_candidates_in_scope"), 2)
        self.assertEqual(rcm.get("safe_count"), 2)

    def test_missing_create_post_rejects_future_inventory_start_date(self):
        self._login()
        baseline = RunJob.objects.filter(status=RunJob.STATUS_QUEUED).count()
        with TemporaryDirectory(dir=str(settings.BASE_DIR)) as td:
            final_audit = _write_final_audit(Path(td))
            self._create_inventory_artifact(company_key="company_a", final_audit=final_audit)

            with mock.patch(
                "code_scripts.inventory_review_missing_candidates.load_category_mapping_for_company_key",
                return_value=(PRODUCT_MAPPING, ""),
            ), mock.patch(
                "code_scripts.inventory_review_missing_candidates.load_qbo_base_name_keys_for_company_key",
                return_value=({"aquafina 50cl"}, ""),
            ):
                response = self.client.post(
                    reverse(
                        "epos_qbo:company_inventory_missing_create",
                        kwargs={"company_key": "company_a"},
                    ),
                    {"inventory_start_date": "2099-01-01"},
                )

        self.assertEqual(response.status_code, 302)
        self.assertEqual(RunJob.objects.filter(status=RunJob.STATUS_QUEUED).count(), baseline)

    def test_missing_create_post_rejects_when_qbo_snapshot_unavailable(self):
        self._login()
        baseline_queued = RunJob.objects.filter(status=RunJob.STATUS_QUEUED).count()
        with TemporaryDirectory(dir=str(settings.BASE_DIR)) as td:
            final_audit = _write_final_audit(Path(td))
            self._create_inventory_artifact(company_key="company_a", final_audit=final_audit)

            with mock.patch(
                "code_scripts.inventory_review_missing_candidates.load_category_mapping_for_company_key",
                return_value=(PRODUCT_MAPPING, ""),
            ), mock.patch(
                "code_scripts.inventory_review_missing_candidates.load_qbo_base_name_keys_for_company_key",
                return_value=(set(), "QBO inventory snapshot not found"),
            ):
                response = self.client.post(
                    reverse(
                        "epos_qbo:company_inventory_missing_create",
                        kwargs={"company_key": "company_a"},
                    ),
                    {"inventory_start_date": "2026-04-29"},
                )

        self.assertEqual(response.status_code, 302)
        self.assertEqual(
            RunJob.objects.filter(status=RunJob.STATUS_QUEUED).count(),
            baseline_queued,
        )

    def test_catalog_cleanup_post_sends_one_queued_slack_notification(self):
        self._login()
        with TemporaryDirectory(dir=str(settings.BASE_DIR)) as td:
            final_audit = _write_final_audit(Path(td))
            self._create_inventory_artifact(company_key="company_a", final_audit=final_audit)

            cfg = mock.Mock()
            cfg.slack_webhook_url = "https://hooks.slack.com/services/FAKE"
            with mock.patch(
                "apps.epos_qbo.views.dispatch_next_queued_job",
                return_value=(None, "queued"),
            ), mock.patch(
                "apps.epos_qbo.services.inventory_review_slack.load_company_config",
                return_value=cfg,
            ), mock.patch(
                "apps.epos_qbo.services.inventory_review_slack.send_slack_success",
            ) as slack_mock:
                response = self.client.post(
                    reverse(
                        "epos_qbo:company_inventory_retry_catalog_cleanup",
                        kwargs={"company_key": "company_a"},
                    )
                )

        self.assertEqual(response.status_code, 302)
        slack_mock.assert_called_once()
        msg = slack_mock.call_args.args[0]
        self.assertIn("Catalog cleanup retry", msg)
        self.assertIn("Affected items: 1", msg)
        self.assertIn("op", msg)
        self.assertIn("Source audit:", msg)
        self.assertIn("inventory_pipeline_company_a_", msg)

    def test_quantity_adjustment_post_sends_one_queued_slack_notification(self):
        self._login()
        with TemporaryDirectory(dir=str(settings.BASE_DIR)) as td:
            final_audit = _write_final_audit(Path(td))
            self._create_inventory_artifact(company_key="company_a", final_audit=final_audit)

            cfg = mock.Mock()
            cfg.slack_webhook_url = "https://hooks.slack.com/services/FAKE"
            with mock.patch(
                "apps.epos_qbo.views.dispatch_next_queued_job",
                return_value=(None, "queued"),
            ), mock.patch(
                "apps.epos_qbo.services.inventory_review_slack.load_company_config",
                return_value=cfg,
            ), mock.patch(
                "apps.epos_qbo.services.inventory_review_slack.send_slack_success",
            ) as slack_mock:
                response = self.client.post(
                    reverse(
                        "epos_qbo:company_inventory_retry_quantity_adjustments",
                        kwargs={"company_key": "company_a"},
                    )
                )

        self.assertEqual(response.status_code, 302)
        slack_mock.assert_called_once()
        msg = slack_mock.call_args.args[0]
        self.assertIn("Quantity adjustment retry", msg)
        self.assertIn("Affected items: 1", msg)
        self.assertIn("op", msg)

    def test_missing_create_post_sends_one_queued_slack_notification(self):
        self._login()
        with TemporaryDirectory(dir=str(settings.BASE_DIR)) as td:
            final_audit = _write_final_audit(Path(td))
            self._create_inventory_artifact(company_key="company_a", final_audit=final_audit)

            cfg = mock.Mock()
            cfg.slack_webhook_url = "https://hooks.slack.com/services/FAKE"
            with mock.patch(
                "code_scripts.inventory_review_missing_candidates.load_category_mapping_for_company_key",
                return_value=(PRODUCT_MAPPING, ""),
            ), mock.patch(
                "code_scripts.inventory_review_missing_candidates.load_qbo_base_name_keys_for_company_key",
                return_value=({"aquafina 50cl"}, ""),
            ), mock.patch(
                "apps.epos_qbo.views.dispatch_next_queued_job",
                return_value=(None, "queued"),
            ), mock.patch(
                "apps.epos_qbo.services.inventory_review_slack.load_company_config",
                return_value=cfg,
            ), mock.patch(
                "apps.epos_qbo.services.inventory_review_slack.send_slack_success",
            ) as slack_mock:
                response = self.client.post(
                    reverse(
                        "epos_qbo:company_inventory_missing_create",
                        kwargs={"company_key": "company_a"},
                    ),
                    {"inventory_start_date": "2026-04-29"},
                )

        self.assertEqual(response.status_code, 302)
        slack_mock.assert_called_once()
        msg = slack_mock.call_args.args[0]
        self.assertIn("Missing item creation", msg)
        self.assertIn("Safe candidates: 2", msg)
        self.assertIn("Blocked: 2", msg)
        self.assertIn("InvStartDate: 2026-04-29", msg)
        self.assertIn("op", msg)

    def test_confirm_and_preview_get_send_no_slack(self):
        self._login()
        cfg = mock.Mock()
        cfg.slack_webhook_url = "https://hooks.slack.com/services/FAKE"
        with TemporaryDirectory(dir=str(settings.BASE_DIR)) as td:
            final_audit = _write_final_audit(Path(td))
            self._create_inventory_artifact(company_key="company_a", final_audit=final_audit)

            with mock.patch(
                "apps.epos_qbo.services.inventory_review_slack.load_company_config",
                return_value=cfg,
            ), mock.patch(
                "apps.epos_qbo.services.inventory_review_slack.send_slack_success",
            ) as slack_mock:
                self.client.get(
                    reverse(
                        "epos_qbo:company_inventory_retry_catalog_cleanup_confirm",
                        kwargs={"company_key": "company_a"},
                    )
                )
                self.client.get(
                    reverse(
                        "epos_qbo:company_inventory_retry_quantity_adjustments_confirm",
                        kwargs={"company_key": "company_a"},
                    )
                )
                with mock.patch(
                    "code_scripts.inventory_review_missing_candidates.load_category_mapping_for_company_key",
                    return_value=(PRODUCT_MAPPING, ""),
                ), mock.patch(
                    "code_scripts.inventory_review_missing_candidates.load_qbo_base_name_keys_for_company_key",
                    return_value=({"aquafina 50cl"}, ""),
                ):
                    self.client.get(
                        reverse(
                            "epos_qbo:company_inventory_missing_preview",
                            kwargs={"company_key": "company_a"},
                        )
                    )

        slack_mock.assert_not_called()

    def test_validation_failure_and_empty_queue_send_no_slack(self):
        self._login()
        cfg = mock.Mock()
        cfg.slack_webhook_url = "https://hooks.slack.com/services/FAKE"

        with mock.patch(
            "apps.epos_qbo.services.inventory_review_slack.load_company_config",
            return_value=cfg,
        ), mock.patch(
            "apps.epos_qbo.services.inventory_review_slack.send_slack_success",
        ) as slack_mock:
            with TemporaryDirectory(dir=str(settings.BASE_DIR)) as td:
                final_audit = _write_final_audit(Path(td))
                self._create_inventory_artifact(company_key="company_a", final_audit=final_audit)

                with mock.patch(
                    "code_scripts.inventory_review_missing_candidates.load_category_mapping_for_company_key",
                    return_value=(PRODUCT_MAPPING, ""),
                ), mock.patch(
                    "code_scripts.inventory_review_missing_candidates.load_qbo_base_name_keys_for_company_key",
                    return_value=({"aquafina 50cl"}, ""),
                ):
                    self.client.post(
                        reverse(
                            "epos_qbo:company_inventory_missing_create",
                            kwargs={"company_key": "company_a"},
                        ),
                        {"inventory_start_date": "2099-01-01"},
                    )

        slack_mock.assert_not_called()

        with mock.patch(
            "apps.epos_qbo.services.inventory_review_slack.load_company_config",
            return_value=cfg,
        ), mock.patch(
            "apps.epos_qbo.services.inventory_review_slack.send_slack_success",
        ) as slack_mock2:
            with TemporaryDirectory(dir=str(settings.BASE_DIR)) as td:
                rows = [r for r in FINAL_AUDIT_ROWS if "Pack Conflict" not in r]
                minimal = Path(td) / "minimal.csv"
                minimal.write_text("\n".join(rows), encoding="utf-8")
                self._create_inventory_artifact(company_key="company_a", final_audit=minimal)

                with mock.patch(
                    "apps.epos_qbo.views.dispatch_next_queued_job",
                    return_value=(None, "queued"),
                ):
                    self.client.post(
                        reverse(
                            "epos_qbo:company_inventory_retry_catalog_cleanup",
                            kwargs={"company_key": "company_a"},
                        )
                    )

        slack_mock2.assert_not_called()

    def test_missing_preview_disables_review_link_when_qbo_snapshot_unavailable(self):
        self._login()
        with TemporaryDirectory(dir=str(settings.BASE_DIR)) as td:
            final_audit = _write_final_audit(Path(td))
            self._create_inventory_artifact(company_key="company_a", final_audit=final_audit)

            with mock.patch(
                "code_scripts.inventory_review_missing_candidates.load_category_mapping_for_company_key",
                return_value=(PRODUCT_MAPPING, ""),
            ), mock.patch(
                "code_scripts.inventory_review_missing_candidates.load_qbo_base_name_keys_for_company_key",
                return_value=(set(), "QBO inventory snapshot not found"),
            ):
                response = self.client.get(
                    reverse(
                        "epos_qbo:company_inventory_missing_preview",
                        kwargs={"company_key": "company_a"},
                    )
                )

        self.assertEqual(response.status_code, 200)
        html = response.content.decode("utf-8")
        self.assertIn("QBO snapshot could not be loaded", html)
        self.assertIn("pointer-events-none", html)
        self.assertNotIn(
            reverse(
                "epos_qbo:company_inventory_missing_create",
                kwargs={"company_key": "company_a"},
            ),
            html,
        )

    def test_missing_create_post_does_not_queue_when_no_safe_candidates(self):
        self._login()
        baseline_queued = RunJob.objects.filter(status=RunJob.STATUS_QUEUED).count()
        with TemporaryDirectory(dir=str(settings.BASE_DIR)) as td:
            final_audit = _write_final_audit(Path(td))
            self._create_inventory_artifact(company_key="company_a", final_audit=final_audit)

            def _no_safe(_company_key, _path):
                return {
                    "rows": [],
                    "safe_count": 0,
                    "blocked_count": 2,
                    "mapping_loaded": True,
                    "mapping_error": "",
                    "qbo_base_names_loaded": True,
                    "qbo_base_names_error": "",
                }

            with mock.patch(
                "apps.epos_qbo.services.inventory_review_actions.classify_missing_items_for_audit_file",
                side_effect=_no_safe,
            ):
                response = self.client.post(
                    reverse(
                        "epos_qbo:company_inventory_missing_create",
                        kwargs={"company_key": "company_a"},
                    ),
                    {"inventory_start_date": "2026-04-29"},
                )

        self.assertEqual(response.status_code, 302)
        self.assertEqual(
            RunJob.objects.filter(status=RunJob.STATUS_QUEUED).count(),
            baseline_queued,
        )

    def test_missing_preview_rejects_inventory_disabled_company(self):
        self._login()
        response = self.client.get(
            reverse(
                "epos_qbo:company_inventory_missing_preview",
                kwargs={"company_key": "company_b"},
            )
        )
        self.assertEqual(response.status_code, 302)


class InventoryReviewItemModelGuardTests(TestCase):
    """Make sure the reverted DB-backed review model is not silently re-added."""

    def test_no_inventoryreviewitem_model_exists(self):
        from django.apps import apps

        with self.assertRaises(LookupError):
            apps.get_model("epos_qbo", "InventoryReviewItem")
