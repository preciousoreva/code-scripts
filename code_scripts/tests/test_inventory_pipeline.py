from __future__ import annotations

import argparse
import json
import os
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

import pandas as pd

from code_scripts import inventory_pipeline, inventory_sync
from code_scripts.inventory_safety import InventoryApplyDisabledError


class InventoryPipelineOrchestrationTests(unittest.TestCase):
    def setUp(self):
        self._inventory_apply_env = mock.patch.dict(
            os.environ,
            {"OIAT_ALLOW_INVENTORY_APPLY": "true"},
            clear=False,
        )
        self._inventory_apply_env.start()
        self.addCleanup(self._inventory_apply_env.stop)

    def _args(self, td: str, **overrides) -> argparse.Namespace:
        stock_path = Path(td) / "stock.csv"
        stock_path.write_text("Name,MeasuredCurrentStock\nWidget,5\n", encoding="utf-8")
        defaults = {
            "company": "company_a",
            "stock_csv": str(stock_path),
            "auto_download": False,
            "download_headful": False,
            "download_timeout_ms": None,
            "download_output_dir": None,
            "qbo_csv": None,
            "auto_fetch_qbo": True,
            "qbo_force_refresh": True,
            "qbo_cache_max_age_hours": 24,
            "qbo_export_path": None,
            "categories": [],
            "product_filter": None,
            "max_catalog_fixes": None,
            "max_quantity_adjustments": None,
            "max_qty_delta": None,
            "max_apply_qty_delta": None,
            "max_apply_value_impact": None,
            "allow_zero_cost_apply": False,
            "allow_negative_qbo_qty_apply": False,
            "adjust_account_id": None,
            "txn_date": "2026-04-28",
            "mode": "audit_only",
            "dry_run": False,
            "no_slack": True,
            "summary_output_dir": str(Path(td) / "summaries"),
        }
        defaults.update(overrides)
        return argparse.Namespace(**defaults)

    def _cfg(self) -> SimpleNamespace:
        return SimpleNamespace(
            company_key="company_a",
            display_name="Company A",
            realm_id="123",
            inventory_adjustment_account_id="99",
            inventory_max_qty_delta=None,
            _data={},
            slack_webhook_url="",
        )

    def _audit_result(
        self,
        phase: str,
        qbo_path: Path,
        report: pd.DataFrame | None = None,
    ) -> inventory_pipeline.AuditResult:
        if report is None:
            report = self._quantity_report()
        return inventory_pipeline.AuditResult(
            phase=phase,
            report=report,
            report_path=qbo_path.with_name(f"{phase}.csv"),
            qbo_path=qbo_path,
        )

    def _quantity_report(self, count: int = 1) -> pd.DataFrame:
        return pd.DataFrame(
            [
                {
                    "base_name": f"Widget {i}",
                    "epos_single_units": 5.0,
                    "qbo_qty_on_hand": 2.0,
                    "status": "needs_adjustment",
                    "catalog_issue_type": "exact_name_match",
                }
                for i in range(1, count + 1)
            ]
        )

    def _supported_plan(self, count: int = 1) -> pd.DataFrame:
        return pd.DataFrame(
            [
                {
                    "company_key": "company_a",
                    "base_name": f"Widget {i}",
                    "epos_single_units": 5.0,
                    "catalog_issue_type": "base_with_pack_variants",
                    "planned_action": "consolidate_existing_base_pack_variants",
                    "action_eligible": True,
                    "block_reason": "",
                }
                for i in range(1, count + 1)
            ]
        )

    def _unsupported_plan(self) -> pd.DataFrame:
        return pd.DataFrame(
            [
                {
                    "company_key": "company_a",
                    "base_name": "Pack Only",
                    "epos_single_units": 5.0,
                    "catalog_issue_type": "only_pack_variant_exists",
                    "planned_action": "create_base_then_consolidate_pack_variant",
                    "action_eligible": True,
                    "block_reason": "",
                },
                {
                    "company_key": "company_a",
                    "base_name": "Missing",
                    "epos_single_units": 5.0,
                    "catalog_issue_type": "missing_from_qbo",
                    "planned_action": "create_inventory_item",
                    "action_eligible": False,
                    "block_reason": "missing_in_qbo_requires_item_creation",
                },
                {
                    "company_key": "company_a",
                    "base_name": "Duplicate",
                    "epos_single_units": 5.0,
                    "catalog_issue_type": "multiple_active_base_items",
                    "planned_action": "manual_review_duplicate_base_items",
                    "action_eligible": False,
                    "block_reason": "duplicate_base_items_require_manual_review",
                },
            ]
        )

    def _duplicate_resolvable_plan(self) -> pd.DataFrame:
        return pd.DataFrame(
            [
                {
                    "company_key": "company_a",
                    "base_name": "SMIRNOFF ICE DOUBLE BLACK CAN 330ml",
                    "epos_single_units": 5.0,
                    "catalog_issue_type": "multiple_active_base_items",
                    "planned_action": "resolve_duplicate_base_items",
                    "action_eligible": True,
                    "block_reason": "",
                }
            ]
        )

    def _unsupported_action_plan(self) -> pd.DataFrame:
        return pd.DataFrame(
            [
                {
                    "company_key": "company_a",
                    "base_name": "Wrong Action",
                    "epos_single_units": 5.0,
                    "catalog_issue_type": "base_with_pack_variants",
                    "planned_action": "manual_review_unexpected_action",
                    "action_eligible": True,
                    "block_reason": "",
                }
            ]
        )

    def test_supported_catalog_rows_includes_resolvable_duplicate_base_items(self):
        supported = inventory_pipeline._supported_catalog_rows(self._duplicate_resolvable_plan())

        self.assertEqual(len(supported), 1)
        row = supported.iloc[0].to_dict()
        self.assertEqual(row["planned_action"], "resolve_duplicate_base_items")
        self.assertTrue(row["action_eligible"])

    def _qbo_rows(self, count: int = 1) -> pd.DataFrame:
        return pd.DataFrame(
            [
                {
                    "base_name": f"Widget {i}",
                    "Name": f"Widget {i}",
                    "Id": str(i),
                    "qbo_qty_on_hand": 2.0,
                    "qbo_has_pack": False,
                    "PurchaseCost": 10.0,
                }
                for i in range(1, count + 1)
            ]
        )

    def _risk_policy(self, **overrides) -> inventory_pipeline.QuantityRiskPolicy:
        defaults = {
            "max_apply_qty_delta": 100.0,
            "max_apply_value_impact": 50_000.0,
            "allow_zero_cost_apply": False,
            "allow_negative_qbo_qty_apply": False,
        }
        defaults.update(overrides)
        return inventory_pipeline.QuantityRiskPolicy(**defaults)

    def _patch_common(
        self,
        td: str,
        *,
        plan: pd.DataFrame,
        qbo_paths: list[Path],
        qbo_rows: pd.DataFrame | None = None,
        audit_report: pd.DataFrame | None = None,
    ):
        cfg = self._cfg()
        if qbo_rows is None:
            qbo_rows = self._qbo_rows()

        def audit_side_effect(**kwargs):
            return self._audit_result(kwargs["phase"], kwargs["qbo_path"], report=audit_report)

        patches = [
            mock.patch.object(inventory_pipeline, "load_company_config", return_value=cfg),
            mock.patch.object(inventory_pipeline, "ensure_company_runtime_compatible"),
            mock.patch.object(inventory_pipeline, "_resolve_qbo_snapshot", side_effect=qbo_paths),
            mock.patch.object(inventory_pipeline, "load_qbo_inventory_item_rows", return_value=qbo_rows),
            mock.patch.object(inventory_pipeline, "_run_audit_phase", side_effect=audit_side_effect),
            mock.patch.object(inventory_pipeline, "plan_catalog_cleanup", return_value=plan),
            mock.patch.object(inventory_pipeline, "_catalog_output_path", return_value=Path(td) / "catalog.csv"),
        ]
        entered = [p.start() for p in patches]
        self.addCleanup(lambda: [p.stop() for p in reversed(patches)])
        return entered

    def _summary_payload(self, **overrides) -> dict:
        payload = {
            "display_name": "Company A",
            "company_key": "company_a",
            "scope": "",
            "products_checked": 3,
            "already_correct": 1,
            "in_sync": 1,
            "catalog_fixes_applied": 1,
            "base_items_created": 0,
            "duplicate_base_items_resolved": 0,
            "quantity_updates_applied": 1,
            "blocked_items": 2,
            "missing_base_item_in_qbo": 1,
            "duplicate_base_items_in_qbo": 0,
            "epos_negative_rows_clamped": 0,
            "epos_negative_units_clamped": 0.0,
            "epos_negative_stock_policy": "clamp_to_zero",
            "skipped_unsupported": 2,
            "still_needs_review": 2,
            "unsupported_catalog_issues": {
                "only_pack_variant_exists": 1,
                "missing_from_qbo": 1,
                "multiple_active_base_items": 0,
            },
            "blocked_catalog_examples": [],
            "final_status_counts": {"in_sync": 1, "needs_adjustment": 2},
            "final_catalog_issue_counts": {"exact_name_match": 1, "missing_from_qbo": 1},
            "child_reports": {"final_audit": "/tmp/final.csv"},
            "completion_status": "completed_with_blocked_items",
            "max_catalog_fixes": None,
            "max_quantity_adjustments": None,
            "summary_json": "/tmp/report.json",
        }
        payload.update(overrides)
        return payload

    def test_final_summary_omits_limits_when_unlimited(self):
        msg = inventory_pipeline._format_final_summary(self._summary_payload())
        self.assertIn("Inventory sync completed with blocked items", msg)
        self.assertIn("Blocked items: 2", msg)
        self.assertIn("Missing base item in QBO: 1", msg)
        self.assertIn("Duplicate base items in QBO: 0", msg)
        self.assertNotIn("Catalog fixes limit:", msg)
        self.assertNotIn("Quantity updates limit:", msg)
        self.assertNotIn("only_pack_variant_exists", msg)
        self.assertNotIn("multiple_active_base_items", msg)

    def test_final_summary_includes_limits_when_explicit(self):
        msg = inventory_pipeline._format_final_summary(
            self._summary_payload(max_catalog_fixes=1, max_quantity_adjustments=2)
        )
        self.assertIn("Catalog fixes limit: 1", msg)
        self.assertIn("Quantity updates limit: 2", msg)

    def test_final_summary_includes_base_items_created(self):
        msg = inventory_pipeline._format_final_summary(self._summary_payload(base_items_created=3))
        self.assertIn("Base items created: 3", msg)

    def test_final_summary_includes_duplicate_base_items_resolved(self):
        msg = inventory_pipeline._format_final_summary(self._summary_payload(duplicate_base_items_resolved=1))
        self.assertIn("Duplicate base items resolved: 1", msg)

    def test_final_summary_omits_epos_negative_clamp_line_when_zero(self):
        msg = inventory_pipeline._format_final_summary(self._summary_payload(epos_negative_rows_clamped=0))
        self.assertNotIn("EPOS negative rows clamped to zero:", msg)

    def test_final_summary_includes_epos_negative_clamp_line_when_nonzero(self):
        msg = inventory_pipeline._format_final_summary(
            self._summary_payload(epos_negative_rows_clamped=1, epos_negative_units_clamped=30.0)
        )
        self.assertIn("EPOS negative rows clamped to zero: 1", msg)
        self.assertIn("(30.0 units)", msg)

    def test_final_summary_uses_success_wording_for_clean_run(self):
        msg = inventory_pipeline._format_final_summary(
            self._summary_payload(
                products_checked=147,
                already_correct=147,
                in_sync=147,
                blocked_items=0,
                missing_base_item_in_qbo=0,
                duplicate_base_items_in_qbo=0,
                still_needs_review=0,
                final_status_counts={"in_sync": 147},
                final_catalog_issue_counts={"exact_name_match": 147},
                completion_status="clean",
                skipped_unsupported=0,
            )
        )
        self.assertIn("Inventory sync completed successfully", msg)
        self.assertIn("In sync / Products clean: 147", msg)

    def test_final_summary_uses_failed_wording_when_requested(self):
        msg = inventory_pipeline._format_final_summary(
            self._summary_payload(completion_status="failed")
        )
        self.assertIn("Inventory sync failed", msg)

    def test_final_summary_includes_blocked_examples_when_blocked_count_is_small(self):
        msg = inventory_pipeline._format_final_summary(
            self._summary_payload(
                skipped_unsupported=2,
                blocked_catalog_examples=[
                    "BACARDI WHITE RUM 750ml — QBO only has pack variant BACARDI WHITE RUM 750ml*12",
                    "SMIRNOFF ICE DOUBLE BLACK CAN 330ml — multiple active base items in QBO",
                ],
            )
        )
        self.assertIn("Blocked examples:", msg)
        self.assertIn("BACARDI WHITE RUM 750ml", msg)
        self.assertIn("SMIRNOFF ICE DOUBLE BLACK CAN 330ml", msg)
        self.assertNotIn("only_pack_variant_exists", msg)

    def test_product_filtered_summary_includes_reconciliation_details(self):
        msg = inventory_pipeline._format_final_summary(
            self._summary_payload(
                product_details=[
                    {
                        "base_name": "ACTION BITTERS50ml",
                        "epos_expected_qty": 15.0,
                        "qbo_final_qty": 15.0,
                        "delta": 0.0,
                        "catalog_fix_applied": False,
                        "base_item_created": False,
                        "duplicate_base_resolved": False,
                        "quantity_adjustment_applied": True,
                        "final_status": "in_sync",
                        "epos_negative_rows_clamped": 1,
                        "epos_negative_units_clamped": 30.0,
                    }
                ]
            )
        )
        self.assertIn("Product details:", msg)
        self.assertIn("ACTION BITTERS50ml: EPOS=15.0 QBO=15.0 delta=0.0 status=in_sync", msg)
        self.assertIn("negative_rows_clamped=1", msg)
        self.assertIn("negative_units_clamped=30.0", msg)

    def test_product_filtered_blocked_summary_includes_reason(self):
        msg = inventory_pipeline._format_final_summary(
            self._summary_payload(
                product_details=[
                    {
                        "base_name": "Blocked Widget",
                        "epos_expected_qty": 5.0,
                        "qbo_final_qty": 0.0,
                        "delta": 5.0,
                        "final_status": "missing_in_qbo",
                        "blocked_reason": "product not found in QuickBooks",
                    }
                ]
            )
        )
        self.assertIn("Blocked Widget", msg)
        self.assertIn("reason=product not found in QuickBooks", msg)

    def test_slack_product_success_is_compact_and_operator_friendly(self):
        msg = inventory_pipeline._format_slack_summary(
            self._summary_payload(
                scope="product=ACTION BITTERS50ml",
                products_checked=1,
                already_correct=1,
                in_sync=1,
                catalog_fixes_applied=0,
                blocked_items=0,
                still_needs_review=0,
                quantity_updates_applied=1,
                completion_status="clean",
                run_url="https://portal.example/epos-qbo/runs/job-1/",
                product_details=[
                    {
                        "base_name": "ACTION BITTERS50ml",
                        "epos_expected_qty": 15.0,
                        "qbo_final_qty": 15.0,
                        "delta": 0.0,
                        "final_status": "in_sync",
                    }
                ],
            )
        )

        self.assertIn("✅ Inventory synced — ACTION BITTERS50ml", msg)
        self.assertIn("EPOS: 15", msg)
        self.assertIn("QBO: 15", msg)
        self.assertIn("Difference: 0", msg)
        self.assertIn("Updated in QBO: Yes", msg)
        self.assertIn("Catalog fixes: 0", msg)
        self.assertIn("Blocked: 0", msg)
        self.assertIn("Run: <https://portal.example/epos-qbo/runs/job-1/|Inventory Run", msg)
        self.assertNotIn("negative EPOS", msg)
        self.assertNotIn("Report path:", msg)
        self.assertNotIn("epos_expected_qty", msg)
        self.assertNotIn("EPOS=", msg)

    def test_slack_category_success_includes_sync_ratio_and_negative_note(self):
        msg = inventory_pipeline._format_slack_summary(
            self._summary_payload(
                scope="category=ALCOHOLS & SPIRITS",
                products_checked=147,
                already_correct=147,
                in_sync=147,
                catalog_fixes_applied=0,
                quantity_updates_applied=0,
                blocked_items=0,
                still_needs_review=0,
                completion_status="clean",
                epos_negative_rows_clamped=1,
                epos_negative_units_clamped=30.0,
                run_url="https://portal.example/epos-qbo/runs/job-2/",
            )
        )

        self.assertIn("✅ Inventory synced — ALCOHOLS & SPIRITS", msg)
        self.assertIn("• In sync: 147 / 147", msg)
        self.assertIn("• Catalog fixes: 0", msg)
        self.assertIn("• Quantity updates: 0", msg)
        self.assertIn("• Blocked: 0", msg)
        self.assertIn("Negative EPOS stock: 1 row(s) treated as 0 for sync safety (30 units).", msg)
        self.assertNotIn("negative_rows_clamped", msg)

    def test_slack_blocked_summary_shows_first_item_and_reason(self):
        msg = inventory_pipeline._format_slack_summary(
            self._summary_payload(
                scope="category=ALCOHOLS & SPIRITS",
                products_checked=147,
                already_correct=146,
                in_sync=146,
                blocked_items=1,
                still_needs_review=1,
                completion_status="completed_with_blocked_items",
                blocked_catalog_examples=[
                    "SMIRNOFF ICE DOUBLE BLACK CAN 330ml — multiple_active_base_items",
                ],
                run_url="https://portal.example/epos-qbo/runs/job-3/",
            )
        )

        self.assertIn("⚠️ *Inventory Sync needs review* — Company A", msg)
        self.assertIn("• In sync: 146 / 147", msg)
        self.assertIn("• Blocked: 1", msg)
        self.assertIn("SMIRNOFF ICE DOUBLE BLACK CAN 330ml", msg)
        self.assertIn("duplicate base items in QBO", msg)
        self.assertNotIn("catalog_issue_type", msg)
        self.assertNotIn("multiple_active_base_items", msg)
        self.assertNotIn("/tmp/report.json", msg)

    def test_slack_product_blocked_summary_keeps_reconciliation_numbers(self):
        msg = inventory_pipeline._format_slack_summary(
            self._summary_payload(
                scope="product=ACTION BITTERS50ml",
                products_checked=1,
                already_correct=0,
                in_sync=0,
                blocked_items=1,
                still_needs_review=1,
                completion_status="completed_with_blocked_items",
                product_details=[
                    {
                        "base_name": "ACTION BITTERS50ml",
                        "epos_expected_qty": 15.0,
                        "qbo_final_qty": -15.0,
                        "delta": 30.0,
                        "final_status": "needs_adjustment",
                    }
                ],
                run_url="https://portal.example/epos-qbo/runs/job-5/",
            )
        )

        self.assertIn("⚠️ *Inventory Sync needs review* — Company A", msg)
        self.assertIn("• EPOS: 15", msg)
        self.assertIn("• QBO: -15", msg)
        self.assertIn("• Difference: 30", msg)
        self.assertIn("• Blocked: 1", msg)
        self.assertIn("Reason: quantity mismatch needs review", msg)
        self.assertNotIn("In sync:", msg)

    def test_slack_failed_summary_uses_short_reason_and_report_filename_without_run_link(self):
        msg = inventory_pipeline._format_slack_summary(
            self._summary_payload(
                scope="category=ALCOHOLS & SPIRITS",
                completion_status="failed",
                error=(
                    "QBO query failed: HTTP 400: QueryValidationError: "
                    "Property SubItem not found for Entity Item"
                ),
                run_url="",
                summary_json="/tmp/reports/inventory_pipeline_company_a_120000.json",
            )
        )

        self.assertIn("❌ Inventory sync failed — ALCOHOLS & SPIRITS", msg)
        self.assertIn("Reason: QBO query failed", msg)
        self.assertNotIn("SubItem", msg)
        self.assertIn("Report: inventory_pipeline_company_a_120000.json", msg)
        self.assertNotIn("/tmp/reports", msg)

    def test_slack_report_filename_is_hidden_when_run_link_exists(self):
        msg = inventory_pipeline._format_slack_summary(
            self._summary_payload(
                scope="category=ALCOHOLS & SPIRITS",
                products_checked=1,
                already_correct=1,
                in_sync=1,
                blocked_items=0,
                still_needs_review=0,
                completion_status="clean",
                run_url="https://portal.example/epos-qbo/runs/job-4/",
                summary_json="/tmp/reports/inventory_pipeline_company_a_120000.json",
            )
        )

        self.assertIn("Run: <https://portal.example/epos-qbo/runs/job-4/|Inventory Run", msg)
        self.assertNotIn("Report:", msg)
        self.assertNotIn("inventory_pipeline_company_a_120000.json", msg)

    def test_write_summary_reports_includes_blocked_fields_in_csv_and_json(self):
        with tempfile.TemporaryDirectory() as td:
            summary = self._summary_payload(
                skipped_unsupported=2,
                unsupported_catalog_issues={
                    "only_pack_variant_exists": 1,
                    "multiple_active_base_items": 1,
                },
                blocked_catalog_examples=[
                    "BACARDI WHITE RUM 750ml — QBO only has pack variant BACARDI WHITE RUM 750ml*12",
                ],
            )
            json_path, csv_path = inventory_pipeline._write_summary_reports(summary, output_dir=td)
            payload = json.loads(Path(json_path).read_text(encoding="utf-8"))
            self.assertEqual(
                payload["blocked_catalog_examples"][0],
                "BACARDI WHITE RUM 750ml — QBO only has pack variant BACARDI WHITE RUM 750ml*12",
            )

            csv_rows = pd.read_csv(csv_path).to_dict(orient="records")
            self.assertEqual(len(csv_rows), 1)
            row = csv_rows[0]
            self.assertEqual(int(row["blocked_items"]), 2)
            self.assertEqual(int(row["missing_base_item_in_qbo"]), 1)
            self.assertEqual(int(row["duplicate_base_items_in_qbo"]), 0)
            self.assertEqual(int(row["epos_negative_rows_clamped"]), 0)
            self.assertEqual(float(row["epos_negative_units_clamped"]), 0.0)
            self.assertEqual(row["epos_negative_stock_policy"], "clamp_to_zero")
            self.assertEqual(row["final_audit"], "/tmp/final.csv")
            self.assertEqual(row["run_type"], "inventory_pipeline")

    def test_summary_schema_always_includes_stable_fields_and_zero_values(self):
        with tempfile.TemporaryDirectory() as td:
            json_path, _csv_path = inventory_pipeline._write_summary_reports(
                {
                    "run_type": "inventory_pipeline",
                    "company_key": "company_a",
                    "display_name": "Company A",
                    "child_reports": {"final_audit": "/tmp/final.csv"},
                },
                output_dir=td,
            )
            payload = json.loads(Path(json_path).read_text(encoding="utf-8"))

        for key in [
            "run_type",
            "company_key",
            "display_name",
            "scope",
            "started_at",
            "finished_at",
            "run_job_id",
            "stock_csv",
            "qbo_csv",
            "child_reports",
            "summary_json",
            "summary_csv",
            "dry_run",
            "products_checked",
            "already_correct",
            "in_sync",
            "catalog_fixes_applied",
            "base_items_created",
            "duplicate_base_items_resolved",
            "quantity_updates_applied",
            "blocked_items",
            "missing_base_item_in_qbo",
            "duplicate_base_items_in_qbo",
            "epos_negative_rows_clamped",
            "epos_negative_units_clamped",
            "epos_negative_stock_policy",
            "still_needs_review",
            "skipped_unsupported",
            "unsupported_catalog_issues",
            "blocked_catalog_examples",
            "created_base_details",
            "duplicate_resolution_details",
            "quantity_adjustment_stats",
            "final_status_counts",
            "final_catalog_issue_counts",
        ]:
            self.assertIn(key, payload)
        self.assertEqual(payload["blocked_items"], 0)
        self.assertEqual(payload["catalog_fixes_applied"], 0)
        self.assertEqual(payload["epos_negative_rows_clamped"], 0)
        self.assertEqual(payload["epos_negative_units_clamped"], 0.0)
        self.assertEqual(payload["epos_negative_stock_policy"], "clamp_to_zero")

    def test_pipeline_summary_derives_epos_negative_clamp_totals_and_product_detail(self):
        with tempfile.TemporaryDirectory() as td:
            qbo_initial = Path(td) / "qbo-initial.csv"
            audit_report = pd.DataFrame(
                [
                    {
                        "base_name": "ACTION BITTERS50ml",
                        "epos_single_units": 15.0,
                        "qbo_qty_on_hand": 15.0,
                        "delta": 0.0,
                        "status": "in_sync",
                        "catalog_issue_type": "exact_name_match",
                        "epos_negative_rows_clamped": 1,
                        "epos_negative_units_clamped": 30.0,
                        "epos_negative_stock_policy": "clamp_to_zero",
                        "epos_negative_clamped_row_names": "ACTION BITTERS50ml*120",
                    }
                ]
            )
            self._patch_common(
                td,
                plan=pd.DataFrame(),
                qbo_paths=[qbo_initial],
                audit_report=audit_report,
            )
            summary = inventory_pipeline.run_inventory_pipeline(
                self._args(td, product_filter="ACTION BITTERS50ml")
            )

        self.assertEqual(summary["epos_negative_stock_policy"], "clamp_to_zero")
        self.assertEqual(summary["epos_negative_rows_clamped"], 1)
        self.assertEqual(summary["epos_negative_units_clamped"], 30.0)
        self.assertEqual(summary["product_details"][0]["base_name"], "ACTION BITTERS50ml")
        self.assertEqual(summary["product_details"][0]["epos_expected_qty"], 15.0)
        self.assertEqual(summary["product_details"][0]["epos_negative_rows_clamped"], 1)
        self.assertEqual(summary["product_details"][0]["epos_negative_units_clamped"], 30.0)

    def test_product_filter_with_pack_multiplier_runs_real_audit_path(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            stock_path = root / "goldberg_stock.csv"
            stock_path.write_text(
                "Name,CategoryName,MeasuredCurrentStock\n"
                "GOLDBERG CAN 50cl*24,ALCOHOLS & SPIRITS,2\n",
                encoding="utf-8",
            )
            qbo_path = root / "qbo.csv"
            qbo_path.write_text(
                "Id,Name,Type,TrackQtyOnHand,QtyOnHand\n"
                "99,GOLDBERG CAN 50cl*24,Inventory,true,1\n",
                encoding="utf-8",
            )
            cfg = self._cfg()
            with mock.patch.object(inventory_pipeline, "load_company_config", return_value=cfg), \
                 mock.patch.object(inventory_pipeline, "ensure_company_runtime_compatible"), \
                 mock.patch.object(inventory_pipeline, "_resolve_qbo_snapshot", return_value=qbo_path), \
                 mock.patch.object(inventory_pipeline, "_catalog_output_path", return_value=root / "catalog.csv"), \
                 mock.patch.object(
                     inventory_pipeline,
                     "_run_apply_for_existing_base_pack_variants",
                     return_value={
                         "exit_code": 0,
                         "attempted": 1,
                         "consolidated": 1,
                         "cleaned_up": 1,
                         "skipped": 0,
                         "failed": 0,
                         "base_items_created": 1,
                         "created_base_details": [{"base_name": "GOLDBERG CAN 50cl", "base_item_id": "123", "created": True}],
                     },
                 ) as cleanup_mock:
                summary = inventory_pipeline.run_inventory_pipeline(
                    self._args(
                        td,
                        stock_csv=str(stock_path),
                        product_filter="GOLDBERG CAN 50cl*24",
                        categories=["ALCOHOLS & SPIRITS"],
                        mode="catalog_apply_admin_only",
                        max_catalog_fixes=1,
                    )
                )

        cleanup_mock.assert_called_once()
        self.assertEqual(summary["products_checked"], 1)
        self.assertEqual(summary["catalog_fixes_applied"], 1)
        self.assertEqual(summary["base_items_created"], 1)
        self.assertEqual(summary["quantity_updates_applied"], 0)
        self.assertEqual(summary["unsupported_catalog_issues"].get("only_pack_variant_exists", 0), 0)

    def test_pipeline_auto_fetch_retries_when_optional_qbo_diagnostics_are_unavailable(self):
        from urllib.parse import parse_qs, unquote, urlparse

        class FakeResponse:
            def __init__(self, status_code, text="", payload=None):
                self.status_code = status_code
                self.text = text
                self._payload = payload or {}

            def json(self):
                return self._payload

        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            stock_path = root / "action_stock.csv"
            stock_path.write_text(
                "Name,CategoryName,MeasuredCurrentStock,Current Volume,Total Stock\n"
                "ACTION BITTERS50ml*20,ALCOHOLS & SPIRITS,0,15 of 20 Each,0.75\n"
                "ACTION BITTERS50ml*120,ALCOHOLS & SPIRITS,0,-30 of 120 Each,-0.25\n",
                encoding="utf-8",
            )
            qbo_export_path = root / "qbo.csv"
            queries: list[str] = []
            responses = [
                FakeResponse(
                    400,
                    "QueryValidationError: Property ParentRef not found for Entity Item",
                ),
                FakeResponse(
                    200,
                    payload={
                        "QueryResponse": {
                            "Item": [
                                {
                                    "Id": "10",
                                    "Name": "ACTION BITTERS50ml",
                                    "Type": "Inventory",
                                    "TrackQtyOnHand": True,
                                    "QtyOnHand": 15,
                                    "Active": True,
                                }
                            ]
                        }
                    },
                ),
            ]

            def fake_request(_method, url, _token_mgr):
                raw_query = parse_qs(urlparse(url).query)["query"][0]
                queries.append(unquote(raw_query))
                return responses.pop(0)

            cfg = self._cfg()
            with mock.patch.object(inventory_pipeline, "load_company_config", return_value=cfg), \
                 mock.patch.object(inventory_pipeline, "ensure_company_runtime_compatible"), \
                 mock.patch.object(inventory_sync, "verify_realm_match"), \
                 mock.patch.object(inventory_sync, "TokenManager", return_value=mock.Mock()), \
                 mock.patch.object(inventory_sync, "get_qbo_api_base_url", return_value="https://qbo.example"), \
                 mock.patch.object(inventory_sync, "_make_qbo_request", side_effect=fake_request), \
                 mock.patch.object(inventory_pipeline, "_catalog_output_path", return_value=root / "catalog.csv"):
                summary = inventory_pipeline.run_inventory_pipeline(
                    self._args(
                        td,
                        stock_csv=str(stock_path),
                        qbo_export_path=str(qbo_export_path),
                        product_filter="ACTION BITTERS50ml",
                        categories=["ALCOHOLS & SPIRITS"],
                    )
                )

        self.assertEqual(summary["completion_status"], "clean")
        self.assertEqual(summary["products_checked"], 1)
        self.assertEqual(summary["in_sync"], 1)
        self.assertEqual(summary["epos_negative_rows_clamped"], 1)
        self.assertEqual(len(queries), 2)
        self.assertIn("ParentRef", queries[0])
        self.assertNotIn("SubItem", queries[0])
        self.assertNotIn("ParentRef", queries[1])
        self.assertIn("Active", queries[1])

    def test_case_insensitive_base_detection_enables_pack_consolidation_flow(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            stock_path = root / "legend_stock.csv"
            stock_path.write_text(
                "Name,CategoryName,MeasuredCurrentStock\n"
                "LEGEND EXTRA STOUT CAN 440ml*24,ALCOHOLS & SPIRITS,1\n",
                encoding="utf-8",
            )
            qbo_path = root / "qbo.csv"
            qbo_path.write_text(
                "Id,Name,Type,TrackQtyOnHand,QtyOnHand\n"
                "10,LEGEND EXTRA STOUT CAN 440ML,Inventory,true,-299\n"
                "11,LEGEND EXTRA STOUT CAN 440ml*12,Inventory,true,10\n"
                "12,LEGEND EXTRA STOUT CAN 440ml*24,Inventory,true,20\n",
                encoding="utf-8",
            )
            cfg = self._cfg()
            with mock.patch.object(inventory_pipeline, "load_company_config", return_value=cfg), \
                 mock.patch.object(inventory_pipeline, "ensure_company_runtime_compatible"), \
                 mock.patch.object(inventory_pipeline, "_resolve_qbo_snapshot", return_value=qbo_path), \
                 mock.patch.object(inventory_pipeline, "_catalog_output_path", return_value=root / "catalog.csv"), \
                 mock.patch.object(inventory_pipeline, "_run_apply_for_existing_base_pack_variants", return_value=0) as cleanup_mock:
                summary = inventory_pipeline.run_inventory_pipeline(
                    self._args(
                        td,
                        stock_csv=str(stock_path),
                        product_filter="LEGEND EXTRA STOUT CAN 440ml*24",
                        categories=["ALCOHOLS & SPIRITS"],
                        mode="catalog_apply_admin_only",
                        max_catalog_fixes=1,
                    )
                )

        cleanup_mock.assert_called_once()
        self.assertEqual(summary["products_checked"], 1)
        self.assertEqual(summary["catalog_fixes_applied"], 1)
        self.assertEqual(summary["skipped_unsupported"], 0)
        self.assertEqual(summary["quantity_updates_applied"], 0)

    def test_default_mode_performs_no_qbo_writes(self):
        with tempfile.TemporaryDirectory() as td:
            qbo_path = Path(td) / "qbo.csv"
            self._patch_common(td, plan=self._supported_plan(), qbo_paths=[qbo_path])
            with mock.patch.object(
                inventory_pipeline,
                "_run_apply_for_existing_base_pack_variants",
            ) as catalog_mock, mock.patch.object(
                inventory_pipeline,
                "_apply_exact_match_quantity_adjustments",
            ) as quantity_mock:
                summary = inventory_pipeline.run_inventory_pipeline(self._args(td))

            catalog_mock.assert_not_called()
            quantity_mock.assert_not_called()
            self.assertEqual(summary["inventory_mode"], "audit_only")
            self.assertEqual(summary["catalog_fixes_applied"], 0)
            self.assertEqual(summary["quantity_updates_applied"], 0)

    def test_quantity_preview_reports_planned_quantity_without_posting(self):
        with tempfile.TemporaryDirectory() as td:
            qbo_path = Path(td) / "qbo.csv"
            self._patch_common(
                td,
                plan=pd.DataFrame(),
                qbo_paths=[qbo_path],
                qbo_rows=self._qbo_rows(),
                audit_report=self._quantity_report(),
            )
            with mock.patch.object(inventory_pipeline, "post_inventory_adjustment") as post_mock:
                summary = inventory_pipeline.run_inventory_pipeline(
                    self._args(td, mode="quantity_preview")
                )

            self.assertEqual(summary["inventory_mode"], "quantity_preview")
            self.assertEqual(summary["write_intent"], "preview")
            self.assertEqual(summary["quantity_adjustment_stats"]["planned"], 1)
            detail = summary["quantity_adjustment_stats"]["details"][0]
            self.assertEqual(detail["epos_product_name"], "Widget 1")
            self.assertEqual(detail["normalized_base_name"], "widget 1")
            self.assertEqual(detail["qbo_item_id"], "1")
            self.assertEqual(detail["qbo_item_name"], "Widget 1")
            self.assertEqual(detail["qbo_qty"], 2.0)
            self.assertEqual(detail["epos_expected_qty"], 5.0)
            self.assertEqual(detail["qty_delta"], 3.0)
            self.assertEqual(detail["qbo_cost"], 10.0)
            self.assertEqual(detail["estimated_value_impact"], 30.0)
            self.assertEqual(detail["risk_flags"], "")
            self.assertEqual(detail["risk_level"], "low")
            self.assertEqual(detail["recommended_action"], "eligible_for_apply")
            self.assertTrue(detail["apply_eligible"])
            self.assertEqual(summary["quantity_preview_candidates"], 1)
            self.assertEqual(summary["quantity_apply_eligible"], 1)
            self.assertEqual(summary["quantity_blocked_by_risk"], 0)
            self.assertIn("quantity_preview_csv", summary["child_reports"])
            payload = json.loads(Path(summary["summary_json"]).read_text(encoding="utf-8"))
            self.assertEqual(payload["quantity_preview_candidates"], 1)
            self.assertEqual(payload["quantity_risk_flag_counts"], {})
            self.assertEqual(summary["quantity_updates_applied"], 0)
            post_mock.assert_not_called()

    def test_quantity_preview_blocks_negative_qbo_quantity(self):
        qbo_rows = self._qbo_rows()
        qbo_rows.loc[0, "qbo_qty_on_hand"] = -2.0
        result = inventory_pipeline._apply_exact_match_quantity_adjustments(
            cfg=self._cfg(),
            audit_df=self._quantity_report(),
            qbo_item_rows=qbo_rows,
            max_quantity_adjustments=None,
            max_qty_delta=None,
            risk_policy=self._risk_policy(),
            adjust_account_id=None,
            adjust_account_name="Inventory Shrinkage",
            txn_date="2026-04-28",
            dry_run=True,
        )

        detail = result["details"][0]
        self.assertIn("negative_qbo_qty", detail["risk_flags"])
        self.assertFalse(detail["apply_eligible"])
        self.assertEqual(result["planned"], 0)
        self.assertEqual(result["quantity_blocked_by_risk"], 1)

    def test_quantity_preview_blocks_zero_or_missing_cost_by_default(self):
        for cost in (0.0, None):
            with self.subTest(cost=cost):
                qbo_rows = self._qbo_rows()
                qbo_rows.loc[0, "PurchaseCost"] = cost
                result = inventory_pipeline._apply_exact_match_quantity_adjustments(
                    cfg=self._cfg(),
                    audit_df=self._quantity_report(),
                    qbo_item_rows=qbo_rows,
                    max_quantity_adjustments=None,
                    max_qty_delta=None,
                    risk_policy=self._risk_policy(),
                    adjust_account_id=None,
                    adjust_account_name="Inventory Shrinkage",
                    txn_date="2026-04-28",
                    dry_run=True,
                )

                detail = result["details"][0]
                self.assertIn("missing_or_zero_cost", detail["risk_flags"])
                self.assertFalse(detail["apply_eligible"])
                self.assertEqual(result["planned"], 0)

    def test_quantity_preview_blocks_large_delta(self):
        audit = self._quantity_report()
        audit.loc[0, "epos_single_units"] = 500.0
        result = inventory_pipeline._apply_exact_match_quantity_adjustments(
            cfg=self._cfg(),
            audit_df=audit,
            qbo_item_rows=self._qbo_rows(),
            max_quantity_adjustments=None,
            max_qty_delta=None,
            risk_policy=self._risk_policy(max_apply_qty_delta=100.0),
            adjust_account_id=None,
            adjust_account_name="Inventory Shrinkage",
            txn_date="2026-04-28",
            dry_run=True,
        )

        detail = result["details"][0]
        self.assertIn("large_delta", detail["risk_flags"])
        self.assertFalse(detail["apply_eligible"])

    def test_quantity_preview_blocks_large_value_impact(self):
        qbo_rows = self._qbo_rows()
        qbo_rows.loc[0, "PurchaseCost"] = 1000.0
        result = inventory_pipeline._apply_exact_match_quantity_adjustments(
            cfg=self._cfg(),
            audit_df=self._quantity_report(),
            qbo_item_rows=qbo_rows,
            max_quantity_adjustments=None,
            max_qty_delta=None,
            risk_policy=self._risk_policy(max_apply_value_impact=100.0),
            adjust_account_id=None,
            adjust_account_name="Inventory Shrinkage",
            txn_date="2026-04-28",
            dry_run=True,
        )

        detail = result["details"][0]
        self.assertIn("large_value_impact", detail["risk_flags"])
        self.assertFalse(detail["apply_eligible"])

    def test_quantity_apply_posts_only_eligible_rows_and_skips_blocked(self):
        qbo_rows = self._qbo_rows(count=2)
        qbo_rows.loc[1, "PurchaseCost"] = 0.0
        audit = self._quantity_report(count=2)
        lock = mock.Mock()
        lock.acquire.return_value = SimpleNamespace(acquired=True, reason="")
        with mock.patch.object(inventory_pipeline, "verify_realm_match"), \
             mock.patch.object(inventory_pipeline, "TokenManager"), \
             mock.patch.object(inventory_pipeline, "GlobalRunLock", return_value=lock), \
             mock.patch.object(inventory_pipeline, "post_inventory_adjustment", return_value={}) as post_mock, \
             mock.patch.object(inventory_pipeline, "mark_qbo_snapshot_stale"):
            result = inventory_pipeline._apply_exact_match_quantity_adjustments(
                cfg=self._cfg(),
                audit_df=audit,
                qbo_item_rows=qbo_rows,
                max_quantity_adjustments=None,
                max_qty_delta=None,
                risk_policy=self._risk_policy(),
                adjust_account_id=None,
                adjust_account_name="Inventory Shrinkage",
                txn_date="2026-04-28",
                dry_run=False,
            )

        self.assertEqual(post_mock.call_count, 1)
        self.assertEqual(result["posted"], 1)
        self.assertEqual(result["skipped_blocked_by_risk"], 1)
        self.assertEqual(result["quantity_apply_eligible"], 1)
        self.assertEqual(result["quantity_blocked_by_risk"], 1)

    def test_audit_only_summary_includes_mode_write_fields(self):
        with tempfile.TemporaryDirectory() as td:
            qbo_path = Path(td) / "qbo.csv"
            self._patch_common(td, plan=self._supported_plan(), qbo_paths=[qbo_path])
            summary = inventory_pipeline.run_inventory_pipeline(self._args(td))
            payload = json.loads(Path(summary["summary_json"]).read_text(encoding="utf-8"))

        self.assertEqual(payload["inventory_mode"], "audit_only")
        self.assertEqual(payload["write_intent"], "none")
        self.assertFalse(payload["qbo_write_attempted"])
        self.assertFalse(payload["catalog_apply_enabled"])
        self.assertFalse(payload["quantity_apply_enabled"])
        self.assertFalse(payload["missing_item_create_enabled"])

    def test_catalog_plan_only_writes_plan_but_does_not_apply(self):
        with tempfile.TemporaryDirectory() as td:
            qbo_path = Path(td) / "qbo.csv"
            self._patch_common(td, plan=self._supported_plan(), qbo_paths=[qbo_path])
            with mock.patch.object(
                inventory_pipeline,
                "_run_apply_for_existing_base_pack_variants",
            ) as cleanup_mock:
                summary = inventory_pipeline.run_inventory_pipeline(
                    self._args(td, mode="catalog_plan_only")
                )

            cleanup_mock.assert_not_called()
            self.assertEqual(summary["inventory_mode"], "catalog_plan_only")
            self.assertIn("catalog_cleanup", summary["child_reports"])
            self.assertEqual(summary["catalog_fixes_applied"], 0)

    def test_catalog_apply_admin_only_blocks_production_without_override(self):
        with tempfile.TemporaryDirectory() as td:
            qbo_path = Path(td) / "qbo.csv"
            cfg = self._cfg()
            cfg.qbo_environment = "production"
            self._patch_common(td, plan=self._supported_plan(), qbo_paths=[qbo_path])
            with mock.patch.dict(os.environ, {}, clear=True), \
                 mock.patch.object(inventory_pipeline, "load_company_config", return_value=cfg), \
                 mock.patch.object(
                     inventory_pipeline,
                     "_run_apply_for_existing_base_pack_variants",
                     side_effect=InventoryApplyDisabledError("blocked"),
                 ) as cleanup_mock:
                with self.assertRaises(InventoryApplyDisabledError):
                    inventory_pipeline.run_inventory_pipeline(
                        self._args(td, mode="catalog_apply_admin_only", max_catalog_fixes=1)
                    )

        cleanup_mock.assert_called_once()

    def test_quantity_apply_blocks_production_without_inventory_apply_override(self):
        cfg = self._cfg()
        cfg.qbo_environment = "production"
        with mock.patch.dict(os.environ, {}, clear=True), \
             mock.patch.object(inventory_pipeline, "post_inventory_adjustment") as post_mock, \
             mock.patch.object(inventory_pipeline, "TokenManager") as token_mock:
            with self.assertRaises(InventoryApplyDisabledError):
                inventory_pipeline._apply_exact_match_quantity_adjustments(
                    cfg=cfg,
                    audit_df=self._quantity_report(),
                    qbo_item_rows=self._qbo_rows(),
                    max_quantity_adjustments=None,
                    max_qty_delta=None,
                    risk_policy=self._risk_policy(),
                    adjust_account_id=None,
                    adjust_account_name="Inventory Shrinkage",
                    txn_date="2026-04-28",
                    dry_run=False,
                )
        post_mock.assert_not_called()
        token_mock.assert_not_called()

    def test_quantity_dry_run_preview_allowed_in_production_without_override(self):
        cfg = self._cfg()
        cfg.qbo_environment = "production"
        with mock.patch.dict(os.environ, {}, clear=True), \
             mock.patch.object(inventory_pipeline, "post_inventory_adjustment") as post_mock, \
             mock.patch.object(inventory_pipeline, "TokenManager") as token_mock:
            result = inventory_pipeline._apply_exact_match_quantity_adjustments(
                cfg=cfg,
                audit_df=self._quantity_report(),
                qbo_item_rows=self._qbo_rows(),
                max_quantity_adjustments=None,
                max_qty_delta=None,
                risk_policy=self._risk_policy(),
                adjust_account_id=None,
                adjust_account_name="Inventory Shrinkage",
                txn_date="2026-04-28",
                dry_run=True,
            )

        self.assertEqual(result["planned"], 1)
        self.assertEqual(result["posted"], 0)
        post_mock.assert_not_called()
        token_mock.assert_not_called()

    def test_review_create_missing_items_blocks_production_without_override(self):
        cfg = self._cfg()
        cfg.qbo_environment = "production"
        with tempfile.TemporaryDirectory() as td:
            audit_path = Path(td) / "final.csv"
            audit_path.write_text("product,status\nWidget,missing_from_qbo\n", encoding="utf-8")
            args = self._args(td, dry_run=False)
            spec = {
                "source_final_audit": str(audit_path),
                "affected_base_names": ["Widget"],
                "item_inv_start_date": "2026-04-28",
            }
            with mock.patch.dict(os.environ, {}, clear=True), \
                 mock.patch.object(inventory_pipeline, "classify_missing_items_for_audit_file", return_value={
                     "rows": [
                         {
                             "suggested_qbo_name": "Widget",
                             "is_safe": True,
                             "product": "Widget",
                             "category": "CAT",
                             "epos_expected_qty": "5",
                         }
                     ]
                 }), \
                 mock.patch.object(inventory_pipeline, "load_category_account_mapping", return_value={}), \
                 mock.patch.object(inventory_pipeline, "create_inventory_item") as create_mock:
                with self.assertRaises(InventoryApplyDisabledError):
                    inventory_pipeline._run_review_create_missing_items_phase(
                        args,
                        cfg,
                        spec,
                        started_at="2026-04-28T00:00:00",
                    )
        create_mock.assert_not_called()

    def test_review_create_missing_items_dry_run_allowed_in_production(self):
        cfg = self._cfg()
        cfg.qbo_environment = "production"
        with tempfile.TemporaryDirectory() as td:
            audit_path = Path(td) / "final.csv"
            audit_path.write_text("product,status\nWidget,missing_from_qbo\n", encoding="utf-8")
            args = self._args(td, dry_run=True)
            spec = {
                "source_final_audit": str(audit_path),
                "affected_base_names": ["Widget"],
                "item_inv_start_date": "2026-04-28",
            }
            with mock.patch.dict(os.environ, {}, clear=True), \
                 mock.patch.object(inventory_pipeline, "classify_missing_items_for_audit_file", return_value={
                     "rows": [
                         {
                             "suggested_qbo_name": "Widget",
                             "is_safe": True,
                             "product": "Widget",
                             "category": "CAT",
                             "epos_expected_qty": "5",
                         }
                     ]
                 }), \
                 mock.patch.object(inventory_pipeline, "load_category_account_mapping", return_value={}), \
                 mock.patch.object(inventory_pipeline, "_resolve_qbo_snapshot", return_value=Path(td) / "qbo.csv"), \
                 mock.patch.object(inventory_pipeline, "load_qbo_inventory_item_rows", return_value=pd.DataFrame()), \
                 mock.patch.object(inventory_pipeline, "create_inventory_item") as create_mock, \
                 mock.patch.object(inventory_pipeline, "_write_summary_reports", return_value=(Path(td) / "s.json", Path(td) / "s.txt")):
                result = inventory_pipeline._run_review_create_missing_items_phase(
                    args,
                    cfg,
                    spec,
                    started_at="2026-04-28T00:00:00",
                )

        self.assertTrue(result["dry_run"])
        self.assertEqual(result["products_checked"], 1)
        self.assertEqual(result["base_items_created"], 0)
        create_mock.assert_not_called()

    def test_catalog_apply_mode_skips_quantity_sync(self):
        with tempfile.TemporaryDirectory() as td:
            qbo_path = Path(td) / "qbo.csv"
            sequence: list[str] = []
            self._patch_common(td, plan=self._supported_plan(), qbo_paths=[qbo_path, qbo_path])
            with mock.patch.object(
                inventory_pipeline,
                "_run_apply_for_existing_base_pack_variants",
                side_effect=lambda **kwargs: sequence.append("catalog") or 0,
            ), mock.patch.object(
                inventory_pipeline,
                "_apply_exact_match_quantity_adjustments",
                side_effect=lambda **kwargs: sequence.append("quantity") or {
                    "posted": 0,
                    "planned": 0,
                    "skipped": 0,
                    "skipped_due_to_cap": 0,
                    "skipped_non_exact": 0,
                    "changed_qbo": False,
                },
            ):
                inventory_pipeline.run_inventory_pipeline(
                    self._args(td, mode="catalog_apply_admin_only", max_catalog_fixes=1)
                )

            self.assertEqual(sequence, ["catalog"])

    def test_qbo_snapshot_refreshes_after_catalog_cleanup_writes(self):
        with tempfile.TemporaryDirectory() as td:
            qbo_initial = Path(td) / "qbo-initial.csv"
            qbo_after_catalog = Path(td) / "qbo-after-catalog.csv"
            patches = self._patch_common(
                td,
                plan=self._supported_plan(),
                qbo_paths=[qbo_initial, qbo_after_catalog],
            )
            resolve_mock = patches[2]
            with mock.patch.object(
                inventory_pipeline,
                "_run_apply_for_existing_base_pack_variants",
                return_value=0,
            ), mock.patch.object(
                inventory_pipeline,
                "_apply_exact_match_quantity_adjustments",
                return_value={
                    "posted": 0,
                    "planned": 0,
                    "skipped": 0,
                    "skipped_due_to_cap": 0,
                    "skipped_non_exact": 0,
                    "changed_qbo": False,
                },
            ):
                summary = inventory_pipeline.run_inventory_pipeline(
                    self._args(td, mode="catalog_apply_admin_only", max_catalog_fixes=1)
                )

            self.assertEqual(resolve_mock.call_count, 2)
            self.assertEqual(summary["qbo_csv"], str(qbo_after_catalog))
            self.assertEqual(summary["catalog_fixes_applied"], 1)

    def test_omitted_caps_apply_all_supported_catalog_rows(self):
        with tempfile.TemporaryDirectory() as td:
            qbo_initial = Path(td) / "qbo-initial.csv"
            qbo_after_catalog = Path(td) / "qbo-after-catalog.csv"
            self._patch_common(
                td,
                plan=self._supported_plan(count=3),
                qbo_paths=[qbo_initial, qbo_after_catalog],
            )
            with mock.patch.object(
                inventory_pipeline,
                "_run_apply_for_existing_base_pack_variants",
                return_value={
                    "exit_code": 0,
                    "attempted": 3,
                    "consolidated": 3,
                    "cleaned_up": 3,
                    "skipped": 0,
                    "failed": 0,
                },
            ) as cleanup_mock, mock.patch.object(
                inventory_pipeline,
                "_apply_exact_match_quantity_adjustments",
                return_value={
                    "posted": 0,
                    "planned": 0,
                    "skipped": 0,
                    "skipped_due_to_cap": 0,
                    "skipped_non_exact": 0,
                    "changed_qbo": False,
                },
            ):
                summary = inventory_pipeline.run_inventory_pipeline(
                    self._args(td, mode="catalog_apply_admin_only", max_catalog_fixes=3)
                )

            self.assertEqual(cleanup_mock.call_args.kwargs["max_products"], 3)
            self.assertEqual(summary["max_catalog_fixes"], 3)
            self.assertEqual(summary["catalog_fixes_applied"], 3)
            self.assertEqual(summary["skipped_unsupported"], 0)

    def test_explicit_catalog_cap_limits_supported_rows(self):
        with tempfile.TemporaryDirectory() as td:
            qbo_initial = Path(td) / "qbo-initial.csv"
            qbo_after_catalog = Path(td) / "qbo-after-catalog.csv"
            self._patch_common(
                td,
                plan=self._supported_plan(count=3),
                qbo_paths=[qbo_initial, qbo_after_catalog],
            )
            with mock.patch.object(
                inventory_pipeline,
                "_run_apply_for_existing_base_pack_variants",
                return_value={
                    "exit_code": 0,
                    "attempted": 1,
                    "consolidated": 1,
                    "cleaned_up": 1,
                    "skipped": 0,
                    "failed": 0,
                },
            ) as cleanup_mock, mock.patch.object(
                inventory_pipeline,
                "_apply_exact_match_quantity_adjustments",
                return_value={
                    "posted": 0,
                    "planned": 0,
                    "skipped": 0,
                    "skipped_due_to_cap": 0,
                    "skipped_non_exact": 0,
                    "changed_qbo": False,
                },
            ):
                summary = inventory_pipeline.run_inventory_pipeline(
                    self._args(td, mode="catalog_apply_admin_only", max_catalog_fixes=1)
                )

            self.assertEqual(cleanup_mock.call_args.kwargs["max_products"], 1)
            self.assertEqual(summary["max_catalog_fixes"], 1)
            self.assertEqual(summary["catalog_fixes_applied"], 1)
            self.assertEqual(summary["skipped_safely"], 2)

    def test_omitted_caps_apply_all_exact_name_quantity_updates(self):
        with tempfile.TemporaryDirectory() as td:
            qbo_initial = Path(td) / "qbo-initial.csv"
            qbo_after_quantity = Path(td) / "qbo-after-quantity.csv"
            qbo_rows = self._qbo_rows(count=3)
            audit_report = self._quantity_report(count=3)
            self._patch_common(
                td,
                plan=pd.DataFrame(),
                qbo_paths=[qbo_initial, qbo_after_quantity],
                qbo_rows=qbo_rows,
                audit_report=audit_report,
            )
            lock = mock.Mock()
            lock.acquire.return_value = SimpleNamespace(acquired=True, reason="")
            with mock.patch.object(inventory_pipeline, "verify_realm_match"), \
                 mock.patch.object(inventory_pipeline, "TokenManager"), \
                 mock.patch.object(inventory_pipeline, "GlobalRunLock", return_value=lock), \
                 mock.patch.object(inventory_pipeline, "post_inventory_adjustment", return_value={}) as post_mock, \
                 mock.patch.object(inventory_pipeline, "mark_qbo_snapshot_stale") as stale_mock:
                summary = inventory_pipeline.run_inventory_pipeline(
                    self._args(td, mode="quantity_apply")
                )

            self.assertIsNone(summary["max_quantity_adjustments"])
            self.assertEqual(post_mock.call_count, 3)
            stale_mock.assert_called_once_with("company_a", reason="inventory_pipeline_quantity_adjustments_posted")
            self.assertEqual(summary["quantity_updates_applied"], 3)
            self.assertEqual(summary["quantity_adjustment_stats"]["skipped_due_to_cap"], 0)

    def test_explicit_quantity_cap_limits_exact_name_updates(self):
        with tempfile.TemporaryDirectory() as td:
            qbo_initial = Path(td) / "qbo-initial.csv"
            qbo_after_quantity = Path(td) / "qbo-after-quantity.csv"
            qbo_rows = self._qbo_rows(count=3)
            audit_report = self._quantity_report(count=3)
            self._patch_common(
                td,
                plan=pd.DataFrame(),
                qbo_paths=[qbo_initial, qbo_after_quantity],
                qbo_rows=qbo_rows,
                audit_report=audit_report,
            )
            lock = mock.Mock()
            lock.acquire.return_value = SimpleNamespace(acquired=True, reason="")
            with mock.patch.object(inventory_pipeline, "verify_realm_match"), \
                 mock.patch.object(inventory_pipeline, "TokenManager"), \
                 mock.patch.object(inventory_pipeline, "GlobalRunLock", return_value=lock), \
                 mock.patch.object(inventory_pipeline, "post_inventory_adjustment", return_value={}) as post_mock, \
                 mock.patch.object(inventory_pipeline, "mark_qbo_snapshot_stale"):
                summary = inventory_pipeline.run_inventory_pipeline(
                    self._args(td, mode="quantity_apply", max_quantity_adjustments=1)
                )

            self.assertEqual(summary["max_quantity_adjustments"], 1)
            self.assertEqual(post_mock.call_count, 1)
            self.assertEqual(summary["quantity_updates_applied"], 1)
            self.assertEqual(summary["quantity_adjustment_stats"]["skipped_due_to_cap"], 2)

    def test_summary_includes_run_job_id_from_dashboard_env(self):
        with tempfile.TemporaryDirectory() as td:
            qbo_path = Path(td) / "qbo.csv"
            self._patch_common(td, plan=self._supported_plan(), qbo_paths=[qbo_path, qbo_path])
            with mock.patch.object(
                inventory_pipeline,
                "_run_apply_for_existing_base_pack_variants",
                return_value=0,
            ), mock.patch.object(
                inventory_pipeline,
                "_apply_exact_match_quantity_adjustments",
                return_value={
                    "posted": 0,
                    "planned": 0,
                    "skipped": 0,
                    "skipped_due_to_cap": 0,
                    "skipped_non_exact": 0,
                    "changed_qbo": False,
                },
            ), mock.patch.dict(os.environ, {"OIAT_RUN_JOB_ID": "job-123"}):
                summary = inventory_pipeline.run_inventory_pipeline(self._args(td))

            self.assertEqual(summary["run_job_id"], "job-123")
            payload = json.loads(Path(summary["summary_json"]).read_text(encoding="utf-8"))
            self.assertEqual(payload["run_job_id"], "job-123")

    def test_run_link_uses_portal_base_url_when_available(self):
        with mock.patch.dict(
            os.environ,
            {
                "OIAT_RUN_JOB_ID": "job-123",
                "OIAT_PORTAL_BASE_URL": "https://portal.oiatsolutions.com",
            },
            clear=False,
        ):
            self.assertEqual(
                inventory_pipeline._build_run_detail_url(),
                "https://portal.oiatsolutions.com/epos-qbo/runs/job-123/",
            )

        with mock.patch.dict(os.environ, {"OIAT_RUN_JOB_ID": "job-123"}, clear=True):
            self.assertEqual(inventory_pipeline._build_run_detail_url(), "")

    def test_slack_summary_prefers_run_link_with_operator_label(self):
        summary = {
            "run_type": "inventory_pipeline",
            "company_key": "company_a",
            "display_name": "Co A",
            "scope": "product=TROPHY",
            "started_at": "2026-04-29T14:52:00+00:00",
            "finished_at": "2026-04-29T14:53:00+00:00",
            "run_job_id": "e8333646-3066-4953-9627-b0b4b1526f86",
            "run_url": "https://portal.oiatsolutions.com/epos-qbo/runs/e8333646-3066-4953-9627-b0b4b1526f86/",
            "summary_json": "/tmp/inventory_pipeline_company_a_145421.json",
            "products_checked": 0,
            "in_sync": 0,
            "catalog_fixes_applied": 0,
            "base_items_created": 0,
            "duplicate_base_items_resolved": 0,
            "quantity_updates_applied": 0,
            "blocked_items": 0,
            "still_needs_review": 0,
            "final_status_counts": {"in_sync": 0, "needs_adjustment": 0, "ambiguous_in_qbo": 0, "missing_in_qbo": 0},
            "final_catalog_issue_counts": {},
            "unsupported_catalog_issues": {},
            "blocked_catalog_examples": [],
            "product_details": [{"base_name": "TROPHY", "epos_expected_qty": 0, "qbo_final_qty": 0, "delta": 0, "final_status": "in_sync"}],
        }
        msg = inventory_pipeline._format_slack_summary(summary)
        self.assertIn(
            "Run: <https://portal.oiatsolutions.com/epos-qbo/runs/e8333646-3066-4953-9627-b0b4b1526f86/|Inventory Run INV-0429-1452-E833>",
            msg,
        )
        self.assertNotIn("Report: inventory_pipeline_company_a_145421.json", msg)

    def test_no_quantity_adjustment_path_still_sets_final_audit_child_report(self):
        with tempfile.TemporaryDirectory() as td:
            qbo_path = Path(td) / "qbo.csv"
            clean_report = pd.DataFrame(
                [
                    {
                        "base_name": "Widget",
                        "epos_single_units": 5.0,
                        "qbo_qty_on_hand": 5.0,
                        "delta": 0.0,
                        "status": "in_sync",
                        "catalog_issue_type": "exact_name_match",
                    }
                ]
            )
            self._patch_common(td, plan=pd.DataFrame(), qbo_paths=[qbo_path], audit_report=clean_report)
            with mock.patch.object(
                inventory_pipeline,
                "_apply_exact_match_quantity_adjustments",
                return_value={
                    "posted": 0,
                    "planned": 0,
                    "skipped": 0,
                    "skipped_due_to_cap": 0,
                    "skipped_non_exact": 0,
                    "changed_qbo": False,
                    "details": [],
                },
            ):
                summary = inventory_pipeline.run_inventory_pipeline(self._args(td))

            self.assertIn("final_audit", summary["child_reports"])
            self.assertTrue(Path(summary["child_reports"]["final_audit"]).name.startswith("inventory_audit_company_a_final_"))
            payload = json.loads(Path(summary["summary_json"]).read_text(encoding="utf-8"))
            self.assertEqual(payload["child_reports"]["final_audit"], summary["child_reports"]["final_audit"])
            self.assertEqual(summary["completion_status"], "clean")

    def test_already_clean_run_has_stable_zero_fields(self):
        with tempfile.TemporaryDirectory() as td:
            qbo_path = Path(td) / "qbo.csv"
            clean_report = pd.DataFrame(
                [
                    {
                        "base_name": "Widget",
                        "epos_single_units": 5.0,
                        "qbo_qty_on_hand": 5.0,
                        "delta": 0.0,
                        "status": "in_sync",
                        "catalog_issue_type": "exact_name_match",
                    }
                ]
            )
            self._patch_common(td, plan=pd.DataFrame(), qbo_paths=[qbo_path], audit_report=clean_report)
            summary = inventory_pipeline.run_inventory_pipeline(self._args(td))

            self.assertEqual(summary["blocked_items"], 0)
            self.assertEqual(summary["in_sync"], 1)
            self.assertEqual(summary["catalog_fixes_applied"], 0)
            self.assertEqual(summary["quantity_updates_applied"], 0)
            self.assertIn("final_audit", summary["child_reports"])

    def test_unsupported_catalog_issue_types_are_reported_not_applied(self):
        with tempfile.TemporaryDirectory() as td:
            qbo_path = Path(td) / "qbo.csv"
            self._patch_common(td, plan=self._unsupported_plan(), qbo_paths=[qbo_path])
            with mock.patch.object(
                inventory_pipeline,
                "_run_apply_for_existing_base_pack_variants",
                return_value={
                    "exit_code": 0,
                    "attempted": 1,
                    "consolidated": 0,
                    "cleaned_up": 0,
                    "skipped": 1,
                    "failed": 0,
                    "base_items_created": 0,
                    "created_base_details": [],
                },
            ) as cleanup_mock, mock.patch.object(
                inventory_pipeline,
                "_apply_exact_match_quantity_adjustments",
                return_value={
                    "posted": 0,
                    "planned": 0,
                    "skipped": 0,
                    "skipped_due_to_cap": 0,
                    "skipped_non_exact": 0,
                    "changed_qbo": False,
                },
            ):
                summary = inventory_pipeline.run_inventory_pipeline(
                    self._args(td, mode="catalog_plan_only")
                )

            cleanup_mock.assert_not_called()
            self.assertEqual(summary["catalog_fixes_applied"], 0)
            self.assertEqual(summary["unsupported_catalog_issues"].get("only_pack_variant_exists", 0), 0)
            self.assertEqual(summary["unsupported_catalog_issues"]["missing_from_qbo"], 1)
            self.assertEqual(summary["unsupported_catalog_issues"]["multiple_active_base_items"], 1)

    def test_unsupported_catalog_action_is_reported_not_applied(self):
        with tempfile.TemporaryDirectory() as td:
            qbo_path = Path(td) / "qbo.csv"
            self._patch_common(td, plan=self._unsupported_action_plan(), qbo_paths=[qbo_path])
            with mock.patch.object(
                inventory_pipeline,
                "_run_apply_for_existing_base_pack_variants",
            ) as cleanup_mock, mock.patch.object(
                inventory_pipeline,
                "_apply_exact_match_quantity_adjustments",
                return_value={
                    "posted": 0,
                    "planned": 0,
                    "skipped": 0,
                    "skipped_due_to_cap": 0,
                    "skipped_non_exact": 0,
                    "changed_qbo": False,
                },
            ):
                summary = inventory_pipeline.run_inventory_pipeline(
                    self._args(td, mode="catalog_plan_only")
                )

            cleanup_mock.assert_not_called()
            self.assertEqual(summary["catalog_fixes_applied"], 0)
            self.assertEqual(summary["skipped_unsupported"], 1)
            self.assertEqual(summary["unsupported_catalog_issues"]["base_with_pack_variants"], 1)

    def test_duplicate_base_resolution_reduces_blocked_count_when_applied(self):
        with tempfile.TemporaryDirectory() as td:
            qbo_path = Path(td) / "qbo.csv"
            self._patch_common(td, plan=self._duplicate_resolvable_plan(), qbo_paths=[qbo_path, qbo_path])
            with mock.patch.object(
                inventory_pipeline,
                "_run_apply_for_existing_base_pack_variants",
                return_value={
                    "exit_code": 0,
                    "attempted": 1,
                    "consolidated": 1,
                    "cleaned_up": 1,
                    "skipped": 0,
                    "failed": 0,
                    "base_items_created": 0,
                    "duplicate_base_items_resolved": 1,
                    "created_base_details": [],
                },
            ), mock.patch.object(
                inventory_pipeline,
                "_apply_exact_match_quantity_adjustments",
                return_value={
                    "posted": 0,
                    "planned": 0,
                    "skipped": 0,
                    "skipped_due_to_cap": 0,
                    "skipped_non_exact": 0,
                    "changed_qbo": False,
                },
            ):
                summary = inventory_pipeline.run_inventory_pipeline(
                    self._args(td, mode="catalog_apply_admin_only", max_catalog_fixes=1)
                )

            self.assertEqual(summary["catalog_fixes_applied"], 1)
            self.assertEqual(summary["duplicate_base_items_resolved"], 1)
            self.assertEqual(summary["skipped_unsupported"], 0)


class InventoryReviewActionEnvelopeLoaderTests(unittest.TestCase):
    def test_load_review_action_envelope_from_env_json(self):
        payload = {"kind": "review_retry", "intent": "review_retry_quantity_adjustments"}
        raw = json.dumps(payload)
        with mock.patch.dict(os.environ, {"OIAT_INVENTORY_REVIEW_ACTION_JSON": raw}, clear=False):
            self.assertEqual(inventory_pipeline._load_review_action_envelope(), payload)

    def test_load_review_action_envelope_empty_returns_none(self):
        with mock.patch.dict(os.environ, {"OIAT_INVENTORY_REVIEW_ACTION_JSON": ""}, clear=False):
            self.assertIsNone(inventory_pipeline._load_review_action_envelope())


if __name__ == "__main__":
    unittest.main()
