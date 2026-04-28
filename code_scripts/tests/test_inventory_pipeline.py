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

from code_scripts import inventory_pipeline


class InventoryPipelineOrchestrationTests(unittest.TestCase):
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
            "adjust_account_id": None,
            "txn_date": "2026-04-28",
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

    def _qbo_rows(self, count: int = 1) -> pd.DataFrame:
        return pd.DataFrame(
            [
                {
                    "base_name": f"Widget {i}",
                    "Name": f"Widget {i}",
                    "Id": str(i),
                    "qbo_qty_on_hand": 2.0,
                    "qbo_has_pack": False,
                }
                for i in range(1, count + 1)
            ]
        )

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
            "catalog_fixes_applied": 1,
            "quantity_updates_applied": 1,
            "skipped_unsupported": 2,
            "still_needs_review": 2,
            "unsupported_catalog_issues": {
                "only_pack_variant_exists": 1,
                "missing_from_qbo": 1,
                "multiple_active_base_items": 0,
            },
            "max_catalog_fixes": None,
            "max_quantity_adjustments": None,
            "summary_json": "/tmp/report.json",
        }
        payload.update(overrides)
        return payload

    def test_final_summary_omits_limits_when_unlimited(self):
        msg = inventory_pipeline._format_final_summary(self._summary_payload())
        self.assertIn("Skipped unsupported: 2", msg)
        self.assertIn("Only pack variant exists: 1", msg)
        self.assertIn("Missing from QuickBooks: 1", msg)
        self.assertNotIn("Catalog fixes limit:", msg)
        self.assertNotIn("Quantity updates limit:", msg)

    def test_final_summary_includes_limits_when_explicit(self):
        msg = inventory_pipeline._format_final_summary(
            self._summary_payload(max_catalog_fixes=1, max_quantity_adjustments=2)
        )
        self.assertIn("Catalog fixes limit: 1", msg)
        self.assertIn("Quantity updates limit: 2", msg)

    def test_dry_run_passes_dry_flags_and_reports_no_writes(self):
        with tempfile.TemporaryDirectory() as td:
            qbo_path = Path(td) / "qbo.csv"
            sequence: list[str] = []
            self._patch_common(td, plan=self._supported_plan(), qbo_paths=[qbo_path])
            with mock.patch.object(
                inventory_pipeline,
                "_run_apply_for_existing_base_pack_variants",
                side_effect=lambda **kwargs: sequence.append(f"catalog:{kwargs['dry_run']}") or 0,
            ), mock.patch.object(
                inventory_pipeline,
                "_apply_exact_match_quantity_adjustments",
                side_effect=lambda **kwargs: sequence.append(f"quantity:{kwargs['dry_run']}") or {
                    "posted": 0,
                    "planned": 1,
                    "skipped": 0,
                    "skipped_due_to_cap": 0,
                    "skipped_non_exact": 0,
                    "changed_qbo": False,
                },
            ):
                summary = inventory_pipeline.run_inventory_pipeline(
                    self._args(td, dry_run=True)
                )

            self.assertEqual(sequence, ["catalog:True", "quantity:True"])
            self.assertEqual(summary["catalog_fixes_applied"], 0)
            self.assertEqual(summary["quantity_updates_applied"], 0)

    def test_catalog_cleanup_runs_before_quantity_sync(self):
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
                inventory_pipeline.run_inventory_pipeline(self._args(td))

            self.assertEqual(sequence, ["catalog", "quantity"])

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
                summary = inventory_pipeline.run_inventory_pipeline(self._args(td))

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
                summary = inventory_pipeline.run_inventory_pipeline(self._args(td))

            self.assertEqual(cleanup_mock.call_args.kwargs["max_products"], 3)
            self.assertIsNone(summary["max_catalog_fixes"])
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
                    self._args(td, max_catalog_fixes=1)
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
                 mock.patch.object(inventory_pipeline, "mark_qbo_snapshot_stale"):
                summary = inventory_pipeline.run_inventory_pipeline(self._args(td))

            self.assertIsNone(summary["max_quantity_adjustments"])
            self.assertEqual(post_mock.call_count, 3)
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
                    self._args(td, max_quantity_adjustments=1)
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

    def test_unsupported_catalog_issue_types_are_reported_not_applied(self):
        with tempfile.TemporaryDirectory() as td:
            qbo_path = Path(td) / "qbo.csv"
            self._patch_common(td, plan=self._unsupported_plan(), qbo_paths=[qbo_path])
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
                summary = inventory_pipeline.run_inventory_pipeline(self._args(td))

            cleanup_mock.assert_not_called()
            self.assertEqual(summary["catalog_fixes_applied"], 0)
            self.assertEqual(summary["unsupported_catalog_issues"]["only_pack_variant_exists"], 1)
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
                summary = inventory_pipeline.run_inventory_pipeline(self._args(td))

            cleanup_mock.assert_not_called()
            self.assertEqual(summary["catalog_fixes_applied"], 0)
            self.assertEqual(summary["skipped_unsupported"], 1)
            self.assertEqual(summary["unsupported_catalog_issues"]["base_with_pack_variants"], 1)


if __name__ == "__main__":
    unittest.main()
