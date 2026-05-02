"""Execution-level tests for review-triggered missing Inventory item creation."""

from __future__ import annotations

import argparse
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase, mock

import pandas as pd

from code_scripts.inventory_pipeline import _run_review_create_missing_items_phase
from code_scripts.inventory_sync import _normalize_name_key


def _classify_rows(*, with_safe: bool = True, include_blocked: bool = True):
    rows = []
    if with_safe:
        rows.append(
            {
                "product": "SAFE1",
                "base_name": "SAFE1",
                "suggested_qbo_name": "SAFE1",
                "category": "Beer",
                "epos_expected_qty": "6",
                "inventory_account": "Inv",
                "revenue_account": "Rev",
                "cogs_account": "COGS",
                "safety_status": "Safe candidate",
                "block_reason": "",
                "is_safe": True,
            }
        )
    if include_blocked:
        rows.append(
            {
                "product": "PACK*12",
                "base_name": "PACK",
                "suggested_qbo_name": "PACK",
                "category": "Beer",
                "epos_expected_qty": "1",
                "inventory_account": "",
                "revenue_account": "",
                "cogs_account": "",
                "safety_status": "Pack variant of existing base",
                "block_reason": "blocked",
                "is_safe": False,
            }
        )
    safe_count = sum(1 for r in rows if r["is_safe"])
    return {
        "rows": rows,
        "safe_count": safe_count,
        "blocked_count": len(rows) - safe_count,
        "mapping_loaded": True,
        "mapping_error": "",
        "qbo_base_names_loaded": True,
        "qbo_base_names_error": "",
        "parse_error": "",
    }


class ReviewMissingCreatePhaseTests(TestCase):
    def _cfg(self):
        cfg = mock.Mock()
        cfg.company_key = "company_a"
        cfg.display_name = "Co A"
        cfg.realm_id = "realm-1"
        cfg.slack_webhook_url = None
        return cfg

    def _args(self, *, txn_date: str | None, summary_dir: Path, dry_run: bool = False):
        return argparse.Namespace(
            company="company_a",
            stock_csv=None,
            auto_download=False,
            download_headful=False,
            download_timeout_ms=None,
            download_output_dir=None,
            qbo_csv=str(summary_dir / "qbo.csv"),
            auto_fetch_qbo=False,
            qbo_force_refresh=False,
            qbo_cache_max_age_hours=24,
            qbo_export_path=None,
            categories=[],
            product_filter=None,
            base_names=[],
            max_catalog_fixes=None,
            max_quantity_adjustments=None,
            max_qty_delta=None,
            adjust_account_id=None,
            txn_date=txn_date,
            dry_run=dry_run,
            review_create_missing_items=True,
            no_slack=True,
            summary_output_dir=str(summary_dir),
        )

    def test_create_only_safe_with_expected_qty_and_txn_date(self):
        with TemporaryDirectory() as td:
            td_path = Path(td)
            audit = td_path / "final.csv"
            audit.write_text("base_name,status\n", encoding="utf-8")
            spec = {
                "source_final_audit": str(audit),
                "affected_base_names": ["SAFE1"],
                "item_inv_start_date": "2026-04-30",
            }
            data = _classify_rows(with_safe=True, include_blocked=True)
            empty_df = pd.DataFrame(columns=["base_name_norm"])

            with mock.patch(
                "code_scripts.inventory_pipeline.load_category_account_mapping",
                return_value={"Beer": {"asset": "A", "income": "I", "expense": "E"}},
            ), mock.patch(
                "code_scripts.inventory_pipeline.classify_missing_items_for_audit_file",
                return_value=data,
            ), mock.patch(
                "code_scripts.inventory_pipeline.verify_realm_match",
            ), mock.patch(
                "code_scripts.inventory_pipeline.TokenManager",
            ), mock.patch(
                "code_scripts.inventory_pipeline.GlobalRunLock"
            ) as lock_cls, mock.patch(
                "code_scripts.inventory_pipeline._resolve_qbo_snapshot",
                return_value=td_path / "qbo.csv",
            ), mock.patch(
                "code_scripts.inventory_pipeline.load_qbo_inventory_item_rows",
                return_value=empty_df,
            ), mock.patch(
                "code_scripts.inventory_pipeline.get_or_create_item_category_id",
                return_value="cat-9",
            ), mock.patch(
                "code_scripts.inventory_pipeline.create_inventory_item",
                return_value="new-id",
            ) as create_mock, mock.patch(
                "code_scripts.qbo_upload.build_account_refs_for_category",
                return_value={
                    "IncomeAccountRef": {"value": "1"},
                    "AssetAccountRef": {"value": "2"},
                    "ExpenseAccountRef": {"value": "3"},
                },
            ), mock.patch(
                "code_scripts.inventory_pipeline.post_inventory_adjustment"
            ) as adj_mock, mock.patch(
                "code_scripts.inventory_pipeline.mark_qbo_snapshot_stale",
            ), mock.patch(
                "code_scripts.inventory_pipeline.send_slack_success",
            ):
                lock_inst = lock_cls.return_value
                lock_inst.acquire.return_value = mock.Mock(acquired=True, reason="")
                summary = _run_review_create_missing_items_phase(
                    self._args(txn_date="2026-04-30", summary_dir=td_path, dry_run=False),
                    self._cfg(),
                    spec,
                    started_at="2026-05-01T00:00:00+00:00",
                )

            create_mock.assert_called_once()
            self.assertEqual(create_mock.call_args.kwargs.get("target_date"), "2026-04-30")
            self.assertEqual(create_mock.call_args.kwargs.get("qty_on_hand"), 6.0)
            adj_mock.assert_not_called()
            self.assertEqual(summary.get("base_items_created"), 1)

    def test_txn_date_falls_back_to_spec_when_args_empty(self):
        with TemporaryDirectory() as td:
            td_path = Path(td)
            audit = td_path / "final.csv"
            audit.write_text("x\n", encoding="utf-8")
            spec = {
                "source_final_audit": str(audit),
                "affected_base_names": ["SAFE1"],
                "item_inv_start_date": "2026-05-15",
            }
            data = _classify_rows(with_safe=True, include_blocked=False)
            empty_df = pd.DataFrame(columns=["base_name_norm"])

            with mock.patch(
                "code_scripts.inventory_pipeline.load_category_account_mapping",
                return_value={"Beer": {"asset": "A", "income": "I", "expense": "E"}},
            ), mock.patch(
                "code_scripts.inventory_pipeline.classify_missing_items_for_audit_file",
                return_value=data,
            ), mock.patch(
                "code_scripts.inventory_pipeline.verify_realm_match",
            ), mock.patch(
                "code_scripts.inventory_pipeline.TokenManager",
            ), mock.patch(
                "code_scripts.inventory_pipeline.GlobalRunLock"
            ) as lock_cls, mock.patch(
                "code_scripts.inventory_pipeline._resolve_qbo_snapshot",
                return_value=td_path / "qbo.csv",
            ), mock.patch(
                "code_scripts.inventory_pipeline.load_qbo_inventory_item_rows",
                return_value=empty_df,
            ), mock.patch(
                "code_scripts.inventory_pipeline.get_or_create_item_category_id",
                return_value="c1",
            ), mock.patch(
                "code_scripts.inventory_pipeline.create_inventory_item",
                return_value="id2",
            ) as create_mock, mock.patch(
                "code_scripts.qbo_upload.build_account_refs_for_category",
                return_value={
                    "IncomeAccountRef": {"value": "1"},
                    "AssetAccountRef": {"value": "2"},
                    "ExpenseAccountRef": {"value": "3"},
                },
            ), mock.patch(
                "code_scripts.inventory_pipeline.post_inventory_adjustment"
            ) as adj_mock, mock.patch(
                "code_scripts.inventory_pipeline.mark_qbo_snapshot_stale",
            ), mock.patch(
                "code_scripts.inventory_pipeline.send_slack_success",
            ):
                lock_inst = lock_cls.return_value
                lock_inst.acquire.return_value = mock.Mock(acquired=True, reason="")
                _run_review_create_missing_items_phase(
                    self._args(txn_date=None, summary_dir=td_path, dry_run=False),
                    self._cfg(),
                    spec,
                    started_at="2026-05-01T00:00:00+00:00",
                )

            self.assertEqual(create_mock.call_args.kwargs.get("target_date"), "2026-05-15")
            adj_mock.assert_not_called()

    def test_skips_when_item_exists_in_snapshot(self):
        with TemporaryDirectory() as td:
            td_path = Path(td)
            audit = td_path / "final.csv"
            audit.write_text("x\n", encoding="utf-8")
            spec = {
                "source_final_audit": str(audit),
                "affected_base_names": ["SAFE1"],
                "item_inv_start_date": "2026-04-30",
            }
            data = _classify_rows(with_safe=True, include_blocked=False)
            norm = _normalize_name_key("SAFE1")
            df = pd.DataFrame({"base_name_norm": [norm]})

            with mock.patch(
                "code_scripts.inventory_pipeline.load_category_account_mapping",
                return_value={"Beer": {"asset": "A", "income": "I", "expense": "E"}},
            ), mock.patch(
                "code_scripts.inventory_pipeline.classify_missing_items_for_audit_file",
                return_value=data,
            ), mock.patch(
                "code_scripts.inventory_pipeline.verify_realm_match",
            ), mock.patch(
                "code_scripts.inventory_pipeline.TokenManager",
            ), mock.patch(
                "code_scripts.inventory_pipeline.GlobalRunLock"
            ) as lock_cls, mock.patch(
                "code_scripts.inventory_pipeline._resolve_qbo_snapshot",
                return_value=td_path / "qbo.csv",
            ), mock.patch(
                "code_scripts.inventory_pipeline.load_qbo_inventory_item_rows",
                return_value=df,
            ), mock.patch(
                "code_scripts.inventory_pipeline.create_inventory_item",
            ) as create_mock, mock.patch(
                "code_scripts.inventory_pipeline.post_inventory_adjustment"
            ) as adj_mock, mock.patch(
                "code_scripts.inventory_pipeline.mark_qbo_snapshot_stale",
            ), mock.patch(
                "code_scripts.inventory_pipeline.send_slack_success",
            ):
                lock_inst = lock_cls.return_value
                lock_inst.acquire.return_value = mock.Mock(acquired=True, reason="")
                summary = _run_review_create_missing_items_phase(
                    self._args(txn_date="2026-04-30", summary_dir=td_path, dry_run=False),
                    self._cfg(),
                    spec,
                    started_at="2026-05-01T00:00:00+00:00",
                )

            create_mock.assert_not_called()
            adj_mock.assert_not_called()
            self.assertEqual(summary.get("base_items_created"), 0)

    def test_blocked_candidate_not_created_even_if_in_payload(self):
        with TemporaryDirectory() as td:
            td_path = Path(td)
            audit = td_path / "final.csv"
            audit.write_text("x\n", encoding="utf-8")
            spec = {
                "source_final_audit": str(audit),
                "affected_base_names": ["PACK", "SAFE1"],
                "item_inv_start_date": "2026-04-30",
            }
            data = _classify_rows(with_safe=True, include_blocked=True)
            empty_df = pd.DataFrame(columns=["base_name_norm"])

            with mock.patch(
                "code_scripts.inventory_pipeline.load_category_account_mapping",
                return_value={"Beer": {"asset": "A", "income": "I", "expense": "E"}},
            ), mock.patch(
                "code_scripts.inventory_pipeline.classify_missing_items_for_audit_file",
                return_value=data,
            ), mock.patch(
                "code_scripts.inventory_pipeline.verify_realm_match",
            ), mock.patch(
                "code_scripts.inventory_pipeline.TokenManager",
            ), mock.patch(
                "code_scripts.inventory_pipeline.GlobalRunLock"
            ) as lock_cls, mock.patch(
                "code_scripts.inventory_pipeline._resolve_qbo_snapshot",
                return_value=td_path / "qbo.csv",
            ), mock.patch(
                "code_scripts.inventory_pipeline.load_qbo_inventory_item_rows",
                return_value=empty_df,
            ), mock.patch(
                "code_scripts.inventory_pipeline.get_or_create_item_category_id",
                return_value="c1",
            ), mock.patch(
                "code_scripts.inventory_pipeline.create_inventory_item",
                return_value="id",
            ) as create_mock, mock.patch(
                "code_scripts.qbo_upload.build_account_refs_for_category",
                return_value={
                    "IncomeAccountRef": {"value": "1"},
                    "AssetAccountRef": {"value": "2"},
                    "ExpenseAccountRef": {"value": "3"},
                },
            ), mock.patch(
                "code_scripts.inventory_pipeline.post_inventory_adjustment"
            ) as adj_mock, mock.patch(
                "code_scripts.inventory_pipeline.mark_qbo_snapshot_stale",
            ), mock.patch(
                "code_scripts.inventory_pipeline.send_slack_success",
            ):
                lock_inst = lock_cls.return_value
                lock_inst.acquire.return_value = mock.Mock(acquired=True, reason="")
                _run_review_create_missing_items_phase(
                    self._args(txn_date="2026-04-30", summary_dir=td_path, dry_run=False),
                    self._cfg(),
                    spec,
                    started_at="2026-05-01T00:00:00+00:00",
                )

            create_mock.assert_called_once()
            self.assertEqual(create_mock.call_args[0][0], "SAFE1")
            adj_mock.assert_not_called()

    def test_review_action_envelope_completion_slack_message(self):
        """Portal jobs pass OIAT_INVENTORY_REVIEW_ACTION_JSON; completion uses review formatter."""
        with TemporaryDirectory() as td:
            td_path = Path(td)
            audit = td_path / "final.csv"
            audit.write_text("base_name,status\n", encoding="utf-8")
            spec = {
                "source_final_audit": str(audit),
                "affected_base_names": ["SAFE1"],
                "item_inv_start_date": "2026-04-30",
            }
            data = _classify_rows(with_safe=True, include_blocked=True)
            empty_df = pd.DataFrame(columns=["base_name_norm"])
            review_env = {
                "kind": "review_create_missing",
                "intent": "review_create_missing_items",
                "source_final_audit_name": "final.csv",
                "safe_count": 1,
                "blocked_count": 1,
                "category_label": "All categories",
                "txn_date": "2026-04-30",
            }

            with mock.patch(
                "code_scripts.inventory_pipeline.load_category_account_mapping",
                return_value={"Beer": {"asset": "A", "income": "I", "expense": "E"}},
            ), mock.patch(
                "code_scripts.inventory_pipeline.classify_missing_items_for_audit_file",
                return_value=data,
            ), mock.patch(
                "code_scripts.inventory_pipeline.verify_realm_match",
            ), mock.patch(
                "code_scripts.inventory_pipeline.TokenManager",
            ), mock.patch(
                "code_scripts.inventory_pipeline.GlobalRunLock"
            ) as lock_cls, mock.patch(
                "code_scripts.inventory_pipeline._resolve_qbo_snapshot",
                return_value=td_path / "qbo.csv",
            ), mock.patch(
                "code_scripts.inventory_pipeline.load_qbo_inventory_item_rows",
                return_value=empty_df,
            ), mock.patch(
                "code_scripts.inventory_pipeline.get_or_create_item_category_id",
                return_value="cat-9",
            ), mock.patch(
                "code_scripts.inventory_pipeline.create_inventory_item",
                return_value="new-id",
            ), mock.patch(
                "code_scripts.qbo_upload.build_account_refs_for_category",
                return_value={
                    "IncomeAccountRef": {"value": "1"},
                    "AssetAccountRef": {"value": "2"},
                    "ExpenseAccountRef": {"value": "3"},
                },
            ), mock.patch(
                "code_scripts.inventory_pipeline.post_inventory_adjustment"
            ), mock.patch(
                "code_scripts.inventory_pipeline.mark_qbo_snapshot_stale",
            ), mock.patch(
                "code_scripts.inventory_pipeline.send_slack_success",
            ) as slack_mock:
                lock_inst = lock_cls.return_value
                lock_inst.acquire.return_value = mock.Mock(acquired=True, reason="")
                cfg = self._cfg()
                cfg.slack_webhook_url = "https://hooks.slack.com/services/FAKE"
                args = self._args(txn_date="2026-04-30", summary_dir=td_path, dry_run=False)
                args.no_slack = False
                _run_review_create_missing_items_phase(
                    args,
                    cfg,
                    spec,
                    started_at="2026-05-01T00:00:00+00:00",
                    review_action_env=review_env,
                )

            slack_mock.assert_called_once()
            msg = slack_mock.call_args.args[0]
            self.assertIn("Missing item creation", msg)
            self.assertIn("Created: 1", msg)
            self.assertIn("Inventory Review Action Completed", msg)
