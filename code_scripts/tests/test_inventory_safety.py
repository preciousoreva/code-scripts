from __future__ import annotations

import os
import unittest
from types import SimpleNamespace
from unittest import mock

import pandas as pd

from code_scripts import inventory_catalog_cleanup, inventory_pipeline
from code_scripts.inventory_safety import (
    ALLOW_INVENTORY_APPLY_ENV,
    INVENTORY_APPLY_DISABLED_MESSAGE,
    InventoryApplyDisabledError,
    assert_inventory_apply_allowed,
)


class InventorySafetyGuardTests(unittest.TestCase):
    def _cfg(self, *, environment: str = "production") -> SimpleNamespace:
        return SimpleNamespace(
            company_key="company_a",
            display_name="Company A",
            realm_id="123",
            qbo_environment=environment,
            inventory_adjustment_account_id="88",
            inventory_max_qty_delta=None,
            default_qty_on_hand=0,
        )

    def test_production_blocks_catalog_cleanup_apply(self):
        plan = pd.DataFrame(
            [
                {
                    "base_name": "Widget",
                    "planned_action": "consolidate_existing_base_pack_variants",
                    "action_eligible": True,
                }
            ]
        )
        env = {"OIAT_RUNTIME_ENV": "production"}
        with mock.patch.dict(os.environ, env, clear=True):
            with self.assertRaisesRegex(InventoryApplyDisabledError, INVENTORY_APPLY_DISABLED_MESSAGE):
                inventory_catalog_cleanup._run_apply_for_existing_base_pack_variants(
                    cfg=self._cfg(),
                    plan_df=plan,
                    qbo_item_rows=pd.DataFrame(),
                    txn_date="2026-05-06",
                    max_products=1,
                    dry_run=False,
                )

    def test_production_blocks_quantity_adjustment_apply(self):
        audit = pd.DataFrame(
            [
                {
                    "base_name": "Widget",
                    "epos_single_units": 5,
                    "status": "needs_adjustment",
                    "catalog_issue_type": "exact_name_match",
                }
            ]
        )
        qbo_rows = pd.DataFrame(
            [
                {
                    "Id": "10",
                    "Name": "Widget",
                    "base_name": "Widget",
                    "base_name_norm": "widget",
                    "qbo_qty_on_hand": 1,
                }
            ]
        )
        env = {"OIAT_RUNTIME_ENV": "production"}
        with mock.patch.dict(os.environ, env, clear=True):
            with self.assertRaisesRegex(InventoryApplyDisabledError, INVENTORY_APPLY_DISABLED_MESSAGE):
                inventory_pipeline._apply_exact_match_quantity_adjustments(
                    cfg=self._cfg(),
                    audit_df=audit,
                    qbo_item_rows=qbo_rows,
                    max_quantity_adjustments=None,
                    max_qty_delta=None,
                    adjust_account_id=None,
                    txn_date="2026-05-06",
                    dry_run=False,
                )

    def test_preview_path_is_allowed(self):
        cfg = self._cfg()
        audit = pd.DataFrame(
            [
                {
                    "base_name": "Widget",
                    "epos_single_units": 5,
                    "status": "needs_adjustment",
                    "catalog_issue_type": "exact_name_match",
                }
            ]
        )
        qbo_rows = pd.DataFrame(
            [
                {
                    "Id": "10",
                    "Name": "Widget",
                    "base_name": "Widget",
                    "base_name_norm": "widget",
                    "qbo_qty_on_hand": 1,
                }
            ]
        )
        with mock.patch.dict(os.environ, {"OIAT_RUNTIME_ENV": "production"}, clear=True), \
             mock.patch.object(inventory_pipeline, "post_inventory_adjustment") as post_mock:
            result = inventory_pipeline._apply_exact_match_quantity_adjustments(
                cfg=cfg,
                audit_df=audit,
                qbo_item_rows=qbo_rows,
                max_quantity_adjustments=None,
                max_qty_delta=None,
                adjust_account_id=None,
                txn_date="2026-05-06",
                dry_run=True,
            )
        self.assertEqual(result["planned"], 1)
        post_mock.assert_not_called()

    def test_non_inventory_sales_sync_is_unaffected(self):
        from code_scripts import qbo_upload

        self.assertFalse(hasattr(qbo_upload, "assert_inventory_apply_allowed"))

    def test_explicit_override_allows_apply(self):
        cfg = self._cfg()
        with mock.patch.dict(
            os.environ,
            {"OIAT_RUNTIME_ENV": "production", ALLOW_INVENTORY_APPLY_ENV: "true"},
            clear=True,
        ):
            assert_inventory_apply_allowed(cfg)
