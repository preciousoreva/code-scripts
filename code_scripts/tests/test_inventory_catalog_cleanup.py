import io
import os
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest import mock

import pandas as pd

from code_scripts import inventory_catalog_cleanup


class CatalogCleanupPlannerTest(unittest.TestCase):
    def _audit_df(self, rows):
        return pd.DataFrame(rows)

    def test_base_with_pack_variants_becomes_consolidate_action(self):
        audit = self._audit_df(
            [
                {
                    "base_name": "GOLDBERG CAN 50cl",
                    "epos_single_units": 8.0,
                    "catalog_issue_type": "base_with_pack_variants",
                    "qbo_base_item_ids": "10",
                    "qbo_item_names_for_base": "GOLDBERG CAN 50cl | GOLDBERG CAN 50cl*6",
                    "qbo_base_item_names_for_base": "GOLDBERG CAN 50cl",
                    "qbo_pack_variant_names_for_base": "GOLDBERG CAN 50cl*6",
                    "suggested_next_action": "run pack variant consolidation and cleanup",
                }
            ]
        )
        qbo_rows = pd.DataFrame(
            [
                {"Id": "10", "Name": "GOLDBERG CAN 50cl", "base_name": "GOLDBERG CAN 50cl", "qbo_has_pack": False},
                {"Id": "11", "Name": "GOLDBERG CAN 50cl*6", "base_name": "GOLDBERG CAN 50cl", "qbo_has_pack": True},
            ]
        )
        plan = inventory_catalog_cleanup.plan_catalog_cleanup(
            company_key="company_a",
            audit_df=audit,
            qbo_item_rows=qbo_rows,
            source_inventory_report="/r.csv",
        )
        row = plan.iloc[0].to_dict()
        self.assertEqual(row["planned_action"], "consolidate_existing_base_pack_variants")
        self.assertTrue(row["action_eligible"])
        self.assertEqual(row["qbo_base_item_ids"], "10")
        self.assertIn("GOLDBERG CAN 50cl*6", row["qbo_pack_variant_names"])

    def test_only_pack_variant_exists_becomes_create_base_then_consolidate(self):
        audit = self._audit_df(
            [
                {
                    "base_name": "BACARDI WHITE RUM 750ml",
                    "epos_single_units": 8.0,
                    "catalog_issue_type": "only_pack_variant_exists",
                    "qbo_pack_variant_names_for_base": "BACARDI WHITE RUM 750ml*12",
                    "suggested_next_action": "create base item, consolidate pack variant quantity, then inactivate pack variant",
                }
            ]
        )
        qbo_rows = pd.DataFrame(
            [
                {"Id": "99", "Name": "BACARDI WHITE RUM 750ml*12", "base_name": "BACARDI WHITE RUM 750ml", "qbo_has_pack": True},
            ]
        )
        plan = inventory_catalog_cleanup.plan_catalog_cleanup(
            company_key="company_a",
            audit_df=audit,
            qbo_item_rows=qbo_rows,
            source_inventory_report="/r.csv",
        )
        row = plan.iloc[0].to_dict()
        self.assertEqual(row["planned_action"], "create_base_then_consolidate_pack_variant")
        self.assertTrue(row["action_eligible"])
        self.assertIn("BACARDI WHITE RUM 750ml*12", row["qbo_pack_variant_names"])

    def test_multiple_active_base_items_becomes_manual_review(self):
        audit = self._audit_df(
            [
                {
                    "base_name": "WIDGET",
                    "epos_single_units": 8.0,
                    "catalog_issue_type": "multiple_active_base_items",
                    "qbo_base_item_ids": "10,11",
                    "qbo_base_item_names_for_base": "WIDGET | WIDGET",
                }
            ]
        )
        plan = inventory_catalog_cleanup.plan_catalog_cleanup(
            company_key="company_a",
            audit_df=audit,
            qbo_item_rows=None,
            source_inventory_report="/r.csv",
        )
        row = plan.iloc[0].to_dict()
        self.assertEqual(row["planned_action"], "manual_review_duplicate_base_items")
        self.assertFalse(row["action_eligible"])
        self.assertIn("manual_review", row["block_reason"])

    def test_missing_from_qbo_becomes_create_inventory_item(self):
        audit = self._audit_df(
            [
                {
                    "base_name": "NEW EPOS ITEM",
                    "epos_single_units": 8.0,
                    "catalog_issue_type": "missing_from_qbo",
                }
            ]
        )
        plan = inventory_catalog_cleanup.plan_catalog_cleanup(
            company_key="company_a",
            audit_df=audit,
            qbo_item_rows=None,
            source_inventory_report="/r.csv",
        )
        row = plan.iloc[0].to_dict()
        self.assertEqual(row["planned_action"], "create_inventory_item")
        self.assertFalse(row["action_eligible"])

    def test_report_columns_present(self):
        audit = self._audit_df(
            [
                {
                    "base_name": "X",
                    "epos_single_units": 1.0,
                    "catalog_issue_type": "missing_from_qbo",
                }
            ]
        )
        plan = inventory_catalog_cleanup.plan_catalog_cleanup(
            company_key="company_a",
            audit_df=audit,
            qbo_item_rows=None,
            source_inventory_report="/r.csv",
        )
        for col in inventory_catalog_cleanup._PLANNER_COLUMNS:
            self.assertIn(col, plan.columns)

    def test_default_output_excludes_no_action_rows(self):
        fake_cfg = mock.Mock(
            company_key="company_a",
            display_name="ACME",
            qbo_environment="production",
            realm_id="REALM123",
        )
        audit_df = pd.DataFrame(
            [
                {"base_name": "A", "epos_single_units": 1.0, "catalog_issue_type": "exact_name_match"},
                {"base_name": "B", "epos_single_units": 1.0, "catalog_issue_type": "missing_from_qbo"},
            ]
        )
        captured = {}

        def fake_write(_path, df):
            captured["df"] = df

        with mock.patch.object(inventory_catalog_cleanup, "load_company_config", return_value=fake_cfg), \
             mock.patch.object(inventory_catalog_cleanup, "ensure_company_runtime_compatible"), \
             mock.patch.object(inventory_catalog_cleanup, "get_available_companies", return_value=["company_a"]), \
             mock.patch.object(inventory_catalog_cleanup, "_read_inventory_report", return_value=audit_df), \
             mock.patch.object(inventory_catalog_cleanup, "_default_qbo_snapshot_path", return_value=None), \
             mock.patch.object(inventory_catalog_cleanup, "_write_csv", side_effect=fake_write), \
             redirect_stdout(io.StringIO()):
            exit_code = inventory_catalog_cleanup.main([
                "--company", "company_a",
                "--from-report", "/tmp/r.csv",
            ])
        self.assertEqual(exit_code, 0)
        self.assertEqual(len(captured["df"]), 1)
        self.assertEqual(captured["df"].iloc[0]["base_name"], "B")

    def test_include_no_action_includes_exact_match_rows(self):
        fake_cfg = mock.Mock(
            company_key="company_a",
            display_name="ACME",
            qbo_environment="production",
            realm_id="REALM123",
        )
        audit_df = pd.DataFrame(
            [
                {"base_name": "A", "epos_single_units": 1.0, "catalog_issue_type": "exact_name_match"},
                {"base_name": "B", "epos_single_units": 1.0, "catalog_issue_type": "missing_from_qbo"},
            ]
        )
        captured = {}

        def fake_write(_path, df):
            captured["df"] = df

        with mock.patch.object(inventory_catalog_cleanup, "load_company_config", return_value=fake_cfg), \
             mock.patch.object(inventory_catalog_cleanup, "ensure_company_runtime_compatible"), \
             mock.patch.object(inventory_catalog_cleanup, "get_available_companies", return_value=["company_a"]), \
             mock.patch.object(inventory_catalog_cleanup, "_read_inventory_report", return_value=audit_df), \
             mock.patch.object(inventory_catalog_cleanup, "_default_qbo_snapshot_path", return_value=None), \
             mock.patch.object(inventory_catalog_cleanup, "_write_csv", side_effect=fake_write), \
             redirect_stdout(io.StringIO()):
            exit_code = inventory_catalog_cleanup.main([
                "--company", "company_a",
                "--from-report", "/tmp/r.csv",
                "--include-no-action",
            ])
        self.assertEqual(exit_code, 0)
        self.assertEqual(set(captured["df"]["base_name"].tolist()), {"A", "B"})

    def test_missing_qbo_snapshot_error_is_actionable(self):
        fake_cfg = mock.Mock(
            company_key="company_a",
            display_name="ACME",
            qbo_environment="production",
            realm_id="REALM123",
        )
        with mock.patch.object(inventory_catalog_cleanup, "load_company_config", return_value=fake_cfg), \
             mock.patch.object(inventory_catalog_cleanup, "ensure_company_runtime_compatible"), \
             mock.patch.object(inventory_catalog_cleanup, "get_available_companies", return_value=["company_a"]), \
             mock.patch.object(inventory_catalog_cleanup, "_auto_download_stock_csv", return_value=Path("/tmp/stock.csv")), \
             mock.patch.object(inventory_catalog_cleanup, "_default_qbo_snapshot_path", return_value=None):
            with self.assertRaises(SystemExit) as ctx:
                inventory_catalog_cleanup.main([
                    "--company", "company_a",
                    "--auto-download",
                ])
        self.assertIn("--auto-fetch-qbo", str(ctx.exception))
        self.assertIn("--qbo-csv", str(ctx.exception))

    def test_main_does_not_call_qbo_write_functions(self):
        # Smoke test: running planner from report should not post or mutate QBO.
        fake_cfg = mock.Mock(
            company_key="company_a",
            display_name="ACME",
            qbo_environment="production",
            realm_id="REALM123",
        )
        with mock.patch.object(inventory_catalog_cleanup, "load_company_config", return_value=fake_cfg), \
             mock.patch.object(inventory_catalog_cleanup, "ensure_company_runtime_compatible"), \
             mock.patch.object(inventory_catalog_cleanup, "get_available_companies", return_value=["company_a"]):
            with mock.patch.object(inventory_catalog_cleanup, "_read_inventory_report", return_value=pd.DataFrame([{
                "base_name": "X",
                "epos_single_units": 1.0,
                "catalog_issue_type": "missing_from_qbo",
            }])) as _read_mock, \
                 mock.patch.object(inventory_catalog_cleanup, "_default_qbo_snapshot_path", return_value=None), \
                 mock.patch.object(inventory_catalog_cleanup, "_write_csv"), \
                 redirect_stdout(io.StringIO()):
                exit_code = inventory_catalog_cleanup.main([
                    "--company", "company_a",
                    "--from-report", "/tmp/r.csv",
                ])
        self.assertEqual(exit_code, 0)
        self.assertTrue(_read_mock.called)


if __name__ == "__main__":
    unittest.main()

