import io
import os
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest import mock

import pandas as pd

from code_scripts import inventory_catalog_cleanup, inventory_sync


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

    def test_case_insensitive_base_matching_still_plans_consolidation(self):
        audit = self._audit_df(
            [
                {
                    "base_name": "LEGEND EXTRA STOUT CAN 440ml",
                    "epos_single_units": 24.0,
                    "catalog_issue_type": "base_with_pack_variants",
                    "qbo_base_item_ids": "10",
                    "qbo_item_names_for_base": "LEGEND EXTRA STOUT CAN 440ML | LEGEND EXTRA STOUT CAN 440ml*24",
                    "qbo_base_item_names_for_base": "LEGEND EXTRA STOUT CAN 440ML",
                    "qbo_pack_variant_names_for_base": "LEGEND EXTRA STOUT CAN 440ml*24",
                    "suggested_next_action": "run pack variant consolidation and cleanup",
                }
            ]
        )
        qbo_rows = pd.DataFrame(
            [
                {"Id": "10", "Name": "LEGEND EXTRA STOUT CAN 440ML", "base_name": "LEGEND EXTRA STOUT CAN 440ML", "base_name_norm": "legend extra stout can 440ml", "qbo_has_pack": False},
                {"Id": "11", "Name": "LEGEND EXTRA STOUT CAN 440ml*24", "base_name": "LEGEND EXTRA STOUT CAN 440ml", "base_name_norm": "legend extra stout can 440ml", "qbo_has_pack": True},
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
        self.assertIn("LEGEND EXTRA STOUT CAN 440ML", row["qbo_base_item_names"])
        self.assertIn("LEGEND EXTRA STOUT CAN 440ml*24", row["qbo_pack_variant_names"])

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

    def test_multiple_active_base_items_with_typo_selects_safe_canonical(self):
        audit = self._audit_df(
            [
                {
                    "base_name": "SMIRNOFF ICE DOUBLE BLACK CAN 330ml",
                    "epos_single_units": 8.0,
                    "catalog_issue_type": "multiple_active_base_items",
                }
            ]
        )
        qbo_rows = pd.DataFrame(
            [
                {"Id": "9355", "Name": "SMIRNOFF ICE DOUBLE BLACK  CAN 330ml", "base_name": "SMIRNOFF ICE DOUBLE BLACK CAN 330ml", "base_name_norm": "smirnoff ice double black can 330ml", "qbo_has_pack": False, "qbo_qty_on_hand": 10},
                {"Id": "13875", "Name": "SMIRNOFF ICE DOUBLE BLACK CAN 330ml", "base_name": "SMIRNOFF ICE DOUBLE BLACK CAN 330ml", "base_name_norm": "smirnoff ice double black can 330ml", "qbo_has_pack": False, "qbo_qty_on_hand": -229},
                {"Id": "13956", "Name": "SMIRNOFF ICE DOUBLE BLACK CAN 330ml*12", "base_name": "SMIRNOFF ICE DOUBLE BLACK CAN 330ml", "base_name_norm": "smirnoff ice double black can 330ml", "qbo_has_pack": True, "qbo_qty_on_hand": -1},
            ]
        )
        plan = inventory_catalog_cleanup.plan_catalog_cleanup(
            company_key="company_a",
            audit_df=audit,
            qbo_item_rows=qbo_rows,
            source_inventory_report="/r.csv",
        )
        row = plan.iloc[0].to_dict()
        self.assertEqual(row["planned_action"], "resolve_duplicate_base_items")
        self.assertTrue(row["action_eligible"])

        canonical, reason = inventory_catalog_cleanup._select_duplicate_base_canonical(
            base_name="SMIRNOFF ICE DOUBLE BLACK CAN 330ml",
            qbo_group=qbo_rows,
        )
        self.assertEqual(reason, "")
        self.assertEqual(str(canonical["Id"]), "13875")

    def test_qbo_loader_preserves_raw_typo_space_for_duplicate_base_planning(self):
        import tempfile

        with tempfile.TemporaryDirectory() as td:
            qbo_csv = Path(td) / "qbo.csv"
            qbo_csv.write_text(
                "Id,Name,Type,TrackQtyOnHand,QtyOnHand\n"
                "9355,SMIRNOFF ICE DOUBLE BLACK  CAN 330ml,Inventory,true,10\n"
                "13875,SMIRNOFF ICE DOUBLE BLACK CAN 330ml,Inventory,true,-229\n"
                "13956,SMIRNOFF ICE DOUBLE BLACK CAN 330ml*12,Inventory,true,-1\n"
                "13942,SMIRNOFF ICE DOUBLE BLACK CAN 330ml*24,Inventory,true,-2\n",
                encoding="utf-8",
            )
            qbo_rows = inventory_sync.load_qbo_inventory_item_rows(str(qbo_csv))

        typo = qbo_rows[qbo_rows["Id"] == "9355"].iloc[0]
        self.assertEqual(typo["Name"], "SMIRNOFF ICE DOUBLE BLACK  CAN 330ml")
        self.assertEqual(typo["qbo_name_original"], "SMIRNOFF ICE DOUBLE BLACK  CAN 330ml")
        self.assertEqual(typo["qbo_name_raw"], "SMIRNOFF ICE DOUBLE BLACK  CAN 330ml")
        self.assertEqual(typo["qbo_name_display"], "SMIRNOFF ICE DOUBLE BLACK CAN 330ml")

        audit = self._audit_df(
            [
                {
                    "base_name": "SMIRNOFF ICE DOUBLE BLACK CAN 330ml",
                    "epos_single_units": 0.0,
                    "catalog_issue_type": "multiple_active_base_items",
                }
            ]
        )
        plan = inventory_catalog_cleanup.plan_catalog_cleanup(
            company_key="company_a",
            audit_df=audit,
            qbo_item_rows=qbo_rows,
            source_inventory_report="/r.csv",
        )
        row = plan.iloc[0].to_dict()
        self.assertEqual(row["planned_action"], "resolve_duplicate_base_items")
        self.assertTrue(row["action_eligible"])

        canonical, reason = inventory_catalog_cleanup._select_duplicate_base_canonical(
            base_name="SMIRNOFF ICE DOUBLE BLACK CAN 330ml",
            qbo_group=qbo_rows,
        )
        self.assertEqual(reason, "")
        self.assertEqual(str(canonical["Id"]), "13875")

    def test_mutated_display_name_still_uses_original_name_for_canonical_selection(self):
        audit = self._audit_df(
            [
                {
                    "base_name": "SMIRNOFF ICE DOUBLE BLACK CAN 330ml",
                    "epos_single_units": 0.0,
                    "catalog_issue_type": "multiple_active_base_items",
                }
            ]
        )
        qbo_rows = pd.DataFrame(
            [
                {
                    "Id": "9355",
                    "Name": "SMIRNOFF ICE DOUBLE BLACK CAN 330ml",
                    "qbo_name_original": "SMIRNOFF ICE DOUBLE BLACK  CAN 330ml",
                    "base_name": "SMIRNOFF ICE DOUBLE BLACK CAN 330ml",
                    "base_name_norm": "smirnoff ice double black can 330ml",
                    "qbo_has_pack": False,
                },
                {
                    "Id": "13875",
                    "Name": "SMIRNOFF ICE DOUBLE BLACK CAN 330ml",
                    "qbo_name_original": "SMIRNOFF ICE DOUBLE BLACK CAN 330ml",
                    "base_name": "SMIRNOFF ICE DOUBLE BLACK CAN 330ml",
                    "base_name_norm": "smirnoff ice double black can 330ml",
                    "qbo_has_pack": False,
                },
            ]
        )
        plan = inventory_catalog_cleanup.plan_catalog_cleanup(
            company_key="company_a",
            audit_df=audit,
            qbo_item_rows=qbo_rows,
            source_inventory_report="/r.csv",
        )
        row = plan.iloc[0].to_dict()
        self.assertEqual(row["planned_action"], "resolve_duplicate_base_items")
        self.assertTrue(row["action_eligible"])

        canonical, reason = inventory_catalog_cleanup._select_duplicate_base_canonical(
            base_name="SMIRNOFF ICE DOUBLE BLACK CAN 330ml",
            qbo_group=qbo_rows,
        )
        self.assertEqual(reason, "")
        self.assertEqual(str(canonical["Id"]), "13875")

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

    def test_product_filters_planner_rows(self):
        fake_cfg = mock.Mock(
            company_key="company_a",
            display_name="ACME",
            qbo_environment="production",
            realm_id="REALM123",
        )
        audit_df = pd.DataFrame(
            [
                {"base_name": "BACARDI WHITE RUM 750ml", "epos_single_units": 1.0, "catalog_issue_type": "missing_from_qbo"},
                {"base_name": "BAILEYS SALTED CARAMEL IRISH CREAM 700ml", "epos_single_units": 1.0, "catalog_issue_type": "missing_from_qbo"},
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
                "--product", "baileys",
            ])
        self.assertEqual(exit_code, 0)
        self.assertEqual(len(captured["df"]), 1)
        self.assertIn("BAILEYS", captured["df"].iloc[0]["base_name"])

    def test_product_filter_is_literal_text_not_regex(self):
        fake_cfg = mock.Mock(
            company_key="company_a",
            display_name="ACME",
            qbo_environment="production",
            realm_id="REALM123",
        )
        audit_df = pd.DataFrame(
            [
                {"base_name": "A+B ITEM", "epos_single_units": 1.0, "catalog_issue_type": "missing_from_qbo"},
                {"base_name": "AB ITEM", "epos_single_units": 1.0, "catalog_issue_type": "missing_from_qbo"},
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
                "--product", "a+b",
            ])
        self.assertEqual(exit_code, 0)
        self.assertEqual(len(captured["df"]), 1)
        self.assertEqual(captured["df"].iloc[0]["base_name"], "A+B ITEM")

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

    def test_auto_fetch_qbo_calls_fetch_helper_and_uses_returned_path(self):
        fake_cfg = mock.Mock(
            company_key="company_a",
            display_name="ACME",
            qbo_environment="production",
            realm_id="REALM123",
        )
        audit_df = pd.DataFrame(
            [
                {"base_name": "A", "epos_single_units": 1.0, "catalog_issue_type": "missing_from_qbo"},
            ]
        )
        qbo_item_rows = pd.DataFrame(
            [
                {"Id": "10", "Name": "A", "base_name": "A", "qbo_has_pack": False, "qbo_qty_on_hand": 1},
            ]
        )
        import tempfile
        with tempfile.TemporaryDirectory() as td, \
             mock.patch.object(inventory_catalog_cleanup, "load_company_config", return_value=fake_cfg), \
             mock.patch.object(inventory_catalog_cleanup, "ensure_company_runtime_compatible"), \
             mock.patch.object(inventory_catalog_cleanup, "get_available_companies", return_value=["company_a"]), \
             mock.patch.object(inventory_catalog_cleanup, "_read_inventory_report", return_value=audit_df), \
             mock.patch.object(inventory_catalog_cleanup, "_default_qbo_snapshot_path", return_value=None), \
             mock.patch.object(inventory_catalog_cleanup, "get_qbo_snapshot_path", return_value=Path(td) / "qbo.csv"), \
             mock.patch.object(inventory_catalog_cleanup, "fetch_qbo_inventory_items_snapshot", return_value=Path(td) / "qbo.csv") as fetch_mock, \
             mock.patch.object(inventory_catalog_cleanup, "load_qbo_inventory_item_rows", return_value=qbo_item_rows) as load_rows_mock, \
             mock.patch.object(inventory_catalog_cleanup, "_write_csv"), \
             redirect_stdout(io.StringIO()):
            (Path(td) / "qbo.csv").write_text("Id,Name,Type,TrackQtyOnHand,QtyOnHand\n", encoding="utf-8")
            exit_code = inventory_catalog_cleanup.main([
                "--company", "company_a",
                "--from-report", "/tmp/r.csv",
                "--auto-fetch-qbo",
            ])
        self.assertEqual(exit_code, 0)
        fetch_mock.assert_called_once()
        load_rows_mock.assert_called()

    def test_qbo_force_refresh_passes_flag_through(self):
        fake_cfg = mock.Mock(
            company_key="company_a",
            display_name="ACME",
            qbo_environment="production",
            realm_id="REALM123",
        )
        audit_df = pd.DataFrame(
            [
                {"base_name": "A", "epos_single_units": 1.0, "catalog_issue_type": "missing_from_qbo"},
            ]
        )
        qbo_item_rows = pd.DataFrame(
            [
                {"Id": "10", "Name": "A", "base_name": "A", "qbo_has_pack": False, "qbo_qty_on_hand": 1},
            ]
        )
        recorded = {}

        def fake_fetch(**kwargs):
            recorded.update(kwargs)
            return Path(kwargs["output_path"])

        import tempfile
        with tempfile.TemporaryDirectory() as td, \
             mock.patch.object(inventory_catalog_cleanup, "load_company_config", return_value=fake_cfg), \
             mock.patch.object(inventory_catalog_cleanup, "ensure_company_runtime_compatible"), \
             mock.patch.object(inventory_catalog_cleanup, "get_available_companies", return_value=["company_a"]), \
             mock.patch.object(inventory_catalog_cleanup, "_read_inventory_report", return_value=audit_df), \
             mock.patch.object(inventory_catalog_cleanup, "_default_qbo_snapshot_path", return_value=None), \
             mock.patch.object(inventory_catalog_cleanup, "get_qbo_snapshot_path", return_value=Path(td) / "qbo.csv"), \
             mock.patch.object(inventory_catalog_cleanup, "fetch_qbo_inventory_items_snapshot", side_effect=fake_fetch), \
             mock.patch.object(inventory_catalog_cleanup, "load_qbo_inventory_item_rows", return_value=qbo_item_rows), \
             mock.patch.object(inventory_catalog_cleanup, "_write_csv"), \
             redirect_stdout(io.StringIO()):
            (Path(td) / "qbo.csv").write_text("Id,Name,Type,TrackQtyOnHand,QtyOnHand\n", encoding="utf-8")
            exit_code = inventory_catalog_cleanup.main([
                "--company", "company_a",
                "--from-report", "/tmp/r.csv",
                "--auto-fetch-qbo",
                "--qbo-force-refresh",
            ])
        self.assertEqual(exit_code, 0)
        self.assertTrue(recorded.get("force_refresh"))

    def test_apply_requires_max_products(self):
        fake_cfg = mock.Mock(
            company_key="company_a",
            display_name="ACME",
            qbo_environment="production",
            realm_id="REALM123",
            inventory_adjustment_account_id="88",
        )
        with mock.patch.object(inventory_catalog_cleanup, "load_company_config", return_value=fake_cfg), \
             mock.patch.object(inventory_catalog_cleanup, "ensure_company_runtime_compatible"), \
             mock.patch.object(inventory_catalog_cleanup, "get_available_companies", return_value=["company_a"]):
            with self.assertRaises(SystemExit) as ctx:
                inventory_catalog_cleanup.main([
                    "--company", "company_a",
                    "--from-report", "/tmp/r.csv",
                    "--apply",
                ])
        self.assertIn("--max-products", str(ctx.exception))

    def test_dry_run_does_not_call_qbo_write_functions(self):
        fake_cfg = mock.Mock(
            company_key="company_a",
            display_name="ACME",
            qbo_environment="production",
            realm_id="REALM123",
            inventory_adjustment_account_id="88",
        )
        audit_df = pd.DataFrame(
            [
                {"base_name": "GOLDBERG CAN 50cl", "epos_single_units": 8.0, "catalog_issue_type": "base_with_pack_variants"},
            ]
        )
        qbo_item_rows = pd.DataFrame(
            [
                {"Id": "10", "Name": "GOLDBERG CAN 50cl", "base_name": "GOLDBERG CAN 50cl", "qbo_has_pack": False, "qbo_qty_on_hand": 1},
                {"Id": "11", "Name": "GOLDBERG CAN 50cl*6", "base_name": "GOLDBERG CAN 50cl", "qbo_has_pack": True, "qbo_qty_on_hand": 1},
            ]
        )
        import tempfile
        with tempfile.TemporaryDirectory() as td, \
             mock.patch.object(inventory_catalog_cleanup, "load_company_config", return_value=fake_cfg), \
             mock.patch.object(inventory_catalog_cleanup, "ensure_company_runtime_compatible"), \
             mock.patch.object(inventory_catalog_cleanup, "get_available_companies", return_value=["company_a"]), \
             mock.patch.object(inventory_catalog_cleanup, "_read_inventory_report", return_value=audit_df), \
             mock.patch.object(inventory_catalog_cleanup, "_default_qbo_snapshot_path", return_value=Path(td) / "qbo.csv"), \
             mock.patch.object(inventory_catalog_cleanup, "load_qbo_inventory_item_rows", return_value=qbo_item_rows), \
             mock.patch.object(inventory_catalog_cleanup, "verify_realm_match") as verify_mock, \
             mock.patch.object(inventory_catalog_cleanup, "TokenManager") as token_mock, \
             mock.patch.object(inventory_catalog_cleanup, "post_inventory_adjustment") as post_mock, \
             mock.patch.object(inventory_catalog_cleanup, "_post_inactivate") as inact_mock, \
             mock.patch.object(inventory_catalog_cleanup, "mark_qbo_snapshot_stale") as stale_mock, \
             mock.patch.object(inventory_catalog_cleanup, "_write_csv"), \
             redirect_stdout(io.StringIO()):
            (Path(td) / "qbo.csv").write_text("Id,Name,Type,TrackQtyOnHand,QtyOnHand\n", encoding="utf-8")
            exit_code = inventory_catalog_cleanup.main([
                "--company", "company_a",
                "--from-report", "/tmp/r.csv",
                "--dry-run",
                "--max-products", "1",
                "--qbo-csv", str(Path(td) / "qbo.csv"),
            ])
        self.assertEqual(exit_code, 0)
        post_mock.assert_not_called()
        inact_mock.assert_not_called()
        stale_mock.assert_not_called()
        verify_mock.assert_not_called()
        token_mock.assert_not_called()

    def test_dry_run_only_pack_variant_create_path_does_not_write(self):
        fake_cfg = mock.Mock(
            company_key="company_a",
            display_name="ACME",
            qbo_environment="production",
            realm_id="REALM123",
            inventory_adjustment_account_id="88",
            default_qty_on_hand=0,
            inventory_start_date="2026-01-01",
            tax_code_id="2",
        )
        audit_df = pd.DataFrame(
            [
                {
                    "base_name": "BACARDI WHITE RUM 750ml",
                    "epos_single_units": 12.0,
                    "epos_categories": "ALCOHOLS & SPIRITS",
                    "catalog_issue_type": "only_pack_variant_exists",
                },
            ]
        )
        qbo_item_rows = pd.DataFrame(
            [
                {"Id": "99", "Name": "BACARDI WHITE RUM 750ml*12", "base_name": "BACARDI WHITE RUM 750ml", "base_name_norm": "bacardi white rum 750ml", "qbo_has_pack": True, "qbo_qty_on_hand": 2},
            ]
        )
        import tempfile
        with tempfile.TemporaryDirectory() as td, \
             mock.patch.object(inventory_catalog_cleanup, "load_company_config", return_value=fake_cfg), \
             mock.patch.object(inventory_catalog_cleanup, "ensure_company_runtime_compatible"), \
             mock.patch.object(inventory_catalog_cleanup, "get_available_companies", return_value=["company_a"]), \
             mock.patch.object(inventory_catalog_cleanup, "_read_inventory_report", return_value=audit_df), \
             mock.patch.object(inventory_catalog_cleanup, "_default_qbo_snapshot_path", return_value=Path(td) / "qbo.csv"), \
             mock.patch.object(inventory_catalog_cleanup, "load_qbo_inventory_item_rows", return_value=qbo_item_rows), \
             mock.patch.object(inventory_catalog_cleanup, "load_category_account_mapping", return_value={"ALCOHOLS & SPIRITS": {"asset": "a", "income": "i", "expense": "e"}}), \
             mock.patch.object(inventory_catalog_cleanup, "create_inventory_item") as create_mock, \
             mock.patch.object(inventory_catalog_cleanup, "post_inventory_adjustment") as post_mock, \
             mock.patch.object(inventory_catalog_cleanup, "_post_inactivate") as inact_mock, \
             mock.patch.object(inventory_catalog_cleanup, "verify_realm_match") as verify_mock, \
             mock.patch.object(inventory_catalog_cleanup, "TokenManager") as token_mock, \
             mock.patch.object(inventory_catalog_cleanup, "_write_csv"), \
             redirect_stdout(io.StringIO()):
            (Path(td) / "qbo.csv").write_text("Id,Name,Type,TrackQtyOnHand,QtyOnHand\n", encoding="utf-8")
            exit_code = inventory_catalog_cleanup.main([
                "--company", "company_a",
                "--from-report", "/tmp/r.csv",
                "--dry-run",
                "--max-products", "1",
                "--qbo-csv", str(Path(td) / "qbo.csv"),
            ])
        self.assertEqual(exit_code, 0)
        create_mock.assert_not_called()
        post_mock.assert_not_called()
        inact_mock.assert_not_called()
        verify_mock.assert_not_called()
        token_mock.assert_not_called()

    def test_apply_only_pack_variant_creates_base_then_consolidates(self):
        fake_cfg = mock.Mock(
            company_key="company_a",
            display_name="ACME",
            qbo_environment="production",
            realm_id="REALM123",
            inventory_adjustment_account_id="88",
            default_qty_on_hand=0,
            inventory_start_date="2026-01-01",
            tax_code_id="2",
        )
        audit_df = pd.DataFrame(
            [
                {
                    "base_name": "BACARDI WHITE RUM 750ml",
                    "epos_single_units": 12.0,
                    "epos_categories": "ALCOHOLS & SPIRITS",
                    "catalog_issue_type": "only_pack_variant_exists",
                },
            ]
        )
        qbo_item_rows = pd.DataFrame(
            [
                {"Id": "99", "Name": "BACARDI WHITE RUM 750ml*12", "base_name": "BACARDI WHITE RUM 750ml", "base_name_norm": "bacardi white rum 750ml", "qbo_has_pack": True, "qbo_qty_on_hand": 2},
            ]
        )

        def fake_fetch(_tm, _realm, item_id):
            if str(item_id) == "99":
                return {"Id": "99", "Name": "BACARDI WHITE RUM 750ml*12", "SyncToken": "0", "QtyOnHand": 0, "UnitPrice": 2000.0, "PurchaseCost": 1500.0}
            return {"Id": str(item_id), "Name": "BACARDI WHITE RUM 750ml", "SyncToken": "0", "QtyOnHand": 0}

        import tempfile
        with tempfile.TemporaryDirectory() as td, \
             mock.patch.object(inventory_catalog_cleanup, "load_company_config", return_value=fake_cfg), \
             mock.patch.object(inventory_catalog_cleanup, "ensure_company_runtime_compatible"), \
             mock.patch.object(inventory_catalog_cleanup, "get_available_companies", return_value=["company_a"]), \
             mock.patch.object(inventory_catalog_cleanup, "_read_inventory_report", return_value=audit_df), \
             mock.patch.object(inventory_catalog_cleanup, "_default_qbo_snapshot_path", return_value=Path(td) / "qbo.csv"), \
             mock.patch.object(inventory_catalog_cleanup, "load_qbo_inventory_item_rows", return_value=qbo_item_rows), \
             mock.patch.object(inventory_catalog_cleanup, "load_category_account_mapping", return_value={"ALCOHOLS & SPIRITS": {"asset": "a", "income": "i", "expense": "e"}}), \
             mock.patch.object(inventory_catalog_cleanup, "get_or_create_item_category_id", return_value="321"), \
             mock.patch.object(inventory_catalog_cleanup, "create_inventory_item", return_value="123") as create_mock, \
             mock.patch.object(inventory_catalog_cleanup, "_fetch_item_with_sync_token", side_effect=fake_fetch), \
             mock.patch.object(inventory_catalog_cleanup, "post_inventory_adjustment", return_value={"InventoryAdjustment": {"Id": "1"}}) as post_mock, \
             mock.patch.object(inventory_catalog_cleanup, "_post_inactivate", return_value={}) as inact_mock, \
             mock.patch.object(inventory_catalog_cleanup, "mark_qbo_snapshot_stale") as stale_mock, \
             mock.patch.object(inventory_catalog_cleanup, "verify_realm_match"), \
             mock.patch.object(inventory_catalog_cleanup, "TokenManager", return_value=mock.Mock()), \
             mock.patch.object(inventory_catalog_cleanup, "_write_csv"), \
             redirect_stdout(io.StringIO()):
            (Path(td) / "qbo.csv").write_text("Id,Name,Type,TrackQtyOnHand,QtyOnHand\n", encoding="utf-8")
            exit_code = inventory_catalog_cleanup.main([
                "--company", "company_a",
                "--from-report", "/tmp/r.csv",
                "--apply",
                "--max-products", "1",
                "--qbo-csv", str(Path(td) / "qbo.csv"),
            ])

        self.assertEqual(exit_code, 0)
        create_mock.assert_called_once()
        post_mock.assert_called_once()
        inact_mock.assert_called_once()
        stale_mock.assert_called_once_with("company_a", reason="inventory_catalog_cleanup_applied")

    def test_apply_only_pack_variant_skips_when_mapping_missing(self):
        fake_cfg = mock.Mock(
            company_key="company_a",
            display_name="ACME",
            qbo_environment="production",
            realm_id="REALM123",
            inventory_adjustment_account_id="88",
            default_qty_on_hand=0,
            inventory_start_date="2026-01-01",
            tax_code_id="2",
        )
        audit_df = pd.DataFrame(
            [
                {
                    "base_name": "BACARDI WHITE RUM 750ml",
                    "epos_single_units": 12.0,
                    "epos_categories": "ALCOHOLS & SPIRITS",
                    "catalog_issue_type": "only_pack_variant_exists",
                },
            ]
        )
        qbo_item_rows = pd.DataFrame(
            [
                {"Id": "99", "Name": "BACARDI WHITE RUM 750ml*12", "base_name": "BACARDI WHITE RUM 750ml", "base_name_norm": "bacardi white rum 750ml", "qbo_has_pack": True, "qbo_qty_on_hand": 2},
            ]
        )
        import tempfile
        buf = io.StringIO()
        with tempfile.TemporaryDirectory() as td, \
             mock.patch.object(inventory_catalog_cleanup, "load_company_config", return_value=fake_cfg), \
             mock.patch.object(inventory_catalog_cleanup, "ensure_company_runtime_compatible"), \
             mock.patch.object(inventory_catalog_cleanup, "get_available_companies", return_value=["company_a"]), \
             mock.patch.object(inventory_catalog_cleanup, "_read_inventory_report", return_value=audit_df), \
             mock.patch.object(inventory_catalog_cleanup, "_default_qbo_snapshot_path", return_value=Path(td) / "qbo.csv"), \
             mock.patch.object(inventory_catalog_cleanup, "load_qbo_inventory_item_rows", return_value=qbo_item_rows), \
             mock.patch.object(inventory_catalog_cleanup, "load_category_account_mapping", side_effect=RuntimeError("mapping missing")), \
             mock.patch.object(inventory_catalog_cleanup, "create_inventory_item") as create_mock, \
             mock.patch.object(inventory_catalog_cleanup, "verify_realm_match"), \
             mock.patch.object(inventory_catalog_cleanup, "TokenManager", return_value=mock.Mock()), \
             mock.patch.object(inventory_catalog_cleanup, "_write_csv"), \
             redirect_stdout(buf):
            (Path(td) / "qbo.csv").write_text("Id,Name,Type,TrackQtyOnHand,QtyOnHand\n", encoding="utf-8")
            exit_code = inventory_catalog_cleanup.main([
                "--company", "company_a",
                "--from-report", "/tmp/r.csv",
                "--apply",
                "--max-products", "1",
                "--qbo-csv", str(Path(td) / "qbo.csv"),
            ])
        self.assertEqual(exit_code, 0)
        create_mock.assert_not_called()
        self.assertIn("missing_account_mapping", buf.getvalue())

    def test_apply_only_pack_variant_does_not_create_when_base_exists_case_insensitive(self):
        fake_cfg = mock.Mock(
            company_key="company_a",
            display_name="ACME",
            qbo_environment="production",
            realm_id="REALM123",
            inventory_adjustment_account_id="88",
        )
        audit_df = pd.DataFrame(
            [
                {
                    "base_name": "LEGEND EXTRA STOUT CAN 440ml",
                    "epos_single_units": 12.0,
                    "epos_categories": "ALCOHOLS & SPIRITS",
                    "catalog_issue_type": "only_pack_variant_exists",
                },
            ]
        )
        qbo_item_rows = pd.DataFrame(
            [
                {"Id": "10", "Name": "LEGEND EXTRA STOUT CAN 440ML", "base_name": "LEGEND EXTRA STOUT CAN 440ML", "base_name_norm": "legend extra stout can 440ml", "qbo_has_pack": False, "qbo_qty_on_hand": 1},
                {"Id": "11", "Name": "LEGEND EXTRA STOUT CAN 440ml*24", "base_name": "LEGEND EXTRA STOUT CAN 440ml", "base_name_norm": "legend extra stout can 440ml", "qbo_has_pack": True, "qbo_qty_on_hand": 1},
            ]
        )
        import tempfile
        buf = io.StringIO()
        with tempfile.TemporaryDirectory() as td, \
             mock.patch.object(inventory_catalog_cleanup, "load_company_config", return_value=fake_cfg), \
             mock.patch.object(inventory_catalog_cleanup, "ensure_company_runtime_compatible"), \
             mock.patch.object(inventory_catalog_cleanup, "get_available_companies", return_value=["company_a"]), \
             mock.patch.object(inventory_catalog_cleanup, "_read_inventory_report", return_value=audit_df), \
             mock.patch.object(inventory_catalog_cleanup, "_default_qbo_snapshot_path", return_value=Path(td) / "qbo.csv"), \
             mock.patch.object(inventory_catalog_cleanup, "load_qbo_inventory_item_rows", return_value=qbo_item_rows), \
             mock.patch.object(inventory_catalog_cleanup, "create_inventory_item") as create_mock, \
             mock.patch.object(inventory_catalog_cleanup, "verify_realm_match"), \
             mock.patch.object(inventory_catalog_cleanup, "TokenManager", return_value=mock.Mock()), \
             mock.patch.object(inventory_catalog_cleanup, "_write_csv"), \
             redirect_stdout(buf):
            (Path(td) / "qbo.csv").write_text("Id,Name,Type,TrackQtyOnHand,QtyOnHand\n", encoding="utf-8")
            exit_code = inventory_catalog_cleanup.main([
                "--company", "company_a",
                "--from-report", "/tmp/r.csv",
                "--apply",
                "--max-products", "1",
                "--qbo-csv", str(Path(td) / "qbo.csv"),
            ])
        self.assertEqual(exit_code, 0)
        create_mock.assert_not_called()
        self.assertIn("active_base_already_exists", buf.getvalue())

    def test_apply_processes_only_consolidate_rows_and_respects_cap(self):
        fake_cfg = mock.Mock(
            company_key="company_a",
            display_name="ACME",
            qbo_environment="production",
            realm_id="REALM123",
            inventory_adjustment_account_id="88",
        )
        audit_df = pd.DataFrame(
            [
                {"base_name": "A", "epos_single_units": 8.0, "catalog_issue_type": "base_with_pack_variants"},
                {"base_name": "B", "epos_single_units": 8.0, "catalog_issue_type": "base_with_pack_variants"},
                {"base_name": "C", "epos_single_units": 8.0, "catalog_issue_type": "only_pack_variant_exists"},
            ]
        )
        qbo_item_rows = pd.DataFrame(
            [
                {"Id": "10", "Name": "A", "base_name": "A", "qbo_has_pack": False, "qbo_qty_on_hand": 1},
                {"Id": "11", "Name": "A*6", "base_name": "A", "qbo_has_pack": True, "qbo_qty_on_hand": 1},
                {"Id": "20", "Name": "B", "base_name": "B", "qbo_has_pack": False, "qbo_qty_on_hand": 1},
                {"Id": "21", "Name": "B*6", "base_name": "B", "qbo_has_pack": True, "qbo_qty_on_hand": 1},
            ]
        )

        def fake_fetch_item(_tm, _realm, item_id):
            return {"Id": item_id, "Name": f"X*6", "SyncToken": "0", "QtyOnHand": 0}

        import tempfile
        buf = io.StringIO()
        with tempfile.TemporaryDirectory() as td, \
             mock.patch.object(inventory_catalog_cleanup, "load_company_config", return_value=fake_cfg), \
             mock.patch.object(inventory_catalog_cleanup, "ensure_company_runtime_compatible"), \
             mock.patch.object(inventory_catalog_cleanup, "get_available_companies", return_value=["company_a"]), \
             mock.patch.object(inventory_catalog_cleanup, "_read_inventory_report", return_value=audit_df), \
             mock.patch.object(inventory_catalog_cleanup, "_default_qbo_snapshot_path", return_value=Path(td) / "qbo.csv"), \
             mock.patch.object(inventory_catalog_cleanup, "load_qbo_inventory_item_rows", return_value=qbo_item_rows), \
             mock.patch.object(inventory_catalog_cleanup, "verify_realm_match") as verify_mock, \
             mock.patch.object(inventory_catalog_cleanup, "TokenManager", return_value=mock.Mock()) as token_mock, \
             mock.patch.object(inventory_catalog_cleanup, "post_inventory_adjustment", return_value={"InventoryAdjustment": {"Id": "1"}}) as post_mock, \
             mock.patch.object(inventory_catalog_cleanup, "_fetch_item_with_sync_token", side_effect=fake_fetch_item), \
             mock.patch.object(inventory_catalog_cleanup, "_post_inactivate", return_value={}) as inact_mock, \
             mock.patch.object(inventory_catalog_cleanup, "mark_qbo_snapshot_stale") as stale_mock, \
             mock.patch.object(inventory_catalog_cleanup, "_write_csv"), \
             redirect_stdout(buf):
            (Path(td) / "qbo.csv").write_text("Id,Name,Type,TrackQtyOnHand,QtyOnHand\n", encoding="utf-8")
            exit_code = inventory_catalog_cleanup.main([
                "--company", "company_a",
                "--from-report", "/tmp/r.csv",
                "--apply",
                "--max-products", "1",
                "--product", "A",
                "--qbo-csv", str(Path(td) / "qbo.csv"),
            ])
        self.assertEqual(exit_code, 0)
        self.assertEqual(post_mock.call_count, 1)
        self.assertGreaterEqual(inact_mock.call_count, 1)
        stale_mock.assert_called()
        verify_mock.assert_called_once()
        token_mock.assert_called_once()
        out = buf.getvalue()
        self.assertIn("[OK] Posted InventoryAdjustment", out)
        self.assertIn("[OK] Inactivated pack_variant_id=", out)

    def test_apply_duplicate_base_items_resolves_and_inactivates_duplicates_and_packs(self):
        fake_cfg = mock.Mock(
            company_key="company_a",
            display_name="ACME",
            qbo_environment="production",
            realm_id="REALM123",
            inventory_adjustment_account_id="88",
        )
        audit_df = pd.DataFrame(
            [
                {"base_name": "SMIRNOFF ICE DOUBLE BLACK CAN 330ml", "epos_single_units": 8.0, "catalog_issue_type": "multiple_active_base_items"},
            ]
        )
        qbo_item_rows = pd.DataFrame(
            [
                {"Id": "9355", "Name": "SMIRNOFF ICE DOUBLE BLACK  CAN 330ml", "base_name": "SMIRNOFF ICE DOUBLE BLACK CAN 330ml", "base_name_norm": "smirnoff ice double black can 330ml", "qbo_has_pack": False, "qbo_qty_on_hand": 10},
                {"Id": "13875", "Name": "SMIRNOFF ICE DOUBLE BLACK CAN 330ml", "base_name": "SMIRNOFF ICE DOUBLE BLACK CAN 330ml", "base_name_norm": "smirnoff ice double black can 330ml", "qbo_has_pack": False, "qbo_qty_on_hand": -229},
                {"Id": "13956", "Name": "SMIRNOFF ICE DOUBLE BLACK CAN 330ml*12", "base_name": "SMIRNOFF ICE DOUBLE BLACK CAN 330ml", "base_name_norm": "smirnoff ice double black can 330ml", "qbo_has_pack": True, "qbo_qty_on_hand": -1},
                {"Id": "13942", "Name": "SMIRNOFF ICE DOUBLE BLACK CAN 330ml*24", "base_name": "SMIRNOFF ICE DOUBLE BLACK CAN 330ml", "base_name_norm": "smirnoff ice double black can 330ml", "qbo_has_pack": True, "qbo_qty_on_hand": -2},
            ]
        )

        def fake_fetch(_tm, _realm, item_id):
            return {"Id": item_id, "Name": f"n-{item_id}", "SyncToken": "0", "QtyOnHand": 0}

        import tempfile
        with tempfile.TemporaryDirectory() as td, \
             mock.patch.object(inventory_catalog_cleanup, "load_company_config", return_value=fake_cfg), \
             mock.patch.object(inventory_catalog_cleanup, "ensure_company_runtime_compatible"), \
             mock.patch.object(inventory_catalog_cleanup, "get_available_companies", return_value=["company_a"]), \
             mock.patch.object(inventory_catalog_cleanup, "_read_inventory_report", return_value=audit_df), \
             mock.patch.object(inventory_catalog_cleanup, "_default_qbo_snapshot_path", return_value=Path(td) / "qbo.csv"), \
             mock.patch.object(inventory_catalog_cleanup, "load_qbo_inventory_item_rows", return_value=qbo_item_rows), \
             mock.patch.object(inventory_catalog_cleanup, "verify_realm_match"), \
             mock.patch.object(inventory_catalog_cleanup, "TokenManager", return_value=mock.Mock()), \
             mock.patch.object(inventory_catalog_cleanup, "post_inventory_adjustment", return_value={"InventoryAdjustment": {"Id": "1"}}) as post_mock, \
             mock.patch.object(inventory_catalog_cleanup, "_fetch_item_with_sync_token", side_effect=fake_fetch), \
             mock.patch.object(inventory_catalog_cleanup, "_post_inactivate", return_value={}) as inact_mock, \
             mock.patch.object(inventory_catalog_cleanup, "mark_qbo_snapshot_stale") as stale_mock, \
             mock.patch.object(inventory_catalog_cleanup, "_write_csv"), \
             redirect_stdout(io.StringIO()):
            (Path(td) / "qbo.csv").write_text("Id,Name,Type,TrackQtyOnHand,QtyOnHand\n", encoding="utf-8")
            exit_code = inventory_catalog_cleanup.main([
                "--company", "company_a",
                "--from-report", "/tmp/r.csv",
                "--apply",
                "--max-products", "1",
                "--qbo-csv", str(Path(td) / "qbo.csv"),
            ])
        self.assertEqual(exit_code, 0)
        post_mock.assert_called_once()
        self.assertGreaterEqual(inact_mock.call_count, 3)
        stale_mock.assert_called_once_with("company_a", reason="inventory_catalog_cleanup_applied")

    def test_dry_run_duplicate_base_items_prints_canonical_and_duplicate_ids(self):
        fake_cfg = mock.Mock(
            company_key="company_a",
            display_name="ACME",
            qbo_environment="production",
            realm_id="REALM123",
            inventory_adjustment_account_id="88",
        )
        plan_df = pd.DataFrame(
            [
                {
                    "company_key": "company_a",
                    "base_name": "SMIRNOFF ICE DOUBLE BLACK CAN 330ml",
                    "epos_single_units": 0.0,
                    "catalog_issue_type": "multiple_active_base_items",
                    "planned_action": "resolve_duplicate_base_items",
                    "action_eligible": True,
                    "block_reason": "",
                }
            ]
        )
        qbo_item_rows = pd.DataFrame(
            [
                {"Id": "9355", "Name": "SMIRNOFF ICE DOUBLE BLACK  CAN 330ml", "qbo_name_original": "SMIRNOFF ICE DOUBLE BLACK  CAN 330ml", "base_name": "SMIRNOFF ICE DOUBLE BLACK CAN 330ml", "base_name_norm": "smirnoff ice double black can 330ml", "qbo_has_pack": False, "qbo_qty_on_hand": 10},
                {"Id": "13875", "Name": "SMIRNOFF ICE DOUBLE BLACK CAN 330ml", "qbo_name_original": "SMIRNOFF ICE DOUBLE BLACK CAN 330ml", "base_name": "SMIRNOFF ICE DOUBLE BLACK CAN 330ml", "base_name_norm": "smirnoff ice double black can 330ml", "qbo_has_pack": False, "qbo_qty_on_hand": -229},
                {"Id": "13956", "Name": "SMIRNOFF ICE DOUBLE BLACK CAN 330ml*12", "qbo_name_original": "SMIRNOFF ICE DOUBLE BLACK CAN 330ml*12", "base_name": "SMIRNOFF ICE DOUBLE BLACK CAN 330ml", "base_name_norm": "smirnoff ice double black can 330ml", "qbo_has_pack": True, "qbo_qty_on_hand": -1},
                {"Id": "13942", "Name": "SMIRNOFF ICE DOUBLE BLACK CAN 330ml*24", "qbo_name_original": "SMIRNOFF ICE DOUBLE BLACK CAN 330ml*24", "base_name": "SMIRNOFF ICE DOUBLE BLACK CAN 330ml", "base_name_norm": "smirnoff ice double black can 330ml", "qbo_has_pack": True, "qbo_qty_on_hand": -2},
            ]
        )
        buf = io.StringIO()
        with redirect_stdout(buf):
            exit_code = inventory_catalog_cleanup._run_apply_for_existing_base_pack_variants(
                cfg=fake_cfg,
                plan_df=plan_df,
                qbo_item_rows=qbo_item_rows,
                txn_date="2026-04-28",
                max_products=1,
                dry_run=True,
            )

        self.assertEqual(exit_code, 0)
        out = buf.getvalue()
        self.assertIn("[PLAN] resolve_duplicate_base", out)
        self.assertIn("canonical=13875:'SMIRNOFF ICE DOUBLE BLACK CAN 330ml'", out)
        self.assertIn("[DRY-RUN] canonical_base=13875:'SMIRNOFF ICE DOUBLE BLACK CAN 330ml'", out)
        self.assertIn("SMIRNOFF ICE DOUBLE BLACK  CAN 330ml", out)

    def test_duplicate_base_items_without_unique_canonical_remain_manual_review(self):
        audit = self._audit_df(
            [{"base_name": "WIDGET", "epos_single_units": 1.0, "catalog_issue_type": "multiple_active_base_items"}]
        )
        qbo_rows = pd.DataFrame(
            [
                {"Id": "1", "Name": "WIDGET", "base_name": "WIDGET", "base_name_norm": "widget", "qbo_has_pack": False},
                {"Id": "2", "Name": "widget", "base_name": "WIDGET", "base_name_norm": "widget", "qbo_has_pack": False},
            ]
        )
        plan = inventory_catalog_cleanup.plan_catalog_cleanup(
            company_key="company_a",
            audit_df=audit,
            qbo_item_rows=qbo_rows,
            source_inventory_report="/r.csv",
        )
        row = plan.iloc[0].to_dict()
        self.assertEqual(row["planned_action"], "manual_review_duplicate_base_items")
        self.assertFalse(row["action_eligible"])
        self.assertEqual(row["block_reason"], "duplicate_base_items_multiple_strict_canonical_candidates")

    def test_duplicate_base_items_with_multiple_collapsed_matches_remain_manual_review(self):
        audit = self._audit_df(
            [
                {
                    "base_name": "WIDGET CAN 330ml",
                    "epos_single_units": 1.0,
                    "catalog_issue_type": "multiple_active_base_items",
                }
            ]
        )
        qbo_rows = pd.DataFrame(
            [
                {
                    "Id": "1",
                    "Name": "WIDGET  CAN 330ml",
                    "qbo_name_original": "WIDGET  CAN 330ml",
                    "base_name": "WIDGET CAN 330ml",
                    "base_name_norm": "widget can 330ml",
                    "qbo_has_pack": False,
                },
                {
                    "Id": "2",
                    "Name": "WIDGET   CAN 330ml",
                    "qbo_name_original": "WIDGET   CAN 330ml",
                    "base_name": "WIDGET CAN 330ml",
                    "base_name_norm": "widget can 330ml",
                    "qbo_has_pack": False,
                },
            ]
        )
        plan = inventory_catalog_cleanup.plan_catalog_cleanup(
            company_key="company_a",
            audit_df=audit,
            qbo_item_rows=qbo_rows,
            source_inventory_report="/r.csv",
        )
        row = plan.iloc[0].to_dict()
        self.assertEqual(row["planned_action"], "manual_review_duplicate_base_items")
        self.assertFalse(row["action_eligible"])
        self.assertEqual(row["block_reason"], "duplicate_base_items_multiple_collapsed_canonical_candidates")

    def test_non_inventory_duplicate_remains_manual_review(self):
        audit = self._audit_df(
            [{"base_name": "WIDGET", "epos_single_units": 1.0, "catalog_issue_type": "multiple_active_base_items"}]
        )
        qbo_rows = pd.DataFrame(
            [
                {"Id": "1", "Name": "WIDGET", "base_name": "WIDGET", "base_name_norm": "widget", "qbo_has_pack": False, "Type": "Inventory"},
                {"Id": "2", "Name": "widget", "base_name": "WIDGET", "base_name_norm": "widget", "qbo_has_pack": False, "Type": "Service"},
            ]
        )
        plan = inventory_catalog_cleanup.plan_catalog_cleanup(
            company_key="company_a",
            audit_df=audit,
            qbo_item_rows=qbo_rows,
            source_inventory_report="/r.csv",
        )
        row = plan.iloc[0].to_dict()
        self.assertEqual(row["planned_action"], "manual_review_duplicate_base_items")
        self.assertFalse(row["action_eligible"])

    def test_snapshot_path_is_printed_when_loading_from_report(self):
        fake_cfg = mock.Mock(
            company_key="company_a",
            display_name="ACME",
            qbo_environment="production",
            realm_id="REALM123",
        )
        audit_df = pd.DataFrame(
            [
                {"base_name": "A", "epos_single_units": 1.0, "catalog_issue_type": "missing_from_qbo"},
            ]
        )
        import tempfile
        buf = io.StringIO()
        with tempfile.TemporaryDirectory() as td, \
             mock.patch.object(inventory_catalog_cleanup, "load_company_config", return_value=fake_cfg), \
             mock.patch.object(inventory_catalog_cleanup, "ensure_company_runtime_compatible"), \
             mock.patch.object(inventory_catalog_cleanup, "get_available_companies", return_value=["company_a"]), \
             mock.patch.object(inventory_catalog_cleanup, "_read_inventory_report", return_value=audit_df), \
             mock.patch.object(inventory_catalog_cleanup, "_write_csv"), \
             redirect_stdout(buf):
            qbo_path = Path(td) / "qbo.csv"
            qbo_path.write_text("Id,Name,Type,TrackQtyOnHand,QtyOnHand\n", encoding="utf-8")
            # Patch the loader to avoid reading the dummy file.
            with mock.patch.object(inventory_catalog_cleanup, "load_qbo_inventory_item_rows", return_value=pd.DataFrame([{
                "Id": "10", "Name": "A", "base_name": "A", "qbo_has_pack": False, "qbo_qty_on_hand": 1,
            }])):
                exit_code = inventory_catalog_cleanup.main([
                    "--company", "company_a",
                    "--from-report", "/tmp/r.csv",
                    "--qbo-csv", str(qbo_path),
                ])
        self.assertEqual(exit_code, 0)
        self.assertIn("[INFO] QBO snapshot:", buf.getvalue())
        self.assertIn(str(qbo_path), buf.getvalue())

    def test_partial_failure_when_cleanup_fails_after_consolidation(self):
        fake_cfg = mock.Mock(
            company_key="company_a",
            display_name="ACME",
            qbo_environment="production",
            realm_id="REALM123",
            inventory_adjustment_account_id="88",
        )
        audit_df = pd.DataFrame(
            [
                {"base_name": "A", "epos_single_units": 8.0, "catalog_issue_type": "base_with_pack_variants"},
            ]
        )
        qbo_item_rows = pd.DataFrame(
            [
                {"Id": "10", "Name": "A", "base_name": "A", "qbo_has_pack": False, "qbo_qty_on_hand": 1},
                {"Id": "11", "Name": "A*6", "base_name": "A", "qbo_has_pack": True, "qbo_qty_on_hand": 1},
            ]
        )

        def fake_fetch_item(_tm, _realm, item_id):
            return {"Id": item_id, "Name": f"A*6", "SyncToken": "0", "QtyOnHand": 0}

        buf = io.StringIO()
        import tempfile
        with tempfile.TemporaryDirectory() as td, \
             mock.patch.object(inventory_catalog_cleanup, "load_company_config", return_value=fake_cfg), \
             mock.patch.object(inventory_catalog_cleanup, "ensure_company_runtime_compatible"), \
             mock.patch.object(inventory_catalog_cleanup, "get_available_companies", return_value=["company_a"]), \
             mock.patch.object(inventory_catalog_cleanup, "_read_inventory_report", return_value=audit_df), \
             mock.patch.object(inventory_catalog_cleanup, "_default_qbo_snapshot_path", return_value=Path(td) / "qbo.csv"), \
             mock.patch.object(inventory_catalog_cleanup, "load_qbo_inventory_item_rows", return_value=qbo_item_rows), \
             mock.patch.object(inventory_catalog_cleanup, "verify_realm_match"), \
             mock.patch.object(inventory_catalog_cleanup, "TokenManager", return_value=mock.Mock()), \
             mock.patch.object(inventory_catalog_cleanup, "post_inventory_adjustment", return_value={"InventoryAdjustment": {"Id": "1"}}), \
             mock.patch.object(inventory_catalog_cleanup, "_fetch_item_with_sync_token", side_effect=fake_fetch_item), \
             mock.patch.object(inventory_catalog_cleanup, "_post_inactivate", side_effect=RuntimeError("boom")), \
             mock.patch.object(inventory_catalog_cleanup, "mark_qbo_snapshot_stale"), \
             mock.patch.object(inventory_catalog_cleanup, "_write_csv"), \
             redirect_stdout(buf):
            (Path(td) / "qbo.csv").write_text("Id,Name,Type,TrackQtyOnHand,QtyOnHand\n", encoding="utf-8")
            exit_code = inventory_catalog_cleanup.main([
                "--company", "company_a",
                "--from-report", "/tmp/r.csv",
                "--apply",
                "--max-products", "1",
                "--qbo-csv", str(Path(td) / "qbo.csv"),
            ])
        self.assertEqual(exit_code, 1)
        self.assertIn("partial", buf.getvalue().lower())

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
