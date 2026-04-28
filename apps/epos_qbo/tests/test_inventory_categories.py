from __future__ import annotations

import tempfile
import os
from pathlib import Path
from unittest import mock

from django.test import TestCase

from apps.epos_qbo.forms import InventoryTriggerForm
from apps.epos_qbo.models import CompanyConfigRecord
from apps.epos_qbo.services import inventory_categories


class InventoryCategoryLoadingTests(TestCase):
    def test_loads_company_categories_from_default_mapping_csv(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            mapping_dir = root / "mappings"
            mapping_dir.mkdir()
            (mapping_dir / "company_a_product_mapping.csv").write_text(
                "Product,Categories\n"
                "TROPHY,ALCOHOLS & SPIRITS\n"
                "WIDGET,DRINKS & BEVERAGES\n"
                "OTHER,ALCOHOLS & SPIRITS\n",
                encoding="utf-8",
            )

            with mock.patch.object(inventory_categories, "REPO_CODE_SCRIPTS_DIR", root):
                categories = inventory_categories.load_inventory_categories_for_company(
                    "company_a",
                    {},
                )

        self.assertEqual(categories, ["ALCOHOLS & SPIRITS", "DRINKS & BEVERAGES"])

    def test_configured_mapping_path_takes_precedence(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            mapping_dir = root / "mappings"
            mapping_dir.mkdir()
            (mapping_dir / "company_a_product_mapping.csv").write_text(
                "Product,Categories\nTROPHY,Default Category\n",
                encoding="utf-8",
            )
            custom = root / "custom.csv"
            custom.write_text(
                "Product,Category\nTROPHY,Configured Category\n",
                encoding="utf-8",
            )

            with mock.patch.object(inventory_categories, "REPO_CODE_SCRIPTS_DIR", root):
                categories = inventory_categories.load_inventory_categories_for_company(
                    "company_a",
                    {"inventory": {"product_mapping_file": "custom.csv"}},
                )

        self.assertEqual(categories, ["Configured Category"])

    def test_bad_configured_mapping_missing_category_column_falls_back_to_default_mapping(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            mapping_dir = root / "mappings"
            mapping_dir.mkdir()
            (mapping_dir / "company_a_product_mapping.csv").write_text(
                "Product,Categories\nTROPHY,Default Category\n",
                encoding="utf-8",
            )
            custom = root / "custom.csv"
            custom.write_text(
                "Product,NotACategory\nTROPHY,Nope\n",
                encoding="utf-8",
            )

            with mock.patch.object(inventory_categories, "REPO_CODE_SCRIPTS_DIR", root):
                categories = inventory_categories.load_inventory_categories_for_company(
                    "company_a",
                    {"inventory": {"product_mapping_file": "custom.csv"}},
                )

        self.assertEqual(categories, ["Default Category"])

    def test_bad_configured_mapping_with_no_categories_falls_back_to_default_mapping(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            mapping_dir = root / "mappings"
            mapping_dir.mkdir()
            (mapping_dir / "company_a_product_mapping.csv").write_text(
                "Product,Categories\nTROPHY,Default Category\n",
                encoding="utf-8",
            )
            custom = root / "custom.csv"
            custom.write_text(
                "Product,Category\nTROPHY,\n",
                encoding="utf-8",
            )

            with mock.patch.object(inventory_categories, "REPO_CODE_SCRIPTS_DIR", root):
                categories = inventory_categories.load_inventory_categories_for_company(
                    "company_a",
                    {"inventory": {"product_mapping_file": "custom.csv"}},
                )

        self.assertEqual(categories, ["Default Category"])

    def test_mapping_candidates_does_not_duplicate_default_path(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            default_path = root / "mappings" / "company_a_product_mapping.csv"
            default_path.parent.mkdir()

            with mock.patch.object(inventory_categories, "REPO_CODE_SCRIPTS_DIR", root):
                candidates = inventory_categories._mapping_candidates(
                    "company_a",
                    {"inventory": {"product_mapping_file": "mappings/company_a_product_mapping.csv"}},
                )

        self.assertEqual(candidates, [default_path])

    def test_falls_back_to_latest_company_stock_report(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            day_old = root / "2026-04-27"
            day_new = root / "2026-04-28"
            day_old.mkdir()
            day_new.mkdir()
            old_path = day_old / "company_a_StockReport_1000.csv"
            new_path = day_new / "company_a_StockReport_1100.csv"
            old_path.write_text("Name,CategoryName\nWidget,Old Category\n", encoding="utf-8")
            new_path.write_text("Name,CategoryName\nWidget,New Category\n", encoding="utf-8")
            os.utime(old_path, (100, 100))
            os.utime(new_path, (200, 200))

            with mock.patch.object(inventory_categories, "REPO_CODE_SCRIPTS_DIR", root / "repo"), \
                 mock.patch.object(inventory_categories, "stock_exports_dir", return_value=day_new):
                categories = inventory_categories.load_inventory_categories_for_company(
                    "company_a",
                    {},
                )

        self.assertEqual(categories, ["New Category"])

    def test_categories_by_company_is_company_specific(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            mapping_dir = root / "mappings"
            mapping_dir.mkdir()
            (mapping_dir / "company_a_product_mapping.csv").write_text(
                "Product,Categories\nA,Company A Category\n",
                encoding="utf-8",
            )
            (mapping_dir / "company_b_product_mapping.csv").write_text(
                "Product,Categories\nB,Company B Category\n",
                encoding="utf-8",
            )
            company_a = CompanyConfigRecord(company_key="company_a", config_json={})
            company_b = CompanyConfigRecord(company_key="company_b", config_json={})

            with mock.patch.object(inventory_categories, "REPO_CODE_SCRIPTS_DIR", root):
                by_company = inventory_categories.load_inventory_categories_by_company(
                    [company_a, company_b],
                )

        self.assertEqual(by_company["company_a"], ["Company A Category"])
        self.assertEqual(by_company["company_b"], ["Company B Category"])


class InventoryTriggerFormTests(TestCase):
    def test_pipeline_caps_default(self):
        form = InventoryTriggerForm(data={"company_key": "company_a"})
        self.assertTrue(form.is_valid(), form.errors.as_text())
        self.assertEqual(form.cleaned_data["max_catalog_fixes"], 5)
        self.assertEqual(form.cleaned_data["max_quantity_adjustments"], 10)

    def test_pipeline_accepts_optional_scope_and_caps(self):
        form = InventoryTriggerForm(
            data={
                "company_key": "company_a",
                "category": " ALCOHOLS & SPIRITS ",
                "product_filter": " Trophy ",
                "max_catalog_fixes": "3",
                "max_quantity_adjustments": "8",
            }
        )
        self.assertTrue(form.is_valid(), form.errors.as_text())
        self.assertEqual(form.cleaned_data["category"], "ALCOHOLS & SPIRITS")
        self.assertEqual(form.cleaned_data["product_filter"], "Trophy")
        self.assertEqual(form.cleaned_data["max_catalog_fixes"], 3)
        self.assertEqual(form.cleaned_data["max_quantity_adjustments"], 8)

    def test_zero_caps_are_allowed(self):
        form = InventoryTriggerForm(
            data={
                "company_key": "company_a",
                "max_catalog_fixes": "0",
                "max_quantity_adjustments": "0",
            }
        )
        self.assertTrue(form.is_valid(), form.errors.as_text())
        self.assertEqual(form.cleaned_data["max_catalog_fixes"], 0)
        self.assertEqual(form.cleaned_data["max_quantity_adjustments"], 0)
