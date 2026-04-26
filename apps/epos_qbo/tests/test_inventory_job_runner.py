from __future__ import annotations

from datetime import date

from django.test import TestCase

from apps.epos_qbo.models import RunJob
from apps.epos_qbo.services.job_runner import build_command, build_command_for_job


class InventoryBuildCommandTests(TestCase):
    def _base_cleaned(self, **overrides) -> dict:
        cleaned = {
            "scope": RunJob.SCOPE_INVENTORY_SYNC,
            "company_key": "company_a",
            "date_mode": "yesterday",
            "stock_csv": "/path/to/stock.csv",
            "inventory_options": {},
        }
        cleaned.update(overrides)
        return cleaned

    def test_minimum_required_args(self):
        cmd = build_command(self._base_cleaned())
        # Flatten to a string for easy assertions
        flat = " ".join(cmd)
        self.assertIn("-m", cmd)
        self.assertIn("code_scripts.inventory_sync", cmd)
        self.assertIn("--company", cmd)
        self.assertIn("company_a", cmd)
        self.assertIn("--stock-csv", cmd)
        self.assertIn("/path/to/stock.csv", cmd)
        # Should NOT include any optional flags we didn't set
        self.assertNotIn("--apply", flat)
        self.assertNotIn("--dry-run", flat)
        self.assertNotIn("--allow-ambiguous", flat)

    def test_all_optional_flags_propagate(self):
        cleaned = self._base_cleaned(
            inventory_options={
                "stock_csv": "/p/stock.csv",
                "qbo_csv": "/p/qbo.csv",
                "product_filter": "WIDGET",
                "categories": ["Beverages"],
                "tolerance": 0.5,
                "apply": True,
                "allow_ambiguous": True,
                "max_adjustments": 5,
                "max_qty_delta": 100,
                "adjust_account_id": "99",
                "txn_date": "2026-04-14",
            }
        )
        cmd = build_command(cleaned)
        self.assertIn("--qbo-csv", cmd)
        self.assertIn("/p/qbo.csv", cmd)
        self.assertIn("--product", cmd)
        self.assertIn("WIDGET", cmd)
        self.assertIn("--category", cmd)
        self.assertIn("Beverages", cmd)
        self.assertIn("--tolerance", cmd)
        self.assertIn("0.5", cmd)
        self.assertIn("--apply", cmd)
        self.assertIn("--allow-ambiguous", cmd)
        self.assertIn("--max-adjustments", cmd)
        self.assertIn("5", cmd)
        self.assertIn("--max-qty-delta", cmd)
        self.assertIn("100", cmd)
        self.assertIn("--adjust-account-id", cmd)
        self.assertIn("99", cmd)
        self.assertIn("--txn-date", cmd)
        self.assertIn("2026-04-14", cmd)

    def test_dry_run_mutually_exclusive_with_apply_is_not_enforced_here(self):
        """build_command trusts the caller; the form validates the combination."""
        cleaned = self._base_cleaned(inventory_options={"stock_csv": "/p/s.csv", "dry_run": True})
        cmd = build_command(cleaned)
        self.assertIn("--dry-run", cmd)

    def test_build_command_for_job_uses_inventory_options(self):
        job = RunJob.objects.create(
            scope=RunJob.SCOPE_INVENTORY_SYNC,
            company_key="company_a",
            inventory_options_json={
                "stock_csv": "/p/s.csv",
                "apply": True,
                "max_qty_delta": 50,
            },
        )
        cmd = build_command_for_job(job)
        self.assertIn("--stock-csv", cmd)
        self.assertIn("/p/s.csv", cmd)
        self.assertIn("--apply", cmd)
        self.assertIn("--max-qty-delta", cmd)
        self.assertIn("50", cmd)

    def test_missing_stock_csv_raises(self):
        with self.assertRaises(ValueError):
            build_command(self._base_cleaned(stock_csv="", inventory_options={}))
