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
            "inventory_options": {},
        }
        cleaned.update(overrides)
        return cleaned

    def test_minimum_required_args_emits_auto_download(self):
        """Portal-triggered audits never carry a stock_csv path; we always
        auto-download a fresh EPOS Stock Report."""
        cmd = build_command(self._base_cleaned())
        flat = " ".join(cmd)
        self.assertIn("-m", cmd)
        self.assertIn("code_scripts.inventory_sync", cmd)
        self.assertIn("--company", cmd)
        self.assertIn("company_a", cmd)
        self.assertIn("--auto-download", cmd)
        self.assertNotIn("--stock-csv", cmd)
        # Should NOT include any optional flags we didn't set
        self.assertNotIn("--apply", flat)
        self.assertNotIn("--dry-run", flat)
        self.assertNotIn("--allow-ambiguous", flat)
        self.assertNotIn("--allow-fallback-picks", flat)

    def test_explicit_stock_csv_overrides_auto_download(self):
        """Advanced callers can pre-populate inventory_options['stock_csv']
        to point at an existing CSV; --auto-download is suppressed."""
        cmd = build_command(
            self._base_cleaned(inventory_options={"stock_csv": "/path/to/stock.csv"})
        )
        self.assertIn("--stock-csv", cmd)
        self.assertIn("/path/to/stock.csv", cmd)
        self.assertNotIn("--auto-download", cmd)

    def test_all_optional_flags_propagate(self):
        cleaned = self._base_cleaned(
            inventory_options={
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
        self.assertIn("--auto-download", cmd)
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
        self.assertNotIn("--allow-fallback-picks", cmd)
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
        cleaned = self._base_cleaned(inventory_options={"dry_run": True})
        cmd = build_command(cleaned)
        self.assertIn("--dry-run", cmd)

    def test_category_product_mode_and_cap_become_cli_args(self):
        cmd = build_command(
            self._base_cleaned(
                inventory_options={
                    "categories": ["ALCOHOLS & SPIRITS"],
                    "product_filter": "TROPHY",
                    "dry_run": True,
                    "max_adjustments": 3,
                }
            )
        )
        self.assertIn("--auto-download", cmd)
        self.assertIn("--category", cmd)
        self.assertIn("ALCOHOLS & SPIRITS", cmd)
        self.assertIn("--product", cmd)
        self.assertIn("TROPHY", cmd)
        self.assertIn("--dry-run", cmd)
        self.assertIn("--max-adjustments", cmd)
        self.assertIn("3", cmd)

    def test_build_command_for_job_uses_inventory_options(self):
        job = RunJob.objects.create(
            scope=RunJob.SCOPE_INVENTORY_SYNC,
            company_key="company_a",
            inventory_options_json={
                "apply": True,
                "max_qty_delta": 50,
            },
        )
        cmd = build_command_for_job(job)
        self.assertIn("--auto-download", cmd)
        self.assertIn("--apply", cmd)
        self.assertIn("--max-qty-delta", cmd)
        self.assertIn("50", cmd)
        self.assertNotIn("--allow-fallback-picks", cmd)

    def test_missing_company_key_raises(self):
        with self.assertRaises(ValueError):
            build_command(self._base_cleaned(company_key=""))
