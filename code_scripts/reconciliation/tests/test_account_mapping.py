"""Unit tests for account mapping loaders."""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from code_scripts.reconciliation.account_mapping import (
    load_account_mapping,
    load_account_mapping_details,
)


class TestAccountMappingLoaders(unittest.TestCase):
    def test_load_account_mapping_details_uses_account_name_column(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "account_mapping.csv"
            path.write_text(
                "Monipoint Store Account Name,Monipoint Online Account,"
                "QBO / Bank Statement Account Number,Monipoint Account Name,STATUS\n"
                "5024249823,4000850527,4000850527,AKPONORA VENTURES LTD - NORA MINI MART 3,\n",
                encoding="utf-8",
            )

            details = load_account_mapping_details(path)
            self.assertIn("4000850527", details)
            row = details["4000850527"]
            self.assertEqual(row["moniepoint_store_account"], "5024249823")
            self.assertEqual(row["account_name"], "AKPONORA VENTURES LTD - NORA MINI MART 3")

    def test_load_account_mapping_simple(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "account_mapping.csv"
            path.write_text(
                "QBO / Bank Statement Account Number,Monipoint Account Name\n"
                "4000700275,AKPONORA VENTURES LTD - NORA MINI MART\n",
                encoding="utf-8",
            )

            mapping = load_account_mapping(path)
            self.assertEqual(
                mapping.get("4000700275"),
                "AKPONORA VENTURES LTD - NORA MINI MART",
            )

