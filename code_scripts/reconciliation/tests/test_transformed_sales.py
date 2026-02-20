"""Unit tests for transformed sales tender summaries."""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import pandas as pd

from code_scripts.reconciliation.transformed_sales import (
    classify_tender_bucket,
    summarize_transformed_sales,
)


class TestTenderClassification(unittest.TestCase):
    def test_classify_tender_bucket(self) -> None:
        self.assertEqual(classify_tender_bucket("Card"), "card")
        self.assertEqual(classify_tender_bucket("Transfer"), "transfer")
        self.assertEqual(classify_tender_bucket("Cash"), "cash")
        self.assertEqual(classify_tender_bucket("Card/Transfer"), "card_transfer_combo")
        self.assertEqual(classify_tender_bucket("Cash/Transfer"), "mixed_with_cash")
        self.assertEqual(classify_tender_bucket("Wallet"), "other")


class TestSummarizeTransformedSales(unittest.TestCase):
    def test_summarize_totals(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "single_sales_receipts_test_2026-02-18.csv"
            df = pd.DataFrame(
                [
                    {"Memo": "Card", "TOTAL Sales": 1000.0},
                    {"Memo": "Transfer", "TOTAL Sales": 2000.0},
                    {"Memo": "Card/Transfer", "TOTAL Sales": 300.0},
                    {"Memo": "Cash", "TOTAL Sales": 500.0},
                    {"Memo": "Cash/Transfer", "TOTAL Sales": 250.0},
                    {"Memo": "Unknown", "TOTAL Sales": 50.0},
                ]
            )
            df.to_csv(path, index=False)

            totals = summarize_transformed_sales(path)

            self.assertAlmostEqual(totals.actual_sales_total, 4100.0, places=2)
            self.assertAlmostEqual(totals.card_total, 1000.0, places=2)
            self.assertAlmostEqual(totals.transfer_total, 2000.0, places=2)
            self.assertAlmostEqual(totals.card_transfer_combo_total, 300.0, places=2)
            self.assertAlmostEqual(totals.cash_total, 500.0, places=2)
            self.assertAlmostEqual(totals.mixed_with_cash_total, 250.0, places=2)
            self.assertAlmostEqual(totals.other_tender_total, 50.0, places=2)
            self.assertAlmostEqual(totals.electronic_total, 3300.0, places=2)
            self.assertAlmostEqual(totals.potential_cash_total, 750.0, places=2)

