"""Unit tests for daily totals reconciliation helper behavior."""

from __future__ import annotations

import unittest

from code_scripts.reconciliation.bank_statements import StatementCreditTotal
from code_scripts.reconciliation.transformed_sales import TenderTotals
from code_scripts.scripts.reconciliation.reconcile_sales_to_bank import (
    _build_variance_analysis,
    _dedupe_statement_totals,
)


class TestDailyTotalsHelpers(unittest.TestCase):
    def test_dedupe_statement_totals(self) -> None:
        rows = [
            StatementCreditTotal(
                source_file="a.xlsx",
                sheet_name="S1",
                account_number="4001",
                account_name="Acc",
                statement_date_range="18/02/2026 - 18/02/2026",
                total_credit=1000.0,
                total_debit=0.0,
            ),
            StatementCreditTotal(
                source_file="b.xlsx",
                sheet_name="S1",
                account_number="4001",
                account_name="Acc",
                statement_date_range="18/02/2026 - 18/02/2026",
                total_credit=1000.0,
                total_debit=0.0,
            ),
        ]
        deduped, dropped = _dedupe_statement_totals(rows)
        self.assertEqual(len(deduped), 1)
        self.assertEqual(dropped, 1)

    def test_variance_analysis_cash_match(self) -> None:
        tenders = TenderTotals(
            actual_sales_total=1000.0,
            card_total=0.0,
            transfer_total=0.0,
            card_transfer_combo_total=0.0,
            cash_total=100.0,
            mixed_with_cash_total=50.0,
            other_tender_total=0.0,
        )
        result = _build_variance_analysis(
            epos_total=1000.0,
            bank_total_credits=900.0,
            tender_totals=tenders,
            tolerance=1.0,
        )
        self.assertEqual(result["status"], "LIKELY_CASH_EXPLAINS_VARIANCE")

