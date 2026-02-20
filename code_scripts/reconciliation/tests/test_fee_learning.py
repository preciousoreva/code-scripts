"""Unit tests for learned fee-adjustment model."""

from __future__ import annotations

import unittest
from datetime import datetime

from code_scripts.reconciliation.fee_learning import build_fee_adjustment_model
from code_scripts.reconciliation.models import BankTxn, Match, MatchStatus, Receipt, TenderKind


def _receipt(receipt_id: str, expected_credit: float = 995.0) -> Receipt:
    return Receipt(
        receipt_id=receipt_id,
        receipt_datetime=datetime(2026, 2, 15, 12, 0, 0),
        location_name="Store",
        device_name="Till",
        staff="User",
        tender="Transfer",
        tender_kind=TenderKind.ELECTRONIC,
        line_count=1,
        gross_amount=1000.0,
        service_fee=5.0,
        expected_credit=expected_credit,
        expected_emtl=0.0,
    )


def _bank(account: str, credit: float, narration: str = "Transfer from Customer") -> BankTxn:
    return BankTxn(
        account_number=account,
        posted_at=datetime(2026, 2, 15, 12, 1, 0),
        narration=narration,
        reference="ref",
        debit=0.0,
        credit=credit,
    )


class TestFeeLearning(unittest.TestCase):
    def test_learns_median_offset_for_channel(self) -> None:
        # All strict matches imply +3.5 offset.
        matches = []
        for i, credit in enumerate((998.5, 998.5, 998.5), start=1):
            r = _receipt(f"r{i}", expected_credit=995.0)
            b = _bank("4000700275", credit=credit)
            matches.append(
                Match(
                    receipt=r,
                    bank_txn=b,
                    status=MatchStatus.MATCHED,
                    amount_diff=0.0,
                )
            )

        model = build_fee_adjustment_model(matches, min_samples=2)
        self.assertTrue(model.enabled)

        probe_receipt = _receipt("probe", expected_credit=1492.5)
        offset = model.offset_for(
            probe_receipt,
            account_number="4000700275",
            narration="Transfer from Somebody",
        )
        self.assertAlmostEqual(offset, 3.5, places=2)

