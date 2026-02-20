"""Unit tests for matcher behavior and unmatched diagnostics."""

from __future__ import annotations

import unittest
from datetime import datetime, timedelta

from code_scripts.reconciliation.matcher import run_matching
from code_scripts.reconciliation.models import (
    BankTxn,
    MatchStatus,
    Receipt,
    TenderKind,
)


def _receipt(at: datetime, expected_credit: float = 995.0) -> Receipt:
    return Receipt(
        receipt_id="r1",
        receipt_datetime=at,
        location_name="Store",
        device_name="Till1",
        staff="User",
        tender="Transfer",
        tender_kind=TenderKind.ELECTRONIC,
        line_count=1,
        gross_amount=1000.0,
        service_fee=5.0,
        expected_credit=expected_credit,
        expected_emtl=0.0,
    )


def _credit(at: datetime, amount: float = 995.0) -> BankTxn:
    return BankTxn(
        account_number="4000",
        posted_at=at,
        narration="PURCHASE FOR TEST",
        reference="ref",
        debit=0.0,
        credit=amount,
    )


def _transfer_credit(at: datetime, amount: float = 995.0) -> BankTxn:
    return BankTxn(
        account_number="4000",
        posted_at=at,
        narration="Transfer from Customer",
        reference="trf-ref",
        debit=0.0,
        credit=amount,
    )


class TestRunMatchingDiagnostics(unittest.TestCase):
    def test_no_bank_credits_reason(self) -> None:
        receipt = _receipt(datetime(2026, 2, 15, 12, 0, 0))
        matches, _ = run_matching([receipt], [], amount_tolerance=1.0, time_window_mins=180)
        self.assertEqual(len(matches), 1)
        self.assertEqual(matches[0].status, MatchStatus.UNMATCHED_NO_BANK_CREDIT)
        self.assertEqual(matches[0].unmatched_reason, "NO_BANK_CREDITS_AFTER_FILTER")

    def test_time_window_mismatch_reason(self) -> None:
        at = datetime(2026, 2, 15, 12, 0, 0)
        receipt = _receipt(at)
        far_late_credit = _credit(at + timedelta(hours=6), amount=995.0)
        matches, _ = run_matching(
            [receipt],
            [far_late_credit],
            amount_tolerance=1.0,
            time_window_mins=180,
        )
        self.assertEqual(matches[0].status, MatchStatus.UNMATCHED_NO_BANK_CREDIT)
        self.assertEqual(matches[0].unmatched_reason, "TIME_WINDOW_MISMATCH")
        self.assertEqual(matches[0].closest_amount_diff, 0.0)

    def test_amount_time_combination_mismatch_reason(self) -> None:
        """
        Amount and time candidates exist separately, but no single credit satisfies both.
        """
        at = datetime(2026, 2, 15, 12, 0, 0)
        receipt = _receipt(at, expected_credit=995.0)
        amount_only = _credit(at - timedelta(minutes=5), amount=995.0)  # wrong side of time window
        time_only = _credit(at + timedelta(minutes=5), amount=996.5)  # outside amount tolerance
        matches, _ = run_matching(
            [receipt],
            [amount_only, time_only],
            amount_tolerance=1.0,
            time_window_mins=180,
            allow_negative_time_diff_mins=0,
        )
        self.assertEqual(matches[0].status, MatchStatus.UNMATCHED_NO_BANK_CREDIT)
        self.assertEqual(matches[0].unmatched_reason, "AMOUNT_TIME_COMBINATION_MISMATCH")

    def test_allow_negative_time_diff_can_match(self) -> None:
        at = datetime(2026, 2, 15, 12, 0, 0)
        receipt = _receipt(at)
        slightly_earlier_credit = _credit(at - timedelta(minutes=5), amount=995.0)

        matches_default, _ = run_matching(
            [receipt],
            [slightly_earlier_credit],
            amount_tolerance=1.0,
            time_window_mins=180,
            allow_negative_time_diff_mins=0,
        )
        self.assertEqual(matches_default[0].status, MatchStatus.UNMATCHED_NO_BANK_CREDIT)

        matches_relaxed, _ = run_matching(
            [receipt],
            [slightly_earlier_credit],
            amount_tolerance=1.0,
            time_window_mins=180,
            allow_negative_time_diff_mins=10,
        )
        self.assertEqual(matches_relaxed[0].status, MatchStatus.MATCHED)

    def test_relaxed_pass_can_match_with_looser_tolerance(self) -> None:
        at = datetime(2026, 2, 15, 12, 0, 0)
        receipt = _receipt(at, expected_credit=995.0)
        off_by_two = _credit(at + timedelta(minutes=2), amount=997.0)

        strict_only, _ = run_matching(
            [receipt],
            [off_by_two],
            amount_tolerance=1.0,
            time_window_mins=180,
            enable_relaxed_pass=False,
        )
        self.assertEqual(strict_only[0].status, MatchStatus.UNMATCHED_NO_BANK_CREDIT)

        two_pass, _ = run_matching(
            [receipt],
            [off_by_two],
            amount_tolerance=1.0,
            time_window_mins=180,
            enable_relaxed_pass=True,
            relaxed_amount_tolerance=3.0,
            relaxed_time_window_mins=180,
            relaxed_allow_negative_time_diff_mins=0,
            enable_fee_learning=False,
            enable_bundle_matching=False,
        )
        self.assertEqual(two_pass[0].status, MatchStatus.REVIEW_RELAXED_MATCH)
        self.assertEqual(two_pass[0].confidence_tier, "RELAXED")

    def test_bundle_pass_matches_group(self) -> None:
        t0 = datetime(2026, 2, 15, 12, 0, 0)
        r1 = _receipt(t0, expected_credit=500.0)
        r2 = _receipt(t0 + timedelta(minutes=1), expected_credit=600.0)
        r1.receipt_id = "r1"
        r2.receipt_id = "r2"
        b = _credit(t0 + timedelta(minutes=5), amount=1100.0)

        matches, _ = run_matching(
            [r1, r2],
            [b],
            amount_tolerance=1.0,
            time_window_mins=180,
            enable_relaxed_pass=False,
            enable_fee_learning=False,
            enable_bundle_matching=True,
            bundle_max_size=2,
            bundle_amount_tolerance=1.0,
            bundle_time_window_mins=180,
            bundle_allow_negative_time_diff_mins=10,
        )
        statuses = {m.receipt.receipt_id: m.status for m in matches}
        self.assertEqual(statuses["r1"], MatchStatus.REVIEW_BUNDLED_MATCH)
        self.assertEqual(statuses["r2"], MatchStatus.REVIEW_BUNDLED_MATCH)
        bundle_ids = {m.bundle_id for m in matches}
        self.assertEqual(len(bundle_ids), 1)

    def test_transfer_like_strict_match_can_be_review_status(self) -> None:
        at = datetime(2026, 2, 15, 12, 0, 0)
        receipt = _receipt(at, expected_credit=995.0)
        trf_credit = _transfer_credit(at + timedelta(minutes=2), amount=995.0)

        review_mode, _ = run_matching(
            [receipt],
            [trf_credit],
            amount_tolerance=1.0,
            time_window_mins=180,
            transfer_matches_are_review=True,
            enable_relaxed_pass=False,
            enable_bundle_matching=False,
        )
        self.assertEqual(review_mode[0].status, MatchStatus.REVIEW_TRANSFER_MATCH)
        self.assertEqual(review_mode[0].confidence_tier, "STRICT_REVIEW")

        strict_mode, _ = run_matching(
            [receipt],
            [trf_credit],
            amount_tolerance=1.0,
            time_window_mins=180,
            transfer_matches_are_review=False,
            enable_relaxed_pass=False,
            enable_bundle_matching=False,
        )
        self.assertEqual(strict_mode[0].status, MatchStatus.MATCHED)

    def test_strict_candidate_diagnostics_are_reported(self) -> None:
        at = datetime(2026, 2, 15, 12, 0, 0)
        r1 = _receipt(at, expected_credit=995.0)
        r2 = _receipt(at + timedelta(minutes=1), expected_credit=1492.5)
        r1.receipt_id = "r1"
        r2.receipt_id = "r2"
        c1 = _credit(at + timedelta(minutes=2), amount=995.0)
        c2 = _credit(at + timedelta(minutes=3), amount=1492.5)
        c3 = _credit(at + timedelta(minutes=4), amount=1492.5)
        diagnostics: dict[str, float] = {}

        run_matching(
            [r1, r2],
            [c1, c2, c3],
            amount_tolerance=1.0,
            time_window_mins=180,
            diagnostics=diagnostics,
        )
        self.assertEqual(diagnostics.get("candidate_pairs_strict_count"), 3)
        self.assertEqual(diagnostics.get("avg_candidates_per_receipt_strict"), 1.5)
        self.assertEqual(diagnostics.get("strict_receipt_count"), 2)

    def test_strict_match_is_not_overridden_by_relaxed_or_bundle(self) -> None:
        t0 = datetime(2026, 2, 15, 12, 0, 0)
        r_strict = _receipt(t0, expected_credit=995.0)
        r_unmatched = _receipt(t0 + timedelta(minutes=1), expected_credit=500.0)
        r_strict.receipt_id = "strict"
        r_unmatched.receipt_id = "other"

        b_strict = _credit(t0 + timedelta(minutes=1), amount=995.0)
        b_bundle = _credit(t0 + timedelta(minutes=4), amount=500.0)
        b_bundle.reference = "bundle-ref"

        matches, _ = run_matching(
            [r_strict, r_unmatched],
            [b_strict, b_bundle],
            amount_tolerance=1.0,
            time_window_mins=180,
            enable_relaxed_pass=True,
            relaxed_amount_tolerance=3.0,
            enable_bundle_matching=True,
            bundle_max_size=2,
        )
        by_id = {m.receipt.receipt_id: m for m in matches}
        self.assertEqual(by_id["strict"].status, MatchStatus.MATCHED)
        self.assertEqual(by_id["strict"].bank_txn.reference, b_strict.reference)

    def test_used_bank_credit_cannot_be_reused(self) -> None:
        t0 = datetime(2026, 2, 15, 12, 0, 0)
        r1 = _receipt(t0, expected_credit=995.0)
        r2 = _receipt(t0 + timedelta(minutes=1), expected_credit=995.0)
        r3 = _receipt(t0 + timedelta(minutes=2), expected_credit=497.5)
        r4 = _receipt(t0 + timedelta(minutes=3), expected_credit=497.5)
        r1.receipt_id = "r1"
        r2.receipt_id = "r2"
        r3.receipt_id = "r3"
        r4.receipt_id = "r4"

        # b1 should be consumed by strict for r1 only.
        b1 = _credit(t0 + timedelta(minutes=1), amount=995.0)
        b1.reference = "b1"
        # b2 is distinct in time so r1 has a unique best strict candidate (b1).
        b2 = _credit(t0 + timedelta(minutes=20), amount=995.0)
        b2.reference = "b2"

        matches, _ = run_matching(
            [r1, r2, r3, r4],
            [b1, b2],
            amount_tolerance=1.0,
            time_window_mins=180,
            enable_relaxed_pass=True,
            relaxed_amount_tolerance=1.0,
            enable_bundle_matching=True,
            bundle_max_size=2,
            bundle_amount_tolerance=1.0,
        )
        by_id = {m.receipt.receipt_id: m for m in matches}
        self.assertEqual(by_id["r1"].status, MatchStatus.MATCHED)
        self.assertEqual(by_id["r1"].bank_txn.reference, "b1")

        refs = [m.bank_txn.reference for m in matches if m.bank_txn is not None]
        self.assertEqual(refs.count("b1"), 1)

        # r2 must not reuse b1 and remains unmatched/reviewed.
        self.assertNotEqual(by_id["r2"].bank_txn.reference if by_id["r2"].bank_txn else "", "b1")

        # Bundle rows must not use b1 either.
        bundle_rows = [m for m in matches if m.status == MatchStatus.REVIEW_BUNDLED_MATCH]
        for m in bundle_rows:
            self.assertNotEqual(m.bank_txn.reference if m.bank_txn else "", "b1")
