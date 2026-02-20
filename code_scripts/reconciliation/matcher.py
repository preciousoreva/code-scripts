"""Match EPOS receipts to bank credits (strict, relaxed, and bundled passes)."""

from __future__ import annotations

from dataclasses import dataclass
from itertools import combinations
from typing import Any, Dict, List, Optional, Set

from code_scripts.reconciliation.bank_statements import (
    is_purchase_narration,
    is_transfer_like_narration,
)
from code_scripts.reconciliation.fee_learning import FeeAdjustmentModel, build_fee_adjustment_model
from code_scripts.reconciliation.models import (
    BankTxn,
    Match,
    MatchStatus,
    Receipt,
    TenderKind,
)

BankIdentity = tuple[str, str, str, float, float, str]


@dataclass
class Candidate:
    bank: BankTxn
    amount_diff: float
    time_diff_minutes: float
    score: float
    expected_credit_used: float
    fee_adjustment_applied: float


@dataclass
class BundleCandidate:
    receipts: List[Receipt]
    total_expected_credit: float
    total_amount_diff: float
    per_receipt: Dict[str, tuple[float, float, float]]  # receipt_id -> (time_diff_mins, expected_credit_used, fee_adj)
    score: float


def _bank_identity(bank: BankTxn) -> BankIdentity:
    """
    Stable identity for one bank transaction across passes/process boundaries.
    """
    narration = " ".join((bank.narration or "").lower().split())[:64]
    reference = (bank.reference or "").strip()
    posted = bank.posted_at.strftime("%Y-%m-%d %H:%M:%S")
    return (
        str(bank.account_number).strip(),
        posted,
        reference,
        round(float(bank.credit or 0.0), 2),
        round(float(bank.debit or 0.0), 2),
        narration,
    )


def _score_candidate(
    bank: BankTxn,
    time_diff_minutes: float,
    amount_diff: float,
    fee_adjustment_applied: float = 0.0,
) -> float:
    """
    Lower is better.
    """
    score = amount_diff + (abs(time_diff_minutes) / 60.0)
    # Small penalty for larger learned offsets keeps relaxed pass conservative.
    score += abs(fee_adjustment_applied) / 50.0
    if is_purchase_narration(bank.narration):
        score -= 0.1
    return max(0.0, score)


def _strict_match_status_for_bank(
    bank: BankTxn,
    transfer_matches_are_review: bool,
) -> MatchStatus:
    """
    Strict matches on transfer-like narrations should remain review-tier when enabled.
    """
    if (
        transfer_matches_are_review
        and is_transfer_like_narration(bank.narration)
        and not is_purchase_narration(bank.narration)
    ):
        return MatchStatus.REVIEW_TRANSFER_MATCH
    return MatchStatus.MATCHED


def _expected_credit_used(
    receipt: Receipt,
    bank: BankTxn,
    fee_model: Optional[FeeAdjustmentModel] = None,
) -> tuple[float, float]:
    adjustment = 0.0
    if fee_model and fee_model.enabled:
        adjustment = fee_model.offset_for(receipt, bank.account_number, bank.narration)
    expected = receipt.expected_credit + adjustment
    return expected, adjustment


def _candidates_for_receipt(
    receipt: Receipt,
    bank_credits: List[BankTxn],
    amount_tolerance: float,
    time_window_mins: int,
    allow_negative_time_diff_mins: int = 0,
    fee_model: Optional[FeeAdjustmentModel] = None,
) -> List[Candidate]:
    """
    Return candidates within amount tolerance and time window.
    """
    out: List[Candidate] = []
    for b in bank_credits:
        expected_used, fee_adj = _expected_credit_used(receipt, b, fee_model=fee_model)
        amount_diff = abs(b.credit - expected_used)
        if amount_diff > amount_tolerance:
            continue
        time_diff = (b.posted_at - receipt.receipt_datetime).total_seconds() / 60.0
        if time_diff < -allow_negative_time_diff_mins or time_diff > time_window_mins:
            continue
        out.append(
            Candidate(
                bank=b,
                amount_diff=amount_diff,
                time_diff_minutes=time_diff,
                score=_score_candidate(
                    b,
                    time_diff_minutes=time_diff,
                    amount_diff=amount_diff,
                    fee_adjustment_applied=fee_adj,
                ),
                expected_credit_used=expected_used,
                fee_adjustment_applied=fee_adj,
            )
        )
    return out


def _pick_best_candidate(
    candidates: List[Candidate],
    score_tie_threshold: float,
) -> tuple[Optional[Candidate], bool]:
    if not candidates:
        return None, False
    best = min(candidates, key=lambda c: c.score)
    ties = [c for c in candidates if abs(c.score - best.score) < score_tie_threshold]
    if len(ties) > 1:
        return None, True
    return best, False


def _diagnose_unmatched(
    receipt: Receipt,
    bank_credits: List[BankTxn],
    used_bank_keys: Set[BankIdentity],
    amount_tolerance: float,
    time_window_mins: int,
    allow_negative_time_diff_mins: int,
    fee_model: Optional[FeeAdjustmentModel] = None,
) -> tuple[str, Optional[float], Optional[float]]:
    """
    Return (reason, closest_amount_diff, closest_time_diff_minutes) for unmatched electronic receipt.
    """
    if not bank_credits:
        return "NO_BANK_CREDITS_AFTER_FILTER", None, None

    rows: List[tuple[BankTxn, float, float]] = []
    for b in bank_credits:
        expected_used, _ = _expected_credit_used(receipt, b, fee_model=fee_model)
        amount_diff = abs(b.credit - expected_used)
        time_diff = (b.posted_at - receipt.receipt_datetime).total_seconds() / 60.0
        rows.append((b, amount_diff, time_diff))

    closest_amount = min((r[1] for r in rows), default=None)
    closest_time = min((r[2] for r in rows), key=abs, default=None)

    amount_ok = [r for r in rows if r[1] <= amount_tolerance]
    time_ok = [
        r for r in rows
        if -allow_negative_time_diff_mins <= r[2] <= time_window_mins
    ]
    both_ok = [
        r for r in rows
        if (r[1] <= amount_tolerance and -allow_negative_time_diff_mins <= r[2] <= time_window_mins)
    ]
    available_both_ok = [r for r in both_ok if _bank_identity(r[0]) not in used_bank_keys]

    if both_ok and not available_both_ok:
        return "CANDIDATE_ALREADY_MATCHED", closest_amount, closest_time
    if amount_ok and time_ok and not both_ok:
        return "AMOUNT_TIME_COMBINATION_MISMATCH", closest_amount, closest_time
    if not amount_ok and not time_ok:
        return "AMOUNT_AND_TIME_MISMATCH", closest_amount, closest_time
    if not amount_ok:
        return "AMOUNT_MISMATCH", closest_amount, closest_time
    if not time_ok:
        return "TIME_WINDOW_MISMATCH", closest_amount, closest_time
    return "NO_ELIGIBLE_CANDIDATE", closest_amount, closest_time


def _match_from_candidate(
    receipt: Receipt,
    candidate: Candidate,
    status: MatchStatus,
    confidence_tier: str,
    account_name_by_number: Dict[str, str],
) -> Match:
    return Match(
        receipt=receipt,
        bank_txn=candidate.bank,
        status=status,
        time_diff_minutes=candidate.time_diff_minutes,
        amount_diff=candidate.amount_diff,
        account_name=account_name_by_number.get(candidate.bank.account_number, ""),
        candidates_count=1,
        confidence_tier=confidence_tier,
        fee_adjustment_applied=candidate.fee_adjustment_applied,
        expected_credit_used=candidate.expected_credit_used,
    )


def _find_bundle_for_bank(
    bank: BankTxn,
    receipts: List[Receipt],
    fee_model: Optional[FeeAdjustmentModel],
    max_bundle_size: int,
    base_amount_tolerance: float,
    time_window_mins: int,
    allow_negative_time_diff_mins: int,
    max_receipt_span_mins: int = 60,
    max_receipt_candidates: int = 12,
) -> Optional[BundleCandidate]:
    """
    Find best 2..N receipt subset for one bank credit.
    """
    entries: List[tuple[Receipt, float, float, float]] = []  # receipt, time_diff, expected_used, fee_adj
    for r in receipts:
        time_diff = (bank.posted_at - r.receipt_datetime).total_seconds() / 60.0
        if time_diff < -allow_negative_time_diff_mins or time_diff > time_window_mins:
            continue
        expected_used, fee_adj = _expected_credit_used(r, bank, fee_model=fee_model)
        entries.append((r, time_diff, expected_used, fee_adj))

    if len(entries) < 2:
        return None

    entries.sort(key=lambda x: abs(x[1]))
    entries = entries[:max_receipt_candidates]

    best: Optional[BundleCandidate] = None
    max_size = min(max_bundle_size, len(entries))
    for size in range(2, max_size + 1):
        for combo in combinations(entries, size):
            receipt_times = [e[0].receipt_datetime for e in combo]
            time_span_mins = (
                (max(receipt_times) - min(receipt_times)).total_seconds() / 60.0
                if receipt_times else 0.0
            )
            if time_span_mins > max_receipt_span_mins:
                continue
            total_expected = sum(e[2] for e in combo)
            total_diff = abs(bank.credit - total_expected)
            # Tighter tolerance growth than linear; cap growth for larger bundle sizes.
            scaled_tolerance = min(
                max(base_amount_tolerance, base_amount_tolerance + (0.5 * (size - 1))),
                max(5.0, base_amount_tolerance),
            )
            if total_diff > scaled_tolerance:
                continue
            avg_abs_time = sum(abs(e[1]) for e in combo) / size
            score = total_diff + (avg_abs_time / 60.0) + (0.02 * size)
            per_receipt = {
                e[0].receipt_id: (e[1], e[2], e[3]) for e in combo
            }
            candidate = BundleCandidate(
                receipts=[e[0] for e in combo],
                total_expected_credit=total_expected,
                total_amount_diff=total_diff,
                per_receipt=per_receipt,
                score=score,
            )
            if best is None or candidate.score < best.score:
                best = candidate
    return best


def run_matching(
    receipts: List[Receipt],
    bank_credits: List[BankTxn],
    amount_tolerance: float = 1.0,
    time_window_mins: int = 180,
    allow_negative_time_diff_mins: int = 0,
    account_name_by_number: Optional[Dict[str, str]] = None,
    enable_relaxed_pass: bool = False,
    relaxed_amount_tolerance: Optional[float] = None,
    relaxed_time_window_mins: Optional[int] = None,
    relaxed_allow_negative_time_diff_mins: Optional[int] = None,
    enable_fee_learning: bool = True,
    fee_learning_min_samples: int = 5,
    enable_bundle_matching: bool = False,
    bundle_max_size: int = 3,
    bundle_amount_tolerance: Optional[float] = None,
    bundle_time_window_mins: Optional[int] = None,
    bundle_allow_negative_time_diff_mins: Optional[int] = None,
    bundle_max_receipt_span_mins: int = 60,
    transfer_matches_are_review: bool = True,
    diagnostics: Optional[Dict[str, Any]] = None,
) -> tuple[List[Match], List[BankTxn]]:
    """
    Multi-stage matching:
    1) strict 1:1 MATCHED (or REVIEW_TRANSFER_MATCH for transfer-like narrations when enabled)
    2) optional relaxed 1:1 REVIEW_RELAXED_MATCH (uses learned fee offsets)
    3) optional bundled 1:many REVIEW_BUNDLED_MATCH
    """
    account_name_by_number = account_name_by_number or {}
    bank_list = list(bank_credits)
    used_bank_keys: Set[BankIdentity] = set()
    sorted_receipts = sorted(receipts, key=lambda r: r.receipt_datetime)
    matches_by_receipt: Dict[str, Match] = {}
    electronic_receipts: List[Receipt] = []
    strict_candidate_pairs_count = 0

    # Pre-classify non-electronic receipts.
    for receipt in sorted_receipts:
        if receipt.tender_kind == TenderKind.CASH:
            matches_by_receipt[receipt.receipt_id] = Match(
                receipt=receipt,
                status=MatchStatus.UNMATCHED_CASH,
                unmatched_reason="TENDER_IS_CASH",
            )
            continue
        if receipt.tender_kind == TenderKind.MIXED:
            matches_by_receipt[receipt.receipt_id] = Match(
                receipt=receipt,
                status=MatchStatus.REVIEW_MIXED_TENDER,
                unmatched_reason="MIXED_TENDER_REVIEW",
            )
            continue
        if receipt.collision:
            matches_by_receipt[receipt.receipt_id] = Match(
                receipt=receipt,
                status=MatchStatus.REVIEW_COLLISION,
                unmatched_reason="COLLISION_REVIEW",
            )
            continue
        electronic_receipts.append(receipt)

    # Pass 1: strict matching.
    for receipt in electronic_receipts:
        candidates = _candidates_for_receipt(
            receipt,
            bank_list,
            amount_tolerance=amount_tolerance,
            time_window_mins=time_window_mins,
            allow_negative_time_diff_mins=allow_negative_time_diff_mins,
            fee_model=None,
        )
        strict_candidate_pairs_count += len(candidates)
        available = [c for c in candidates if _bank_identity(c.bank) not in used_bank_keys]
        best, ambiguous = _pick_best_candidate(available, score_tie_threshold=0.01)
        if ambiguous:
            matches_by_receipt[receipt.receipt_id] = Match(
                receipt=receipt,
                status=MatchStatus.REVIEW_MULTIPLE_CANDIDATES,
                candidates_count=len(available),
                unmatched_reason="AMBIGUOUS_MULTIPLE_CANDIDATES",
            )
            continue
        if best is None:
            reason, closest_amount, closest_time = _diagnose_unmatched(
                receipt,
                bank_list,
                used_bank_keys=used_bank_keys,
                amount_tolerance=amount_tolerance,
                time_window_mins=time_window_mins,
                allow_negative_time_diff_mins=allow_negative_time_diff_mins,
                fee_model=None,
            )
            matches_by_receipt[receipt.receipt_id] = Match(
                receipt=receipt,
                status=MatchStatus.UNMATCHED_NO_BANK_CREDIT,
                unmatched_reason=reason,
                closest_amount_diff=closest_amount,
                closest_time_diff_minutes=closest_time,
            )
            continue

        used_bank_keys.add(_bank_identity(best.bank))
        strict_status = _strict_match_status_for_bank(
            bank=best.bank,
            transfer_matches_are_review=transfer_matches_are_review,
        )
        strict_match = _match_from_candidate(
            receipt=receipt,
            candidate=best,
            status=strict_status,
            confidence_tier="STRICT" if strict_status == MatchStatus.MATCHED else "STRICT_REVIEW",
            account_name_by_number=account_name_by_number,
        )
        strict_match.candidates_count = len(available)
        matches_by_receipt[receipt.receipt_id] = strict_match

    # Learn fee adjustments from strict matches.
    fee_model = FeeAdjustmentModel(offsets={}, support={}, global_offset=0.0, enabled=False)
    if enable_fee_learning:
        fee_model = build_fee_adjustment_model(
            list(matches_by_receipt.values()),
            min_samples=max(2, int(fee_learning_min_samples)),
        )

    # Pass 2: relaxed matching.
    if enable_relaxed_pass:
        rel_amount_tol = (
            float(relaxed_amount_tolerance)
            if relaxed_amount_tolerance is not None
            else max(amount_tolerance * 2.0, amount_tolerance + 1.0)
        )
        rel_time_window = (
            int(relaxed_time_window_mins)
            if relaxed_time_window_mins is not None
            else max(int(time_window_mins), 360)
        )
        rel_neg = (
            int(relaxed_allow_negative_time_diff_mins)
            if relaxed_allow_negative_time_diff_mins is not None
            else max(int(allow_negative_time_diff_mins), 10)
        )

        for receipt in electronic_receipts:
            current = matches_by_receipt.get(receipt.receipt_id)
            if current is None or current.status not in {
                MatchStatus.UNMATCHED_NO_BANK_CREDIT,
                MatchStatus.REVIEW_MULTIPLE_CANDIDATES,
            }:
                continue

            candidates = _candidates_for_receipt(
                receipt,
                bank_list,
                amount_tolerance=rel_amount_tol,
                time_window_mins=rel_time_window,
                allow_negative_time_diff_mins=rel_neg,
                fee_model=fee_model,
            )
            available = [c for c in candidates if _bank_identity(c.bank) not in used_bank_keys]
            best, ambiguous = _pick_best_candidate(available, score_tie_threshold=0.05)
            if ambiguous:
                matches_by_receipt[receipt.receipt_id] = Match(
                    receipt=receipt,
                    status=MatchStatus.REVIEW_MULTIPLE_CANDIDATES,
                    candidates_count=len(available),
                    unmatched_reason="AMBIGUOUS_RELAXED_CANDIDATES",
                )
                continue
            if best is None:
                reason, closest_amount, closest_time = _diagnose_unmatched(
                    receipt,
                    bank_list,
                    used_bank_keys=used_bank_keys,
                    amount_tolerance=rel_amount_tol,
                    time_window_mins=rel_time_window,
                    allow_negative_time_diff_mins=rel_neg,
                    fee_model=fee_model,
                )
                current.unmatched_reason = reason
                current.closest_amount_diff = closest_amount
                current.closest_time_diff_minutes = closest_time
                matches_by_receipt[receipt.receipt_id] = current
                continue

            used_bank_keys.add(_bank_identity(best.bank))
            relaxed_match = _match_from_candidate(
                receipt=receipt,
                candidate=best,
                status=MatchStatus.REVIEW_RELAXED_MATCH,
                confidence_tier="RELAXED",
                account_name_by_number=account_name_by_number,
            )
            relaxed_match.candidates_count = len(available)
            matches_by_receipt[receipt.receipt_id] = relaxed_match

    # Pass 3: bundle matching (one bank credit to multiple receipts).
    if enable_bundle_matching:
        bundle_time = (
            int(bundle_time_window_mins)
            if bundle_time_window_mins is not None
            else (
                int(relaxed_time_window_mins)
                if relaxed_time_window_mins is not None
                else max(int(time_window_mins), 360)
            )
        )
        bundle_neg = (
            int(bundle_allow_negative_time_diff_mins)
            if bundle_allow_negative_time_diff_mins is not None
            else (
                int(relaxed_allow_negative_time_diff_mins)
                if relaxed_allow_negative_time_diff_mins is not None
                else max(int(allow_negative_time_diff_mins), 10)
            )
        )
        base_bundle_amount_tol = (
            float(bundle_amount_tolerance)
            if bundle_amount_tolerance is not None
            else (
                float(relaxed_amount_tolerance)
                if relaxed_amount_tolerance is not None
                else max(amount_tolerance * 2.0, amount_tolerance + 1.0)
            )
        )

        bundle_counter = 0
        remaining_banks = [b for b in bank_list if _bank_identity(b) not in used_bank_keys]
        for bank in sorted(remaining_banks, key=lambda b: b.posted_at):
            unmatched_receipts = [
                r for r in electronic_receipts
                if matches_by_receipt.get(r.receipt_id) is not None
                and matches_by_receipt[r.receipt_id].status in {
                    MatchStatus.UNMATCHED_NO_BANK_CREDIT,
                    MatchStatus.REVIEW_MULTIPLE_CANDIDATES,
                }
            ]
            if len(unmatched_receipts) < 2:
                break

            bundle = _find_bundle_for_bank(
                bank=bank,
                receipts=unmatched_receipts,
                fee_model=fee_model,
                max_bundle_size=max(2, int(bundle_max_size)),
                base_amount_tolerance=base_bundle_amount_tol,
                time_window_mins=bundle_time,
                allow_negative_time_diff_mins=bundle_neg,
                max_receipt_span_mins=max(15, int(bundle_max_receipt_span_mins)),
            )
            if bundle is None:
                continue

            bundle_counter += 1
            bundle_id = f"B{bundle_counter:04d}"
            used_bank_keys.add(_bank_identity(bank))
            per_receipt_diff = bundle.total_amount_diff / len(bundle.receipts)

            for receipt in bundle.receipts:
                detail = bundle.per_receipt.get(receipt.receipt_id)
                if detail is None:
                    continue
                time_diff, expected_used, fee_adj = detail
                matches_by_receipt[receipt.receipt_id] = Match(
                    receipt=receipt,
                    bank_txn=bank,
                    status=MatchStatus.REVIEW_BUNDLED_MATCH,
                    time_diff_minutes=time_diff,
                    amount_diff=per_receipt_diff,
                    account_name=account_name_by_number.get(bank.account_number, ""),
                    candidates_count=len(bundle.receipts),
                    confidence_tier="BUNDLE",
                    fee_adjustment_applied=fee_adj,
                    expected_credit_used=expected_used,
                    bundle_id=bundle_id,
                    bundle_size=len(bundle.receipts),
                    bundle_total_expected=bundle.total_expected_credit,
                )

    matches: List[Match] = []
    for receipt in sorted_receipts:
        match = matches_by_receipt.get(receipt.receipt_id)
        if match is not None:
            matches.append(match)

    if diagnostics is not None:
        strict_receipt_count = len(electronic_receipts)
        diagnostics["candidate_pairs_strict_count"] = int(strict_candidate_pairs_count)
        diagnostics["avg_candidates_per_receipt_strict"] = (
            round(strict_candidate_pairs_count / strict_receipt_count, 4)
            if strict_receipt_count > 0
            else 0.0
        )
        diagnostics["strict_receipt_count"] = strict_receipt_count

    unmatched_bank = [b for b in bank_list if _bank_identity(b) not in used_bank_keys]
    return matches, unmatched_bank
