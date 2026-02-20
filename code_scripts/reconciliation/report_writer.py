"""Write reconciliation CSV outputs and summary JSON."""

from __future__ import annotations

import csv
import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

from code_scripts.reconciliation.models import BankTxn, Match, MatchStatus, Receipt


def _dt_str(dt: datetime | None) -> str:
    if dt is None:
        return ""
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def write_epos_to_bank_matches(matches: List[Match], path: Path) -> None:
    """Receipt-centric: epos_to_bank_matches.csv."""
    path.parent.mkdir(parents=True, exist_ok=True)
    rows: List[Dict[str, Any]] = []
    for m in matches:
        r = m.receipt
        row = {
            "receipt_id": r.receipt_id,
            "receipt_datetime": _dt_str(r.receipt_datetime),
            "location_name": r.location_name,
            "device_name": r.device_name,
            "staff": r.staff,
            "tender": r.tender,
            "line_count": r.line_count,
            "gross_amount": round(r.gross_amount, 2),
            "service_fee": round(r.service_fee, 2),
            "expected_credit": round(r.expected_credit, 2),
            "expected_credit_used": round(m.expected_credit_used, 2) if m.expected_credit_used is not None else round(r.expected_credit, 2),
            "expected_emtl": round(r.expected_emtl, 2),
            "match_status": m.status.value,
            "confidence_tier": m.confidence_tier or "",
            "matched_account_number": m.bank_txn.account_number if m.bank_txn else "",
            "matched_account_name": m.account_name or "",
            "bank_posted_at": _dt_str(m.bank_txn.posted_at) if m.bank_txn else "",
            "bank_credit_amount": round(m.bank_txn.credit, 2) if m.bank_txn else "",
            "bank_reference": m.bank_txn.reference if m.bank_txn else "",
            "bank_narration": m.bank_txn.narration if m.bank_txn else "",
            "time_diff_minutes": round(m.time_diff_minutes, 2) if m.time_diff_minutes is not None else "",
            "amount_diff": round(m.amount_diff, 2) if m.amount_diff is not None else "",
            "candidates_count": m.candidates_count,
            "unmatched_reason": m.unmatched_reason or "",
            "closest_amount_diff": round(m.closest_amount_diff, 2) if m.closest_amount_diff is not None else "",
            "closest_time_diff_minutes": round(m.closest_time_diff_minutes, 2) if m.closest_time_diff_minutes is not None else "",
            "fee_adjustment_applied": round(m.fee_adjustment_applied, 2),
            "bundle_id": m.bundle_id or "",
            "bundle_size": m.bundle_size,
            "bundle_total_expected": round(m.bundle_total_expected, 2) if m.bundle_total_expected is not None else "",
        }
        rows.append(row)
    fieldnames = [
        "receipt_id", "receipt_datetime", "location_name", "device_name", "staff", "tender",
        "line_count", "gross_amount", "service_fee", "expected_credit", "expected_credit_used", "expected_emtl",
        "match_status", "matched_account_number", "matched_account_name", "bank_posted_at",
        "bank_credit_amount", "bank_reference", "bank_narration", "time_diff_minutes", "amount_diff",
        "confidence_tier", "candidates_count", "unmatched_reason", "closest_amount_diff", "closest_time_diff_minutes",
        "fee_adjustment_applied", "bundle_id", "bundle_size", "bundle_total_expected",
    ]
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)


def write_bank_credits_to_epos(
    matches: List[Match],
    unmatched_bank: List[BankTxn],
    account_name_by_number: Dict[str, str],
    path: Path,
) -> None:
    """Bank-centric: bank_credits_to_epos.csv."""
    path.parent.mkdir(parents=True, exist_ok=True)
    rows: List[Dict[str, Any]] = []
    for m in matches:
        if m.bank_txn is None:
            continue
        b = m.bank_txn
        rows.append({
            "account_number": b.account_number,
            "account_name": account_name_by_number.get(b.account_number, ""),
            "posted_at": _dt_str(b.posted_at),
            "credit_amount": round(b.credit, 2),
            "reference": b.reference,
            "narration": b.narration,
            "matched_receipt_id": m.receipt.receipt_id,
            "match_status": m.status.value,
            "confidence_tier": m.confidence_tier or "",
            "expected_gross_estimate": round(m.receipt.gross_amount, 2),
            "expected_credit_used": round(m.expected_credit_used, 2) if m.expected_credit_used is not None else round(m.receipt.expected_credit, 2),
            "fee_adjustment_applied": round(m.fee_adjustment_applied, 2),
            "bundle_id": m.bundle_id or "",
            "bundle_size": m.bundle_size,
            "linked_emtl_debit": round(b.linked_emtl_amount, 2),
            "linked_emtl_posted_at": _dt_str(b.linked_emtl_posted_at),
        })
    for b in unmatched_bank:
        if b.credit <= 0:
            continue
        rows.append({
            "account_number": b.account_number,
            "account_name": account_name_by_number.get(b.account_number, ""),
            "posted_at": _dt_str(b.posted_at),
            "credit_amount": round(b.credit, 2),
            "reference": b.reference,
            "narration": b.narration,
            "matched_receipt_id": "",
            "match_status": MatchStatus.UNMATCHED_BANK.value,
            "confidence_tier": "",
            "expected_gross_estimate": "",
            "expected_credit_used": "",
            "fee_adjustment_applied": "",
            "bundle_id": "",
            "bundle_size": "",
            "linked_emtl_debit": round(b.linked_emtl_amount, 2),
            "linked_emtl_posted_at": _dt_str(b.linked_emtl_posted_at),
        })
    if not rows:
        with open(path, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(
                f,
                fieldnames=[
                    "account_number",
                    "account_name",
                    "posted_at",
                    "credit_amount",
                    "reference",
                    "narration",
                    "matched_receipt_id",
                    "match_status",
                    "confidence_tier",
                    "expected_gross_estimate",
                    "expected_credit_used",
                    "fee_adjustment_applied",
                    "bundle_id",
                    "bundle_size",
                    "linked_emtl_debit",
                    "linked_emtl_posted_at",
                ],
            )
            w.writeheader()
        return
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)


def write_debug_unmatched_epos(matches: List[Match], path: Path) -> None:
    """debug_unmatched_epos.csv: receipts without bank assignment (strict/relaxed/bundle excluded)."""
    path.parent.mkdir(parents=True, exist_ok=True)
    matched_statuses = {
        MatchStatus.MATCHED,
        MatchStatus.REVIEW_TRANSFER_MATCH,
        MatchStatus.REVIEW_RELAXED_MATCH,
        MatchStatus.REVIEW_BUNDLED_MATCH,
    }
    unmatched = [m for m in matches if m.status not in matched_statuses]
    rows = []
    for m in unmatched:
        r = m.receipt
        rows.append({
            "receipt_id": r.receipt_id,
            "receipt_datetime": _dt_str(r.receipt_datetime),
            "location_name": r.location_name,
            "tender": r.tender,
            "gross_amount": round(r.gross_amount, 2),
            "expected_credit": round(r.expected_credit, 2),
            "match_status": m.status.value,
            "unmatched_reason": m.unmatched_reason or "",
            "candidates_count": m.candidates_count,
            "closest_amount_diff": round(m.closest_amount_diff, 2) if m.closest_amount_diff is not None else "",
            "closest_time_diff_minutes": round(m.closest_time_diff_minutes, 2) if m.closest_time_diff_minutes is not None else "",
            "confidence_tier": m.confidence_tier or "",
            "fee_adjustment_applied": round(m.fee_adjustment_applied, 2),
            "bundle_id": m.bundle_id or "",
            "bundle_size": m.bundle_size,
        })
    with open(path, "w", newline="", encoding="utf-8") as f:
        if rows:
            w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
            w.writeheader()
            w.writerows(rows)
        else:
            w = csv.writer(f)
            w.writerow([
                "receipt_id",
                "receipt_datetime",
                "location_name",
                "tender",
                "gross_amount",
                "expected_credit",
                "match_status",
                "unmatched_reason",
                "candidates_count",
                "closest_amount_diff",
                "closest_time_diff_minutes",
                "confidence_tier",
                "fee_adjustment_applied",
                "bundle_id",
                "bundle_size",
            ])


def write_debug_unmatched_bank(unmatched_bank: List[BankTxn], path: Path) -> None:
    """debug_unmatched_bank.csv: bank credits with no EPOS match."""
    path.parent.mkdir(parents=True, exist_ok=True)
    credits = [b for b in unmatched_bank if b.credit > 0]
    rows = []
    for b in credits:
        rows.append({
            "account_number": b.account_number,
            "posted_at": _dt_str(b.posted_at),
            "credit_amount": round(b.credit, 2),
            "reference": b.reference,
            "narration": b.narration[:200] if b.narration else "",
        })
    with open(path, "w", newline="", encoding="utf-8") as f:
        if rows:
            w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
            w.writeheader()
            w.writerows(rows)
        else:
            w = csv.writer(f)
            w.writerow(["account_number", "posted_at", "credit_amount", "reference", "narration"])


def build_summary(
    matches: List[Match],
    unmatched_bank: List[BankTxn],
    diagnostics: Dict[str, Any] | None = None,
) -> Dict[str, Any]:
    """Build reconciliation_summary.json content."""
    by_status: Dict[str, int] = {}
    reason_counts: Dict[str, int] = {}
    for m in matches:
        by_status[m.status.value] = by_status.get(m.status.value, 0) + 1
        if m.unmatched_reason:
            reason_counts[m.unmatched_reason] = reason_counts.get(m.unmatched_reason, 0) + 1
    electronic_gross = sum(
        m.receipt.gross_amount for m in matches
        if m.receipt.tender_kind.value == "electronic"
    )
    cash_gross = sum(
        m.receipt.gross_amount for m in matches
        if m.receipt.tender_kind.value == "cash"
    )
    mixed_gross = sum(
        m.receipt.gross_amount for m in matches
        if m.receipt.tender_kind.value == "mixed"
    )
    total_epos_gross = sum(m.receipt.gross_amount for m in matches)
    total_service_fee = sum(m.receipt.service_fee for m in matches)
    total_bank_credits = sum(b.credit for b in unmatched_bank if b.credit > 0)
    matched_credits = sum(
        m.bank_txn.credit for m in matches if m.bank_txn is not None
    )
    total_bank_credits += matched_credits
    emtl_count = sum(1 for m in matches if m.bank_txn and m.bank_txn.linked_emtl_amount > 0)
    emtl_sum = sum(
        m.bank_txn.linked_emtl_amount for m in matches if m.bank_txn
    )
    learned_adjustment_applied_count = sum(
        1 for m in matches
        if m.bank_txn is not None and abs(m.fee_adjustment_applied) > 0
    )
    learned_adjustment_applied_sum = sum(
        m.fee_adjustment_applied for m in matches if m.bank_txn is not None
    )
    strict_matched_statuses = {MatchStatus.MATCHED}
    matched_statuses_total = {
        MatchStatus.MATCHED,
        MatchStatus.REVIEW_TRANSFER_MATCH,
        MatchStatus.REVIEW_RELAXED_MATCH,
        MatchStatus.REVIEW_BUNDLED_MATCH,
    }
    matched_count_strict = sum(1 for m in matches if m.status in strict_matched_statuses)
    matched_count = sum(1 for m in matches if m.status in matched_statuses_total)
    electronic_count = sum(
        1 for m in matches if m.receipt.tender_kind.value == "electronic"
    )
    match_rate = (matched_count / electronic_count * 100) if electronic_count else 0.0
    match_rate_strict = (matched_count_strict / electronic_count * 100) if electronic_count else 0.0
    summary: Dict[str, Any] = {
        "totals_by_tender": {
            "electronic_gross": round(electronic_gross, 2),
            "cash_gross": round(cash_gross, 2),
            "mixed_gross": round(mixed_gross, 2),
        },
        "total_epos_gross": round(total_epos_gross, 2),
        "total_bank_credits": round(total_bank_credits, 2),
        "inferred_service_fees_sum": round(total_service_fee, 2),
        "emtl_count": emtl_count,
        "emtl_sum": round(emtl_sum, 2),
        "learned_adjustment_applied_count": learned_adjustment_applied_count,
        "learned_adjustment_applied_sum": round(learned_adjustment_applied_sum, 2),
        "match_counts_by_status": by_status,
        "reason_counts": reason_counts,
        "matched_count": matched_count,
        "matched_count_strict": matched_count_strict,
        "electronic_receipt_count": electronic_count,
        "match_rate_percent": round(match_rate, 2),
        "match_rate_percent_strict": round(match_rate_strict, 2),
    }
    if diagnostics:
        summary.update(diagnostics)
    return summary


def write_reconciliation_summary(summary: Dict[str, Any], path: Path) -> None:
    """Write reconciliation_summary.json."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)


def write_all_reports(
    out_dir: Path,
    matches: List[Match],
    unmatched_bank: List[BankTxn],
    account_name_by_number: Dict[str, str],
    diagnostics: Dict[str, Any] | None = None,
) -> Dict[str, Any]:
    """Write all 5 outputs to out_dir and return summary dict."""
    out_dir = Path(out_dir)
    write_epos_to_bank_matches(matches, out_dir / "epos_to_bank_matches.csv")
    write_bank_credits_to_epos(
        matches, unmatched_bank, account_name_by_number,
        out_dir / "bank_credits_to_epos.csv",
    )
    write_debug_unmatched_epos(matches, out_dir / "debug_unmatched_epos.csv")
    write_debug_unmatched_bank(unmatched_bank, out_dir / "debug_unmatched_bank.csv")
    summary = build_summary(matches, unmatched_bank, diagnostics=diagnostics)
    write_reconciliation_summary(summary, out_dir / "reconciliation_summary.json")
    return summary
