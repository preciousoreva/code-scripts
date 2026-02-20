"""
Daily EPOS-to-bank reconciliation (totals-first).

Process:
1) Read transformed sales CSV (`*_sales_receipts_*`) for the date.
2) Extract statement-level `Total Credit` from daily bank statements.
3) Compare EPOS total sales vs statement total credits.
4) Explain variance with tender totals (cash/mixed-cash hints).
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
from pathlib import Path
from typing import Dict, List, Optional

# Ensure repo root on path when run as script.
_REPO_ROOT = Path(__file__).resolve().parent.parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from code_scripts.company_config import load_company_config
from code_scripts.paths import OPS_REPORTS_DIR, OPS_UPLOADED_DIR
from code_scripts.reconciliation.account_mapping import load_account_mapping_details
from code_scripts.reconciliation.bank_statements import (
    StatementCreditTotal,
    load_statement_credit_totals,
)
from code_scripts.reconciliation.config import (
    ReconciliationConfig,
    resolve_default_base_dir,
    resolve_default_out_dir,
)
from code_scripts.reconciliation.io_discovery import discover_transformed_sales_file
from code_scripts.reconciliation.transformed_sales import (
    TenderTotals,
    summarize_transformed_sales,
)

LEGACY_REPORT_FILES = (
    "epos_to_bank_matches.csv",
    "bank_credits_to_epos.csv",
    "debug_unmatched_epos.csv",
    "debug_unmatched_bank.csv",
)


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description=(
            "Reconcile daily transformed EPOS sales totals against daily bank statement "
            "header Total Credit values."
        ),
    )
    p.add_argument("--date", required=True, help="Date filter YYYY-MM-DD")
    p.add_argument(
        "--company",
        default=None,
        help="Company key (e.g. company_a). Used to derive default folders and transformed file prefix.",
    )
    p.add_argument(
        "--base-dir",
        type=Path,
        default=None,
        help="Override base input dir (default: docs/<company>/bank-reconciliation when --company is set).",
    )
    p.add_argument(
        "--epos-file",
        type=Path,
        default=None,
        help="Explicit transformed sales file path (overrides discovery).",
    )
    p.add_argument(
        "--epos-dir",
        type=Path,
        default=None,
        help="Directory to search for transformed sales CSV (default with --company: <base-dir>/epos; else code_scripts/Uploaded).",
    )
    p.add_argument(
        "--statements-dir",
        type=Path,
        default=None,
        help="Directory containing daily statement .xlsx files (default with --company: <base-dir>/statements/<date>/).",
    )
    p.add_argument(
        "--account-mapping",
        type=Path,
        default=None,
        help="Path to account mapping CSV (default with --company: <base-dir>/account_mapping.csv).",
    )
    p.add_argument(
        "--out-dir",
        type=Path,
        default=None,
        help="Base output directory (default with --company: code_scripts/reports/reconciliation/<company>/).",
    )
    p.add_argument(
        "--vat-rate",
        type=float,
        default=0.075,
        help="VAT rate to compute VAT/Sales-after-tax rows (default: 0.075).",
    )
    p.add_argument(
        "--variance-tolerance",
        type=float,
        default=5.0,
        help="Tolerance (Naira) for variance explanation matching (default: 5.0).",
    )
    return p.parse_args()


def _resolve_paths(
    args: argparse.Namespace,
) -> tuple[Path | None, Path | None, Path | None, Path | None, Path, str | None]:
    """
    Resolve base_dir, epos_dir, statements_dir, account_mapping_path, out_dir, company.
    """
    base_dir: Path | None = None
    company: str | None = args.company
    if args.base_dir is not None:
        base_dir = Path(args.base_dir).resolve()
    elif company:
        base_dir = resolve_default_base_dir(company)

    epos_dir: Path | None = None
    statements_dir: Path | None = args.statements_dir
    account_mapping_path: Path | None = args.account_mapping
    out_dir: Path = OPS_REPORTS_DIR / "reconciliation"

    if base_dir is not None:
        epos_dir = Path(args.epos_dir).resolve() if args.epos_dir is not None else (base_dir / "epos")
        if statements_dir is None:
            statements_dir = base_dir / "statements" / args.date
        if account_mapping_path is None:
            account_mapping_path = base_dir / "account_mapping.csv"
        out_dir = resolve_default_out_dir(company, args.date, args.out_dir)
    else:
        if args.epos_dir is not None:
            epos_dir = Path(args.epos_dir).resolve()
        if args.out_dir is not None:
            out_dir = Path(args.out_dir).resolve()
        else:
            out_dir = (OPS_REPORTS_DIR / "reconciliation").resolve()

    return base_dir, epos_dir, statements_dir, account_mapping_path, out_dir, company


def _resolve_transformed_prefix(company: Optional[str]) -> Optional[str]:
    if not company:
        return None
    try:
        cfg = load_company_config(company)
        return cfg.csv_prefix
    except Exception:
        return None


def _resolve_transformed_sales_file(
    date: str,
    company: Optional[str],
    epos_file: Optional[Path],
    epos_dir: Optional[Path],
) -> Optional[Path]:
    if epos_file is not None and Path(epos_file).exists():
        return Path(epos_file).resolve()

    prefix = _resolve_transformed_prefix(company)
    search_dir = Path(epos_dir).resolve() if epos_dir is not None else OPS_UPLOADED_DIR
    path = discover_transformed_sales_file(search_dir, date_filter=date, csv_prefix=prefix)
    if path is not None:
        return Path(path).resolve()

    # Fallback to Uploaded root/date where transformed files are commonly archived.
    if search_dir != OPS_UPLOADED_DIR.resolve():
        fallback = discover_transformed_sales_file(OPS_UPLOADED_DIR, date_filter=date, csv_prefix=prefix)
        if fallback is not None:
            return Path(fallback).resolve()
    return None


def _cleanup_legacy_reports(output_dir: Path) -> List[str]:
    removed: List[str] = []
    for name in LEGACY_REPORT_FILES:
        p = output_dir / name
        if p.exists():
            p.unlink()
            removed.append(name)
    return removed


def _classify_statement_channel(account_name: str, mapped_detail: Dict[str, str]) -> str:
    name = (mapped_detail.get("account_name") or account_name or "").lower()
    status = (mapped_detail.get("status") or "").lower()
    if "expense" in status:
        return "EXPENSE_OR_OTHER"
    if "mini mart" in name or "pos" in name:
        return "POS"
    return "TRANSFER_OR_OTHER"


def _dedupe_statement_totals(rows: List[StatementCreditTotal]) -> tuple[List[StatementCreditTotal], int]:
    """
    Remove exact duplicate statement totals that come from duplicate downloaded files.

    Identity excludes source filename so repeated exports for the same account/day collapse.
    """
    seen: set[tuple[str, str, float, float]] = set()
    unique: List[StatementCreditTotal] = []
    dropped = 0
    for row in rows:
        key = (
            str(row.account_number).strip(),
            str(row.statement_date_range).strip(),
            round(float(row.total_credit), 2),
            round(float(row.total_debit), 2),
        )
        if key in seen:
            dropped += 1
            continue
        seen.add(key)
        unique.append(row)
    return unique, dropped


def _enrich_statement_rows(
    rows: List[StatementCreditTotal],
    details_by_account: Dict[str, Dict[str, str]],
) -> List[Dict[str, object]]:
    out: List[Dict[str, object]] = []
    for row in rows:
        detail = details_by_account.get(str(row.account_number).strip(), {})
        channel = _classify_statement_channel(row.account_name, detail)
        out.append(
            {
                "source_file": row.source_file,
                "sheet_name": row.sheet_name,
                "account_number": row.account_number,
                "statement_account_name": row.account_name,
                "mapped_account_name": detail.get("account_name", ""),
                "moniepoint_store_account": detail.get("moniepoint_store_account", ""),
                "moniepoint_online_account": detail.get("moniepoint_online_account", ""),
                "channel_classification": channel,
                "include_in_sales_total": channel != "EXPENSE_OR_OTHER",
                "total_credit": round(row.total_credit, 2),
                "total_debit": round(row.total_debit, 2),
                "statement_date_range": row.statement_date_range,
            }
        )
    out.sort(key=lambda x: float(x.get("total_credit", 0.0)), reverse=True)
    return out


def _build_variance_analysis(
    epos_total: float,
    bank_total_credits: float,
    tender_totals: TenderTotals,
    tolerance: float,
) -> Dict[str, object]:
    variance = float(epos_total - bank_total_credits)
    cash_only_gap = variance - tender_totals.cash_total
    cash_plus_mixed_gap = variance - tender_totals.potential_cash_total

    if abs(variance) <= tolerance:
        status = "BALANCED_WITHIN_TOLERANCE"
        note = "EPOS total and bank Total Credit are aligned within tolerance."
    elif abs(cash_only_gap) <= tolerance:
        status = "LIKELY_CASH_EXPLAINS_VARIANCE"
        note = "Variance is approximately equal to Cash tender total."
    elif abs(cash_plus_mixed_gap) <= tolerance:
        status = "LIKELY_CASH_AND_MIXED_EXPLAIN_VARIANCE"
        note = "Variance is approximately equal to Cash + mixed-with-cash tender totals."
    elif variance > 0:
        status = "REVIEW_EPOS_ABOVE_BANK"
        note = "EPOS total exceeds bank Total Credit; review cash, mixed tenders, and posting timing."
    else:
        status = "REVIEW_BANK_ABOVE_EPOS"
        note = "Bank Total Credit exceeds EPOS total; review non-sales credits/transfers."

    return {
        "variance_epos_minus_bank": round(variance, 2),
        "cash_only_gap_after_variance": round(cash_only_gap, 2),
        "cash_plus_mixed_gap_after_variance": round(cash_plus_mixed_gap, 2),
        "status": status,
        "note": note,
    }


def _write_csv(path: Path, rows: List[Dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        with open(path, "w", newline="", encoding="utf-8") as f:
            csv.writer(f).writerow([])
        return
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def _write_json(path: Path, payload: Dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)


def _overview_rows(summary: Dict[str, object]) -> List[Dict[str, object]]:
    totals = summary.get("totals", {}) if isinstance(summary.get("totals"), dict) else {}
    tender = summary.get("tender_totals", {}) if isinstance(summary.get("tender_totals"), dict) else {}
    variance = (
        summary.get("variance_analysis", {})
        if isinstance(summary.get("variance_analysis"), dict)
        else {}
    )
    return [
        {"metric": "DATE", "value": summary.get("date", "")},
        {"metric": "ACTUAL_SALES", "value": totals.get("actual_sales", 0)},
        {"metric": "BANK_TOTAL_CREDITS_SALES", "value": totals.get("bank_total_credits", 0)},
        {"metric": "BANK_TOTAL_CREDITS_ALL", "value": totals.get("bank_total_credits_all", 0)},
        {"metric": "VARIANCE_EPOS_MINUS_BANK", "value": variance.get("variance_epos_minus_bank", 0)},
        {"metric": "CARD_TOTAL", "value": tender.get("card_total", 0)},
        {"metric": "TRANSFER_TOTAL", "value": tender.get("transfer_total", 0)},
        {"metric": "CARD_TRANSFER_COMBO_TOTAL", "value": tender.get("card_transfer_combo_total", 0)},
        {"metric": "CASH_TOTAL", "value": tender.get("cash_total", 0)},
        {"metric": "MIXED_WITH_CASH_TOTAL", "value": tender.get("mixed_with_cash_total", 0)},
        {"metric": "POTENTIAL_CASH_TOTAL", "value": tender.get("potential_cash_total", 0)},
        {"metric": "VAT_AMOUNT", "value": totals.get("vat_amount", 0)},
        {"metric": "SALES_AFTER_TAX", "value": totals.get("sales_after_tax", 0)},
        {"metric": "REVIEW_STATUS", "value": variance.get("status", "")},
        {"metric": "REVIEW_NOTE", "value": variance.get("note", "")},
    ]


def main() -> int:
    args = _parse_args()
    _, epos_dir, statements_dir, account_mapping_path, out_dir, company = _resolve_paths(args)

    if statements_dir is None or not Path(statements_dir).is_dir():
        print(
            f"ERROR: Statements directory not found: {statements_dir}. "
            "Use --statements-dir or provide --company/--base-dir.",
            file=sys.stderr,
        )
        return 1

    transformed_file = _resolve_transformed_sales_file(
        date=args.date,
        company=company,
        epos_file=args.epos_file,
        epos_dir=epos_dir,
    )
    if transformed_file is None:
        print(
            f"ERROR: No transformed sales file found for {args.date}. "
            "Expected *_sales_receipts_* file in epos dir or Uploaded.",
            file=sys.stderr,
        )
        return 1

    # Summarize transformed sales totals.
    tender_totals = summarize_transformed_sales(transformed_file)
    print(f"Loaded transformed sales file: {transformed_file.name}")

    # Load statement-level Total Credit rows.
    statement_totals = load_statement_credit_totals(Path(statements_dir))
    if not statement_totals:
        print(
            "ERROR: Could not extract any 'Total Credit' values from statements. "
            "Confirm daily statement format includes header totals.",
            file=sys.stderr,
        )
        return 1
    statement_totals, dropped_dupes = _dedupe_statement_totals(statement_totals)
    print(
        f"Loaded statement totals from {len(statement_totals)} unique statement sheet(s)"
        + (f" (dropped {dropped_dupes} duplicate export(s))" if dropped_dupes else "")
    )

    details_by_account: Dict[str, Dict[str, str]] = {}
    if account_mapping_path and Path(account_mapping_path).exists():
        details_by_account = load_account_mapping_details(Path(account_mapping_path))
        print(f"Loaded account mapping details: {len(details_by_account)} account row(s)")

    statement_rows = _enrich_statement_rows(statement_totals, details_by_account)
    bank_total_credits_all = sum(float(r.total_credit) for r in statement_totals)
    bank_total_debits_all = sum(float(r.total_debit) for r in statement_totals)
    bank_total_credits = sum(float(r.get("total_credit", 0.0)) for r in statement_rows if bool(r.get("include_in_sales_total")))
    bank_total_debits = sum(float(r.get("total_debit", 0.0)) for r in statement_rows if bool(r.get("include_in_sales_total")))

    vat_amount = tender_totals.actual_sales_total * float(args.vat_rate)
    sales_after_tax = tender_totals.actual_sales_total - vat_amount
    variance_analysis = _build_variance_analysis(
        epos_total=tender_totals.actual_sales_total,
        bank_total_credits=bank_total_credits,
        tender_totals=tender_totals,
        tolerance=max(0.0, float(args.variance_tolerance)),
    )

    summary: Dict[str, object] = {
        "date": args.date,
        "company": company or "",
        "source_files": {
            "transformed_sales_file": str(transformed_file),
            "statements_dir": str(Path(statements_dir).resolve()),
            "account_mapping_file": str(Path(account_mapping_path).resolve()) if account_mapping_path else "",
        },
        "totals": {
            "actual_sales": round(tender_totals.actual_sales_total, 2),
            "bank_total_credits": round(bank_total_credits, 2),
            "bank_total_debits": round(bank_total_debits, 2),
            "bank_total_credits_all": round(bank_total_credits_all, 2),
            "bank_total_debits_all": round(bank_total_debits_all, 2),
            "vat_rate": float(args.vat_rate),
            "vat_amount": round(vat_amount, 2),
            "sales_after_tax": round(sales_after_tax, 2),
        },
        "tender_totals": tender_totals.as_dict(),
        "variance_analysis": variance_analysis,
        "statement_account_totals": statement_rows,
    }

    config = ReconciliationConfig(
        date=args.date,
        epos_file=transformed_file,
        epos_dir=epos_dir if epos_dir is not None else OPS_UPLOADED_DIR,
        statements_dir=Path(statements_dir),
        account_mapping_path=account_mapping_path,
        out_dir=out_dir,
        company=company,
    )
    output_dir = config.output_date_dir()
    output_dir.mkdir(parents=True, exist_ok=True)

    removed_legacy = _cleanup_legacy_reports(output_dir)
    if removed_legacy:
        print(f"Removed legacy receipt-level reports: {', '.join(removed_legacy)}")

    _write_json(output_dir / "reconciliation_summary.json", summary)
    _write_csv(output_dir / "statement_credit_totals.csv", statement_rows)
    _write_csv(output_dir / "epos_tender_totals.csv", [tender_totals.as_dict()])
    _write_csv(output_dir / "daily_reconciliation_overview.csv", _overview_rows(summary))

    print(f"Wrote totals reconciliation reports to {output_dir}")
    print(
        "Totals: "
        f"EPOS={summary['totals']['actual_sales']}, "
        f"BankCredits(sales accounts)={summary['totals']['bank_total_credits']}, "
        f"Variance={summary['variance_analysis']['variance_epos_minus_bank']}"
    )
    print(
        f"Review status: {summary['variance_analysis']['status']} "
        f"({summary['variance_analysis']['note']})"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
