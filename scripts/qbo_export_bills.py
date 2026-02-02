#!/usr/bin/env python3
"""
Export QBO Bills to CSV (header + line details) for a given date range.
Use this before manually deleting Bills in QBO so you can recreate later.

Usage (example):
  python scripts/qbo_export_bills.py --company company_a --from 2020-01-01 --to 2026-01-31 --out ./exports/company_a_bills/

Notes:
- Uses QBO Query API with pagination (STARTPOSITION / MAXRESULTS).
- For each Bill found, fetches full Bill via GET /bill/{id} to reliably capture Line details.
"""
from __future__ import annotations

import argparse
import csv
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import quote

_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from load_env import load_env_file
from company_config import load_company_config, get_available_companies
from token_manager import verify_realm_match
from qbo_upload import BASE_URL, _make_qbo_request, TokenManager

load_env_file()

MINORVERSION = "70"

HEADER_FIELDS = [
    "BillId",
    "DocNumber",
    "TxnDate",
    "DueDate",
    "VendorId",
    "VendorName",
    "APAccountId",
    "APAccountName",
    "Currency",
    "ExchangeRate",
    "TotalAmt",
    "Balance",
    "PrivateNote",
    "SyncToken",
    "MetaData_CreateTime",
    "MetaData_LastUpdatedTime",
]

LINE_FIELDS = [
    "BillId",
    "LineId",
    "LineNum",
    "DetailType",
    "Amount",
    "Description",
    "ItemId",
    "ItemName",
    "Qty",
    "UnitPrice",
    "BillableStatus",
    "CustomerId",
    "CustomerName",
    "ClassId",
    "ClassName",
    "TaxCodeId",
    "TaxCodeName",
    "AccountId",
    "AccountName",
]


def _pick_ref(ref_obj: Optional[Dict[str, Any]]) -> Tuple[Optional[str], Optional[str]]:
    """Return (value, name) from QBO *Ref objects like VendorRef, ItemRef, AccountRef, etc."""
    if not isinstance(ref_obj, dict):
        return (None, None)
    return (ref_obj.get("value"), ref_obj.get("name"))


def _ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def _query_bills(
    token_mgr: TokenManager,
    realm_id: str,
    date_from: str,
    date_to: str,
    start_pos: int,
    page_size: int,
) -> List[Dict[str, Any]]:
    """Run QBO query for Bills in date range; return list of Bill entities (may be partial)."""
    query = (
        f"SELECT * FROM Bill WHERE TxnDate >= '{date_from}' AND TxnDate <= '{date_to}' "
        f"STARTPOSITION {start_pos} MAXRESULTS {page_size}"
    )
    url = f"{BASE_URL}/v3/company/{realm_id}/query?query={quote(query)}&minorversion={MINORVERSION}"
    resp = _make_qbo_request("GET", url, token_mgr)
    if resp.status_code != 200:
        print(f"HTTP {resp.status_code}", resp.text[:1000], file=sys.stderr)
        sys.exit(1)
    bills = resp.json().get("QueryResponse", {}).get("Bill") or []
    if not isinstance(bills, list):
        bills = [bills] if bills else []
    return bills


def _get_bill(token_mgr: TokenManager, realm_id: str, bill_id: str) -> Dict[str, Any]:
    """GET /bill/{id} and return the Bill entity."""
    url = f"{BASE_URL}/v3/company/{realm_id}/bill/{bill_id}?minorversion={MINORVERSION}"
    resp = _make_qbo_request("GET", url, token_mgr)
    if resp.status_code != 200:
        print(f"HTTP {resp.status_code} for bill {bill_id}", resp.text[:500], file=sys.stderr)
        sys.exit(1)
    return resp.json().get("Bill") or {}


def _bill_to_header_row(bill: Dict[str, Any]) -> Dict[str, Any]:
    """Build one header CSV row from a full Bill entity."""
    bill_id = str(bill.get("Id", ""))
    vendor_id, vendor_name = _pick_ref(bill.get("VendorRef"))
    ap_id, ap_name = _pick_ref(bill.get("APAccountRef"))
    meta = bill.get("MetaData") or {}
    currency_ref = bill.get("CurrencyRef") or {}
    currency = currency_ref.get("value") or currency_ref.get("name")
    return {
        "BillId": bill_id,
        "DocNumber": bill.get("DocNumber"),
        "TxnDate": bill.get("TxnDate"),
        "DueDate": bill.get("DueDate"),
        "VendorId": vendor_id,
        "VendorName": vendor_name,
        "APAccountId": ap_id,
        "APAccountName": ap_name,
        "Currency": currency,
        "ExchangeRate": bill.get("ExchangeRate"),
        "TotalAmt": bill.get("TotalAmt"),
        "Balance": bill.get("Balance"),
        "PrivateNote": bill.get("PrivateNote"),
        "SyncToken": bill.get("SyncToken"),
        "MetaData_CreateTime": meta.get("CreateTime"),
        "MetaData_LastUpdatedTime": meta.get("LastUpdatedTime"),
    }


def _bill_lines_to_rows(bill_id: str, bill: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Build line CSV rows from a full Bill entity."""
    rows = []
    for ln in bill.get("Line") or []:
        detail_type = ln.get("DetailType")
        amount = ln.get("Amount")
        line_id = ln.get("Id")
        line_num = ln.get("LineNum")
        desc = ln.get("Description")

        item_id = item_name = None
        qty = unit_price = None
        billable_status = None
        cust_id = cust_name = None
        class_id = class_name = None
        tax_id = tax_name = None
        acct_id = acct_name = None

        if detail_type == "ItemBasedExpenseLineDetail":
            d = ln.get("ItemBasedExpenseLineDetail") or {}
            item_id, item_name = _pick_ref(d.get("ItemRef"))
            qty = d.get("Qty")
            unit_price = d.get("UnitPrice")
            billable_status = d.get("BillableStatus")
            cust_id, cust_name = _pick_ref(d.get("CustomerRef"))
            class_id, class_name = _pick_ref(d.get("ClassRef"))
            tax_id, tax_name = _pick_ref(d.get("TaxCodeRef"))
        elif detail_type == "AccountBasedExpenseLineDetail":
            d = ln.get("AccountBasedExpenseLineDetail") or {}
            acct_id, acct_name = _pick_ref(d.get("AccountRef"))
            billable_status = d.get("BillableStatus")
            cust_id, cust_name = _pick_ref(d.get("CustomerRef"))
            class_id, class_name = _pick_ref(d.get("ClassRef"))
            tax_id, tax_name = _pick_ref(d.get("TaxCodeRef"))

        rows.append({
            "BillId": bill_id,
            "LineId": line_id,
            "LineNum": line_num,
            "DetailType": detail_type,
            "Amount": amount,
            "Description": desc,
            "ItemId": item_id,
            "ItemName": item_name,
            "Qty": qty,
            "UnitPrice": unit_price,
            "BillableStatus": billable_status,
            "CustomerId": cust_id,
            "CustomerName": cust_name,
            "ClassId": class_id,
            "ClassName": class_name,
            "TaxCodeId": tax_id,
            "TaxCodeName": tax_name,
            "AccountId": acct_id,
            "AccountName": acct_name,
        })
    return rows


def run_export(
    token_mgr: TokenManager,
    realm_id: str,
    date_from: str,
    date_to: str,
    out_dir: str,
    page_size: int,
    dry_run: bool,
) -> None:
    """Query Bills in date range; if dry_run, print count and first 3 IDs; else write CSVs."""
    start_pos = 1
    all_bill_ids: List[str] = []

    if dry_run:
        while True:
            bills = _query_bills(token_mgr, realm_id, date_from, date_to, start_pos, page_size)
            if not bills:
                break
            for b in bills:
                all_bill_ids.append(str(b.get("Id", "")))
            start_pos += len(bills)
            if len(bills) < page_size:
                break
        first_3 = all_bill_ids[:3]
        ids_str = ", ".join(first_3) if first_3 else "(none)"
        print(f"Dry-run: {len(all_bill_ids)} bill(s) found. First 3 IDs: {ids_str}")
        return

    _ensure_dir(out_dir)
    header_path = os.path.join(out_dir, "bills_header.csv")
    lines_path = os.path.join(out_dir, "bills_lines.csv")

    with open(header_path, "w", newline="", encoding="utf-8") as hf, open(
        lines_path, "w", newline="", encoding="utf-8"
    ) as lf:
        header_writer = csv.DictWriter(hf, fieldnames=HEADER_FIELDS)
        line_writer = csv.DictWriter(lf, fieldnames=LINE_FIELDS)
        header_writer.writeheader()
        line_writer.writeheader()

        total_exported = 0
        while True:
            bills = _query_bills(token_mgr, realm_id, date_from, date_to, start_pos, page_size)
            if not bills:
                break

            for b in bills:
                bill_id = str(b.get("Id", ""))
                full_bill = _get_bill(token_mgr, realm_id, bill_id)
                header_writer.writerow(_bill_to_header_row(full_bill))
                for row in _bill_lines_to_rows(bill_id, full_bill):
                    line_writer.writerow(row)
                total_exported += 1

            start_pos += len(bills)
            if len(bills) < page_size:
                break

    print(f"Export complete: {total_exported} bills")
    print(f"  - {header_path}")
    print(f"  - {lines_path}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Export QBO Bills to CSV (header + lines) for a date range."
    )
    parser.add_argument(
        "--company",
        required=True,
        choices=get_available_companies(),
        help="Company key (e.g. company_a)",
    )
    parser.add_argument(
        "--from",
        dest="date_from",
        required=True,
        help="Start date YYYY-MM-DD",
    )
    parser.add_argument(
        "--to",
        dest="date_to",
        required=True,
        help="End date YYYY-MM-DD",
    )
    parser.add_argument(
        "--out",
        dest="out_dir",
        required=True,
        help="Output directory for CSVs",
    )
    parser.add_argument(
        "--page-size",
        type=int,
        default=1000,
        help="Query page size (default 1000)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Only print count and first 3 bill IDs; do not write files",
    )
    args = parser.parse_args()

    datetime.strptime(args.date_from, "%Y-%m-%d")
    datetime.strptime(args.date_to, "%Y-%m-%d")

    config = load_company_config(args.company)
    verify_realm_match(args.company, config.realm_id)
    token_mgr = TokenManager(config.company_key, config.realm_id)
    realm_id = config.realm_id

    run_export(
        token_mgr,
        realm_id,
        args.date_from,
        args.date_to,
        args.out_dir,
        args.page_size,
        args.dry_run,
    )


if __name__ == "__main__":
    main()
