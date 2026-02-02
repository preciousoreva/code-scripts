#!/usr/bin/env python3
"""
Delete QBO Sales Receipts: single receipt, by target date, or by date range.
Uses QBO operation=delete (see https://developer.intuit.com/app/developer/qbo/docs/api/accounting/all-entities/salesreceipt#delete-a-salesreceipt).

Run from repo root. Requires --company.

Modes:
  --doc-number SR-xxx     Delete one receipt by DocNumber
  --target-date YYYY-MM-DD  Delete all receipts with TxnDate = date
  --from-date / --to-date   Delete all receipts with TxnDate in range (inclusive)

Use --dry-run to list what would be deleted without deleting.
Use --limit N to cap how many to delete (default: no limit).

Examples:
  python scripts/qbo_delete_sales_receipts.py --company company_a --doc-number "SR-20260101-001" --dry-run
  python scripts/qbo_delete_sales_receipts.py --company company_a --target-date 2026-01-15 --dry-run
  python scripts/qbo_delete_sales_receipts.py --company company_a --from-date 2026-01-01 --to-date 2026-01-31 --dry-run
  python scripts/qbo_delete_sales_receipts.py --company company_a --target-date 2026-01-15
  python scripts/qbo_delete_sales_receipts.py --company company_a --from-date 2026-01-01 --to-date 2026-01-31 --limit 100
"""
from __future__ import annotations

import argparse
import csv
import sys
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


def _parse_date(s: str) -> str:
    if len(s) != 10 or s[4] != "-" or s[7] != "-":
        raise ValueError(f"Date must be YYYY-MM-DD, got {s!r}")
    if not (s[:4].isdigit() and s[5:7].isdigit() and s[8:10].isdigit()):
        raise ValueError(f"Date must be YYYY-MM-DD, got {s!r}")
    return s[:10]


def _query_sales_receipts(
    token_mgr: TokenManager,
    realm_id: str,
    *,
    doc_numbers: Optional[List[str]] = None,
    target_date: Optional[str] = None,
    from_date: Optional[str] = None,
    to_date: Optional[str] = None,
    maxresults: int = 1000,
    limit: Optional[int] = None,
) -> List[Dict[str, Any]]:
    """
    Query QBO for Sales Receipts by DocNumber(s), target date, or date range.
    Returns list of {Id, DocNumber, TxnDate, SyncToken}. Fetches SyncToken via GET if not in query.
    """
    receipts: List[Dict[str, Any]] = []

    if doc_numbers:
        # Single or list of DocNumbers
        for i in range(0, len(doc_numbers), 50):
            batch = doc_numbers[i : i + 50]
            safe = "', '".join(d.replace("'", "''") for d in batch)
            query = f"select Id, DocNumber, TxnDate, SyncToken from SalesReceipt where DocNumber in ('{safe}')"
            url = f"{BASE_URL}/v3/company/{realm_id}/query?query={quote(query)}&minorversion={MINORVERSION}"
            resp = _make_qbo_request("GET", url, token_mgr)
            if resp.status_code != 200:
                raise RuntimeError(f"QBO query failed: HTTP {resp.status_code} - {resp.text[:500]}")
            data = resp.json()
            items = data.get("QueryResponse", {}).get("SalesReceipt", [])
            if not isinstance(items, list):
                items = [items] if items else []
            receipts.extend(items)
    else:
        # By target date or date range
        if target_date:
            where = f"where TxnDate = '{target_date}'"
        elif from_date and to_date:
            where = f"where TxnDate >= '{from_date}' and TxnDate <= '{to_date}'"
        else:
            raise ValueError("Need doc_numbers, target_date, or from_date+to_date")
        startposition = 1
        while True:
            query = f"select Id, DocNumber, TxnDate, SyncToken from SalesReceipt {where} startposition {startposition} maxresults {maxresults}"
            url = f"{BASE_URL}/v3/company/{realm_id}/query?query={quote(query)}&minorversion={MINORVERSION}"
            resp = _make_qbo_request("GET", url, token_mgr)
            if resp.status_code != 200:
                raise RuntimeError(f"QBO query failed: HTTP {resp.status_code} - {resp.text[:500]}")
            data = resp.json()
            items = data.get("QueryResponse", {}).get("SalesReceipt", [])
            if not isinstance(items, list):
                items = [items] if items else []
            for it in items:
                if limit and len(receipts) >= limit:
                    break
                # SyncToken may not be returned by query; fetch via GET if missing
                if it.get("SyncToken") is None:
                    rid = it.get("Id")
                    if rid:
                        get_url = f"{BASE_URL}/v3/company/{realm_id}/salesreceipt/{rid}?minorversion={MINORVERSION}"
                        get_resp = _make_qbo_request("GET", get_url, token_mgr)
                        if get_resp.status_code == 200:
                            full = get_resp.json().get("SalesReceipt", {})
                            it["SyncToken"] = full.get("SyncToken")
                receipts.append(it)
                if limit and len(receipts) >= limit:
                    break
            if len(items) < maxresults:
                break
            startposition += maxresults

    return receipts


def _delete_sales_receipt(
    token_mgr: TokenManager,
    realm_id: str,
    receipt_id: str,
    sync_token: str,
) -> Tuple[bool, str]:
    """Delete one Sales Receipt (QBO operation=delete). Returns (success, error_message)."""
    url = f"{BASE_URL}/v3/company/{realm_id}/salesreceipt?operation=delete&minorversion={MINORVERSION}"
    payload = {"Id": receipt_id, "SyncToken": sync_token}
    resp = _make_qbo_request("POST", url, token_mgr, json=payload)
    if resp.status_code in (200, 201):
        return True, ""
    try:
        body = resp.json()
        detail = body.get("Fault", {}).get("Error", [])
        msg = "; ".join(
            (e.get("Message") or e.get("Detail") or str(e)) for e in detail
        ) if detail else resp.text[:500]
    except Exception:
        msg = resp.text[:500] if resp.text else ""
    return False, f"HTTP {resp.status_code}: {msg}"


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Delete QBO Sales Receipts: single receipt, by target date, or by date range.",
    )
    parser.add_argument("--company", required=True, choices=get_available_companies(), help="Company key")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--doc-number", metavar="DOC", help="Delete one receipt by DocNumber (e.g. SR-20260101-001)")
    group.add_argument("--target-date", metavar="YYYY-MM-DD", help="Delete all receipts with TxnDate = date")
    group.add_argument(
        "--from-date",
        metavar="YYYY-MM-DD",
        help="Delete all receipts with TxnDate in range (use with --to-date)",
    )
    parser.add_argument("--to-date", metavar="YYYY-MM-DD", help="End of date range (use with --from-date)")
    parser.add_argument("--dry-run", action="store_true", help="List what would be deleted; do not delete")
    parser.add_argument("--limit", type=int, metavar="N", help="Max number of receipts to delete (default: no limit)")
    parser.add_argument("--report-csv", metavar="PATH", help="Write delete report to CSV (Id, DocNumber, TxnDate, Status, Error)")
    args = parser.parse_args()

    # Validate dates
    if args.from_date and not args.to_date:
        parser.error("--to-date required when using --from-date")
    if args.to_date and not args.from_date:
        parser.error("--from-date required when using --to-date")
    if args.from_date and args.to_date and args.from_date > args.to_date:
        parser.error("--from-date must be <= --to-date")

    try:
        if args.target_date:
            _parse_date(args.target_date)
        if args.from_date:
            _parse_date(args.from_date)
        if args.to_date:
            _parse_date(args.to_date)
    except ValueError as e:
        print(f"[ERROR] {e}", file=sys.stderr)
        return 1

    config = load_company_config(args.company)
    verify_realm_match(args.company, config.realm_id)
    token_mgr = TokenManager(config.company_key, config.realm_id)
    realm_id = config.realm_id

    doc_numbers = [args.doc_number.strip()] if args.doc_number else None
    try:
        receipts = _query_sales_receipts(
            token_mgr,
            realm_id,
            doc_numbers=doc_numbers,
            target_date=args.target_date,
            from_date=args.from_date,
            to_date=args.to_date,
            limit=args.limit,
        )
    except (ValueError, RuntimeError) as e:
        print(f"[ERROR] {e}", file=sys.stderr)
        return 1

    if not receipts:
        print("[INFO] No Sales Receipts found matching criteria.")
        return 0

    print(f"[INFO] Found {len(receipts)} Sales Receipt(s) to delete.")
    for r in receipts[:20]:
        print(f"  Id={r.get('Id')} DocNumber={r.get('DocNumber')} TxnDate={r.get('TxnDate')}")
    if len(receipts) > 20:
        print(f"  ... and {len(receipts) - 20} more")

    if args.dry_run:
        print("[INFO] Dry run: no receipts deleted.")
        return 0

    report: List[Dict[str, Any]] = []
    ok = 0
    fail = 0
    for r in receipts:
        rid = r.get("Id")
        sync = r.get("SyncToken")
        doc = r.get("DocNumber", "")
        txn = r.get("TxnDate", "")
        if not rid or sync is None:
            status = "skipped"
            err = "Missing Id or SyncToken"
            fail += 1
        else:
            success, err = _delete_sales_receipt(token_mgr, realm_id, str(rid), str(sync))
            status = "deleted" if success else "failed"
            if success:
                ok += 1
                print(f"  [OK] {doc} (Id={rid}) deleted")
            else:
                fail += 1
                print(f"  [FAIL] {doc} (Id={rid}): {err}", file=sys.stderr)
        report.append({"Id": rid, "DocNumber": doc, "TxnDate": txn, "Status": status, "Error": err or ""})

    print(f"[INFO] Deleted {ok}, failed {fail}")

    if args.report_csv:
        out = Path(args.report_csv)
        if not out.is_absolute():
            out = _REPO_ROOT / out
        out.parent.mkdir(parents=True, exist_ok=True)
        with open(out, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=["Id", "DocNumber", "TxnDate", "Status", "Error"])
            w.writeheader()
            w.writerows(report)
        print(f"[INFO] Report written to {out}")

    return 1 if fail else 0


if __name__ == "__main__":
    sys.exit(main())
