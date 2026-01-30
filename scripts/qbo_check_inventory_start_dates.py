#!/usr/bin/env python3
"""
Standalone diagnostic: list QBO Inventory items whose InvStartDate is AFTER a cutoff date.
Uses the same auth/token and request utilities as qbo_upload.py (no OAuth reimplementation).

Example:
  python scripts/qbo_check_inventory_start_dates.py --company company_a --cutoff-date 2026-01-28
  python scripts/qbo_check_inventory_start_dates.py --company company_a --cutoff-date 2026-01-28 --export-csv reports/inv_start_date_issues_company_a_2026-01-28.csv
  python scripts/qbo_check_inventory_start_dates.py --company company_a --cutoff-date 2026-01-28 --no-include-inactive --maxresults 500
  python scripts/qbo_check_inventory_start_dates.py --company company_a --cutoff-date 2026-01-28 --first-page-only   # only first page (no pagination)
By default, the script paginates (startposition/maxresults) to fetch all Inventory items, not just the first 1000.
"""
from __future__ import annotations

import argparse
import csv
import sys
from pathlib import Path
from urllib.parse import quote

# Run from repo root (parent of scripts/); add repo root to path for imports
_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from load_env import load_env_file
from company_config import load_company_config, get_available_companies
from token_manager import verify_realm_match
from qbo_upload import BASE_URL, _make_qbo_request, TokenManager

load_env_file()


def parse_cutoff_date(s: str) -> str:
    """Validate and return YYYY-MM-DD string."""
    if len(s) != 10 or s[4] != "-" or s[7] != "-":
        raise ValueError(f"cutoff-date must be YYYY-MM-DD, got {s!r}")
    y, m, d = s[:4], s[5:7], s[8:10]
    if not (y.isdigit() and m.isdigit() and d.isdigit()):
        raise ValueError(f"cutoff-date must be YYYY-MM-DD, got {s!r}")
    return s


def query_inventory_items(
    token_mgr: TokenManager,
    realm_id: str,
    include_inactive: bool,
    maxresults: int,
    fetch_all: bool = True,
) -> list[dict]:
    """
    Query QBO for Inventory items. Read-only.
    When fetch_all is True (default), paginate with startposition/maxresults until all pages are fetched.
    Returns list of raw item dicts (may have missing InvStartDate).
    """
    select_clause = "select Id, Name, Type, TrackQtyOnHand, InvStartDate, Active from Item"
    where = "where Type = 'Inventory'"
    if not include_inactive:
        where += " and Active = true"
    all_items: list[dict] = []
    startposition = 1
    page_size = maxresults

    while True:
        # QBO uses 1-based startposition; maxresults is page size
        query = f"{select_clause} {where} startposition {startposition} maxresults {page_size}"
        url = f"{BASE_URL}/v3/company/{realm_id}/query?query={quote(query)}&minorversion=70"
        resp = _make_qbo_request("GET", url, token_mgr)
        if resp.status_code != 200:
            try:
                body = resp.json()
                fault = body.get("Fault") or body.get("fault")
                if fault:
                    errors = fault.get("Error") or fault.get("error") or []
                    msgs = [e.get("Message") or e.get("message") or str(e) for e in errors]
                    raise RuntimeError(f"QBO query failed: {'; '.join(msgs)}")
            except ValueError:
                pass
            raise RuntimeError(f"QBO query failed: HTTP {resp.status_code} - {resp.text[:500]}")
        data = resp.json()
        items = data.get("QueryResponse", {}).get("Item", [])
        if not isinstance(items, list):
            items = [items] if items else []
        all_items.extend(items)
        # If we got fewer than page_size, we've reached the end
        if not fetch_all or len(items) < page_size:
            break
        startposition += page_size
    return all_items


def filter_issues(items: list[dict], cutoff_date: str) -> list[dict]:
    """Return items where InvStartDate exists and InvStartDate > cutoff_date, sorted by InvStartDate ascending."""
    issues = []
    for item in items:
        inv_start = item.get("InvStartDate")
        if inv_start is None or inv_start == "":
            continue
        inv_date_str = inv_start[:10] if len(str(inv_start)) >= 10 else str(inv_start)
        try:
            if inv_date_str > cutoff_date:
                issues.append({
                    "Id": item.get("Id", ""),
                    "Name": item.get("Name", ""),
                    "Type": item.get("Type", ""),
                    "TrackQtyOnHand": item.get("TrackQtyOnHand", ""),
                    "InvStartDate": inv_date_str,
                    "Active": item.get("Active", ""),
                })
        except (TypeError, ValueError):
            continue
    issues.sort(key=lambda x: x.get("InvStartDate", ""))
    return issues


def count_with_inv_start_date(items: list[dict]) -> int:
    """Count items that have a non-empty InvStartDate."""
    return sum(1 for it in items if it.get("InvStartDate") not in (None, ""))


def items_missing_inv_start_date(items: list[dict]) -> list[dict]:
    """Return items that have no InvStartDate (for debugging / raw keys)."""
    return [it for it in items if it.get("InvStartDate") in (None, "")]


def main() -> int:
    parser = argparse.ArgumentParser(
        description="List QBO Inventory items whose InvStartDate is after a cutoff date (diagnostic for QBO 6270).",
    )
    parser.add_argument(
        "--company",
        required=True,
        choices=get_available_companies(),
        help="Company identifier",
    )
    parser.add_argument(
        "--cutoff-date",
        required=True,
        metavar="YYYY-MM-DD",
        help="Cutoff date (YYYY-MM-DD). Items with InvStartDate > this are reported.",
    )
    parser.add_argument(
        "--no-include-inactive",
        action="store_true",
        help="Exclude inactive items from the query (default: include both active and inactive)",
    )
    parser.add_argument(
        "--maxresults",
        type=int,
        default=1000,
        metavar="N",
        help="Max items to return from QBO query (default: 1000)",
    )
    parser.add_argument(
        "--export-csv",
        metavar="PATH",
        default=None,
        help="Optional path to write CSV of issues (columns: Id, Name, Type, TrackQtyOnHand, InvStartDate, Active)",
    )
    parser.add_argument(
        "--first-page-only",
        action="store_true",
        help="Only fetch the first page of results (no pagination); default is to fetch all pages.",
    )
    args = parser.parse_args()

    try:
        cutoff_date = parse_cutoff_date(args.cutoff_date)
    except ValueError as e:
        print(f"[ERROR] {e}")
        return 1

    if args.maxresults < 1 or args.maxresults > 1000:
        print("[ERROR] --maxresults must be between 1 and 1000")
        return 1

    config = load_company_config(args.company)
    try:
        verify_realm_match(config.company_key, config.realm_id)
    except RuntimeError as e:
        print(f"[ERROR] {e}")
        return 1

    token_mgr = TokenManager(config.company_key, config.realm_id)
    include_inactive = not args.no_include_inactive
    fetch_all = not args.first_page_only

    try:
        items = query_inventory_items(token_mgr, config.realm_id, include_inactive, args.maxresults, fetch_all=fetch_all)
    except RuntimeError as e:
        print(f"[ERROR] {e}")
        return 1

    total_returned = len(items)
    total_with_inv = count_with_inv_start_date(items)
    missing_inv = items_missing_inv_start_date(items)
    issues = filter_issues(items, cutoff_date)
    issues_count = len(issues)

    # Summary
    print("--- Summary ---")
    print(f"Total items returned: {total_returned}")
    print(f"Total inventory items with InvStartDate: {total_with_inv}")
    print(f"Issues (InvStartDate > {cutoff_date}): {issues_count}")
    if missing_inv:
        print(f"Items without InvStartDate: {len(missing_inv)} (Ids: {[it.get('Id') for it in missing_inv[:20]]}{'...' if len(missing_inv) > 20 else ''})")
        print(f"  Raw keys on first such item: {list(missing_inv[0].keys())}")

    # Table-like list of issues
    if issues:
        print("\n--- Items with InvStartDate after cutoff ---")
        col_id = "Id"
        col_name = "Name"
        col_inv = "InvStartDate"
        col_active = "Active"
        col_tqoh = "TrackQtyOnHand"
        widths = [
            max(len(col_id), max(len(str(it["Id"])) for it in issues)),
            max(len(col_name), min(50, max(len(str(it["Name"])) for it in issues))),
            max(len(col_inv), 10),
            max(len(col_active), 5),
            max(len(col_tqoh), 3),
        ]
        fmt = "  ".join(f"{{:<{w}}}" for w in widths)
        print(fmt.format(col_id, col_name[:widths[1]], col_inv, col_active, col_tqoh))
        print("-" * (sum(widths) + 2 * (len(widths) - 1)))
        for it in issues:
            name = str(it["Name"])[:widths[1]]
            print(fmt.format(
                str(it["Id"]),
                name,
                str(it["InvStartDate"]),
                str(it["Active"]),
                str(it["TrackQtyOnHand"]),
            ))
    else:
        print("\nNo issues (no Inventory items with InvStartDate > cutoff).")

    # Optional CSV export
    if args.export_csv and issues:
        out_path = Path(args.export_csv)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        with open(out_path, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=["Id", "Name", "Type", "TrackQtyOnHand", "InvStartDate", "Active"])
            w.writeheader()
            w.writerows(issues)
        print(f"\n[INFO] Wrote {issues_count} row(s) to {out_path}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
