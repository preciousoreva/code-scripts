#!/usr/bin/env python3
"""
List QBO Inventory items whose InvStartDate is after a cutoff date (diagnostic for QBO 6270).
Read-only; uses qbo_upload auth. Paginates by default.

Usage:
  python scripts/qbo_queries/qbo_check_inventory_start_dates.py --company company_a --cutoff-date 2026-01-28
  python scripts/qbo_queries/qbo_check_inventory_start_dates.py --company company_a --cutoff-date 2026-01-28 --export-csv reports/inv_start_date_issues_company_a_2026-01-28.csv
  python scripts/qbo_queries/qbo_check_inventory_start_dates.py --company company_a --cutoff-date 2026-01-28 --first-page-only
"""
from __future__ import annotations

import argparse
import csv
import sys
from pathlib import Path
from urllib.parse import quote

_REPO_ROOT = Path(__file__).resolve().parent.parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from load_env import load_env_file
from company_config import load_company_config, get_available_companies
from token_manager import verify_realm_match
from qbo_upload import BASE_URL, _make_qbo_request, TokenManager

load_env_file()


def parse_cutoff_date(s: str) -> str:
    if len(s) != 10 or s[4] != "-" or s[7] != "-":
        raise ValueError(f"cutoff-date must be YYYY-MM-DD, got {s!r}")
    if not (s[:4].isdigit() and s[5:7].isdigit() and s[8:10].isdigit()):
        raise ValueError(f"cutoff-date must be YYYY-MM-DD, got {s!r}")
    return s


def query_inventory_items(
    token_mgr: TokenManager,
    realm_id: str,
    include_inactive: bool,
    maxresults: int,
    fetch_all: bool = True,
) -> list[dict]:
    select_clause = "select Id, Name, Type, TrackQtyOnHand, InvStartDate, Active from Item"
    where = "where Type = 'Inventory'" + ("" if include_inactive else " and Active = true")
    all_items: list[dict] = []
    startposition = 1
    page_size = maxresults
    while True:
        query = f"{select_clause} {where} startposition {startposition} maxresults {page_size}"
        url = f"{BASE_URL}/v3/company/{realm_id}/query?query={quote(query)}&minorversion=70"
        resp = _make_qbo_request("GET", url, token_mgr)
        if resp.status_code != 200:
            raise RuntimeError(f"QBO query failed: HTTP {resp.status_code} - {resp.text[:500]}")
        data = resp.json()
        items = data.get("QueryResponse", {}).get("Item", [])
        if not isinstance(items, list):
            items = [items] if items else []
        all_items.extend(items)
        if not fetch_all or len(items) < page_size:
            break
        startposition += page_size
    return all_items


def filter_issues(items: list[dict], cutoff_date: str) -> list[dict]:
    issues = []
    for item in items:
        inv_start = item.get("InvStartDate")
        if inv_start is None or inv_start == "":
            continue
        inv_date_str = inv_start[:10] if len(str(inv_start)) >= 10 else str(inv_start)
        try:
            if inv_date_str > cutoff_date:
                issues.append({
                    "Id": item.get("Id", ""), "Name": item.get("Name", ""), "Type": item.get("Type", ""),
                    "TrackQtyOnHand": item.get("TrackQtyOnHand", ""), "InvStartDate": inv_date_str, "Active": item.get("Active", ""),
                })
        except (TypeError, ValueError):
            continue
    issues.sort(key=lambda x: x.get("InvStartDate", ""))
    return issues


def main() -> int:
    parser = argparse.ArgumentParser(description="List QBO Inventory items with InvStartDate after cutoff (QBO 6270 diagnostic).")
    parser.add_argument("--company", required=True, choices=get_available_companies(), help="Company key")
    parser.add_argument("--cutoff-date", required=True, metavar="YYYY-MM-DD", help="Cutoff date (YYYY-MM-DD)")
    parser.add_argument("--no-include-inactive", action="store_true", help="Exclude inactive items")
    parser.add_argument("--maxresults", type=int, default=1000, metavar="N", help="Page size (default 1000)")
    parser.add_argument("--export-csv", metavar="PATH", default=None, help="Write CSV of issues")
    parser.add_argument("--first-page-only", action="store_true", help="Only first page (no pagination)")
    args = parser.parse_args()

    try:
        cutoff_date = parse_cutoff_date(args.cutoff_date)
    except ValueError as e:
        print(f"[ERROR] {e}")
        return 1
    if args.maxresults < 1 or args.maxresults > 1000:
        print("[ERROR] --maxresults must be 1..1000")
        return 1

    config = load_company_config(args.company)
    verify_realm_match(args.company, config.realm_id)
    token_mgr = TokenManager(config.company_key, config.realm_id)
    include_inactive = not args.no_include_inactive
    fetch_all = not args.first_page_only

    try:
        items = query_inventory_items(token_mgr, config.realm_id, include_inactive, args.maxresults, fetch_all=fetch_all)
    except RuntimeError as e:
        print(f"[ERROR] {e}")
        return 1

    issues = filter_issues(items, cutoff_date)
    print("--- Summary ---")
    print(f"Total items returned: {len(items)}")
    print(f"Issues (InvStartDate > {cutoff_date}): {len(issues)}")

    if issues:
        print("\n--- Items with InvStartDate after cutoff ---")
        for it in issues[:50]:
            print(f"  Id={it['Id']} Name={it['Name']!r} InvStartDate={it['InvStartDate']} Active={it['Active']}")
        if len(issues) > 50:
            print(f"  ... and {len(issues) - 50} more")
    else:
        print("\nNo issues (no Inventory items with InvStartDate > cutoff).")

    if args.export_csv and issues:
        out_path = Path(args.export_csv)
        if not out_path.is_absolute():
            out_path = _REPO_ROOT / out_path
        out_path.parent.mkdir(parents=True, exist_ok=True)
        with open(out_path, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=["Id", "Name", "Type", "TrackQtyOnHand", "InvStartDate", "Active"])
            w.writeheader()
            w.writerows(issues)
        print(f"\n[INFO] Wrote {len(issues)} row(s) to {out_path}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
