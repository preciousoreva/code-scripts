#!/usr/bin/env python3
"""
Get a QBO Item by Id (direct fetch) or search by Name. Read-only; uses qbo_upload auth.

Usage:
  python scripts/qbo_queries/qbo_item_get_by_id.py --company company_a --item-id 9109
  python scripts/qbo_queries/qbo_item_get_by_id.py --company company_a --name "MARY & MAY 12g"
"""
from __future__ import annotations

import argparse
import json
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


def _ref_str(ref: dict | None) -> str:
    if not ref or not isinstance(ref, dict):
        return ""
    name = ref.get("name", "")
    value = ref.get("value", "")
    if name:
        return f"{value} - {name}" if value else name
    return value or ""


def main() -> None:
    parser = argparse.ArgumentParser(description="Get QBO Item by Id or search by Name.")
    parser.add_argument("--company", required=True, choices=get_available_companies(), help="Company key")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--item-id", help="QBO Item Id")
    group.add_argument("--name", help="Search by Name")
    parser.add_argument("--raw-json", action="store_true", help="Print raw JSON")
    args = parser.parse_args()

    config = load_company_config(args.company)
    verify_realm_match(args.company, config.realm_id)
    token_mgr = TokenManager(config.company_key, config.realm_id)
    realm_id = config.realm_id

    if args.name:
        safe_name = args.name.strip().replace("'", "''")
        query = f"select Id, Name, Type, Active from Item where Name = '{safe_name}' maxresults 10"
        url = f"{BASE_URL}/v3/company/{realm_id}/query?query={quote(query)}&minorversion=70"
        resp = _make_qbo_request("GET", url, token_mgr)
        if resp.status_code != 200:
            print(f"HTTP {resp.status_code}", resp.text[:1000])
            sys.exit(1)
        items = resp.json().get("QueryResponse", {}).get("Item", [])
        if not isinstance(items, list):
            items = [items] if items else []
        if not items:
            print(f"No items found with Name = {args.name!r}")
            sys.exit(1)
        print(f"Found {len(items)} item(s):")
        for i, item in enumerate(items, 1):
            print(f"  {i}. Id={item.get('Id')} Name={item.get('Name')!r} Type={item.get('Type')}")
        if args.raw_json:
            print(json.dumps(items, indent=2))
        return

    item_id = args.item_id.strip()
    url = f"{BASE_URL}/v3/company/{realm_id}/item/{item_id}?minorversion=70"
    resp = _make_qbo_request("GET", url, token_mgr)
    if resp.status_code != 200:
        print(f"HTTP {resp.status_code}", resp.text[:1000])
        sys.exit(1)
    item = resp.json().get("Item")
    if not item:
        print("No Item in response.")
        sys.exit(1)

    print("Item ID:", item.get("Id", ""))
    print("Name:", item.get("Name", ""))
    print("Type:", item.get("Type", ""))
    print("Active:", item.get("Active", ""))
    print("TrackQtyOnHand:", item.get("TrackQtyOnHand", ""))
    print("UnitPrice:", item.get("UnitPrice", ""))
    print("PurchaseCost:", item.get("PurchaseCost", ""))
    print("IncomeAccountRef:", _ref_str(item.get("IncomeAccountRef")))
    print("ExpenseAccountRef:", _ref_str(item.get("ExpenseAccountRef")))
    print("AssetAccountRef:", _ref_str(item.get("AssetAccountRef")))
    if args.raw_json:
        print("--- Raw JSON ---")
        print(json.dumps(item, indent=2))


if __name__ == "__main__":
    main()
