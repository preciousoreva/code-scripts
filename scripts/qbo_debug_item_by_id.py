#!/usr/bin/env python3
"""
Diagnostic: fetch a QuickBooks Item by ID (direct entity fetch) or search by Name to inspect full metadata
and identify hidden/system items that block inventory creation.
Read-only; reuses auth and _make_qbo_request from qbo_upload.py.

Example:
  python scripts/qbo_debug_item_by_id.py --company company_a --item-id 9109
  python scripts/qbo_debug_item_by_id.py --company company_a --name "MARY & MAY 12g"
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from urllib.parse import quote

# Run from repo root (parent of scripts/); add repo root for imports
_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from load_env import load_env_file
from company_config import load_company_config, get_available_companies
from token_manager import verify_realm_match
from qbo_upload import BASE_URL, _make_qbo_request, TokenManager

load_env_file()


def _ref_str(ref: dict | None) -> str:
    """Format a Ref dict as 'value - name' or empty string."""
    if not ref or not isinstance(ref, dict):
        return ""
    name = ref.get("name", "")
    value = ref.get("value", "")
    if name:
        return f"{value} - {name}" if value else name
    return value or ""


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Fetch a QBO Item by ID (direct entity fetch) or search by Name to inspect metadata and identify system/hidden items."
    )
    parser.add_argument("--company", required=True, choices=get_available_companies(), help="Company key")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--item-id", help="QBO Item Id (e.g. 9109)")
    group.add_argument("--name", help="Search Item by Name (e.g. 'MARY & MAY 12g')")
    args = parser.parse_args()

    config = load_company_config(args.company)
    verify_realm_match(args.company, config.realm_id)
    token_mgr = TokenManager(config.company_key, config.realm_id)
    realm_id = config.realm_id

    if args.name:
        # Search by Name
        safe_name = args.name.strip().replace("'", "''")
        query = f"select Id, Name, Type, Active from Item where Name = '{safe_name}' maxresults 10"
        url = f"{BASE_URL}/v3/company/{realm_id}/query?query={quote(query)}&minorversion=70"
        resp = _make_qbo_request("GET", url, token_mgr)

        if resp.status_code != 200:
            print(f"HTTP status: {resp.status_code}")
            print(f"Response (first 1000 chars): {resp.text[:1000]}")
            sys.exit(1)

        data = resp.json()
        items = data.get("QueryResponse", {}).get("Item", [])
        if not isinstance(items, list):
            items = [items] if items else []

        if not items:
            print(f"No items found with Name = {args.name!r}")
            sys.exit(1)

        print(f"Found {len(items)} item(s) with Name = {args.name!r}:")
        print()
        for i, item in enumerate(items, 1):
            print(f"--- Match {i} ---")
            print("Item ID:", item.get("Id", ""))
            print("Name:", item.get("Name", ""))
            print("Type:", item.get("Type", ""))
            print("Active:", item.get("Active", ""))
            print()
    else:
        # Fetch by ID
        item_id = args.item_id.strip()
        url = f"{BASE_URL}/v3/company/{realm_id}/item/{item_id}?minorversion=70"
        resp = _make_qbo_request("GET", url, token_mgr)

        if resp.status_code != 200:
            print(f"HTTP status: {resp.status_code}")
            print(f"Response (first 1000 chars): {resp.text[:1000]}")
            sys.exit(1)

        data = resp.json()
        item = data.get("Item")
        if not item:
            print("No Item in response.")
            print(json.dumps(data, indent=2))
            sys.exit(1)

        # Clean summary
        print("Item ID:", item.get("Id", ""))
        print("Name:", item.get("Name", ""))
        print("Type:", item.get("Type", ""))
        print("Active:", item.get("Active", ""))
        print("TrackQtyOnHand:", item.get("TrackQtyOnHand", ""))
        print("UnitPrice:", item.get("UnitPrice", ""))
        print("PurchaseCost:", item.get("PurchaseCost", ""))
        print("SalesTaxIncluded:", item.get("SalesTaxIncluded", ""))
        print("PurchaseTaxIncluded:", item.get("PurchaseTaxIncluded", ""))
        print("Taxable:", item.get("Taxable", ""))
        sales_tax_ref = item.get("SalesTaxCodeRef")
        print("SalesTaxCodeRef:", _ref_str(sales_tax_ref) if sales_tax_ref else "")
        purchase_tax_ref = item.get("PurchaseTaxCodeRef")
        print("PurchaseTaxCodeRef:", _ref_str(purchase_tax_ref) if purchase_tax_ref else "")
        inc = item.get("IncomeAccountRef")
        print("IncomeAccount:", _ref_str(inc) if inc else "")
        exp = item.get("ExpenseAccountRef")
        if exp:
            print("ExpenseAccountRef:", _ref_str(exp))
        asset = item.get("AssetAccountRef")
        if asset:
            print("AssetAccountRef:", _ref_str(asset))
        print("Domain:", item.get("domain", ""))
        if "sparse" in item:
            print("Sparse:", item.get("sparse"))

        item_type = (item.get("Type") or "").strip()
        if item_type and item_type != "Inventory":
            print()
            print("This item is a non-inventory Service/item type and will block inventory creation with the same name.")

        print()
        print("--- Raw JSON ---")
        print(json.dumps(item, indent=2))


if __name__ == "__main__":
    main()
