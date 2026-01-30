#!/usr/bin/env python3
"""
Diagnostic: query QBO Items by exact Name to inspect SubItem/ParentRef and parent Category.
Used to verify that newly created Inventory items have ParentRef/SubItem set and parent Type=Category.
Read-only; reuses auth and _make_qbo_request from qbo_upload.py.

Example:
  python scripts/qbo_debug_item_by_name.py --company company_a --name "MARY & MAY 12g"

Expected after run_test_upload (inventory + category):
  Item Type: Inventory
  SubItem: True
  ParentRef.value present
  Parent Type: Category
  Parent Name: COSMETICS AND TOILETRIES (or normalized equivalent)
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
        description="Query QBO Items by Name to inspect SubItem/ParentRef and parent Category item."
    )
    parser.add_argument("--company", required=True, choices=get_available_companies(), help="Company key")
    parser.add_argument("--name", required=True, help="Item name (exact match), e.g. 'MARY & MAY 12g'")
    args = parser.parse_args()

    config = load_company_config(args.company)
    verify_realm_match(args.company, config.realm_id)
    token_mgr = TokenManager(config.company_key, config.realm_id)
    realm_id = config.realm_id

    # QBO Query API does not support SubItem in SELECT; use valid fields only
    safe_name = args.name.strip().replace("'", "''")
    query = (
        "select Id, Name, Type, Active, ParentRef, FullyQualifiedName from Item "
        f"where Name = '{safe_name}' maxresults 10"
    )
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
        item_id = item.get("Id", "")
        print("Item ID:", item_id)
        print("Name:", item.get("Name", ""))
        print("Type:", item.get("Type", ""))
        print("Active:", item.get("Active", ""))
        parent_ref = item.get("ParentRef")
        if parent_ref:
            print("ParentRef (from query):", _ref_str(parent_ref))
            parent_id = parent_ref.get("value") if isinstance(parent_ref, dict) else None
        else:
            parent_id = None
            print("ParentRef (from query):", "")
        if item.get("FullyQualifiedName"):
            print("FullyQualifiedName:", item.get("FullyQualifiedName", ""))

        # GET by Id to read SubItem and full ParentRef (not available in query)
        if item_id:
            get_url = f"{BASE_URL}/v3/company/{realm_id}/item/{item_id}?minorversion=70"
            get_resp = _make_qbo_request("GET", get_url, token_mgr)
            if get_resp.status_code == 200:
                full_item = get_resp.json().get("Item")
                if full_item:
                    print()
                    print("  (from GET by Id:)")
                    print("  SubItem:", full_item.get("SubItem", ""))
                    print("  ParentRef:", _ref_str(full_item.get("ParentRef")))
                    print("  Type:", full_item.get("Type", ""))
                    parent_ref = full_item.get("ParentRef")
                    parent_id = parent_ref.get("value") if isinstance(parent_ref, dict) and parent_ref else None
            else:
                print("  [WARN] GET item by Id failed:", get_resp.status_code)

        if parent_id:
            parent_url = f"{BASE_URL}/v3/company/{realm_id}/item/{parent_id}?minorversion=70"
            parent_resp = _make_qbo_request("GET", parent_url, token_mgr)
            if parent_resp.status_code == 200:
                parent_data = parent_resp.json()
                parent_item = parent_data.get("Item")
                if parent_item:
                    print()
                    print("  Parent item (Category):")
                    print("    Id:", parent_item.get("Id", ""))
                    print("    Name:", parent_item.get("Name", ""))
                    print("    Type:", parent_item.get("Type", ""))
                    print("    Active:", parent_item.get("Active", ""))
                    ptype = (parent_item.get("Type") or "").strip()
                    if ptype != "Category":
                        print("    [WARN] Parent Type is not 'Category'")
                else:
                    print("  [WARN] No Item in parent response")
            else:
                print("  [WARN] Failed to fetch parent item:", parent_resp.status_code)
        else:
            print("  (No ParentRef.value; parent not fetched)")

        print()
        print("--- Raw JSON (query result) ---")
        print(json.dumps(item, indent=2))
        print()


if __name__ == "__main__":
    main()
