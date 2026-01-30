#!/usr/bin/env python3
"""
Look up QBO Item(s) by exact Name. Shows SubItem/ParentRef and FullyQualifiedName.
Read-only; uses auth and _make_qbo_request from qbo_upload.

Usage:
  python scripts/qbo_queries/qbo_item_lookup_by_name.py --company company_a --name "MARY & MAY 12g"
  python scripts/qbo_queries/qbo_item_lookup_by_name.py --company company_a --name "MARY & MAY 12g" --raw-json
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
    parser = argparse.ArgumentParser(description="Look up QBO Item(s) by exact Name (SubItem/ParentRef, FullyQualifiedName).")
    parser.add_argument("--company", required=True, choices=get_available_companies(), help="Company key")
    parser.add_argument("--name", required=True, help="Item name (exact match)")
    parser.add_argument("--raw-json", action="store_true", help="Print raw JSON for each match")
    args = parser.parse_args()

    config = load_company_config(args.company)
    verify_realm_match(args.company, config.realm_id)
    token_mgr = TokenManager(config.company_key, config.realm_id)
    realm_id = config.realm_id

    safe_name = args.name.strip().replace("'", "''")
    query = (
        "select Id, Name, Type, Active, ParentRef, FullyQualifiedName from Item "
        f"where Name = '{safe_name}' maxresults 10"
    )
    url = f"{BASE_URL}/v3/company/{realm_id}/query?query={quote(query)}&minorversion=70"
    resp = _make_qbo_request("GET", url, token_mgr)

    if resp.status_code != 200:
        print(f"HTTP status: {resp.status_code}")
        print(resp.text[:1000])
        sys.exit(1)

    items = resp.json().get("QueryResponse", {}).get("Item", [])
    if not isinstance(items, list):
        items = [items] if items else []
    if not items:
        print(f"No items found with Name = {args.name!r}")
        sys.exit(1)

    print(f"Found {len(items)} item(s) with Name = {args.name!r}:")
    for i, item in enumerate(items, 1):
        print(f"\n--- Match {i} ---")
        item_id = item.get("Id", "")
        print("Item ID:", item_id)
        print("Name:", item.get("Name", ""))
        print("Type:", item.get("Type", ""))
        print("Active:", item.get("Active", ""))
        parent_ref = item.get("ParentRef")
        parent_id = parent_ref.get("value") if isinstance(parent_ref, dict) and parent_ref else None
        if parent_ref:
            print("ParentRef:", _ref_str(parent_ref))
        else:
            print("ParentRef:", "")
        if item.get("FullyQualifiedName"):
            print("FullyQualifiedName:", item.get("FullyQualifiedName", ""))

        if item_id:
            get_url = f"{BASE_URL}/v3/company/{realm_id}/item/{item_id}?minorversion=70"
            get_resp = _make_qbo_request("GET", get_url, token_mgr)
            if get_resp.status_code == 200:
                full_item = get_resp.json().get("Item")
                if full_item:
                    print("  (GET by Id:) SubItem:", full_item.get("SubItem", ""), "ParentRef:", _ref_str(full_item.get("ParentRef")))
                    parent_id = (full_item.get("ParentRef") or {}).get("value") if isinstance(full_item.get("ParentRef"), dict) else None
            if parent_id:
                parent_resp = _make_qbo_request("GET", f"{BASE_URL}/v3/company/{realm_id}/item/{parent_id}?minorversion=70", token_mgr)
                if parent_resp.status_code == 200:
                    parent_item = parent_resp.json().get("Item")
                    if parent_item:
                        print("  Parent (Category):", parent_item.get("Name", ""), "Type:", parent_item.get("Type", ""))

        if args.raw_json:
            print("--- Raw JSON ---")
            print(json.dumps(item, indent=2))


if __name__ == "__main__":
    main()
