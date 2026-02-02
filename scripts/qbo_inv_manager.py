#!/usr/bin/env python3
"""
QBO Inventory manager: get item, list InvStartDate issues, set InvStartDate (single, bulk, or from CSV).

Run from repo root. All subcommands require --company.

Subcommands:
  get              Get item by ID or search by name
  list-invstart    List inventory items with InvStartDate after cutoff (optional export CSV)
  set-invstart     Set InvStartDate for one item to a date you choose
  set-invstart-bulk Find all items with InvStartDate > cutoff, patch each to new date
  set-invstart-from-csv  Patch only items listed in a CSV (e.g. blockers file)
  export-products  Export all active products (Items) to CSV
  import-products  Create QBO Items from a products CSV (uses Product.Mapping for accounts)
  inactivate-all   Set all active products (Items) to inactive; use before re-creating from a fresh list

Examples:
  python scripts/qbo_inv_manager.py --company company_a get --item-id 7220
  python scripts/qbo_inv_manager.py --company company_a get --name "NAN-OPTIPRO"
  python scripts/qbo_inv_manager.py --company company_a list-invstart --cutoff-date 2026-01-01 --export-csv reports/issues.csv
  python scripts/qbo_inv_manager.py --company company_a set-invstart --item-id 7220 --date 2026-01-01
  python scripts/qbo_inv_manager.py --company company_a set-invstart-bulk --cutoff-date 2026-01-01 --new-date 2026-01-01
  python scripts/qbo_inv_manager.py --company company_a set-invstart-from-csv --csv reports/inventory_start_date_blockers_company_a_2026-01-01.csv --new-date 2026-01-01
  python scripts/qbo_inv_manager.py --company company_a export-products --out exports/company_a_products.csv
  python scripts/qbo_inv_manager.py --company company_a export-products --out exports/company_a_inventory.csv --type Inventory
  python scripts/qbo_inv_manager.py --company company_a inactivate-all --dry-run
  python scripts/qbo_inv_manager.py --company company_a inactivate-all --type Inventory --report-csv reports/inactivated_company_a.csv
  python scripts/qbo_inv_manager.py --company company_a import-products --csv exports/company_a_products.csv --dry-run
  python scripts/qbo_inv_manager.py --company company_a import-products --csv exports/company_a_products.csv --as-of-date 2026-01-01 --default-qty 10 --report-csv reports/imported_products.csv
  python scripts/qbo_inv_manager.py --company company_a import-products --csv exports/company_a_products.csv --as-of-date 2026-01-01 --create
  python scripts/qbo_inv_manager.py --company company_a import-products --csv exports/company_a_products.csv --inventory-only --dry-run
"""
from __future__ import annotations

import argparse
import csv
import json
import os
import re
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
from qbo_upload import (
    BASE_URL,
    _make_qbo_request,
    TokenManager,
    patch_item_inv_start_date,
    rename_and_inactivate_item,
    create_inventory_item_from_existing,
    load_category_account_mapping,
    build_account_refs_for_category,
    get_or_create_item_category_id,
)
from qbo_items import load_blocker_item_ids_from_csv
from slack_notify import notify_import_start, notify_import_success, notify_import_failure

load_env_file()

MINORVERSION = "70"


def _get_taxcode_id_by_name(
    name: str,
    token_mgr: TokenManager,
    realm_id: str,
    cache: Dict[str, Optional[str]],
) -> Optional[str]:
    """Resolve TaxCode name to Id via QBO query; cache by name."""
    if not name or not name.strip():
        return None
    name_clean = name.strip()
    if name_clean in cache:
        return cache[name_clean]
    query = "select Id, Name from TaxCode where Active = true"
    url = f"{BASE_URL}/v3/company/{realm_id}/query?query={quote(query)}&minorversion={MINORVERSION}"
    resp = _make_qbo_request("GET", url, token_mgr)
    if resp.status_code != 200:
        cache[name_clean] = None
        return None
    data = resp.json()
    tax_codes = data.get("QueryResponse", {}).get("TaxCode") or []
    if not isinstance(tax_codes, list):
        tax_codes = [tax_codes] if tax_codes else []
    name_to_id: Dict[str, str] = {}
    for tc in tax_codes:
        tid = tc.get("Id")
        tname = (tc.get("Name") or "").strip()
        if tid and tname:
            name_to_id[tname] = str(tid)
    for k, v in name_to_id.items():
        cache.setdefault(k, v)
    tax_code_id = name_to_id.get(name_clean)
    if tax_code_id is None:
        name_lower = name_clean.lower()
        for k, v in name_to_id.items():
            if k.lower() == name_lower:
                tax_code_id = v
                break
    if tax_code_id is None:
        for k, v in name_to_id.items():
            if name_clean in k or k in name_clean:
                tax_code_id = v
                break
    cache[name_clean] = tax_code_id
    if tax_code_id:
        print(f"[INFO] TaxCode {name_clean!r} -> Id {tax_code_id}")
    return tax_code_id


def _ref_str(ref: Optional[Dict]) -> str:
    if not ref or not isinstance(ref, dict):
        return ""
    name = ref.get("name", "")
    value = ref.get("value", "")
    if name:
        return f"{value} - {name}" if value else name
    return value or ""


def _query_all_items_paginated(
    token_mgr: TokenManager,
    realm_id: str,
    active_only: bool = True,
    type_filter: Optional[str] = None,
    maxresults: int = 1000,
) -> List[Dict[str, Any]]:
    """Query all Items (optionally active only and/or by Type); paginate. Returns list of Item dicts."""
    where_parts: List[str] = []
    if active_only:
        where_parts.append("Active = true")
    if type_filter:
        where_parts.append(f"Type = '{type_filter}'")
    where = "where " + " and ".join(where_parts) if where_parts else ""
    # Query API returns limited Item fields; refs/Description often require GET per item
    select = (
        "select Id, Name, Type, Active, FullyQualifiedName, ParentRef, TrackQtyOnHand, QtyOnHand, "
        "InvStartDate, UnitPrice, PurchaseCost from Item"
    )
    all_items: List[Dict[str, Any]] = []
    startposition = 1
    while True:
        query = f"{select} {where} startposition {startposition} maxresults {maxresults}"
        url = f"{BASE_URL}/v3/company/{realm_id}/query?query={quote(query)}&minorversion={MINORVERSION}"
        resp = _make_qbo_request("GET", url, token_mgr)
        if resp.status_code != 200:
            raise RuntimeError(f"QBO query failed: HTTP {resp.status_code} - {resp.text[:500]}")
        data = resp.json()
        items = data.get("QueryResponse", {}).get("Item", [])
        if not isinstance(items, list):
            items = [items] if items else []
        all_items.extend(items)
        if len(items) < maxresults:
            break
        startposition += maxresults
    return all_items


def _item_to_export_row(item: Dict[str, Any]) -> Dict[str, Any]:
    """Flatten one Item dict to a CSV row (refs as _Value, _Name)."""
    def ref_val(ref: Any) -> str:
        if not isinstance(ref, dict):
            return ""
        return str(ref.get("value") or "").strip()

    def ref_name(ref: Any) -> str:
        if not isinstance(ref, dict):
            return ""
        return str(ref.get("name") or "").strip()

    inv = item.get("InvStartDate")
    inv_str = inv[:10] if inv and len(str(inv)) >= 10 else str(inv or "")
    return {
        "Id": str(item.get("Id") or ""),
        "Name": str(item.get("Name") or ""),
        "Type": str(item.get("Type") or ""),
        "Active": str(item.get("Active")),
        "FullyQualifiedName": str(item.get("FullyQualifiedName") or ""),
        "ParentRef_Value": ref_val(item.get("ParentRef")),
        "ParentRef_Name": ref_name(item.get("ParentRef")),
        "TrackQtyOnHand": str(item.get("TrackQtyOnHand")),
        "QtyOnHand": str(item.get("QtyOnHand") or ""),
        "InvStartDate": inv_str,
        "UnitPrice": str(item.get("UnitPrice") or ""),
        "PurchaseCost": str(item.get("PurchaseCost") or ""),
        "IncomeAccountRef_Value": ref_val(item.get("IncomeAccountRef")),
        "IncomeAccountRef_Name": ref_name(item.get("IncomeAccountRef")),
        "ExpenseAccountRef_Value": ref_val(item.get("ExpenseAccountRef")),
        "ExpenseAccountRef_Name": ref_name(item.get("ExpenseAccountRef")),
        "AssetAccountRef_Value": ref_val(item.get("AssetAccountRef")),
        "AssetAccountRef_Name": ref_name(item.get("AssetAccountRef")),
        "Description": str(item.get("Description") or ""),
        "PurchaseDesc": str(item.get("PurchaseDesc") or ""),
    }


def _query_active_items_for_inactivate(
    token_mgr: TokenManager,
    realm_id: str,
    type_filter: Optional[str] = None,
    maxresults: int = 1000,
    limit: Optional[int] = None,
) -> List[Dict[str, Any]]:
    """Query all active Items (optionally by Type) for inactivate-all. Returns list with Id, Name, Type, SyncToken. Fetches SyncToken via GET per item (QBO query may not return SyncToken)."""
    where_parts: List[str] = ["Active = true"]
    if type_filter:
        where_parts.append(f"Type = '{type_filter}'")
    where = " and ".join(where_parts)
    select = "select Id, Name, Type from Item"
    all_items: List[Dict[str, Any]] = []
    startposition = 1
    while True:
        query = f"{select} where {where} startposition {startposition} maxresults {maxresults}"
        url = f"{BASE_URL}/v3/company/{realm_id}/query?query={quote(query)}&minorversion={MINORVERSION}"
        resp = _make_qbo_request("GET", url, token_mgr)
        if resp.status_code != 200:
            raise RuntimeError(f"QBO query failed: HTTP {resp.status_code} - {resp.text[:500]}")
        data = resp.json()
        items = data.get("QueryResponse", {}).get("Item", [])
        if not isinstance(items, list):
            items = [items] if items else []
        for it in items:
            if limit and len(all_items) >= limit:
                break
            rid = it.get("Id")
            if rid:
                get_url = f"{BASE_URL}/v3/company/{realm_id}/item/{rid}?minorversion={MINORVERSION}"
                get_resp = _make_qbo_request("GET", get_url, token_mgr)
                if get_resp.status_code == 200:
                    full = get_resp.json().get("Item", {})
                    it["SyncToken"] = full.get("SyncToken")
            all_items.append(it)
            if limit and len(all_items) >= limit:
                break
        if len(items) < maxresults:
            break
        startposition += maxresults
        if limit and len(all_items) >= limit:
            break
    return all_items


def _inactivate_item(
    token_mgr: TokenManager,
    realm_id: str,
    item_id: str,
    sync_token: str,
) -> Tuple[bool, str]:
    """Set one Item to Active=False (sparse update). Returns (success, error_message)."""
    url = f"{BASE_URL}/v3/company/{realm_id}/item?minorversion={MINORVERSION}"
    payload = {"sparse": True, "Id": item_id, "SyncToken": sync_token, "Active": False}
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


def _query_inventory_items_paginated(
    token_mgr: TokenManager,
    realm_id: str,
    include_inactive: bool,
    maxresults: int = 1000,
) -> List[Dict[str, Any]]:
    """Query all Inventory items (Type=Inventory); paginate. Returns list of Item dicts."""
    where = "where Type = 'Inventory'" + ("" if include_inactive else " and Active = true")
    all_items: List[Dict[str, Any]] = []
    startposition = 1
    while True:
        query = f"select Id, Name, Type, TrackQtyOnHand, InvStartDate, Active from Item {where} startposition {startposition} maxresults {maxresults}"
        url = f"{BASE_URL}/v3/company/{realm_id}/query?query={quote(query)}&minorversion={MINORVERSION}"
        resp = _make_qbo_request("GET", url, token_mgr)
        if resp.status_code != 200:
            raise RuntimeError(f"QBO query failed: HTTP {resp.status_code} - {resp.text[:500]}")
        data = resp.json()
        items = data.get("QueryResponse", {}).get("Item", [])
        if not isinstance(items, list):
            items = [items] if items else []
        all_items.extend(items)
        if len(items) < maxresults:
            break
        startposition += maxresults
    return all_items


def _filter_invstart_issues(items: List[Dict[str, Any]], cutoff_date: str) -> List[Dict[str, Any]]:
    """Filter to items with InvStartDate > cutoff_date (YYYY-MM-DD)."""
    issues: List[Dict[str, Any]] = []
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


def _parse_date(s: str) -> str:
    if len(s) != 10 or s[4] != "-" or s[7] != "-":
        raise ValueError(f"Date must be YYYY-MM-DD, got {s!r}")
    if not (s[:4].isdigit() and s[5:7].isdigit() and s[8:10].isdigit()):
        raise ValueError(f"Date must be YYYY-MM-DD, got {s!r}")
    return s[:10]


def cmd_get(args: argparse.Namespace, token_mgr: TokenManager, realm_id: str) -> int:
    """Get item by ID or search by name."""
    if args.item_id:
        url = f"{BASE_URL}/v3/company/{realm_id}/item/{args.item_id.strip()}?minorversion={MINORVERSION}"
        resp = _make_qbo_request("GET", url, token_mgr)
        if resp.status_code != 200:
            print(f"HTTP {resp.status_code}", resp.text[:1000], file=sys.stderr)
            return 1
        item = resp.json().get("Item")
        if not item:
            print("No Item in response.", file=sys.stderr)
            return 1
        items = [item]
    else:
        safe_name = (args.name or "").strip().replace("'", "''")
        query = f"select Id, Name, Type, Active, ParentRef, FullyQualifiedName, TrackQtyOnHand, InvStartDate, UnitPrice, PurchaseCost, IncomeAccountRef, ExpenseAccountRef, AssetAccountRef from Item where Name = '{safe_name}' maxresults 10"
        url = f"{BASE_URL}/v3/company/{realm_id}/query?query={quote(query)}&minorversion={MINORVERSION}"
        resp = _make_qbo_request("GET", url, token_mgr)
        if resp.status_code != 200:
            print(f"HTTP {resp.status_code}", resp.text[:1000], file=sys.stderr)
            return 1
        items = resp.json().get("QueryResponse", {}).get("Item", [])
        if not isinstance(items, list):
            items = [items] if items else []
        if not items:
            print(f"No items found with Name = {args.name!r}", file=sys.stderr)
            return 1

    for i, item in enumerate(items, 1):
        print(f"--- Item {i} ---")
        print("Id:", item.get("Id", ""))
        print("Name:", item.get("Name", ""))
        print("Type:", item.get("Type", ""))
        print("Active:", item.get("Active", ""))
        print("ParentRef:", _ref_str(item.get("ParentRef")))
        print("FullyQualifiedName:", item.get("FullyQualifiedName", ""))
        print("TrackQtyOnHand:", item.get("TrackQtyOnHand", ""))
        print("InvStartDate:", item.get("InvStartDate", ""))
        print("UnitPrice:", item.get("UnitPrice", ""))
        print("PurchaseCost:", item.get("PurchaseCost", ""))
        print("IncomeAccountRef:", _ref_str(item.get("IncomeAccountRef")))
        print("ExpenseAccountRef:", _ref_str(item.get("ExpenseAccountRef")))
        print("AssetAccountRef:", _ref_str(item.get("AssetAccountRef")))
        if getattr(args, "raw_json", False):
            print("--- Raw JSON ---")
            print(json.dumps(item, indent=2))
    return 0


def cmd_list_invstart(args: argparse.Namespace, token_mgr: TokenManager, realm_id: str) -> int:
    """List inventory items with InvStartDate > cutoff."""
    try:
        cutoff_date = _parse_date(args.cutoff_date)
    except ValueError as e:
        print(f"[ERROR] {e}", file=sys.stderr)
        return 1

    include_inactive = not getattr(args, "no_include_inactive", False)
    try:
        items = _query_inventory_items_paginated(token_mgr, realm_id, include_inactive, args.maxresults)
    except RuntimeError as e:
        print(f"[ERROR] {e}", file=sys.stderr)
        return 1

    issues = _filter_invstart_issues(items, cutoff_date)
    print("--- Summary ---")
    print(f"Total inventory items: {len(items)}")
    print(f"Issues (InvStartDate > {cutoff_date}): {len(issues)}")

    if issues:
        print("\n--- Items with InvStartDate after cutoff ---")
        for it in issues[:50]:
            print(f"  Id={it['Id']} Name={it['Name']!r} InvStartDate={it['InvStartDate']} Active={it['Active']}")
        if len(issues) > 50:
            print(f"  ... and {len(issues) - 50} more")
    else:
        print("\nNo issues (no Inventory items with InvStartDate > cutoff).")

    if getattr(args, "export_csv", None) and issues:
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


def cmd_set_invstart(args: argparse.Namespace, token_mgr: TokenManager, realm_id: str) -> int:
    """Set InvStartDate for one item."""
    try:
        new_date = _parse_date(args.date)
    except ValueError as e:
        print(f"[ERROR] {e}", file=sys.stderr)
        return 1

    item_id = (args.item_id or "").strip()
    if not item_id:
        print("[ERROR] --item-id is required", file=sys.stderr)
        return 1

    success, old_actual, err_msg = patch_item_inv_start_date(token_mgr, realm_id, item_id, new_date)
    if success:
        print(f"[INFO] Item {item_id} InvStartDate set to {new_date}" + (f" (was {old_actual})" if old_actual else ""))
        return 0
    print(f"[ERROR] {err_msg}", file=sys.stderr)
    return 1


def cmd_set_invstart_bulk(args: argparse.Namespace, token_mgr: TokenManager, realm_id: str) -> int:
    """Find all items with InvStartDate > cutoff, patch each to new_date."""
    try:
        cutoff_date = _parse_date(args.cutoff_date)
        new_date = _parse_date(args.new_date)
    except ValueError as e:
        print(f"[ERROR] {e}", file=sys.stderr)
        return 1

    include_inactive = not getattr(args, "no_include_inactive", False)
    try:
        items = _query_inventory_items_paginated(token_mgr, realm_id, include_inactive, args.maxresults)
    except RuntimeError as e:
        print(f"[ERROR] {e}", file=sys.stderr)
        return 1

    issues = _filter_invstart_issues(items, cutoff_date)
    print(f"[INFO] Found {len(issues)} item(s) with InvStartDate > {cutoff_date}; patching to {new_date}")

    if not issues:
        print("Nothing to patch.")
        return 0

    reports_dir = _REPO_ROOT / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)
    safe_key = re.sub(r"[^\w-]", "_", args.company)
    patch_report_path = reports_dir / f"items_patched_invstartdate_{safe_key}_{cutoff_date}.csv"
    patched_report: List[Dict[str, Any]] = []
    any_failed = False

    for item in issues:
        item_id = item.get("Id", "")
        name = item.get("Name", "")
        old_date = item.get("InvStartDate", "")
        success, old_actual, err_msg = patch_item_inv_start_date(token_mgr, realm_id, item_id, new_date)
        status = "ok" if success else "failed"
        patched_report.append({
            "ItemId": item_id,
            "ItemName": name,
            "OldInvStartDate": old_actual or old_date,
            "NewInvStartDate": new_date,
            "Status": status,
            "Error": err_msg or "",
        })
        if success:
            print(f"  [OK] {item_id} {name!r} -> {new_date}")
        else:
            any_failed = True
            print(f"  [FAIL] {item_id} {name!r}: {err_msg}", file=sys.stderr)

    with open(patch_report_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["ItemId", "ItemName", "OldInvStartDate", "NewInvStartDate", "Status", "Error"])
        w.writeheader()
        w.writerows(patched_report)
    print(f"[INFO] Wrote patch report: {patch_report_path}")

    return 1 if any_failed else 0


def cmd_set_invstart_from_csv(args: argparse.Namespace, token_mgr: TokenManager, realm_id: str) -> int:
    """Patch InvStartDate for all item IDs listed in a CSV (e.g. blockers file)."""
    try:
        new_date = _parse_date(args.new_date)
    except ValueError as e:
        print(f"[ERROR] {e}", file=sys.stderr)
        return 1

    csv_path = Path(args.csv)
    if not csv_path.is_absolute():
        csv_path = _REPO_ROOT / csv_path
    if not csv_path.exists():
        print(f"[ERROR] CSV not found: {csv_path}", file=sys.stderr)
        return 1

    item_ids = load_blocker_item_ids_from_csv(csv_path)
    if not item_ids:
        print("[ERROR] No ItemId column or no rows in CSV", file=sys.stderr)
        return 1

    print(f"[INFO] Patching {len(item_ids)} item(s) from {csv_path} to InvStartDate={new_date}")

    reports_dir = _REPO_ROOT / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)
    safe_key = re.sub(r"[^\w-]", "_", args.company)
    patch_report_path = reports_dir / f"items_patched_invstartdate_from_csv_{safe_key}_{new_date.replace('-', '')}.csv"
    patched_report: List[Dict[str, Any]] = []
    any_failed = False

    for item_id in sorted(item_ids):
        success, old_actual, err_msg = patch_item_inv_start_date(token_mgr, realm_id, item_id, new_date)
        status = "ok" if success else "failed"
        patched_report.append({
            "ItemId": item_id,
            "OldInvStartDate": old_actual or "",
            "NewInvStartDate": new_date,
            "Status": status,
            "Error": err_msg or "",
        })
        if success:
            print(f"  [OK] {item_id} -> {new_date}")
        else:
            any_failed = True
            print(f"  [FAIL] {item_id}: {err_msg}", file=sys.stderr)

    with open(patch_report_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["ItemId", "OldInvStartDate", "NewInvStartDate", "Status", "Error"])
        w.writeheader()
        w.writerows(patched_report)
    print(f"[INFO] Wrote patch report: {patch_report_path}")

    return 1 if any_failed else 0


def cmd_recreate_invstart(args: argparse.Namespace, token_mgr: TokenManager, realm_id: str) -> int:
    """
    Inactivate the existing item (rename to 'Name (old-Id)'), then create a new Inventory item
    with the same details and the given InvStartDate. Use when API patch does not update the UI.
    New item gets a new Id; update mappings if needed.
    """
    try:
        new_date = _parse_date(args.date)
    except ValueError as e:
        print(f"[ERROR] {e}", file=sys.stderr)
        return 1

    item_id = (args.item_id or "").strip()
    if not item_id:
        print("[ERROR] --item-id is required", file=sys.stderr)
        return 1

    # GET current item (keep copy for create payload; we need original Name and all fields)
    get_url = f"{BASE_URL}/v3/company/{realm_id}/item/{item_id}?minorversion={MINORVERSION}"
    resp = _make_qbo_request("GET", get_url, token_mgr)
    if resp.status_code != 200:
        print(f"[ERROR] GET item {item_id} failed: HTTP {resp.status_code}", resp.text[:500], file=sys.stderr)
        return 1
    data = resp.json()
    existing_item = data.get("Item")
    if not existing_item:
        print("[ERROR] No Item in response", file=sys.stderr)
        return 1
    if (existing_item.get("Type") or "").strip() != "Inventory":
        print(f"[ERROR] Item {item_id} is not Type=Inventory (Type={existing_item.get('Type')})", file=sys.stderr)
        return 1
    if not existing_item.get("Active", True):
        print("[ERROR] Item is inactive; recreate-invstart is only for active items. Use an active item Id.", file=sys.stderr)
        return 1

    name = (existing_item.get("Name") or "").strip() or f"Item-{item_id}"
    # Base name for new item: strip " (old-...)" and " (deleted)" so the new item gets a clean name
    base_name = name.split(" (old-")[0].strip() if " (old-" in name else name
    base_name = re.sub(r"\s*\(deleted\)\s*$", "", base_name, flags=re.IGNORECASE).strip() or base_name
    legacy_name = f"{base_name} (old-{item_id})"

    print(f"[INFO] Inactivating item {item_id} and renaming to {legacy_name!r}")
    try:
        rename_and_inactivate_item(token_mgr, realm_id, item_id, legacy_name, make_inactive=True)
    except RuntimeError as e:
        print(f"[ERROR] Rename/inactivate failed: {e}", file=sys.stderr)
        return 1

    print(f"[INFO] Creating new Inventory item with Name={base_name!r} and InvStartDate={new_date}")
    # Use base name for new item (in case existing was already renamed)
    existing_for_create = {**existing_item, "Name": base_name}
    try:
        new_id = create_inventory_item_from_existing(token_mgr, realm_id, existing_for_create, new_date)
    except RuntimeError as e:
        print(f"[ERROR] Create from existing failed: {e}", file=sys.stderr)
        return 1

    print(f"[INFO] Old item {item_id} inactivated (renamed to {legacy_name!r}). New item created: Id={new_id}, Name={base_name!r}, InvStartDate={new_date}")
    return 0


EXPORT_PRODUCTS_FIELDS = [
    "Id",
    "Name",
    "Type",
    "Active",
    "FullyQualifiedName",
    "ParentRef_Value",
    "ParentRef_Name",
    "TrackQtyOnHand",
    "QtyOnHand",
    "InvStartDate",
    "UnitPrice",
    "PurchaseCost",
    "IncomeAccountRef_Value",
    "IncomeAccountRef_Name",
    "ExpenseAccountRef_Value",
    "ExpenseAccountRef_Name",
    "AssetAccountRef_Value",
    "AssetAccountRef_Name",
    "Description",
    "PurchaseDesc",
]


def cmd_export_products(args: argparse.Namespace, token_mgr: TokenManager, realm_id: str) -> int:
    """Export all active products (Items) from QBO to a CSV."""
    out_path = Path(args.out)
    if not out_path.is_absolute():
        out_path = _REPO_ROOT / out_path
    out_path.parent.mkdir(parents=True, exist_ok=True)

    active_only = not getattr(args, "include_inactive", False)
    type_filter = getattr(args, "type_filter", None)
    maxresults = getattr(args, "maxresults", 1000)

    print(f"[INFO] Querying QBO Items (active_only={active_only}, type={type_filter or 'all'})...")
    try:
        items = _query_all_items_paginated(
            token_mgr, realm_id, active_only=active_only, type_filter=type_filter, maxresults=maxresults
        )
    except RuntimeError as e:
        print(f"[ERROR] {e}", file=sys.stderr)
        return 1

    rows = [_item_to_export_row(it) for it in items]
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=EXPORT_PRODUCTS_FIELDS, extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)

    print(f"[INFO] Exported {len(rows)} item(s) to {out_path}")
    return 0


def _import_row_val(row: Dict[str, Any], key: str, default: Optional[str] = None) -> Optional[str]:
    v = row.get(key)
    if v is None or (isinstance(v, float) and str(v) == "nan"):
        return default
    s = str(v).strip()
    return s if s else default


def _import_row_float(row: Dict[str, Any], key: str) -> Optional[float]:
    v = row.get(key)
    if v is None or (isinstance(v, float) and str(v) == "nan") or str(v).strip() == "":
        return None
    try:
        return float(v)
    except (ValueError, TypeError):
        return None


def _import_row_int(row: Dict[str, Any], key: str) -> Optional[int]:
    v = row.get(key)
    if v is None or (isinstance(v, float) and str(v) == "nan") or str(v).strip() == "":
        return None
    try:
        return int(float(v))
    except (ValueError, TypeError):
        return None


def cmd_import_products(args: argparse.Namespace, token_mgr: TokenManager, realm_id: str) -> int:
    """Create QBO Inventory Items from a products CSV. Uses Product.Mapping for accounts; optional As of Date, default QtyOnHand, tax code."""
    config = load_company_config(args.company)
    csv_path = Path(getattr(args, "csv", "") or (_REPO_ROOT / "exports" / f"{config.company_key}_products.csv"))
    if not csv_path.is_absolute():
        csv_path = _REPO_ROOT / csv_path
    if not csv_path.exists():
        print(f"[ERROR] CSV not found: {csv_path}", file=sys.stderr)
        return 1

    dry_run = getattr(args, "dry_run", False)
    report_csv_path = getattr(args, "report_csv", None)
    as_of_date = getattr(args, "as_of_date", None)
    if as_of_date and len(as_of_date) > 10:
        as_of_date = as_of_date[:10]
    default_qty = getattr(args, "default_qty", 10)
    force_inventory = not getattr(args, "inventory_only", False)  # default: treat all non-Category as Inventory
    taxcode_id_arg = getattr(args, "taxcode_id", None)
    taxcode_name_arg = getattr(args, "taxcode_name", None)
    if taxcode_name_arg is not None:
        taxcode_name_arg = (str(taxcode_name_arg).strip() or None)

    try:
        mapping_cache = load_category_account_mapping(config)
    except (FileNotFoundError, ValueError) as e:
        print(f"[ERROR] {e}", file=sys.stderr)
        return 1

    account_cache: Dict[str, Optional[str]] = {}
    category_cache: Dict[str, str] = {}
    taxcode_cache: Dict[str, Optional[str]] = {}

    # Resolve TaxCode once: same precedence as qbo_upload (sales receipt / create_inventory_item)
    # 1) --taxcode-id 2) --taxcode-name 3) config.tax_code_id 4) lookup config.tax_code_name 5) fallback by company
    tax_code_id: Optional[str] = None
    if taxcode_id_arg and str(taxcode_id_arg).strip():
        tax_code_id = str(taxcode_id_arg).strip()
        print(f"[INFO] Using TaxCode Id from --taxcode-id: {tax_code_id}")
    elif taxcode_name_arg:
        tax_code_id = _get_taxcode_id_by_name(taxcode_name_arg, token_mgr, realm_id, taxcode_cache)
        if not tax_code_id and not dry_run:
            print(f"[WARN] TaxCode {taxcode_name_arg!r} not found; creating items without tax refs", file=sys.stderr)
    else:
        tax_code_id = config.tax_code_id
        if not tax_code_id and getattr(config, "tax_code_name", None):
            tax_code_id = _get_taxcode_id_by_name(config.tax_code_name, token_mgr, realm_id, taxcode_cache)
        if not tax_code_id:
            tax_code_id = "2" if config.company_key == "company_a" else "22"
            print(f"[INFO] Using TaxCode Id from config/fallback: {tax_code_id} (tax inclusive, same as sales receipt / product creation)")
        else:
            print(f"[INFO] Using TaxCode Id from company config: {tax_code_id}")

    if as_of_date:
        print(f"[INFO] As of Date (InvStartDate) for all items: {as_of_date}")
    print(f"[INFO] Default QtyOnHand: {default_qty}")
    if force_inventory:
        print("[INFO] All non-Category rows will be created as Inventory (use --inventory-only to process only CSV rows with Type=Inventory)")
    else:
        print("[INFO] Inventory-only: only CSV rows with Type=Inventory will be processed")

    with open(csv_path, "r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    if not rows:
        print("[INFO] CSV is empty.")
        return 0

    if not dry_run and config.slack_webhook_url:
        notify_import_start("products", config.company_key, {"total": len(rows)}, config.slack_webhook_url)

    created = 0
    skipped = 0
    failed = 0
    report: List[Dict[str, Any]] = []
    create_url = f"{BASE_URL}/v3/company/{realm_id}/item?minorversion={MINORVERSION}"

    try:
        for i, row in enumerate(rows):
            name = _import_row_val(row, "Name")
            if not name:
                report.append({"Name": "", "Type": _import_row_val(row, "Type"), "Status": "skipped", "NewId": "", "Error": "Missing Name"})
                skipped += 1
                continue

            itype = ( _import_row_val(row, "Type") or "" ).strip()
            if itype == "Category":
                report.append({"Name": name, "Type": itype, "Status": "skipped", "NewId": "", "Error": "Category rows are not created on import"})
                skipped += 1
                continue

            # When not --inventory-only, treat every non-Category row as Inventory
            if force_inventory:
                itype = "Inventory"

            # Only create Inventory items (skip Service/NonInventory when --inventory-only)
            if itype != "Inventory":
                report.append({"Name": name, "Type": itype, "Status": "skipped", "NewId": "", "Error": "Only Inventory rows in CSV processed (omit --inventory-only to create all as Inventory)"})
                skipped += 1
                continue

            category = _import_row_val(row, "ParentRef_Name")
            if not category or not str(category).strip():
                report.append({"Name": name, "Type": itype, "Status": "skipped", "NewId": "", "Error": "No category (ParentRef_Name empty); skipped"})
                skipped += 1
                continue
            if category not in mapping_cache:
                report.append({"Name": name, "Type": itype, "Status": "skipped", "NewId": "", "Error": f"Category {category!r} not in Product.Mapping.csv; skipped"})
                skipped += 1
                continue

            try:
                account_refs = build_account_refs_for_category(
                    category, mapping_cache, account_cache, token_mgr, realm_id, config
                )
            except ValueError as e:
                report.append({"Name": name, "Type": itype, "Status": "failed", "NewId": "", "Error": str(e)})
                failed += 1
                continue

            # InvStartDate: --as-of-date overrides CSV then config
            inv_start = as_of_date or _import_row_val(row, "InvStartDate") or config.inventory_start_date
            if len(inv_start) > 10:
                inv_start = inv_start[:10]
            unit_price = _import_row_float(row, "UnitPrice")
            purchase_cost = _import_row_float(row, "PurchaseCost")
            qty = _import_row_int(row, "QtyOnHand")
            if qty is None:
                qty = default_qty
            qty = max(10, qty)  # never 0 or less than 10 for import-products
            try:
                category_id = get_or_create_item_category_id(token_mgr, realm_id, category, cache=category_cache)
            except (ValueError, RuntimeError) as e:
                report.append({"Name": name, "Type": itype, "Status": "failed", "NewId": "", "Error": str(e)})
                failed += 1
                continue
            payload: Dict[str, Any] = {
                "Name": name,
                "Type": "Inventory",
                "TrackQtyOnHand": True,
                "QtyOnHand": qty,
                "InvStartDate": inv_start,
                "UnitPrice": unit_price if unit_price is not None else 0,
                "PurchaseCost": purchase_cost if purchase_cost is not None else 0,
                "IncomeAccountRef": account_refs["IncomeAccountRef"],
                "ExpenseAccountRef": account_refs["ExpenseAccountRef"],
                "AssetAccountRef": account_refs["AssetAccountRef"],
                "Description": _import_row_val(row, "Description") or f"Sale(s) of {name}",
                "PurchaseDesc": _import_row_val(row, "PurchaseDesc") or f"Purchase of {name}",
                "SubItem": True,
                "ParentRef": {"value": category_id},
            }
            if tax_code_id:
                payload["SalesTaxCodeRef"] = {"value": tax_code_id}
                payload["PurchaseTaxCodeRef"] = {"value": tax_code_id}
                payload["SalesTaxIncluded"] = True
                payload["PurchaseTaxIncluded"] = True
                payload["Taxable"] = True
            if dry_run:
                print(f"[DRY-RUN] Would create Inventory: {name!r} category={category!r} InvStartDate={inv_start}")
                report.append({"Name": name, "Type": itype, "Status": "dry-run", "NewId": "", "Error": ""})
                created += 1
                continue
            resp = _make_qbo_request("POST", create_url, token_mgr, json=payload)
            if resp.status_code in (200, 201):
                created_item = resp.json().get("Item", {})
                new_id = created_item.get("Id", "")
                created += 1
                report.append({"Name": name, "Type": itype, "Status": "created", "NewId": str(new_id), "Error": ""})
                print(f"[INFO] Created Inventory {name!r} Id={new_id}")
            else:
                err = resp.text[:500] if resp.text else f"HTTP {resp.status_code}"
                try:
                    body = resp.json()
                    errs = body.get("Fault", {}).get("Error", []) or body.get("fault", {}).get("error", [])
                    if errs:
                        err = "; ".join(e.get("Message", e.get("Detail", str(e))) for e in errs)
                except Exception:
                    pass
                # Treat duplicate name / already exists as skip so pipeline continues
                err_lower = (err or "").lower()
                if "duplicate" in err_lower or "already exists" in err_lower or "name already" in err_lower:
                    report.append({"Name": name, "Type": itype, "Status": "skipped", "NewId": "", "Error": "Item already exists (duplicate name); skipped"})
                    skipped += 1
                    print(f"[INFO] Skipped {name!r}: item already exists in QBO")
                else:
                    report.append({"Name": name, "Type": itype, "Status": "failed", "NewId": "", "Error": err})
                    failed += 1
                    print(f"[FAIL] {name!r}: {err}", file=sys.stderr)

        print(f"[INFO] Import: created={created}, skipped={skipped}, failed={failed}")
        if not dry_run and config.slack_webhook_url:
            notify_import_success(
                "products",
                config.company_key,
                {"created": created, "skipped": skipped, "failed": failed},
                config.slack_webhook_url,
            )

        if report_csv_path:
            out = Path(report_csv_path)
            if not out.is_absolute():
                out = _REPO_ROOT / out
            out.parent.mkdir(parents=True, exist_ok=True)
            with open(out, "w", newline="", encoding="utf-8") as f:
                w = csv.DictWriter(f, fieldnames=["Name", "Type", "Status", "NewId", "Error"])
                w.writeheader()
                w.writerows(report)
            print(f"[INFO] Report written to {out}")

        return 0
    except Exception as e:
        if config.slack_webhook_url:
            notify_import_failure("products", config.company_key, str(e), config.slack_webhook_url)
        raise


def cmd_inactivate_all(args: argparse.Namespace, token_mgr: TokenManager, realm_id: str) -> int:
    """Set all active products (Items) to inactive. Use before re-creating inventory from a fresh list."""
    type_filter = getattr(args, "type_filter", None)
    limit = getattr(args, "limit", None)
    dry_run = getattr(args, "dry_run", False)
    report_csv = getattr(args, "report_csv", None)

    if dry_run:
        # Fast path: no SyncToken needed
        print(f"[INFO] Querying active Items (type={type_filter or 'all'}) [dry-run]...")
        try:
            items = _query_all_items_paginated(
                token_mgr, realm_id, active_only=True, type_filter=type_filter
            )
            if limit:
                items = items[:limit]
        except RuntimeError as e:
            print(f"[ERROR] {e}", file=sys.stderr)
            return 1
    else:
        print(f"[INFO] Querying active Items (type={type_filter or 'all'}) and fetching SyncToken per item...")
        try:
            items = _query_active_items_for_inactivate(
                token_mgr, realm_id, type_filter=type_filter, limit=limit
            )
        except RuntimeError as e:
            print(f"[ERROR] {e}", file=sys.stderr)
            return 1

    if not items:
        print("[INFO] No active Items found.")
        return 0

    print(f"[INFO] Found {len(items)} active Item(s) to inactivate.")
    for r in items[:20]:
        print(f"  Id={r.get('Id')} Name={r.get('Name')!r} Type={r.get('Type')}")
    if len(items) > 20:
        print(f"  ... and {len(items) - 20} more")

    if dry_run:
        print("[INFO] Dry run: no items inactivated.")
        return 0

    report: List[Dict[str, Any]] = []
    ok = 0
    fail = 0
    for r in items:
        rid = r.get("Id")
        sync = r.get("SyncToken")
        name = r.get("Name", "")
        typ = r.get("Type", "")
        if not rid or sync is None:
            status = "skipped"
            err = "Missing Id or SyncToken"
            fail += 1
        else:
            success, err = _inactivate_item(token_mgr, realm_id, str(rid), str(sync))
            status = "inactivated" if success else "failed"
            if success:
                ok += 1
                print(f"  [OK] {name!r} (Id={rid}) inactivated")
            else:
                fail += 1
                print(f"  [FAIL] {name!r} (Id={rid}): {err}", file=sys.stderr)
        report.append({"Id": rid, "Name": name, "Type": typ, "Status": status, "Error": err or ""})

    print(f"[INFO] Inactivated {ok}, failed {fail}")

    if report_csv:
        out = Path(report_csv)
        if not out.is_absolute():
            out = _REPO_ROOT / out
        out.parent.mkdir(parents=True, exist_ok=True)
        with open(out, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=["Id", "Name", "Type", "Status", "Error"])
            w.writeheader()
            w.writerows(report)
        print(f"[INFO] Report written to {out}")

    return 1 if fail else 0


def main() -> int:
    parser = argparse.ArgumentParser(
        description="QBO Inventory manager: get item, list InvStartDate issues, set InvStartDate, recreate item with new InvStartDate.",
    )
    parser.add_argument("--company", required=True, choices=get_available_companies(), help="Company key")
    subparsers = parser.add_subparsers(dest="command", required=True, help="Subcommand")

    # get
    p_get = subparsers.add_parser("get", help="Get item by ID or search by name")
    grp = p_get.add_mutually_exclusive_group(required=True)
    grp.add_argument("--item-id", help="QBO Item Id")
    grp.add_argument("--name", help="Search by Name")
    p_get.add_argument("--raw-json", action="store_true", help="Print raw JSON")
    p_get.set_defaults(_run=cmd_get)

    # list-invstart
    p_list = subparsers.add_parser("list-invstart", help="List inventory items with InvStartDate after cutoff")
    p_list.add_argument("--cutoff-date", required=True, metavar="YYYY-MM-DD", help="Cutoff date (YYYY-MM-DD)")
    p_list.add_argument("--no-include-inactive", action="store_true", help="Exclude inactive items")
    p_list.add_argument("--maxresults", type=int, default=1000, metavar="N", help="Page size (default 1000)")
    p_list.add_argument("--export-csv", metavar="PATH", help="Write CSV of issues")
    p_list.set_defaults(_run=cmd_list_invstart)

    # set-invstart
    p_set = subparsers.add_parser("set-invstart", help="Set InvStartDate for one item")
    p_set.add_argument("--item-id", required=True, help="QBO Item Id")
    p_set.add_argument("--date", required=True, metavar="YYYY-MM-DD", help="New InvStartDate (YYYY-MM-DD)")
    p_set.set_defaults(_run=cmd_set_invstart)

    # set-invstart-bulk
    p_bulk = subparsers.add_parser("set-invstart-bulk", help="Patch all items with InvStartDate > cutoff to new date")
    p_bulk.add_argument("--cutoff-date", required=True, metavar="YYYY-MM-DD", help="Cutoff date (YYYY-MM-DD)")
    p_bulk.add_argument("--new-date", required=True, metavar="YYYY-MM-DD", help="New InvStartDate (YYYY-MM-DD)")
    p_bulk.add_argument("--no-include-inactive", action="store_true", help="Exclude inactive items")
    p_bulk.add_argument("--maxresults", type=int, default=1000, metavar="N", help="Page size (default 1000)")
    p_bulk.set_defaults(_run=cmd_set_invstart_bulk)

    # set-invstart-from-csv
    p_csv = subparsers.add_parser("set-invstart-from-csv", help="Patch InvStartDate for items listed in a CSV")
    p_csv.add_argument("--csv", required=True, metavar="PATH", help="CSV path (must have ItemId column)")
    p_csv.add_argument("--new-date", required=True, metavar="YYYY-MM-DD", help="New InvStartDate (YYYY-MM-DD)")
    p_csv.set_defaults(_run=cmd_set_invstart_from_csv)

    # recreate-invstart: inactivate old item, create new one with same details and new InvStartDate
    p_recreate = subparsers.add_parser(
        "recreate-invstart",
        help="Inactivate item and create new Inventory item with same details and new InvStartDate (new Id)",
    )
    p_recreate.add_argument("--item-id", required=True, help="QBO Item Id to replace")
    p_recreate.add_argument("--date", required=True, metavar="YYYY-MM-DD", help="InvStartDate for the new item (YYYY-MM-DD)")
    p_recreate.set_defaults(_run=cmd_recreate_invstart)

    # export-products: export all active products (Items) to CSV
    p_export = subparsers.add_parser("export-products", help="Export all active products (Items) to CSV")
    p_export.add_argument("--out", required=True, metavar="PATH", help="Output CSV path (e.g. exports/company_a_products.csv)")
    p_export.add_argument("--include-inactive", action="store_true", help="Include inactive items")
    p_export.add_argument("--type", dest="type_filter", choices=("Inventory", "Service", "NonInventory"), metavar="TYPE", help="Filter by item type (default: all)")
    p_export.add_argument("--maxresults", type=int, default=1000, metavar="N", help="Page size (default 1000)")
    p_export.set_defaults(_run=cmd_export_products)

    # import-products: create QBO Items from a products CSV (uses Product.Mapping for accounts)
    p_import = subparsers.add_parser("import-products", help="Create QBO Items from a products CSV (uses Product.Mapping for accounts)")
    p_import.add_argument("--csv", metavar="PATH", help="Input CSV path (default: exports/<company>_products.csv)")
    p_import.add_argument("--as-of-date", metavar="YYYY-MM-DD", help="InvStartDate (As of Date) for all created Inventory items (overrides CSV/config)")
    p_import.add_argument("--default-qty", type=int, default=10, metavar="N", help="Default QtyOnHand when CSV value is missing (default: 10)")
    p_import.add_argument("--taxcode-name", metavar="NAME", help="TaxCode name to resolve (overrides company config; default: use config tax_code_id / tax_code_name)")
    p_import.add_argument("--taxcode-id", metavar="ID", help="TaxCode Id to use (overrides config and name lookup when set)")
    p_import.add_argument("--inventory-only", action="store_true", help="Only process rows with Type=Inventory in CSV (default: create all non-Category rows as Inventory)")
    p_import.add_argument("--dry-run", action="store_true", help="Validate and list what would be created; do not POST")
    p_import.add_argument("--report-csv", metavar="PATH", help="Write report CSV (Name, Type, Status, NewId, Error)")
    p_import.set_defaults(_run=cmd_import_products)

    # inactivate-all: set all active products (Items) to inactive
    p_inact = subparsers.add_parser(
        "inactivate-all",
        help="Set all active products (Items) to inactive; use before re-creating inventory from a fresh list",
    )
    p_inact.add_argument("--type", dest="type_filter", choices=("Inventory", "Service", "NonInventory", "Category"), metavar="TYPE", help="Inactivate only this type (default: all)")
    p_inact.add_argument("--dry-run", action="store_true", help="List what would be inactivated; do not inactivate")
    p_inact.add_argument("--limit", type=int, metavar="N", help="Max number of items to inactivate (default: no limit)")
    p_inact.add_argument("--report-csv", metavar="PATH", help="Write report CSV (Id, Name, Type, Status, Error)")
    p_inact.set_defaults(_run=cmd_inactivate_all)

    args = parser.parse_args()

    config = load_company_config(args.company)
    verify_realm_match(args.company, config.realm_id)
    token_mgr = TokenManager(config.company_key, config.realm_id)
    realm_id = config.realm_id

    return args._run(args, token_mgr, realm_id)


if __name__ == "__main__":
    sys.exit(main())
