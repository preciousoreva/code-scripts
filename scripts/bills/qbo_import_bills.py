#!/usr/bin/env python3
"""
Re-import one or more QBO Bills from two CSVs (header + lines).

Use this after exporting bills (qbo_export_bills.py), deleting them in QBO,
and updating InvStartDate for inventory items, to re-create bills accurately.

Usage (example):
  python scripts/bills/qbo_import_bills.py --company company_a --bill-id 123 --dry-run
  python scripts/bills/qbo_import_bills.py --company company_a --bill-id 123 --create
  python scripts/bills/qbo_import_bills.py --company company_a --bill-ids 58984 58985 58986 --create
  python scripts/bills/qbo_import_bills.py --company company_a --all --create
  python scripts/bills/qbo_import_bills.py --company company_a --bill-ids 58984 58985 --taxcode-id 4 --create

Pass exactly one of: --bill-id (single), --bill-ids (list), or --all (every BillId in header with lines).
TaxCode is resolved once at start (by name or --taxcode-id) and reused for every bill.
Tax: Default --taxcode-name Exempt and --global-tax-calc TaxInclusive.
DocNumber: Uses header DocNumber if non-empty; otherwise BILL-<VENDOR_CODE>-<YYYYMMDD>-<SHORT_HASH> (max 21 chars).
"""
from __future__ import annotations

import argparse
import hashlib
import json
import logging
import re
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd
from urllib.parse import quote

_REPO_ROOT = Path(__file__).resolve().parent.parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from load_env import load_env_file
from company_config import load_company_config, get_available_companies
from token_manager import verify_realm_match
from qbo_upload import (
    BASE_URL,
    _make_qbo_request,
    TokenManager,
    prefetch_all_items,
)
from slack_notify import notify_import_start, notify_import_success, notify_import_failure

load_env_file()

MINORVERSION = "70"

# Accept header vs line sum within this (covers exact match and float rounding for tax-inclusive).
TOTAL_TOLERANCE = 0.02
# Header TotalAmt may be tax-inclusive (line sum + 7.5%). Accept either exact or tax-inclusive match.
TAX_INCLUSIVE_RATE = 1.075

DOCNUMBER_MAX_LEN = 21

logger = logging.getLogger(__name__)


def _is_nan(x: Any) -> bool:
    try:
        return pd.isna(x)
    except Exception:
        return x is None


def _as_str(x: Any) -> Optional[str]:
    if _is_nan(x):
        return None
    s = str(x).strip()
    return s if s else None


def _as_float(x: Any) -> Optional[float]:
    if _is_nan(x):
        return None
    try:
        return float(x)
    except Exception:
        return None


def _as_int_str(x: Any) -> Optional[str]:
    """
    QBO IDs are strings in payloads.
    CSV may give ints or floats (e.g., 6262.0). Normalize to "6262".
    """
    if _is_nan(x):
        return None
    try:
        n = int(float(x))
        return str(n)
    except Exception:
        s = _as_str(x)
        return s


def _normalize_bill_id_for_filter(df: pd.DataFrame, bill_id: int) -> pd.DataFrame:
    """Filter dataframe to rows where BillId (possibly float in CSV) equals bill_id."""
    if "BillId" not in df.columns:
        return df.loc[[]]
    try:
        normalized = df["BillId"].apply(lambda v: int(float(v)) if pd.notna(v) else None)
        return df[normalized == bill_id]
    except (ValueError, TypeError):
        return df.loc[[]]


def _bill_ids_with_lines(headers_df: pd.DataFrame, lines_df: pd.DataFrame) -> List[int]:
    """Return sorted list of BillIds that appear in header and have at least one line."""
    if "BillId" not in headers_df.columns or "BillId" not in lines_df.columns:
        return []
    try:
        header_ids = set(
            int(float(v)) for v in headers_df["BillId"] if pd.notna(v)
        )
        line_ids = set(
            int(float(v)) for v in lines_df["BillId"] if pd.notna(v)
        )
        return sorted(header_ids & line_ids)
    except (ValueError, TypeError):
        return []


def get_taxcode_id_by_name(
    name: str,
    token_mgr: TokenManager,
    realm_id: str,
    cache: Dict[str, Optional[str]],
) -> Optional[str]:
    """
    Resolve a TaxCode name to a TaxCode Id using QBO query.
    Queries SELECT Id, Name FROM TaxCode WHERE Active = true and caches results by name.
    Matches exact first, then case-insensitive, then first where name contains requested.
    """
    if not name or not name.strip():
        return None
    name_clean = name.strip()
    if name_clean in cache:
        return cache[name_clean]
    query = "select Id, Name from TaxCode where Active = true"
    url = f"{BASE_URL}/v3/company/{realm_id}/query?query={quote(query)}&minorversion={MINORVERSION}"
    resp = _make_qbo_request("GET", url, token_mgr)
    if resp.status_code != 200:
        logger.warning("TaxCode query failed %s: %s", resp.status_code, resp.text[:200])
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
        logger.info("TaxCode %r -> Id %s", name_clean, tax_code_id)
    else:
        logger.warning("TaxCode %r not found among active TaxCodes", name_clean)
    return tax_code_id


def _resolve_entity_id_by_name(
    entity: str,
    name: str,
    token_mgr: TokenManager,
    realm_id: str,
    cache: Dict[str, Optional[str]],
    query_template: str,
) -> Optional[str]:
    """Shared logic: query QBO for entity Id,Name where Active=true; cache; match by name."""
    if not name or not name.strip():
        return None
    name_clean = name.strip()
    cache_key = f"{entity}:{name_clean}"
    if cache_key in cache:
        return cache[cache_key]
    url = f"{BASE_URL}/v3/company/{realm_id}/query?query={quote(query_template)}&minorversion={MINORVERSION}"
    resp = _make_qbo_request("GET", url, token_mgr)
    if resp.status_code != 200:
        logger.warning("%s query failed %s: %s", entity, resp.status_code, resp.text[:200])
        cache[cache_key] = None
        return None
    data = resp.json()
    entities = data.get("QueryResponse", {}).get(entity) or []
    if not isinstance(entities, list):
        entities = [entities] if entities else []
    name_to_id: Dict[str, str] = {}
    for e in entities:
        eid = e.get("Id")
        ename = (e.get("Name") or "").strip()
        if eid and ename:
            name_to_id[ename] = str(eid)
    for k, v in name_to_id.items():
        cache.setdefault(f"{entity}:{k}", v)
    result = name_to_id.get(name_clean)
    if result is None:
        name_lower = name_clean.lower()
        for k, v in name_to_id.items():
            if k.lower() == name_lower:
                result = v
                break
    if result is None:
        for k, v in name_to_id.items():
            if name_clean in k or k in name_clean:
                result = v
                break
    cache[cache_key] = result
    if result:
        logger.info("%s %r -> Id %s", entity, name_clean, result)
    else:
        logger.warning("%s %r not found among active records", entity, name_clean)
    return result


def get_department_id_by_name(
    name: str,
    token_mgr: TokenManager,
    realm_id: str,
    cache: Dict[str, Optional[str]],
) -> Optional[str]:
    """
    Resolve Department (Location) name to Id. Query: select Id, Name from Department where Active = true.
    Location in QBO UI is driven by DepartmentRef.
    """
    return _resolve_entity_id_by_name(
        "Department",
        name,
        token_mgr,
        realm_id,
        cache,
        "select Id, Name from Department where Active = true",
    )


def get_term_id_by_name(
    name: str,
    token_mgr: TokenManager,
    realm_id: str,
    cache: Dict[str, Optional[str]],
) -> Optional[str]:
    """
    Resolve Term (payment terms) name to Id. Query: select Id, Name from Term where Active = true.
    Terms in QBO UI (e.g. "Due on receipt") are driven by SalesTermRef.
    """
    return _resolve_entity_id_by_name(
        "Term",
        name,
        token_mgr,
        realm_id,
        cache,
        "select Id, Name from Term where Active = true",
    )


def build_vendor_code(vendor_name: str) -> str:
    """
    First letters of up to 3 words from vendor name (uppercase, no spaces).
    Example: 'JUST NATURE FOODS AND FARMS ENT' -> 'JNF'
    """
    if not vendor_name or not isinstance(vendor_name, str):
        return ""
    words = re.split(r"\s+", vendor_name.strip(), maxsplit=3)[:3]
    code = "".join((w[0] for w in words if w and w[0].isalnum())).upper()
    return code[:3]


def build_bill_doc_number(header_row: pd.Series) -> str:
    """
    Deterministic DocNumber: BILL-<VENDOR_CODE>-<YYYYMMDD>-<SHORT_HASH>.
    SHORT_HASH = first 4 chars of SHA1(VendorId|TxnDate|TotalAmt|BillId).
    Max length DOCNUMBER_MAX_LEN (21). Vendor code shortened if needed.
    """
    vendor_name = _as_str(header_row.get("VendorName")) or _as_str(header_row.get("VendorId")) or "X"
    vendor_code = build_vendor_code(vendor_name) or "X"
    txn_date = _as_str(header_row.get("TxnDate")) or ""
    txn_compact = txn_date.replace("-", "")[:8] if txn_date else "00000000"
    total_amt = header_row.get("TotalAmt")
    total_str = "" if _is_nan(total_amt) else str(float(total_amt))
    bill_id = header_row.get("BillId")
    bill_str = "" if _is_nan(bill_id) else str(int(float(bill_id)))
    vendor_id = _as_int_str(header_row.get("VendorId")) or "0"
    stable = f"{vendor_id}|{txn_date}|{total_str}|{bill_str}"
    short_hash = hashlib.sha1(stable.encode("utf-8")).hexdigest()[:4].upper()
    base = f"BILL-{vendor_code}-{txn_compact}-{short_hash}"
    if len(base) <= DOCNUMBER_MAX_LEN:
        return base
    for n in range(2, 0, -1):
        vc = vendor_code[:n] or "X"
        candidate = f"BILL-{vc}-{txn_compact}-{short_hash}"
        if len(candidate) <= DOCNUMBER_MAX_LEN:
            return candidate
    return f"BILL-{txn_compact}-{short_hash}"[:DOCNUMBER_MAX_LEN]


def build_bill_payload(
    header_row: pd.Series,
    lines_df: pd.DataFrame,
    *,
    exempt_taxcode_id: Optional[str] = None,
    global_tax_calc: Optional[str] = None,
    department_ref_id: Optional[str] = None,
    sales_term_ref_id: Optional[str] = None,
    active_items_by_name: Optional[Dict[str, Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    """Build QBO Bill JSON payload from one header row and its line rows.
    When exempt_taxcode_id is set, forces TaxCodeRef on all expense lines (Exempt).
    When global_tax_calc is set (e.g. TaxInclusive), sets GlobalTaxCalculation.
    department_ref_id sets Location (DepartmentRef); sales_term_ref_id sets Terms (SalesTermRef).
    DocNumber: uses header if non-empty, else generated BILL-<VENDOR_CODE>-<YYYYMMDD>-<HASH>.
    ItemBasedExpenseLineDetail lines are resolved by ItemName only (CSV ItemId is ignored;
    we always use the current active QBO item with that name).
    """
    bill_id_display = header_row.get("BillId", "?")
    vendor_id = _as_int_str(header_row.get("VendorId"))
    if not vendor_id:
        raise ValueError(f"Header missing VendorId for BillId={bill_id_display}")

    payload: Dict[str, Any] = {
        "VendorRef": {"value": vendor_id},
        "TxnDate": _as_str(header_row.get("TxnDate")),
    }
    txn_date = payload.get("TxnDate")
    if not txn_date:
        raise ValueError(f"Header missing TxnDate for BillId={bill_id_display}")

    if global_tax_calc:
        payload["GlobalTaxCalculation"] = global_tax_calc

    due_date = _as_str(header_row.get("DueDate"))
    if due_date:
        payload["DueDate"] = due_date

    ap_acct = _as_int_str(header_row.get("APAccountId"))
    if ap_acct:
        payload["APAccountRef"] = {"value": ap_acct}

    doc_number = _as_str(header_row.get("DocNumber"))
    if not doc_number:
        doc_number = build_bill_doc_number(header_row)
    if doc_number:
        payload["DocNumber"] = doc_number

    private_note = _as_str(header_row.get("PrivateNote"))
    if private_note:
        payload["PrivateNote"] = private_note

    currency = _as_str(header_row.get("Currency"))
    if currency:
        payload["CurrencyRef"] = {"value": currency}

    exchange_rate = _as_float(header_row.get("ExchangeRate"))
    if exchange_rate is not None:
        payload["ExchangeRate"] = exchange_rate

    if department_ref_id:
        payload["DepartmentRef"] = {"value": department_ref_id}
    if sales_term_ref_id:
        payload["SalesTermRef"] = {"value": sales_term_ref_id}

    line_objs: List[Dict[str, Any]] = []
    for _, r in lines_df.iterrows():
        detail_type = _as_str(r.get("DetailType"))
        amount = _as_float(r.get("Amount"))
        desc = _as_str(r.get("Description"))

        if not detail_type:
            raise ValueError(f"Line missing DetailType for BillId={bill_id_display}")
        if amount is None:
            raise ValueError(f"Line missing Amount for BillId={bill_id_display}")

        line_obj: Dict[str, Any] = {
            "Amount": amount,
            "DetailType": detail_type,
        }
        if desc:
            line_obj["Description"] = desc

        if detail_type == "ItemBasedExpenseLineDetail":
            item_name = _as_str(r.get("ItemName"))
            if not item_name:
                raise ValueError(
                    f"ItemBased line missing ItemName for BillId={bill_id_display} "
                    "(bills resolve by ItemName only; CSV ItemId is ignored)"
                )
            qty = _as_float(r.get("Qty"))
            unit_price = _as_float(r.get("UnitPrice"))

            effective_item_id: Optional[str] = None
            if active_items_by_name:
                active_item = active_items_by_name.get(item_name)
                if isinstance(active_item, dict) and active_item.get("Id"):
                    effective_item_id = str(active_item["Id"])
            if not effective_item_id:
                raise ValueError(
                    f"No active item in QBO for name {item_name!r} (BillId {bill_id_display})"
                )

            detail: Dict[str, Any] = {
                "ItemRef": {"value": effective_item_id},
            }
            if qty is not None:
                detail["Qty"] = qty
            if unit_price is not None:
                detail["UnitPrice"] = unit_price

            if exempt_taxcode_id:
                detail["TaxCodeRef"] = {"value": exempt_taxcode_id}
            else:
                tax_code_id = _as_int_str(r.get("TaxCodeId"))
                if tax_code_id:
                    detail["TaxCodeRef"] = {"value": tax_code_id}

            billable = _as_str(r.get("BillableStatus"))
            if billable:
                detail["BillableStatus"] = billable

            customer_id = _as_int_str(r.get("CustomerId"))
            if customer_id:
                detail["CustomerRef"] = {"value": customer_id}

            class_id = _as_int_str(r.get("ClassId"))
            if class_id:
                detail["ClassRef"] = {"value": class_id}

            line_obj["ItemBasedExpenseLineDetail"] = detail

        elif detail_type == "AccountBasedExpenseLineDetail":
            acct_id = _as_int_str(r.get("AccountId"))
            if not acct_id:
                raise ValueError(f"AccountBased line missing AccountId for BillId={bill_id_display}")
            detail = {
                "AccountRef": {"value": acct_id},
            }
            if exempt_taxcode_id:
                detail["TaxCodeRef"] = {"value": exempt_taxcode_id}
            else:
                tax_code_id = _as_int_str(r.get("TaxCodeId"))
                if tax_code_id:
                    detail["TaxCodeRef"] = {"value": tax_code_id}
            customer_id = _as_int_str(r.get("CustomerId"))
            if customer_id:
                detail["CustomerRef"] = {"value": customer_id}
            class_id = _as_int_str(r.get("ClassId"))
            if class_id:
                detail["ClassRef"] = {"value": class_id}
            line_obj["AccountBasedExpenseLineDetail"] = detail

        else:
            raise ValueError(f"Unsupported DetailType={detail_type!r} for BillId={bill_id_display}")

        line_objs.append(line_obj)

    payload["Line"] = line_objs
    return payload


def validate_totals(payload: Dict[str, Any], header_total: Optional[float]) -> None:
    """Raise ValueError if header TotalAmt exists and does not match sum of line Amount within tolerance.
    Accepts either: header equals line sum (exact), or header equals line sum * TAX_INCLUSIVE_RATE (7.5% tax-inclusive).
    """
    line_sum = sum(float(l["Amount"]) for l in payload.get("Line", []))
    if header_total is None:
        return
    header_total_f = float(header_total)
    exact_ok = abs(line_sum - header_total_f) <= TOTAL_TOLERANCE
    tax_inclusive_ok = abs(header_total_f - line_sum * TAX_INCLUSIVE_RATE) <= TOTAL_TOLERANCE
    if not (exact_ok or tax_inclusive_ok):
        raise ValueError(
            f"Total mismatch: header TotalAmt={header_total} vs line sum={line_sum} "
            f"(tolerance={TOTAL_TOLERANCE}; tax-inclusive rate={TAX_INCLUSIVE_RATE})"
        )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Re-import one QBO Bill from header + lines CSVs (dry-run or create)."
    )
    parser.add_argument(
        "--company",
        required=True,
        choices=get_available_companies(),
        help="Company key (e.g. company_a)",
    )
    parser.add_argument(
        "--headers",
        default="bills_header.csv",
        help="Path to bills header CSV (default: bills_header.csv)",
    )
    parser.add_argument(
        "--lines",
        default="bills_lines.csv",
        help="Path to bills lines CSV (default: bills_lines.csv)",
    )
    parser.add_argument(
        "--bill-id",
        type=int,
        help="Single BillId to import (from CSV; must exist in header and have lines)",
    )
    parser.add_argument(
        "--bill-ids",
        type=int,
        nargs="+",
        metavar="ID",
        help="List of BillIds to import (mutually exclusive with --bill-id and --all)",
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="Import every BillId in header CSV that has lines (mutually exclusive with --bill-id and --bill-ids)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print payload and validation only; do not call QBO",
    )
    parser.add_argument(
        "--create",
        action="store_true",
        help="POST bill to QBO and print created Bill Id and JSON",
    )
    parser.add_argument(
        "--taxcode-name",
        default="Exempt",
        help="TaxCode name to resolve and apply to all lines (default: Exempt)",
    )
    parser.add_argument(
        "--global-tax-calc",
        default="TaxInclusive",
        help="GlobalTaxCalculation value (default: TaxInclusive)",
    )
    parser.add_argument(
        "--taxcode-id",
        type=str,
        help="TaxCode Id to use directly (e.g. 4 for Exempt); skips lookup by name when set",
    )
    parser.add_argument(
        "--location-name",
        default="Plot C, Golf Road",
        help="Department (Location) name to resolve and set on Bill (default: Plot C, Golf Road)",
    )
    parser.add_argument(
        "--terms-name",
        default="Due on receipt",
        help="Term (payment terms) name to resolve and set on Bill (default: Due on receipt)",
    )
    args = parser.parse_args()

    # Exactly one of --bill-id, --bill-ids, or --all
    bill_id_count = sum([args.bill_id is not None, args.bill_ids is not None, args.all])
    if bill_id_count != 1:
        print(
            "Error: pass exactly one of --bill-id, --bill-ids, or --all.",
            file=sys.stderr,
        )
        sys.exit(1)

    if args.dry_run and args.create:
        print("Error: use either --dry-run or --create, not both.", file=sys.stderr)
        sys.exit(1)
    if not args.dry_run and not args.create:
        print("Error: pass --dry-run or --create.", file=sys.stderr)
        sys.exit(1)

    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    headers_path = Path(args.headers)
    lines_path = Path(args.lines)
    if not headers_path.exists():
        print(f"Error: headers file not found: {headers_path}", file=sys.stderr)
        sys.exit(1)
    if not lines_path.exists():
        print(f"Error: lines file not found: {lines_path}", file=sys.stderr)
        sys.exit(1)

    headers_df = pd.read_csv(headers_path)
    lines_df = pd.read_csv(lines_path)

    for col in ("BillId", "VendorId", "TxnDate"):
        if col not in headers_df.columns:
            print(f"Error: headers CSV missing required column: {col}", file=sys.stderr)
            sys.exit(1)

    for col in ("DetailType", "Amount"):
        if col not in lines_df.columns:
            print(f"Error: lines CSV missing required column: {col}", file=sys.stderr)
            sys.exit(1)

    # Compute list of bill IDs to import
    if args.bill_id is not None:
        bill_ids = [args.bill_id]
    elif args.bill_ids is not None:
        bill_ids = list(args.bill_ids)
    else:
        bill_ids = _bill_ids_with_lines(headers_df, lines_df)
        if not bill_ids:
            print("No bills to import (no BillIds in header with lines).", file=sys.stderr)
            sys.exit(0)

    config = load_company_config(args.company)
    verify_realm_match(args.company, config.realm_id)
    token_mgr = TokenManager(config.company_key, config.realm_id)
    realm_id = config.realm_id

    # Resolve TaxCode once: use --taxcode-id if set, else lookup by name
    exempt_taxcode_id: Optional[str] = None
    if args.taxcode_id is not None and str(args.taxcode_id).strip():
        exempt_taxcode_id = str(args.taxcode_id).strip()
        logger.info("Using TaxCode Id from --taxcode-id: %s", exempt_taxcode_id)
    elif args.taxcode_name and args.taxcode_name.strip():
        taxcode_cache: Dict[str, Optional[str]] = {}
        exempt_taxcode_id = get_taxcode_id_by_name(
            args.taxcode_name.strip(), token_mgr, realm_id, taxcode_cache
        )
        if not exempt_taxcode_id and not args.dry_run:
            print(
                f"Error: TaxCode {args.taxcode_name!r} not found. Create or use another name.",
                file=sys.stderr,
            )
            sys.exit(1)

    # Resolve Department (Location) and Term (Terms) once; cache shared for entity lookups
    entity_cache: Dict[str, Optional[str]] = {}
    department_ref_id: Optional[str] = None
    if args.location_name and args.location_name.strip():
        department_ref_id = get_department_id_by_name(
            args.location_name.strip(), token_mgr, realm_id, entity_cache
        )
    sales_term_ref_id: Optional[str] = None
    if args.terms_name and args.terms_name.strip():
        sales_term_ref_id = get_term_id_by_name(
            args.terms_name.strip(), token_mgr, realm_id, entity_cache
        )

    # Resolve item-based lines by ItemName only (ignore CSV ItemIds; they refer to deleted/inactive products)
    active_items_by_name: Dict[str, Dict[str, Any]] = {}
    if "DetailType" in lines_df.columns:
        item_based = lines_df[
            lines_df["DetailType"].astype(str).str.strip() == "ItemBasedExpenseLineDetail"
        ]
        if not item_based.empty:
            prefetch = prefetch_all_items(token_mgr, realm_id)
            active_items_by_name = {
                name: item
                for name, item in prefetch.items()
                if item.get("Active", True)
            }
            # Bill export ItemName is often FullyQualifiedName (e.g. "DRINKS & BEVERAGES:JUST NATURE KUNU");
            # QBO Item.Name is the short name (e.g. "JUST NATURE KUNU"). Key by both so lookups find the item.
            for name, item in list(active_items_by_name.items()):
                fqn = (item.get("FullyQualifiedName") or "").strip()
                if fqn and fqn != name and fqn not in active_items_by_name:
                    active_items_by_name[fqn] = item

    created_ids: List[str] = []
    url = f"{BASE_URL}/v3/company/{realm_id}/bill?minorversion={MINORVERSION}"

    if not args.dry_run and bill_ids:
        print(f"taxcode-name: {args.taxcode_name!r}")
        print(f"resolved TaxCode Id: {exempt_taxcode_id or '(none)'}")
        print(f"GlobalTaxCalculation: {args.global_tax_calc!r}")
        print(f"location-name: {args.location_name!r}")
        print(f"resolved DepartmentRef Id: {department_ref_id or '(none)'}")
        print(f"terms-name: {args.terms_name!r}")
        print(f"resolved SalesTermRef Id: {sales_term_ref_id or '(none)'}")

    if args.dry_run and bill_ids:
        print("\n=== Location & Terms (dry-run) ===")
        print(f"  location-name: {args.location_name!r}")
        print(f"  resolved DepartmentRef Id: {department_ref_id or '(none)'}")
        print(f"  terms-name: {args.terms_name!r}")
        print(f"  resolved SalesTermRef Id: {sales_term_ref_id or '(none)'}")

    if not args.dry_run and bill_ids and config.slack_webhook_url:
        notify_import_start("bills", config.display_name, {"total": len(bill_ids)}, config.slack_webhook_url)

    try:
        for bill_id in bill_ids:
            header_match = _normalize_bill_id_for_filter(headers_df, bill_id)
            if header_match.empty:
                logger.warning("BillId %s not found in headers CSV; skipping.", bill_id)
                continue
            bill_lines = _normalize_bill_id_for_filter(lines_df, bill_id)
            if bill_lines.empty:
                logger.warning("BillId %s has no lines in lines CSV; skipping.", bill_id)
                continue

            header_row = header_match.iloc[0]
            try:
                payload = build_bill_payload(
                    header_row,
                    bill_lines,
                    exempt_taxcode_id=exempt_taxcode_id,
                    global_tax_calc=args.global_tax_calc or None,
                    department_ref_id=department_ref_id,
                    sales_term_ref_id=sales_term_ref_id,
                    active_items_by_name=active_items_by_name,
                )
            except ValueError as e:
                print(f"Error (BillId {bill_id}): {e}", file=sys.stderr)
                if not args.dry_run and config.slack_webhook_url:
                    notify_import_failure("bills", config.display_name, str(e), config.slack_webhook_url)
                sys.exit(1)

            header_total = _as_float(header_row.get("TotalAmt"))
            try:
                validate_totals(payload, header_total)
            except ValueError as e:
                print(f"Error (BillId {bill_id}): {e}", file=sys.stderr)
                if not args.dry_run and config.slack_webhook_url:
                    notify_import_failure("bills", config.display_name, str(e), config.slack_webhook_url)
                sys.exit(1)

            line_count = len(payload.get("Line", []))
            line_sum = sum(float(l["Amount"]) for l in payload["Line"])
            vendor_id = payload.get("VendorRef", {}).get("value", "?")
            txn_date = payload.get("TxnDate", "?")
            doc_number = payload.get("DocNumber", "(none)")

            if args.dry_run:
                print(f"\n--- BillId {bill_id} ---")
                print(f"  VendorId: {vendor_id}")
                print(f"  TxnDate: {txn_date}")
                print(f"  DocNumber: {doc_number}")
                print(f"  Line count: {line_count}")
                print(f"  Line total: {line_sum}")
                if header_total is not None:
                    print(f"  Header TotalAmt: {header_total}")
                if len(bill_ids) == 1:
                    print("\n=== Tax settings ===")
                    print(f"  taxcode-name: {args.taxcode_name!r}")
                    print(f"  resolved TaxCode Id: {exempt_taxcode_id or '(not resolved)'}")
                    print(f"  GlobalTaxCalculation: {payload.get('GlobalTaxCalculation', '(none)')}")
                    print("\n=== Line TaxCodeRef applied ===")
                    for i, line in enumerate(payload.get("Line", [])):
                        detail = line.get("ItemBasedExpenseLineDetail") or line.get("AccountBasedExpenseLineDetail") or {}
                        tax_ref = detail.get("TaxCodeRef", {}).get("value", "(none)")
                        print(f"  Line {i + 1}: TaxCodeRef.value = {tax_ref}")
                    print("\n=== Bill payload (for QBO POST /bill) ===")
                    print(json.dumps(payload, indent=2))
                continue

            if len(bill_ids) == 1:
                print(f"DocNumber: {doc_number}")

            resp = _make_qbo_request("POST", url, token_mgr, json=payload, timeout=60)
            if resp.status_code >= 400:
                err_msg = f"QBO POST failed for BillId {bill_id} ({resp.status_code}): {resp.text[:500]}"
                print(f"Error: {err_msg}", file=sys.stderr)
                if not args.dry_run and config.slack_webhook_url:
                    notify_import_failure("bills", config.display_name, err_msg, config.slack_webhook_url)
                sys.exit(1)

            result = resp.json()
            bill = result.get("Bill") or {}
            created_id = bill.get("Id")
            if created_id:
                created_ids.append(created_id)
            print(f"Created Bill Id: {created_id} (from CSV BillId {bill_id})")
            if len(bill_ids) == 1:
                print("\n=== Created Bill (QBO response) ===")
                print(json.dumps(bill, indent=2))
    except Exception as e:
        if not args.dry_run and config.slack_webhook_url:
            notify_import_failure("bills", config.display_name, str(e), config.slack_webhook_url)
        raise

    if args.dry_run:
        print(f"\nDry-run complete: {len(bill_ids)} bill(s) would be imported.")
    else:
        if config.slack_webhook_url:
            notify_import_success(
                "bills",
                config.display_name,
                {"created": len(created_ids), "total": len(bill_ids)},
                config.slack_webhook_url,
            )
        if created_ids:
            print(f"\nImported {len(created_ids)} bill(s): {', '.join(created_ids)}")


if __name__ == "__main__":
    main()
