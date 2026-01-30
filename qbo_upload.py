from __future__ import annotations

import os
import glob
import json
import argparse
import sys
import re
import csv
from typing import Optional, Dict, Callable, Any, Tuple, List
from urllib.parse import quote
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import requests
from load_env import load_env_file
from company_config import load_company_config, get_available_companies
from token_manager import get_access_token, refresh_access_token, verify_realm_match

# Load .env if present so QBO_* vars are available (shared secrets only)
load_env_file()

BASE_URL = "https://quickbooks.api.intuit.com"

# Tax code id for your 7.5% VAT ("7.5% S")
TAX_CODE_ID = "2"

# Legacy PaymentMethod mapping (Company A) - kept for backward compatibility
# Note: PaymentMethods are now queried from QBO by name per company
LEGACY_PAYMENT_METHOD_BY_NAME = {
    "Card": "5",
    "Cash": "1",
    "Cash/Transfer": "8",
    "Cheque": "2",
    "Credit Card": "3",
    "Direct Debit": "4",
    "Transfer": "6",
    "Card/Transfer": "9",
    "Card/Cash": "7",
    "Card/Cash/Transfer": "10",
}

# CSV column names
AMOUNT_COL = "*ItemAmount"        # GROSS line amount (inclusive of tax) from EPOS
DATE_COL = "*SalesReceiptDate"
MEMO_COL = "Memo"
DOCNUM_COL = "*SalesReceiptNo"
GROUP_COL = "*SalesReceiptNo"
LOCATION_COL = "Location"         # Location name from CSV

# Detail columns
ITEM_NAME_COL = "Item(Product/Service)"  # Product/Service name in QBO
ITEM_DESC_COL = "ItemDescription"        # Line description
QTY_COL = "ItemQuantity"                 # Quantity sold
RATE_COL = "ItemRate"                    # Unit price (can be NaN)
SERVICE_DATE_COL = "Service Date"        # Per-line service date
TAX_AMOUNT_COL = "ItemTaxAmount"        # Per-line tax amount from EPOS (7.5% VAT)

# Item mapping / creation behaviour
DEFAULT_ITEM_ID = "1"           # Fallback generic item
DEFAULT_INCOME_ACCOUNT_ID = "1" # For auto-created items
AUTO_CREATE_ITEMS = True       # Flip to True if you ever want auto item creation


def get_repo_root() -> str:
    """Return the directory this script lives in (the repo root for our purposes)."""
    return os.path.dirname(os.path.abspath(__file__))


def load_uploaded_docnumbers(repo_root: str, config) -> set:
    """Load set of DocNumbers that have been successfully uploaded."""
    ledger_path = os.path.join(repo_root, config.uploaded_docnumbers_file)
    if not os.path.exists(ledger_path):
        return set()
    
    try:
        with open(ledger_path, "r") as f:
            data = json.load(f)
            return set(data.get("docnumbers", []))
    except Exception as e:
        print(f"[WARN] Failed to load {config.uploaded_docnumbers_file}: {e}")
        return set()


def save_uploaded_docnumber(repo_root: str, docnumber: str, config) -> None:
    """Add a DocNumber to the uploaded ledger."""
    ledger_path = os.path.join(repo_root, config.uploaded_docnumbers_file)
    
    # Load existing
    docnumbers = load_uploaded_docnumbers(repo_root, config)
    docnumbers.add(docnumber)
    
    # Save back
    data = {
        "docnumbers": sorted(list(docnumbers)),
        "last_updated": datetime.now().isoformat(),
    }
    
    try:
        with open(ledger_path, "w") as f:
            json.dump(data, f, indent=2)
    except Exception as e:
        print(f"[WARN] Failed to save {config.uploaded_docnumbers_file}: {e}")


def check_qbo_existing_docnumbers(
    docnumbers: list[str],
    token_mgr: TokenManager,
    realm_id: str,
    batch_size: int = 50,
    target_date: Optional[str] = None
) -> Tuple[set, dict]:
    """
    Check QBO for existing SalesReceipts by DocNumber.
    
    Args:
        docnumbers: List of DocNumbers to check
        token_mgr: TokenManager instance
        realm_id: QBO Realm ID
        batch_size: Batch size for queries
        target_date: Optional target date (YYYY-MM-DD). If provided, only receipts with matching TxnDate are considered "existing".
    
    Returns:
        Tuple of (existing_docnumbers, date_mismatches):
        - existing_docnumbers: Set of DocNumbers that exist in QBO with matching TxnDate (or any TxnDate if target_date not provided)
        - date_mismatches: Dict {DocNumber: TxnDate} for receipts that exist but have different TxnDate
    """
    existing = set()
    date_mismatches = {}
    
    # Query in batches to avoid URL length limits
    for i in range(0, len(docnumbers), batch_size):
        batch = docnumbers[i:i + batch_size]
        # Build query: select Id, DocNumber, TxnDate from SalesReceipt where DocNumber in ('SR-...', 'SR-...', ...)
        docnumber_list = "', '".join(d.replace("'", "''") for d in batch)
        query = f"select Id, DocNumber, TxnDate from SalesReceipt where DocNumber in ('{docnumber_list}')"
        url = f"{BASE_URL}/v3/company/{realm_id}/query?query={quote(query)}&minorversion=70"
        
        resp = _make_qbo_request("GET", url, token_mgr)
        if resp.status_code == 200:
            data = resp.json()
            receipts = data.get("QueryResponse", {}).get("SalesReceipt", [])
            if not isinstance(receipts, list):
                receipts = [receipts] if receipts else []
            
            for receipt in receipts:
                doc_num = receipt.get("DocNumber")
                if not doc_num:
                    continue
                
                txn_date = receipt.get("TxnDate")
                
                # If target_date is provided, only consider it "existing" if TxnDate matches
                if target_date and txn_date:
                    if txn_date == target_date:
                        existing.add(doc_num)
                    else:
                        # Receipt exists but with different TxnDate - this is a date mismatch
                        date_mismatches[doc_num] = txn_date
                else:
                    # No target_date provided - consider any match as existing
                    existing.add(doc_num)
    
    return existing, date_mismatches


def find_latest_single_csv(repo_root: str, config) -> str:
    """
    Find the most recently modified CSV file matching company's prefix pattern.
    """
    pattern = os.path.join(repo_root, f"{config.csv_prefix}_*.csv")
    files = glob.glob(pattern)
    if not files:
        raise FileNotFoundError(
            f"No {config.csv_prefix}_*.csv files found in {repo_root}"
        )
    return max(files, key=os.path.getmtime)


def get_payment_method_id_by_name(name: str, token_mgr: TokenManager, realm_id: str, cache: Dict[str, Optional[str]]) -> Optional[str]:
    """
    Resolve a PaymentMethod name to a PaymentMethod Id with simple caching.
    
    - If the name exists in cache, reuse its Id.
    - Otherwise, try a QBO query by Name.
    - Returns None if payment method not found or name is empty.
    """
    if not name or not name.strip():
        return None
    
    name_clean = name.strip()
    if name_clean in cache:
        return cache[name_clean]
    
    safe_name = name_clean.replace("'", "''")
    query = f"select Id, Name from PaymentMethod where Name = '{safe_name}'"
    url = f"{BASE_URL}/v3/company/{realm_id}/query?query={quote(query)}&minorversion=70"
    
    resp = _make_qbo_request("GET", url, token_mgr)
    payment_method_id: Optional[str] = None
    if resp.status_code == 200:
        data = resp.json()
        payment_methods = data.get("QueryResponse", {}).get("PaymentMethod", [])
        if payment_methods:
            payment_method_id = payment_methods[0].get("Id")
    
    if payment_method_id:
        cache[name_clean] = payment_method_id
    else:
        # Cache None to avoid repeated failed queries
        cache[name_clean] = None
    
    return payment_method_id


def infer_payment_method_id(memo: str, token_mgr: TokenManager = None, realm_id: str = None, cache: Dict[str, Optional[str]] = None) -> Optional[str]:
    """
    Try to map the memo text (tender type) to a QBO PaymentMethod Id.
    
    If token_mgr and realm_id are provided, queries QBO by name.
    Otherwise, falls back to legacy hardcoded mapping (Company A).
    
    Includes mapping for common variations (e.g., "Card" -> "Card payment").
    """
    if not memo:
        return None
    memo_clean = memo.strip()
    
    # Payment method name mapping (CSV value -> QBO name)
    # This handles cases where CSV uses different names than QBO
    PAYMENT_METHOD_MAPPING = {
        "Card": "Card payment",  # CSV "Card" -> QBO "Card payment"
    }
    
    # Map CSV value to QBO name if needed
    qbo_name = PAYMENT_METHOD_MAPPING.get(memo_clean, memo_clean)
    
    # If we have QBO access, query by name (preferred)
    if token_mgr and realm_id and cache is not None:
        # Try mapped name first
        payment_method_id = get_payment_method_id_by_name(qbo_name, token_mgr, realm_id, cache)
        if payment_method_id:
            return payment_method_id
        # If mapped name not found, try original name as fallback
        if qbo_name != memo_clean:
            payment_method_id = get_payment_method_id_by_name(memo_clean, token_mgr, realm_id, cache)
            if payment_method_id:
                return payment_method_id
        return None
    
    # Fallback to legacy mapping (backward compatibility)
    return LEGACY_PAYMENT_METHOD_BY_NAME.get(memo_clean)


def _qbo_headers(access_token: str) -> dict:
    return {
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Content-Type": "application/json",
    }


class TokenManager:
    """
    Manages QBO access token state during a run.
    Automatically refreshes token on 401 errors.
    Uses token_manager for company-specific token isolation.
    """
    def __init__(self, company_key: str, realm_id: str):
        self.company_key = company_key
        self.realm_id = realm_id
        self.access_token = get_access_token(company_key, realm_id)
    
    def get(self) -> str:
        """Get the current access token."""
        return self.access_token
    
    def refresh(self) -> str:
        """Refresh the access token and update internal state."""
        tokens = refresh_access_token(self.company_key, self.realm_id)
        self.access_token = tokens["access_token"]
        return self.access_token


def _make_qbo_request(
    method: str,
    url: str,
    token_mgr: TokenManager,
    **kwargs
) -> requests.Response:
    """
    Make a QBO API request with automatic token refresh on 401 errors.
    
    Args:
        method: HTTP method ('GET', 'POST', etc.)
        url: Full URL for the request
        token_mgr: TokenManager instance to get/refresh tokens
        **kwargs: Additional arguments to pass to requests (headers, json, data, etc.)
    
    Returns:
        requests.Response object
    """
    # Ensure headers include the access token
    headers = kwargs.pop("headers", {})
    if "Authorization" not in headers:
        headers.update(_qbo_headers(token_mgr.get()))
    kwargs["headers"] = headers
    
    # Make the request
    resp = requests.request(method, url, **kwargs)
    
    # If we get a 401, refresh token and retry once
    if resp.status_code == 401:
        print("[INFO] Got 401, refreshing access token and retrying...")
        token_mgr.refresh()
        # Update headers with new token
        headers["Authorization"] = f"Bearer {token_mgr.get()}"
        kwargs["headers"] = headers
        resp = requests.request(method, url, **kwargs)
    
    return resp


def get_tax_code_id_by_name(name: str, token_mgr: TokenManager, realm_id: str, cache: Dict[str, Optional[str]]) -> Optional[str]:
    """
    Resolve a TaxCode name to a TaxCode Id with simple caching.
    
    - If the name exists in cache, reuse its Id.
    - Otherwise, try a QBO query by Name.
    - Returns None if tax code not found or name is empty.
    """
    if not name or not name.strip():
        return None
    
    name_clean = name.strip()
    if name_clean in cache:
        return cache[name_clean]
    
    safe_name = name_clean.replace("'", "''")
    query = f"select Id, Name from TaxCode where Name = '{safe_name}'"
    url = f"{BASE_URL}/v3/company/{realm_id}/query?query={quote(query)}&minorversion=70"
    
    resp = _make_qbo_request("GET", url, token_mgr)
    tax_code_id: Optional[str] = None
    if resp.status_code == 200:
        data = resp.json()
        tax_codes = data.get("QueryResponse", {}).get("TaxCode", [])
        if tax_codes:
            tax_code_id = tax_codes[0].get("Id")
    
    if tax_code_id:
        cache[name_clean] = tax_code_id
    else:
        # Cache None to avoid repeated failed queries
        cache[name_clean] = None
    
    return tax_code_id


def load_category_account_mapping(config) -> Dict[str, Dict[str, str]]:
    """
    Load category → account mapping from Product.Mapping.csv.
    Tolerates header variants (Categories/Category, Cost of Sale Account/COGS, etc.)
    and strips whitespace. Ignores unnamed columns.
    
    Args:
        config: CompanyConfig instance
    
    Returns:
        Dict mapping normalized category to account names:
        {category_normalized: {asset: "...", income: "...", expense: "..."}}
    
    Raises:
        FileNotFoundError: If mapping file doesn't exist
        ValueError: If CSV is malformed or required columns missing
    """
    mapping_file = config.product_mapping_file
    
    if not mapping_file.exists():
        raise FileNotFoundError(
            f"Product mapping file not found: {mapping_file}. "
            f"Please ensure mappings/Product.Mapping.csv exists or set product_mapping_file in config."
        )
    
    try:
        df = pd.read_csv(mapping_file)
    except Exception as e:
        raise ValueError(f"Failed to read Product.Mapping.csv: {e}")
    
    # Normalize column names: strip, lowercase for matching
    def norm(col: str) -> str:
        return str(col).strip().lower()
    
    # Synonyms -> canonical names
    synonym_to_canonical = {
        "category": "category",
        "categories": "category",
        "inventory account": "inventory account",
        "revenue account": "revenue account",
        "cost of sale account": "cost of sale account",
        "cost of sale": "cost of sale account",
        "cogs": "cost of sale account",
    }
    required_canonicals = {"category", "inventory account", "revenue account", "cost of sale account"}
    
    # Build canonical -> actual column name (first match wins)
    canonical_to_actual: Dict[str, str] = {}
    detected_headers = list(df.columns)
    
    for actual_col in df.columns:
        n = norm(actual_col)
        if not n or re.match(r"^unnamed", n):
            continue
        canonical = synonym_to_canonical.get(n)
        if canonical and canonical not in canonical_to_actual:
            canonical_to_actual[canonical] = actual_col
    
    missing = required_canonicals - set(canonical_to_actual.keys())
    if missing:
        raise ValueError(
            f"Product.Mapping.csv missing required columns (after normalization): {', '.join(sorted(missing))}. "
            f"Detected headers: {detected_headers}. "
            f"Canonical mapping used: {canonical_to_actual}"
        )
    
    category_col = canonical_to_actual["category"]
    inventory_col = canonical_to_actual["inventory account"]
    revenue_col = canonical_to_actual["revenue account"]
    cost_col = canonical_to_actual["cost of sale account"]
    
    mapping = {}
    for _, row in df.iterrows():
        category = str(row[category_col]).strip()
        category = re.sub(r"\s+", " ", category)
        if not category or category.lower() in ("nan", "none", ""):
            continue
        mapping[category] = {
            "asset": str(row[inventory_col]).strip(),
            "income": str(row[revenue_col]).strip(),
            "expense": str(row[cost_col]).strip(),
        }
    
    if not mapping:
        raise ValueError("Product.Mapping.csv contains no valid category mappings")
    
    print(f"[INFO] Mapping loader: file={mapping_file}, detected_headers={detected_headers}, "
          f"canonical_to_actual={canonical_to_actual}, categories_loaded={len(mapping)}")
    
    return mapping


def resolve_account_id_by_name(account_string: str, token_mgr: TokenManager, realm_id: str, cache: Dict[str, Optional[str]]) -> Optional[str]:
    """
    Resolve an account string to a QBO Account ID using Name-based (leaf) matching only.

    Leaf = substring after last ':' then strip. E.g. "120000 - Inventory:120300 - Non - Food Items" -> "120300 - Non - Food Items".

    Resolution strategy:
    1. Primary: Query by exact Name = leaf
    2. Fallback: Query by Name like '%leaf%'; if multiple, pick exact match if present else first Active

    Args:
        account_string: Account string from mapping CSV
        token_mgr: TokenManager instance
        realm_id: QBO Realm ID
        cache: Account cache dict {account_string: account_id}

    Returns:
        Account ID or None if not found

    Raises:
        ValueError: When resolution fails, with original mapping string, leaf used, and exact QBO query(ies) tried.
    """
    if not account_string or not account_string.strip():
        return None

    account_string = account_string.strip()

    # Check cache first
    if account_string in cache:
        return cache[account_string]

    leaf = account_string.split(":")[-1].strip()
    if not leaf:
        cache[account_string] = None
        raise ValueError(
            f"Account resolution failed: mapping_string={account_string!r} leaf={leaf!r} (empty after last ':')"
        )

    safe_leaf = leaf.replace("'", "''")
    account_id = None
    queries_tried: List[str] = []

    # Primary: exact Name = leaf
    query_exact = f"select Id, Name from Account where Name = '{safe_leaf}' maxresults 10"
    queries_tried.append(query_exact)
    url = f"{BASE_URL}/v3/company/{realm_id}/query?query={quote(query_exact)}&minorversion=70"
    resp = _make_qbo_request("GET", url, token_mgr)
    if resp.status_code == 200:
        data = resp.json()
        accounts = data.get("QueryResponse", {}).get("Account", [])
        if isinstance(accounts, dict):
            accounts = [accounts]
        if accounts:
            account_id = accounts[0].get("Id")
    
    # Fallback: Name like '%leaf%'
    if not account_id:
        safe_like = safe_leaf.replace("%", "").replace("_", "")[:80]
        query_like = f"select Id, Name from Account where Name like '%{safe_like}%' maxresults 10"
        queries_tried.append(query_like)
        url = f"{BASE_URL}/v3/company/{realm_id}/query?query={quote(query_like)}&minorversion=70"
        resp = _make_qbo_request("GET", url, token_mgr)
        if resp.status_code == 200:
            data = resp.json()
            accounts = data.get("QueryResponse", {}).get("Account", [])
            if not isinstance(accounts, list):
                accounts = [accounts] if accounts else []
            if accounts:
                exact = next((a for a in accounts if (a.get("Name") or "") == leaf), None)
                if exact:
                    account_id = exact.get("Id")
                else:
                    active_first = next((a for a in accounts if a.get("Active", True)), accounts[0])
                    account_id = active_first.get("Id")

    if not account_id:
        cache[account_string] = None
        raise ValueError(
            f"Account resolution failed: mapping_string={account_string!r} leaf={leaf!r} queries_tried={queries_tried}"
        )

    cache[account_string] = account_id
    return account_id


def build_account_refs_for_category(
    category: str,
    mapping_cache: Dict[str, Dict[str, str]],
    account_cache: Dict[str, Optional[str]],
    token_mgr: TokenManager,
    realm_id: str,
    config
) -> Dict[str, Dict[str, str]]:
    """
    Build account references for a category.
    
    Args:
        category: Product category from EPOS CSV
        mapping_cache: Category → account names mapping
        account_cache: Account string → account ID cache
        token_mgr: TokenManager instance
        realm_id: QBO Realm ID
        config: CompanyConfig instance
    
    Returns:
        Dict with AssetAccountRef, IncomeAccountRef, ExpenseAccountRef
    
    Raises:
        ValueError: If category missing in mapping or account not found
    """
    # Normalize category: strip whitespace and collapse repeated whitespace
    category_normalized = category.strip()
    category_normalized = re.sub(r'\s+', ' ', category_normalized)
    
    # Lookup category in mapping
    if category_normalized not in mapping_cache:
        raise ValueError(
            f"Missing category '{category}' (normalized: '{category_normalized}') "
            f"in Product.Mapping.csv for company {config.company_key}"
        )
    
    account_names = mapping_cache[category_normalized]
    
    # Resolve each account name → ID
    asset_account_id = resolve_account_id_by_name(
        account_names["asset"], token_mgr, realm_id, account_cache
    )
    income_account_id = resolve_account_id_by_name(
        account_names["income"], token_mgr, realm_id, account_cache
    )
    expense_account_id = resolve_account_id_by_name(
        account_names["expense"], token_mgr, realm_id, account_cache
    )
    
    # Fail fast if any account not found
    if not asset_account_id:
        raise ValueError(
            f"Account '{account_names['asset']}' not found in QBO for category '{category}' "
            f"(company {config.company_key})"
        )
    if not income_account_id:
        raise ValueError(
            f"Account '{account_names['income']}' not found in QBO for category '{category}' "
            f"(company {config.company_key})"
        )
    if not expense_account_id:
        raise ValueError(
            f"Account '{account_names['expense']}' not found in QBO for category '{category}' "
            f"(company {config.company_key})"
        )
    
    return {
        "AssetAccountRef": {"value": asset_account_id},
        "IncomeAccountRef": {"value": income_account_id},
        "ExpenseAccountRef": {"value": expense_account_id},
    }


def get_department_id(name: str, token_mgr: TokenManager, realm_id: str, cache: Dict[str, Optional[str]], config=None) -> Optional[str]:
    """
    Resolve a Department (shown as "Location" in the QBO UI) name to a Department Id with simple caching.

    - First checks if there's a department_mapping in config (maps CSV location -> QBO Department ID)
    - If the name exists in cache, reuse its Id.
    - Otherwise, try a QBO query by Name.
    - Returns None if department not found or name is empty.
    """
    if not name or not name.strip():
        return None
    
    name_clean = name.strip()
    
    # Check config mapping first (if available)
    if config:
        department_mapping = config.get_qbo_config().get("department_mapping", {})
        if name_clean in department_mapping:
            department_id = department_mapping[name_clean]
            cache[name_clean] = department_id  # Cache it for future use
            return department_id
    
    # Check cache
    if name_clean in cache:
        return cache[name_clean]
    
    # Query QBO by name
    safe_name = name_clean.replace("'", "''")
    query = f"select Id from Department where Name = '{safe_name}'"
    url = f"{BASE_URL}/v3/company/{realm_id}/query?query={quote(query)}&minorversion=70"
    
    resp = _make_qbo_request("GET", url, token_mgr)
    department_id: Optional[str] = None
    if resp.status_code == 200:
        data = resp.json()
        departments = data.get("QueryResponse", {}).get("Department", [])
        if departments:
            department_id = departments[0].get("Id")
    
    if department_id:
        cache[name_clean] = department_id
    else:
        # Cache None to avoid repeated failed queries
        cache[name_clean] = None
    
    return department_id


def find_inventory_items_with_future_start_date(
    token_mgr: TokenManager,
    realm_id: str,
    target_date: str,
) -> List[Dict[str, Any]]:
    """
    Find QBO Inventory items whose InvStartDate is after the run target_date.
    Such items can cause QBO error 6270 (Transaction date prior to start date).

    Read-only: does not mutate QBO data.
    """
    # TrackQtyOnHand = true: items that enforce InvStartDate; include all Active states
    query = "select Id, Name, InvStartDate, Active from Item where Type = 'Inventory' and TrackQtyOnHand = true maxresults 1000"
    url = f"{BASE_URL}/v3/company/{realm_id}/query?query={quote(query)}&minorversion=70"
    resp = _make_qbo_request("GET", url, token_mgr)
    issues: List[Dict[str, Any]] = []
    if resp.status_code != 200:
        return issues
    data = resp.json()
    items = data.get("QueryResponse", {}).get("Item", [])
    if not isinstance(items, list):
        items = [items] if items else []
    for item in items:
        inv_start = item.get("InvStartDate")
        if not inv_start:
            continue
        # Parse as YYYY-MM-DD (QBO may return full ISO or date-only)
        inv_date_str = inv_start[:10] if len(inv_start) >= 10 else inv_start
        try:
            if inv_date_str > target_date:
                issues.append({
                    "Id": item.get("Id", ""),
                    "Name": item.get("Name", ""),
                    "InvStartDate": inv_date_str,
                    "Active": item.get("Active", ""),
                })
        except (TypeError, ValueError):
            continue
    issues.sort(key=lambda x: x.get("InvStartDate", ""))
    return issues


def write_inventory_start_date_issues_report(
    issues: List[Dict[str, Any]],
    company_key: str,
    target_date: str,
    out_dir: str = "reports",
) -> Optional[str]:
    """
    Write a CSV report of inventory items with InvStartDate after target_date.
    Returns the file path if written, else None.
    """
    if not issues:
        return None
    Path(out_dir).mkdir(parents=True, exist_ok=True)
    safe_date = target_date.replace("-", "")
    safe_key = re.sub(r"[^\w-]", "_", company_key)
    filename = f"inventory_start_date_issues_{safe_key}_{target_date}.csv"
    filepath = os.path.join(out_dir, filename)
    with open(filepath, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["Id", "Name", "InvStartDate", "Active"])
        w.writeheader()
        w.writerows(issues)
    return filepath


def get_or_create_item_category_id(
    token_mgr: TokenManager,
    realm_id: str,
    category_name: str,
    cache: Optional[Dict[str, str]] = None,
) -> str:
    """
    Resolve or create a QBO Item of Type="Category" for the given category name.
    Used to assign ParentRef/SubItem when creating Inventory items so they appear under the category in QBO UI.

    Args:
        token_mgr: TokenManager instance
        realm_id: QBO Realm ID
        category_name: EPOS category string (e.g. "COSMETICS AND TOILETRIES")
        cache: Optional dict to cache category_name -> item_id (avoids repeated queries)

    Returns:
        QBO Item Id of the Category item

    Raises:
        RuntimeError: If GET or POST fails
    """
    category_normalized = (category_name or "").strip()
    category_normalized = re.sub(r"\s+", " ", category_normalized) if category_normalized else ""
    if not category_normalized:
        raise ValueError("Category name is empty after normalization")

    if cache is not None and category_normalized in cache:
        return cache[category_normalized]

    safe_name = category_normalized.replace("'", "''")
    query = (
        f"select Id, Name, Type, Active from Item where Type = 'Category' and Name = '{safe_name}' maxresults 10"
    )
    url = f"{BASE_URL}/v3/company/{realm_id}/query?query={quote(query)}&minorversion=70"
    resp = _make_qbo_request("GET", url, token_mgr)

    if resp.status_code == 200:
        data = resp.json()
        items = data.get("QueryResponse", {}).get("Item", [])
        if not isinstance(items, list):
            items = [items] if items else []
        if items:
            # Prefer Active=True; if multiple, prefer exact Name match
            active_first = [i for i in items if i.get("Active", True)]
            candidates = active_first if active_first else items
            exact = next(
                (i for i in candidates if (i.get("Name") or "").strip() == category_normalized),
                None,
            )
            chosen = exact or candidates[0]
            cat_id = chosen.get("Id")
            if cat_id:
                if cache is not None:
                    cache[category_normalized] = cat_id
                print(f"[INFO] Reused existing Category item: Name={category_normalized!r} Id={cat_id}")
                return cat_id

    # Create Category item
    create_url = f"{BASE_URL}/v3/company/{realm_id}/item?minorversion=70"
    payload = {
        "Name": category_normalized,
        "Type": "Category",
        "Active": True,
    }
    create_resp = _make_qbo_request("POST", create_url, token_mgr, json=payload)
    if create_resp.status_code not in (200, 201):
        error_msg = f"Failed to create Category item '{category_normalized}': HTTP {create_resp.status_code}"
        try:
            body = create_resp.json()
            fault = body.get("fault")
            if fault:
                errors = fault.get("error", [])
                if errors:
                    error_msg += "\n" + "; ".join(
                        err.get("message", err.get("detail", "")) for err in errors
                    )
        except Exception:
            error_msg += f"\nResponse: {create_resp.text[:500]}"
        raise RuntimeError(error_msg)

    created = create_resp.json().get("Item")
    if not created:
        raise RuntimeError(f"No Item in response when creating Category '{category_normalized}'")
    cat_id = created.get("Id")
    if not cat_id:
        raise RuntimeError(f"Created Category item has no Id: {created}")
    if cache is not None:
        cache[category_normalized] = cat_id
    print(f"[INFO] Created Category item: Name={category_normalized!r} Id={cat_id}")
    return cat_id


def create_inventory_item(
    name: str,
    category: str,
    unit_sales_price: float,
    unit_purchase_cost: float,
    config,
    token_mgr: TokenManager,
    realm_id: str,
    mapping_cache: Dict[str, Dict[str, str]],
    account_cache: Dict[str, Optional[str]],
    target_date: Optional[str] = None,
    category_item_id: Optional[str] = None,
) -> str:
    """
    Create a QBO Inventory item.
    
    Args:
        name: Product name
        category: Product category (normalized)
        unit_sales_price: Per-unit sales price (NET Sales / qty; tax-inclusive in UI)
        unit_purchase_cost: Per-unit purchase cost (Cost Price / qty)
        config: CompanyConfig instance
        token_mgr: TokenManager instance
        realm_id: QBO Realm ID
        mapping_cache: Category → account names mapping
        account_cache: Account string → account ID cache
        target_date: Optional run target date (YYYY-MM-DD). When set, InvStartDate
            is set to target_date so receipts for that date are allowed; otherwise
            config.inventory_start_date is used.
        category_item_id: Optional QBO Item Id of Type="Category" to set as parent (SubItem=True, ParentRef).
            When set, the new Inventory item appears under that category in QBO UI.
    
    Returns:
        Created item ID

    Raises:
        ValueError: If category missing or accounts not found
        RuntimeError: If QBO API call fails
    """
    # Build account references
    account_refs = build_account_refs_for_category(
        category, mapping_cache, account_cache, token_mgr, realm_id, config
    )
    
    # Use run target_date for InvStartDate when set (e.g. "yesterday" run), so receipts
    # for that date are allowed. Otherwise fall back to config.inventory_start_date.
    inv_start_date = target_date if target_date else config.inventory_start_date
    
    tax_code_id = config.tax_code_id or "2"
    
    # Build Item payload: pricing + tax-inclusive + tax code refs
    payload = {
        "Name": name,
        "Type": "Inventory",
        "TrackQtyOnHand": True,
        "QtyOnHand": config.default_qty_on_hand,
        "InvStartDate": inv_start_date,
        "Description": f"Sale(s) of {name}",
        "UnitPrice": unit_sales_price,
        "PurchaseCost": unit_purchase_cost,
        "SalesTaxIncluded": True,
        "PurchaseTaxIncluded": True,
        "Taxable": True,
        "IncomeAccountRef": account_refs["IncomeAccountRef"],
        "AssetAccountRef": account_refs["AssetAccountRef"],
        "ExpenseAccountRef": account_refs["ExpenseAccountRef"],
        "PurchaseDesc": f"Purchase of {name}",
    }
    if tax_code_id:
        payload["SalesTaxCodeRef"] = {"value": tax_code_id}
        payload["PurchaseTaxCodeRef"] = {"value": tax_code_id}

    if category_item_id:
        payload["SubItem"] = True
        payload["ParentRef"] = {"value": category_item_id}
        print(f"[INFO] Attached ParentRef/SubItem to Inventory item '{name}' (category item Id: {category_item_id})")
    else:
        print(f"[WARN] No category item id; creating Inventory item '{name}' without ParentRef/SubItem")
    
    # Create item via QBO API (tax codes applied at SalesReceipt line level; Item-level refs may be rejected in some regions)
    create_url = f"{BASE_URL}/v3/company/{realm_id}/item?minorversion=70"
    create_resp = _make_qbo_request("POST", create_url, token_mgr, json=payload)
    
    if create_resp.status_code in (200, 201):
        created = create_resp.json().get("Item")
        if created:
            item_id = created.get("Id")
            if item_id:
                print(f"[INFO] Created Inventory item '{name}' (ID: {item_id})")
                return item_id
    
    # On 400, retry without tax code refs (QBO may reject them for Inventory in some regions)
    if create_resp.status_code == 400 and ("SalesTaxCodeRef" in payload or "PurchaseTaxCodeRef" in payload):
        payload_retry = {k: v for k, v in payload.items() if k not in ("SalesTaxCodeRef", "PurchaseTaxCodeRef")}
        create_resp = _make_qbo_request("POST", create_url, token_mgr, json=payload_retry)
        if create_resp.status_code in (200, 201):
            created = create_resp.json().get("Item")
            if created and created.get("Id"):
                print(f"[WARN] QBO rejected SalesTaxCodeRef/PurchaseTaxCodeRef on create; item created without them.")
                print(f"[INFO] Created Inventory item '{name}' (ID: {created.get('Id')})")
                return created.get("Id")
    
    # Failed to create
    error_msg = f"Failed to create Inventory item '{name}': HTTP {create_resp.status_code}"
    try:
        error_body = create_resp.json()
        fault = error_body.get("fault")
        if fault:
            errors = fault.get("error", [])
            if errors:
                error_details = [err.get("message", err.get("detail", "")) for err in errors]
                error_msg += f"\nError details: {'; '.join(error_details)}"
    except Exception:
        error_msg += f"\nResponse: {create_resp.text[:500]}"
    
    raise RuntimeError(error_msg)


def rename_and_inactivate_item(
    token_mgr: TokenManager,
    realm_id: str,
    item_id: str,
    new_name: str,
    *,
    make_inactive: bool = True,
) -> dict:
    """
    Rename and optionally inactivate a QBO Item to free its name for inventory creation.
    
    Args:
        token_mgr: TokenManager instance
        realm_id: QBO Realm ID
        item_id: Item ID to update
        new_name: New name for the item
        make_inactive: Whether to set Active=False (default: True)
    
    Returns:
        Updated item JSON from QBO response
    
    Raises:
        RuntimeError: If GET or POST fails
    """
    # Fetch current item to get SyncToken and preserve fields
    get_url = f"{BASE_URL}/v3/company/{realm_id}/item/{item_id}?minorversion=70"
    get_resp = _make_qbo_request("GET", get_url, token_mgr)
    
    if get_resp.status_code != 200:
        error_msg = f"Failed to fetch item {item_id} for rename: HTTP {get_resp.status_code}"
        try:
            error_body = get_resp.json()
            fault = error_body.get("fault")
            if fault:
                errors = fault.get("error", [])
                if errors:
                    error_details = [err.get("message", err.get("detail", "")) for err in errors]
                    error_msg += f"\nError details: {'; '.join(error_details)}"
        except Exception:
            error_msg += f"\nResponse: {get_resp.text[:500]}"
        raise RuntimeError(error_msg)
    
    current_item = get_resp.json().get("Item")
    if not current_item:
        raise RuntimeError(f"No Item in response when fetching {item_id}")
    
    old_name = current_item.get("Name", "")
    sync_token = current_item.get("SyncToken")
    if not sync_token:
        raise RuntimeError(f"Item {item_id} missing SyncToken (required for updates)")
    
    # Idempotency: if name already contains "(LEGACY", do not re-rename; only inactivate
    current_name = (old_name or "").strip()
    if "(LEGACY" in current_name.upper():
        effective_name = old_name  # keep current name, avoid "LEGACY LEGACY ..."
        idempotent_rename = True
    else:
        effective_name = new_name
        idempotent_rename = False

    # Sparse update: only Id, SyncToken, Name, Active (QBO-safe; no Type/account refs required)
    update_payload = {
        "sparse": True,
        "Id": item_id,
        "SyncToken": sync_token,
        "Name": effective_name,
        "Active": False if make_inactive else current_item.get("Active", True),
    }

    # Update item via QBO API
    update_url = f"{BASE_URL}/v3/company/{realm_id}/item?minorversion=70"
    update_resp = _make_qbo_request("POST", update_url, token_mgr, json=update_payload)
    
    if update_resp.status_code not in (200, 201):
        error_msg = f"Failed to rename/inactivate item {item_id}: HTTP {update_resp.status_code}"
        try:
            error_body = update_resp.json()
            fault = error_body.get("fault")
            if fault:
                errors = fault.get("error", [])
                if errors:
                    error_details = [err.get("message", err.get("detail", "")) for err in errors]
                    error_msg += f"\nError details: {'; '.join(error_details)}"
        except Exception:
            error_msg += f"\nResponse: {update_resp.text[:500]}"
        raise RuntimeError(error_msg)
    
    updated_item = update_resp.json().get("Item")
    if not updated_item:
        raise RuntimeError(f"No Item in response when updating {item_id}")
    
    active_status = "Active=False" if make_inactive else f"Active={updated_item.get('Active', True)}"
    if idempotent_rename:
        print(f"[INFO] Inactivated item (already LEGACY-named): Id={item_id} name={effective_name!r} {active_status}")
    else:
        print(f"[INFO] Renamed and inactivated item: Id={item_id} old_name={old_name!r} new_name={effective_name!r} {active_status}")
    
    return updated_item


def get_or_create_item_id(
    name: str,
    token_mgr: TokenManager,
    realm_id: str,
    config,
    cache: Dict[str, str],
    category: Optional[str] = None,
    unit_sales_price: Optional[float] = None,
    unit_purchase_cost: Optional[float] = None,
    mapping_cache: Optional[Dict[str, Dict[str, str]]] = None,
    account_cache: Optional[Dict[str, Optional[str]]] = None,
    target_date: Optional[str] = None,
    items_wrong_type: Optional[List[Dict[str, Any]]] = None,
    items_autofixed: Optional[List[Dict[str, Any]]] = None,
    category_item_cache: Optional[Dict[str, str]] = None,
    items_patched_pricing_tax: Optional[List[Dict[str, Any]]] = None,
) -> Tuple[str, bool, str, Optional[str]]:
    """
    Resolve an Item name to an Item Id with simple caching.
    Returns (item_id, was_created, created_type, fallback_reason).
    created_type: "Inventory" | "Service" | "Default" | "existing" | "existing_inventory" | "existing_non_inventory" | "created_inventory_after_fix"
    fallback_reason: optional, e.g. "blank_name", "inventory_failed", "service_creation_failed"
    When inventory mode is enabled and an existing item has Type != Inventory, appends to items_wrong_type if provided.
    When auto_fix_wrong_type_items is enabled, renames/inactivates wrong-type items and creates inventory items.
    """
    default_item_id = config.get_qbo_config().get("default_item_id", "1")
    default_income_account_id = config.get_qbo_config().get("default_income_account_id", "1")
    auto_create_items = True  # Can be made configurable later
    created_type = "existing"
    fallback_reason: Optional[str] = None

    # Normalize name: strip and collapse internal whitespace (use for cache and QBO)
    name = (name or "").strip()
    name = re.sub(r"\s+", " ", name) if name else ""

    if not name:
        print(f"[WARN] Blank item name → using DEFAULT_ITEM_ID")
        return (default_item_id, False, "Default", "blank_name")

    if name in cache:
        return (cache[name], False, "existing", None)

    safe_name = name.replace("'", "''")
    query = f"select Id, Name, Type, TrackQtyOnHand, InvStartDate, Active from Item where Name = '{safe_name}' maxresults 10"
    url = f"{BASE_URL}/v3/company/{realm_id}/query?query={quote(query)}&minorversion=70"

    resp = _make_qbo_request("GET", url, token_mgr)
    item_id: Optional[str] = None
    was_created = False
    # Track auto-fix state: if we rename/inactivate a wrong-type item, remember its details
    autofixed_old_item_id: Optional[str] = None
    autofixed_old_type: Optional[str] = None
    autofixed_effective_new_name: Optional[str] = None  # actual name after update (for report)
    if resp.status_code == 200:
        data = resp.json()
        items = data.get("QueryResponse", {}).get("Item", [])
        if not isinstance(items, list):
            items = [items] if items else []
        if items:
            first = items[0]
            item_id = first.get("Id")
            item_type = first.get("Type") or ""
            if item_type == "Inventory":
                created_type = "existing_inventory"
                # PATCH existing Inventory: category (ParentRef/SubItem) and/or pricing/tax (UnitPrice, PurchaseCost, tax flags)
                try:
                    get_url = f"{BASE_URL}/v3/company/{realm_id}/item/{item_id}?minorversion=70"
                    get_resp = _make_qbo_request("GET", get_url, token_mgr)
                    if get_resp.status_code == 200:
                        current = get_resp.json().get("Item")
                        if current and current.get("SyncToken") is not None:
                            patch_payload = {
                                "Id": item_id,
                                "SyncToken": current["SyncToken"],
                                "sparse": True,
                                "Type": "Inventory",
                            }
                            # Category: add ParentRef/SubItem if missing
                            parent_ref = current.get("ParentRef")
                            if (category or "").strip() and category_item_cache is not None and (not parent_ref or not parent_ref.get("value")):
                                cat_id = get_or_create_item_category_id(
                                    token_mgr, realm_id, category, cache=category_item_cache
                                )
                                patch_payload["SubItem"] = True
                                patch_payload["ParentRef"] = {"value": cat_id}
                            # Pricing/tax: only set if current is 0/missing/false; do NOT overwrite non-zero prices
                            tax_code_id = config.tax_code_id or "2"
                            cur_unit_price = current.get("UnitPrice")
                            cur_purchase_cost = current.get("PurchaseCost")
                            cur_sales_tax_inc = current.get("SalesTaxIncluded", True)
                            cur_purchase_tax_inc = current.get("PurchaseTaxIncluded", True)
                            cur_taxable = current.get("Taxable", True)
                            cur_sales_tax_ref = (current.get("SalesTaxCodeRef") or {}).get("value") if isinstance(current.get("SalesTaxCodeRef"), dict) else None
                            cur_purchase_tax_ref = (current.get("PurchaseTaxCodeRef") or {}).get("value") if isinstance(current.get("PurchaseTaxCodeRef"), dict) else None
                            pricing_tax_changes = []
                            if (cur_unit_price is None or float(cur_unit_price or 0) == 0) and unit_sales_price is not None and unit_sales_price > 0:
                                patch_payload["UnitPrice"] = unit_sales_price
                                pricing_tax_changes.append(f"UnitPrice:0->{unit_sales_price}")
                            if (cur_purchase_cost is None or float(cur_purchase_cost or 0) == 0) and unit_purchase_cost is not None and unit_purchase_cost > 0:
                                patch_payload["PurchaseCost"] = unit_purchase_cost
                                pricing_tax_changes.append(f"PurchaseCost:0->{unit_purchase_cost}")
                            if not cur_sales_tax_inc:
                                patch_payload["SalesTaxIncluded"] = True
                                pricing_tax_changes.append("SalesTaxIncluded:False->True")
                            if not cur_purchase_tax_inc:
                                patch_payload["PurchaseTaxIncluded"] = True
                                pricing_tax_changes.append("PurchaseTaxIncluded:False->True")
                            if not cur_taxable:
                                patch_payload["Taxable"] = True
                                pricing_tax_changes.append("Taxable:False->True")
                            if tax_code_id and not cur_sales_tax_ref:
                                patch_payload["SalesTaxCodeRef"] = {"value": tax_code_id}
                                pricing_tax_changes.append("SalesTaxCodeRef:->" + tax_code_id)
                            if tax_code_id and not cur_purchase_tax_ref:
                                patch_payload["PurchaseTaxCodeRef"] = {"value": tax_code_id}
                                pricing_tax_changes.append("PurchaseTaxCodeRef:->" + tax_code_id)
                            if len(patch_payload) > 4:
                                patch_url = f"{BASE_URL}/v3/company/{realm_id}/item?minorversion=70"
                                patch_resp = _make_qbo_request("POST", patch_url, token_mgr, json=patch_payload)
                                if patch_resp.status_code in (200, 201):
                                    if "ParentRef" in patch_payload:
                                        print(f"[INFO] Categorized existing Inventory item: Id={item_id} ParentRef={patch_payload['ParentRef']['value']} category={category!r}")
                                    if pricing_tax_changes:
                                        print(f"[INFO] Patched Inventory item fields: Id={item_id} " + " ".join(pricing_tax_changes))
                                        if items_patched_pricing_tax is not None:
                                            items_patched_pricing_tax.append({
                                                "ItemId": item_id,
                                                "Name": name,
                                                "Category": category or "",
                                                "UnitPrice_old": cur_unit_price,
                                                "UnitPrice_new": patch_payload.get("UnitPrice", cur_unit_price),
                                                "PurchaseCost_old": cur_purchase_cost,
                                                "PurchaseCost_new": patch_payload.get("PurchaseCost", cur_purchase_cost),
                                                "SalesTaxIncluded_old/new": f"{cur_sales_tax_inc}->{patch_payload.get('SalesTaxIncluded', cur_sales_tax_inc)}",
                                                "PurchaseTaxIncluded_old/new": f"{cur_purchase_tax_inc}->{patch_payload.get('PurchaseTaxIncluded', cur_purchase_tax_inc)}",
                                                "Taxable_old/new": f"{cur_taxable}->{patch_payload.get('Taxable', cur_taxable)}",
                                                "TxnDate": target_date or "",
                                                "DocNumber": "",
                                            })
                                else:
                                    if "SalesTaxCodeRef" in patch_payload or "PurchaseTaxCodeRef" in patch_payload:
                                        try:
                                            err_body = patch_resp.json()
                                            err_msg = str(err_body.get("fault", {}).get("error", [{}])[0].get("message", patch_resp.text[:200]))
                                            if "400" in str(patch_resp.status_code):
                                                print(f"[WARN] QBO rejected tax refs on item {item_id}: {err_msg}. Taxable/included flags and pricing still applied if sent.")
                                        except Exception:
                                            pass
                                    print(f"[WARN] Failed to PATCH item {item_id}: HTTP {patch_resp.status_code}")
                except Exception as e:
                    print(f"[WARN] Failed to patch existing Inventory item {item_id}: {e}")
                cache[name] = item_id
                return (item_id, False, created_type, None)
            if config.inventory_enabled:
                # Check if auto-fix is enabled
                if config.auto_fix_wrong_type_items:
                    # Try to rename and inactivate the wrong-type item
                    old_item_id = item_id
                    try:
                        new_name = f"{name} (LEGACY {item_type} {old_item_id})"
                        updated_item = rename_and_inactivate_item(token_mgr, realm_id, old_item_id, new_name, make_inactive=True)
                        autofixed_old_item_id = old_item_id
                        autofixed_old_type = item_type
                        autofixed_effective_new_name = (updated_item.get("Name") or "").strip() or new_name
                        item_id = None
                        # Continue to create path below
                    except Exception as e:
                        # Rename failed: fall back to current behavior
                        print(f"[WARN] Failed to auto-fix wrong-type item '{name}' (Id={old_item_id}): {e}. "
                              f"Will use existing item for receipt lines.")
                        if items_wrong_type is not None:
                            items_wrong_type.append({
                                "Name": name,
                                "Id": str(old_item_id) if old_item_id else "",
                                "Type": item_type,
                                "ExpectedType": "Inventory",
                            })
                        created_type = "existing_non_inventory"
                        cache[name] = old_item_id
                        return (old_item_id, False, created_type, None)
                else:
                    # Auto-fix disabled: use current behavior
                    print(f"[WARN] Inventory mode enabled but item exists as Type={item_type!r}. Cannot auto-convert. "
                          f"Will use existing item for receipt lines. name={name!r} Id={item_id}")
                    if items_wrong_type is not None:
                        items_wrong_type.append({
                            "Name": name,
                            "Id": str(item_id) if item_id else "",
                            "Type": item_type,
                            "ExpectedType": "Inventory",
                        })
                    created_type = "existing_non_inventory"
                    cache[name] = item_id
                    return (item_id, False, created_type, None)
            created_type = "existing"

    if not item_id and auto_create_items:
        if config.inventory_enabled:
            # Create Inventory item
            if not category or unit_sales_price is None or unit_purchase_cost is None:
                raise ValueError(
                    f"Inventory mode enabled but missing required parameters for item '{name}'. "
                    f"Need: category, unit_sales_price, unit_purchase_cost"
                )
            if mapping_cache is None or account_cache is None:
                raise ValueError(
                    f"Inventory mode enabled but missing mapping_cache or account_cache for item '{name}'"
                )

            category_item_id_val: Optional[str] = None
            if (category or "").strip():
                try:
                    category_item_id_val = get_or_create_item_category_id(
                        token_mgr, realm_id, category, cache=category_item_cache
                    )
                except Exception as e:
                    print(f"[WARN] Failed to resolve Category item for {category!r}: {e}. Creating Inventory item without ParentRef/SubItem.")
            else:
                print(f"[WARN] Category missing/empty; creating Inventory item '{name}' without ParentRef/SubItem")

            try:
                item_id = create_inventory_item(
                    name, category, unit_sales_price, unit_purchase_cost,
                    config, token_mgr, realm_id, mapping_cache, account_cache,
                    target_date=target_date,
                    category_item_id=category_item_id_val,
                )
                was_created = True
                # Check if this was created after auto-fix
                if autofixed_old_item_id:
                    created_type = "created_inventory_after_fix"
                    # Append to autofix report (DocNumber/TxnDate will be filled by caller)
                    if items_autofixed is not None:
                        items_autofixed.append({
                            "OriginalName": name,
                            "OldItemId": str(autofixed_old_item_id),
                            "OldType": autofixed_old_type or "",
                            "OldActive": "True",
                            "NewName": autofixed_effective_new_name or f"{name} (LEGACY {autofixed_old_type} {autofixed_old_item_id})",
                            "NewInventoryItemId": str(item_id),
                            "TxnDate": target_date or "",
                            "DocNumber": "",  # Will be filled by caller
                        })
                else:
                    created_type = "Inventory"
            except Exception as e:
                # Do NOT fall back to default "Services". Create Service item with product name.
                print(f"[WARN] Failed to create Inventory item '{name}' (category={category!r}): {e}. "
                      f"Falling back to Service item with product name (NOT default Services)")
                create_url = f"{BASE_URL}/v3/company/{realm_id}/item?minorversion=70"
                payload = {
                    "Name": name,
                    "Type": "Service",
                    "IncomeAccountRef": {"value": default_income_account_id},
                }
                create_resp = _make_qbo_request("POST", create_url, token_mgr, json=payload)
                if create_resp.status_code in (200, 201):
                    created = create_resp.json().get("Item")
                    if created:
                        item_id = created.get("Id")
                        was_created = True
                        created_type = "Service"
                        fallback_reason = "inventory_failed"
                if not item_id:
                    raise RuntimeError(
                        f"Could not create Inventory item and Service item fallback also failed for '{name}'. "
                        f"Inventory error: {e}. Do not use default Services for non-blank product names."
                    )
        else:
            # Inventory disabled: Create Service item (existing behavior)
            create_url = f"{BASE_URL}/v3/company/{realm_id}/item?minorversion=70"
            payload = {
                "Name": name,
                "Type": "Service",
                "IncomeAccountRef": {"value": default_income_account_id},
            }
            create_resp = _make_qbo_request("POST", create_url, token_mgr, json=payload)
            if create_resp.status_code in (200, 201):
                created = create_resp.json().get("Item")
                if created:
                    item_id = created.get("Id")
                    was_created = True
                    created_type = "Service"
            else:
                print(f"[WARN] Failed to create Item '{name}': {create_resp.status_code}")
                try:
                    print(create_resp.text)
                except Exception:
                    pass
                print(f"[WARN] Using DEFAULT_ITEM_ID for '{name}' (inventory disabled, service creation failed)")
                created_type = "Default"
                fallback_reason = "service_creation_failed"

    if not item_id:
        item_id = default_item_id
        created_type = "Default"
        if fallback_reason is None:
            fallback_reason = "service_creation_failed"

    cache[name] = item_id
    return (item_id, was_created, created_type, fallback_reason)


def build_sales_receipt_payload(
    group: pd.DataFrame,
    token_mgr: TokenManager,
    realm_id: str,
    config,
    item_cache: Dict[str, str],
    department_cache: Dict[str, Optional[str]],
    payment_method_cache: Dict[str, Optional[str]] = None,
    target_date: Optional[str] = None,
    mapping_cache: Optional[Dict[str, Dict[str, str]]] = None,
    account_cache: Optional[Dict[str, Optional[str]]] = None,
    items_wrong_type: Optional[List[Dict[str, Any]]] = None,
    items_autofixed: Optional[List[Dict[str, Any]]] = None,
    category_item_cache: Optional[Dict[str, str]] = None,
    items_patched_pricing_tax: Optional[List[Dict[str, Any]]] = None,
) -> dict:
    """
    Build a SalesReceipt payload from a group of CSV rows (one SalesReceiptNo).

    Behaviour:
    - One SalesReceipt per group.
    - One line per row in the group.
    - We treat ItemRate / *ItemAmount as GROSS (inclusive of VAT).
    - QBO is told that amounts are tax-inclusive, so it backs out the VAT.
    - The Rate column in QBO will match ItemRate from the CSV whenever valid.
    """
    first_row = group.iloc[0]

    # Determine TxnDate: use target_date if trading_day_enabled and target_date is provided, otherwise parse from CSV
    if config.trading_day_enabled and target_date:
        # Trading day mode: use the target_date (trading date) directly
        txn_date = target_date
        print(f"[INFO] Trading day mode: using target_date={target_date} for TxnDate (overriding CSV date)")
    else:
        # Calendar day mode: parse date from CSV using company's date_format
        txn_date_str = str(first_row[DATE_COL])
        try:
            # Parse using the company's date format
            date_obj = datetime.strptime(txn_date_str, config.date_format)
            # Convert to ISO format (YYYY-MM-DD) for QBO
            txn_date = date_obj.strftime("%Y-%m-%d")
        except (ValueError, TypeError):
            # Fallback: try to parse as ISO format or use pandas
            try:
                date_obj = pd.to_datetime(txn_date_str).to_pydatetime()
                txn_date = date_obj.strftime("%Y-%m-%d")
            except Exception:
                # Last resort: use as-is (may cause QBO errors)
                print(f"[WARN] Could not parse date '{txn_date_str}', using as-is. QBO may reject it.")
                txn_date = txn_date_str
    
    memo = str(first_row[MEMO_COL])
    doc_number = str(first_row[DOCNUM_COL])
    location_name = str(first_row.get(LOCATION_COL, "")).strip()

    lines = []
    gross_total = 0.0
    net_total = 0.0
    inventory_created_count = 0
    service_created_count = 0
    default_fallback_count = 0

    for _, row in group.iterrows():
        # Product/Service: normalize (strip + collapse whitespace)
        item_name = str(row.get(ITEM_NAME_COL, "")).strip()
        item_name = re.sub(r"\s+", " ", item_name) if item_name else ""

        # Extract category from ItemDescription (contains Category from EPOS CSV)
        category = str(row.get(ITEM_DESC_COL, "")).strip()
        if category:
            category = category.strip()
            category = re.sub(r"\s+", " ", category)
        else:
            category = None

        # Extract NET Sales and Cost Price columns
        def safe_numeric(value, default=0.0):
            """Safely convert to float, handling commas, NaN, etc."""
            if pd.isna(value) or value == "" or value is None:
                return default
            try:
                # Strip commas and convert to float
                if isinstance(value, str):
                    value = value.replace(",", "").strip()
                return float(value)
            except (TypeError, ValueError):
                return default
        
        net_sales_total = safe_numeric(row.get("NET Sales", 0))
        cost_price_total = safe_numeric(row.get("Cost Price", 0))
        amount_gross = safe_numeric(row.get(AMOUNT_COL, 0))
        
        # Quantity (default to 1 if missing/NaN or <=0)
        try:
            qty_val = safe_numeric(row.get(QTY_COL, 1))
            if qty_val <= 0:
                qty_val = 1.0
        except (TypeError, ValueError):
            qty_val = 1.0
        
        # Per-unit prices for item create/patch and receipt line
        unit_sales_price = (net_sales_total / qty_val) if qty_val else 0.0
        unit_purchase_cost = (cost_price_total / qty_val) if qty_val else 0.0
        if unit_sales_price == 0 and qty_val and amount_gross > 0:
            unit_sales_price = amount_gross / qty_val
        
        # Get or create item ID (with inventory support if enabled)
        item_ref_id, item_was_created, created_type, fallback_reason = get_or_create_item_id(
            item_name, token_mgr, realm_id, config, item_cache,
            category=category if config.inventory_enabled else None,
            unit_sales_price=unit_sales_price if config.inventory_enabled else None,
            unit_purchase_cost=unit_purchase_cost if config.inventory_enabled else None,
            mapping_cache=mapping_cache if config.inventory_enabled else None,
            account_cache=account_cache if config.inventory_enabled else None,
            target_date=target_date,
            items_wrong_type=items_wrong_type,
            items_autofixed=items_autofixed,
            category_item_cache=category_item_cache if config.inventory_enabled else None,
            items_patched_pricing_tax=items_patched_pricing_tax if config.inventory_enabled else None,
        )

        # Fill in DocNumber and TxnDate for autofixed items
        if created_type == "created_inventory_after_fix" and items_autofixed:
            # Find the most recent entry (last appended) and fill DocNumber/TxnDate
            for entry in reversed(items_autofixed):
                if entry.get("DocNumber") == "" and entry.get("OriginalName") == item_name:
                    entry["DocNumber"] = doc_number
                    entry["TxnDate"] = txn_date
                    break
        # Fill in DocNumber/TxnDate for patched pricing/tax items
        if created_type == "existing_inventory" and items_patched_pricing_tax:
            for entry in reversed(items_patched_pricing_tax):
                if entry.get("DocNumber") == "" and entry.get("Name") == item_name:
                    entry["DocNumber"] = doc_number
                    entry["TxnDate"] = txn_date
                    break

        if created_type == "Inventory" or created_type == "created_inventory_after_fix":
            inventory_created_count += 1
        elif created_type == "Service":
            service_created_count += 1
        elif created_type == "Default":
            default_fallback_count += 1

        log_line = (f"[INFO] Line item: DocNumber={doc_number} TxnDate={txn_date} item_name={item_name!r} category={category!r} "
                    f"qty={qty_val} unit_sales_price={unit_sales_price} unit_purchase_cost={unit_purchase_cost} item_id={item_ref_id} created={item_was_created} type={created_type}")
        if fallback_reason:
            log_line += f" fallback_reason={fallback_reason}"
        print(log_line)

        # Authoritative gross amount from CSV (*ItemAmount) – VAT-inclusive
        try:
            amount_csv = float(row[AMOUNT_COL])
        except (TypeError, ValueError, KeyError):
            amount_csv = 0.0

        # Per-line tax amount from CSV. If missing, derive from the configured rate.
        try:
            tax_amount = float(row.get(TAX_AMOUNT_COL, 0.0) or 0.0)
        except (TypeError, ValueError):
            tax_amount = 0.0

        # For tax-inclusive logic we treat *ItemAmount as the authoritative GROSS
        # line amount. We'll derive a net amount (for Amount) and a net UnitPrice
        # so that QBO's validation rule Amount == UnitPrice * Qty holds.
        amount_gross = amount_csv

        # Service date: fall back to TxnDate if missing or invalid
        # Parse service date using company's date_format and convert to ISO format
        service_date_str = str(row.get(SERVICE_DATE_COL, "")).strip()
        if not service_date_str or service_date_str == "nan" or service_date_str.lower() == "none":
            # Use TxnDate if Service Date is missing or empty
            service_date = txn_date
        else:
            try:
                # Parse using the company's date format
                service_date_obj = datetime.strptime(service_date_str, config.date_format)
                service_date = service_date_obj.strftime("%Y-%m-%d")
            except (ValueError, TypeError):
                # If already in ISO format or can't parse, try pandas or use TxnDate
                try:
                    service_date_obj = pd.to_datetime(service_date_str).to_pydatetime()
                    service_date = service_date_obj.strftime("%Y-%m-%d")
                except Exception:
                    # Fallback to TxnDate (already in ISO format)
                    service_date = txn_date

        # Description: prefer ItemDescription, fall back to memo
        description = str(row.get(ITEM_DESC_COL, memo))

        # Tax code handling: both companies now use vat_inclusive_7_5 mode
        # Use tax_code_id from config (Company A: "2", Company B: "22")
        tax_code_id = config.tax_code_id
        if not tax_code_id:
            # Fallback: try to query by name if tax_code_name is provided
            if hasattr(config, 'tax_code_name') and config.tax_code_name:
                if not hasattr(build_sales_receipt_payload, '_tax_code_cache'):
                    build_sales_receipt_payload._tax_code_cache = {}
                tax_code_id = get_tax_code_id_by_name(
                    config.tax_code_name, 
                    token_mgr, 
                    realm_id, 
                    build_sales_receipt_payload._tax_code_cache
                )
                if not tax_code_id:
                    print(f"[WARN] Tax code '{config.tax_code_name}' not found in QBO. Line will be created without tax code.")
            else:
                # Default fallback
                tax_code_id = "2" if config.company_key == "company_a" else "22"
        
        # QBO API: ItemRef with both value (Id) and name so the receipt shows the product name
        sales_item_detail = {
            "ItemRef": {"value": item_ref_id, "name": item_name},
            "Qty": qty_val,
            # UnitPrice will be set after we compute the net amount below
            "UnitPrice": None,
            "ServiceDate": service_date,
        }
        
        # Add tax code reference if we have one (for both Company A and Company B)
        if tax_code_id:
            sales_item_detail["TaxCodeRef"] = {"value": tax_code_id}

        # Calculate NET amount (exclusive of tax) from GROSS (tax-inclusive amount)
        # For Company A (vat_inclusive_7_5): use ItemTaxAmount from CSV if available
        # For Company B (tax_inclusive_composite): calculate from config tax_rate
        if config.tax_mode == "tax_inclusive_composite":
            # Company B: Calculate net using the full composite tax rate (12.5% = 7.5% + 5%)
            tax_rate = config.tax_rate or 0.125
            raw_amount_net = round(amount_gross / (1 + tax_rate), 2)
        else:
            # Company A: Use ItemTaxAmount from CSV
            raw_amount_net = amount_gross - tax_amount
            if raw_amount_net < 0:
                raw_amount_net = 0.0

        # Net unit price so that Amount == UnitPrice * Qty holds for QBO validation.
        unit_price_net = round(raw_amount_net / qty_val, 2) if qty_val else raw_amount_net
        amount_net = round(unit_price_net * qty_val, 2)
        sales_item_detail["UnitPrice"] = unit_price_net

        # TaxInclusiveAmt tells QBO what the original gross amount is
        # This is needed for both Company A and Company B to show correct totals
        sales_item_detail["TaxInclusiveAmt"] = amount_gross

        lines.append(
            {
                "DetailType": "SalesItemLineDetail",
                "Amount": amount_net,  # net per line; matches QBO's stored Amount
                "Description": description,
                "SalesItemLineDetail": sales_item_detail,
            }
        )
        gross_total += amount_gross
        net_total += amount_net

    payload: dict = {
        "TxnDate": txn_date,
        "PrivateNote": memo,
        "DocNumber": doc_number,
        "Line": lines,
    }
    
    # Tax handling based on company config
    if config.tax_mode == "vat_inclusive_7_5":
        # Tax-inclusive mode for Company A (single-rate VAT)
        payload["GlobalTaxCalculation"] = "TaxInclusive"
        
        # Explicit tax summary for tax-inclusive calculation
        try:
            # Get tax rate from config (Company A: 0.075 = 7.5%)
            tax_rate = config.tax_rate
            tax_percent = tax_rate * 100  # Convert to percentage for QBO
            
            net_base = round(net_total or (gross_total / (1 + tax_rate)), 2)
            total_tax = round(gross_total - net_base, 2)
            
            # Get TaxRate ID from config (required by QBO when TxnTaxDetail is provided)
            tax_rate_id = config.get_qbo_config().get("tax_rate_id")
            if not tax_rate_id:
                # Fallback: try using tax_code_id if tax_rate_id not set (Company A: "2" works for both)
                tax_rate_id = config.tax_code_id
                if not tax_rate_id:
                    raise ValueError("tax_rate_id or tax_code_id must be set in config for tax-inclusive mode")
            
            payload["TxnTaxDetail"] = {
                "TotalTax": total_tax,
                "TaxLine": [
                    {
                        "Amount": total_tax,
                        "DetailType": "TaxLineDetail",
                        "TaxLineDetail": {
                            "TaxRateRef": {"value": tax_rate_id},
                            "PercentBased": True,
                            "TaxPercent": tax_percent,
                            "NetAmountTaxable": net_base,
                        },
                    }
                ],
            }
        except Exception:
            # If anything goes wrong, fall back to letting QBO compute.
            pass
    elif config.tax_mode == "tax_inclusive_composite":
        # Tax-inclusive mode for Company B with composite tax (12.5% = 7.5% VAT + 5% Lagos)
        # 
        # Strategy: Same as Company A but with TWO TaxLines in TxnTaxDetail
        # - Line items have TaxInclusiveAmt = gross amount
        # - GlobalTaxCalculation = "TaxInclusive"
        # - TxnTaxDetail has explicit breakdown for each tax component
        # - Subtotal = gross, Total = gross (tax is INCLUDED, shown as breakdown)
        payload["GlobalTaxCalculation"] = "TaxInclusive"
        
        try:
            # Get tax components from config
            tax_components = config.get_qbo_config().get("tax_components", [])
            if not tax_components:
                raise ValueError("tax_components must be set in config for tax_inclusive_composite mode")
            
            # KEY FIX: Use net_total (sum of per-line amounts) for TxnTaxDetail
            # This matches how Company A does it in the reference script
            # The reference script uses: net_base = net_total or (gross_total / (1 + tax_rate))
            # This ensures TxnTaxDetail matches the actual sum of line amounts
            total_tax_rate = sum(c.get("rate", 0) for c in tax_components)  # 0.125 for 12.5%
            net_base = round(net_total or (gross_total / (1 + total_tax_rate)), 2)
            total_tax = round(gross_total - net_base, 2)
            
            # Build TaxLines for each component
            # Distribute tax proportionally, with last component getting the remainder
            tax_lines = []
            allocated_tax = 0.0
            
            for i, component in enumerate(tax_components):
                rate = component.get("rate", 0)  # e.g., 0.075 for 7.5%
                tax_rate_id = component.get("tax_rate_id")
                
                if not tax_rate_id:
                    raise ValueError(f"tax_rate_id missing for component: {component.get('name')}")
                
                if i == len(tax_components) - 1:
                    # Last component gets the remainder to avoid rounding errors
                    component_tax = round(total_tax - allocated_tax, 2)
                else:
                    # Calculate proportional share: (rate / total_rate) * total_tax
                    component_tax = round((rate / total_tax_rate) * total_tax, 2)
                    allocated_tax += component_tax
                
                tax_lines.append({
                    "Amount": component_tax,
                    "DetailType": "TaxLineDetail",
                    "TaxLineDetail": {
                        "TaxRateRef": {"value": tax_rate_id},
                        "PercentBased": True,
                        "TaxPercent": rate * 100,
                        "NetAmountTaxable": net_base,
                    },
                })
            
            payload["TxnTaxDetail"] = {
                "TotalTax": total_tax,
                "TaxLine": tax_lines,
            }
        except Exception as e:
            # If anything goes wrong, log and let QBO try to compute
            print(f"[WARN] Error building composite tax detail: {e}. QBO may not display tax correctly.")
            pass
    # Note: Company A uses vat_inclusive_7_5 (single-rate with explicit TxnTaxDetail)
    #       Company B uses tax_inclusive_composite (multi-rate with explicit TxnTaxDetail for each component)

    # Payment method (tender type) from memo - query QBO by name
    if payment_method_cache is None:
        payment_method_cache = {}
    payment_method_id = infer_payment_method_id(memo, token_mgr, realm_id, payment_method_cache)
    if payment_method_id:
        payload["PaymentMethodRef"] = {"value": payment_method_id}
    elif memo:
        # Only warn if memo exists but payment method not found
        print(f"[WARN] Payment method '{memo}' not found in QBO, skipping PaymentMethodRef")

    # Location from CSV -> QBO Department (Location tracking)
    if location_name:
        department_id = get_department_id(location_name, token_mgr, realm_id, department_cache, config)
        if department_id:
            payload["DepartmentRef"] = {"value": department_id}
        else:
            print(f"[WARN] Department/Location '{location_name}' not found in QBO, skipping DepartmentRef")
    
    # Deposit account from config (for Company B, may be in CSV; for Company A, use config default)
    deposit_account_name = str(first_row.get("*DepositAccount", "")).strip()
    if deposit_account_name:
        # Try to resolve deposit account by name (Company B pattern)
        # For now, we'll let QBO use its default if not specified
        pass

    # No CustomerRef -> customer left blank (as desired)
    return payload, inventory_created_count, service_created_count, default_fallback_count


def _log_sales_receipt_line_items_for_6270(payload: dict) -> None:
    """
    Log a concise dump of SalesReceipt line items (ItemRef value, name, Qty) for QBO 6270 debugging.
    Does not log secrets or tokens.
    """
    lines = payload.get("Line") or []
    if not lines:
        return
    parts = []
    for i, line in enumerate(lines):
        if line.get("DetailType") != "SalesItemLineDetail":
            continue
        detail = line.get("SalesItemLineDetail") or {}
        item_ref = detail.get("ItemRef") or {}
        item_id = item_ref.get("value", "")
        item_name = item_ref.get("name") or line.get("Description") or ""
        qty = detail.get("Qty", "")
        parts.append(f"  {i + 1}) ItemRef={item_id} name={item_name!r} Qty={qty}")
    if parts:
        print("[INFO] QBO 6270: SalesReceipt line items (InvStartDate may be after TxnDate):")
        print("\n".join(parts))


# Chunk size for QBO "Id in (...)" queries to avoid URL/query limits
_ITEM_IDS_QUERY_CHUNK_SIZE = 20


def _query_items_by_ids(
    token_mgr: TokenManager,
    realm_id: str,
    id_list: List[str],
) -> List[Dict[str, Any]]:
    """
    Query QBO for Item by Id list. Chunked to avoid query limits. Read-only.
    Returns list of dicts with Id, Name, Type, TrackQtyOnHand, InvStartDate, Active.
    """
    if not id_list:
        return []
    result: List[Dict[str, Any]] = []
    for i in range(0, len(id_list), _ITEM_IDS_QUERY_CHUNK_SIZE):
        chunk = id_list[i : i + _ITEM_IDS_QUERY_CHUNK_SIZE]
        safe_ids = [str(uid).replace("'", "''") for uid in chunk]
        id_list_str = "','".join(safe_ids)
        query = f"select Id, Name, Type, TrackQtyOnHand, InvStartDate, Active from Item where Id in ('{id_list_str}')"
        url = f"{BASE_URL}/v3/company/{realm_id}/query?query={quote(query)}&minorversion=70"
        resp = _make_qbo_request("GET", url, token_mgr)
        if resp.status_code != 200:
            continue
        data = resp.json()
        items = data.get("QueryResponse", {}).get("Item", [])
        if not isinstance(items, list):
            items = [items] if items else []
        result.extend(items)
    return result


def _parse_yyyy_mm_dd(s: Optional[str]) -> Optional[datetime]:
    """Parse YYYY-MM-DD from string (uses first 10 chars if longer). Returns None if invalid."""
    if not s:
        return None
    s = str(s).strip()[:10]
    if len(s) != 10:
        return None
    try:
        return datetime.strptime(s, "%Y-%m-%d")
    except ValueError:
        return None


def _diagnose_6270_and_report(
    payload: dict,
    token_mgr: TokenManager,
    realm_id: str,
    config: Any,
) -> None:
    """
    On QBO 6270: parse payload Line[] (ItemRef.value + Qty), query QBO for those
    items by Id (chunked), log receipt-level summary (DocNumber, TxnDate, then
    each item: Name, Id, InvStartDate, Qty), and append rows to
    reports/inventory_start_date_blockers_{company_key}_{TxnDate}.csv.
    Read-only (no QBO writes). Does not fail the run on diagnostic errors.
    """
    doc_number = payload.get("DocNumber", "")
    txn_date = payload.get("TxnDate", "")
    lines = payload.get("Line") or []
    line_items: List[Tuple[str, Any]] = []  # (item_id, qty)
    for line in lines:
        if line.get("DetailType") != "SalesItemLineDetail":
            continue
        detail = line.get("SalesItemLineDetail") or {}
        item_ref = detail.get("ItemRef") or {}
        item_id = item_ref.get("value") or ""
        if not item_id:
            continue
        qty = detail.get("Qty", "")
        line_items.append((str(item_id), qty))

    if not line_items:
        return

    # Collect (item_name, category/description) from payload for observability
    payload_line_names: List[Tuple[str, str]] = []
    for line in lines:
        if line.get("DetailType") != "SalesItemLineDetail":
            continue
        detail = line.get("SalesItemLineDetail") or {}
        item_ref = detail.get("ItemRef") or {}
        item_name = item_ref.get("name", "") or ""
        description = line.get("Description", "") or ""
        payload_line_names.append((item_name, description))
    if payload_line_names:
        print(f"[INFO] QBO 6270: {doc_number} (TxnDate={txn_date}) payload line items (item_name, category): {payload_line_names}")

    # Query QBO for those item IDs (chunked)
    id_list = [item_id for item_id, _ in line_items]
    qbo_items = _query_items_by_ids(token_mgr, realm_id, id_list)
    id_to_item: Dict[str, Dict[str, Any]] = {str(it.get("Id", "")): it for it in qbo_items if it.get("Id")}

    # Parse TxnDate once for comparison (handles YYYY-MM-DD or longer strings like ISO with TZ).
    txn_date_obj = _parse_yyyy_mm_dd(txn_date)

    # Filter to only items where InvStartDate > TxnDate (actual blockers). Compare as dates.
    # Also collect items with missing InvStartDate (shortlist when no blockers found).
    blocking_items: List[Tuple[str, Any]] = []
    items_missing_inv_start: List[Tuple[str, Any]] = []
    for item_id, qty in line_items:
        it = id_to_item.get(item_id, {})
        inv_start = it.get("InvStartDate", "")
        inv_date_str = inv_start[:10] if inv_start and len(str(inv_start)) >= 10 else (str(inv_start) if inv_start else "")
        inv_date_obj = _parse_yyyy_mm_dd(inv_date_str or inv_start)
        if inv_date_obj is None or not inv_date_str.strip():
            items_missing_inv_start.append((item_id, qty))
            continue
        if txn_date_obj is not None and inv_date_obj > txn_date_obj:
            blocking_items.append((item_id, qty))

    # Log only blocking items; or missing shortlist; or single line when neither
    if blocking_items:
        print(f"[INFO] QBO 6270: {doc_number} (TxnDate={txn_date}) blocked by inventory items (InvStartDate > TxnDate):")
        for item_id, qty in blocking_items:
            it = id_to_item.get(item_id, {})
            name = it.get("Name", "")
            inv_start = it.get("InvStartDate", "")
            inv_date_str = inv_start[:10] if inv_start and len(str(inv_start)) >= 10 else str(inv_start or "")
            print(f"  - {name} (Id={item_id}) InvStartDate={inv_date_str} Qty={qty}")
    elif items_missing_inv_start:
        print(f"[INFO] QBO 6270: {doc_number} (TxnDate={txn_date}) — no items with InvStartDate > TxnDate; shortlist of items with missing InvStartDate (possible blockers):")
        for item_id, qty in items_missing_inv_start:
            it = id_to_item.get(item_id, {})
            name = it.get("Name", "") or "(unknown)"
            print(f"  - {name} (Id={item_id}) InvStartDate=(missing) Qty={qty}")
    else:
        print(f"[INFO] QBO 6270: {doc_number} (TxnDate={txn_date}) — no items with InvStartDate > TxnDate (list may be incomplete if QBO did not return all item details)")

    # Append to blockers CSV: blocking items, or (when none) shortlist of items with missing InvStartDate
    if not config:
        return
    company_key = getattr(config, "company_key", "")
    if not company_key or not txn_date:
        return
    rows_to_write = blocking_items if blocking_items else items_missing_inv_start
    if not rows_to_write:
        return
    out_dir = os.path.join(get_repo_root(), "reports")
    Path(out_dir).mkdir(parents=True, exist_ok=True)
    safe_key = re.sub(r"[^\w-]", "_", company_key)
    filename = f"inventory_start_date_blockers_{safe_key}_{txn_date}.csv"
    filepath = os.path.join(out_dir, filename)
    fieldnames = ["DocNumber", "TxnDate", "ItemId", "ItemName", "InvStartDate", "TrackQtyOnHand", "Active", "QuantityOnReceipt"]
    file_exists = os.path.exists(filepath)
    use_missing_shortlist = not blocking_items and items_missing_inv_start
    with open(filepath, "a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        if not file_exists:
            w.writeheader()
        for item_id, qty in rows_to_write:
            it = id_to_item.get(item_id, {})
            inv_start = it.get("InvStartDate", "")
            inv_date_str = inv_start[:10] if inv_start and len(str(inv_start)) >= 10 else str(inv_start or "")
            if use_missing_shortlist:
                inv_date_str = "(missing)"
            w.writerow({
                "DocNumber": doc_number,
                "TxnDate": txn_date,
                "ItemId": item_id,
                "ItemName": it.get("Name", ""),
                "InvStartDate": inv_date_str,
                "TrackQtyOnHand": it.get("TrackQtyOnHand", ""),
                "Active": it.get("Active", ""),
                "QuantityOnReceipt": qty,
            })
    if blocking_items:
        print(f"[INFO] QBO 6270: Appended blockers to {filepath}")
    else:
        print(f"[INFO] QBO 6270: Appended missing-InvStartDate shortlist to {filepath}")


def send_sales_receipt(payload: dict, token_mgr: TokenManager, realm_id: str, config=None):
    """
    Send a Sales Receipt to QuickBooks API.
    
    Args:
        payload: SalesReceipt payload
        token_mgr: TokenManager instance
        realm_id: QBO Realm ID
        config: CompanyConfig instance (optional, required for inventory error handling)
    
    Returns:
        Tuple of (success: bool, inventory_warning: bool, inventory_rejection: bool)
        - success: True if SalesReceipt was created successfully
        - inventory_warning: True if inventory warning detected (but receipt accepted)
        - inventory_rejection: True if inventory rejection detected
    
    Raises RuntimeError if the API returns a non-2xx status code (unless inventory rejection handled).
    """
    url = f"{BASE_URL}/v3/company/{realm_id}/salesreceipt?minorversion=70"

    response = _make_qbo_request(
        "POST",
        url,
        token_mgr,
        json=payload,
    )

    print("Status:", response.status_code)
    
    # Parse response body for logging/error messages
    try:
        body = response.json()
        print(json.dumps(body, indent=2))
    except Exception:
        body = None
        print(response.text)
    
    # Check for inventory-related errors/warnings
    inventory_warning = False
    inventory_rejection = False
    
    # Validate response status first
    is_success = (200 <= response.status_code < 300)
    
    if body:
        # Check for inventory-related messages
        response_text = json.dumps(body).lower()
        inventory_keywords = ["insufficient quantity", "quantity on hand", "inventory", "not enough"]
        
        if is_success:
            # Success response - check for warnings
            if any(phrase in response_text for phrase in inventory_keywords):
                inventory_warning = True
        else:
            # Error response - check if it's inventory-related
            fault = body.get("fault")
            if fault:
                errors = fault.get("error", [])
                if errors:
                    error_text = " ".join([
                        str(err.get("message", "")) + " " + str(err.get("detail", ""))
                        for err in errors
                    ]).lower()
                    if any(phrase in error_text for phrase in inventory_keywords):
                        inventory_rejection = True
    
    # Handle inventory rejection errors
    if not is_success and inventory_rejection:
        if config and config.allow_negative_inventory:
            # QBO rejected due to inventory, but we allow negative inventory
            error_msg = (
                "QBO rejected SalesReceipt due to negative inventory. "
                "Enable negative inventory in QBO settings (Settings → Company Settings → Sales → Allow negative inventory) "
                "or disable inventory items."
            )
            raise RuntimeError(error_msg)
        # If allow_negative_inventory is False, treat as fatal (existing behavior)
    
    # Validate response status - raise error if not successful
    if not is_success:
        error_msg = f"Failed to create Sales Receipt: HTTP {response.status_code}"
        
        # Extract error details from response if available (QBO may use "Fault"/"Error" or "fault"/"error")
        if body:
            fault = body.get("Fault") or body.get("fault")
            if fault:
                errors = fault.get("Error") or fault.get("error") or []
                if errors:
                    error_details = []
                    is_6270 = False
                    for err in errors:
                        if str(err.get("code", "")) == "6270":
                            is_6270 = True
                        detail = err.get("message", err.get("detail", ""))
                        if detail:
                            error_details.append(detail)
                    if error_details:
                        error_msg += f"\nError details: {'; '.join(error_details)}"
                    # On QBO 6270 (InvStartDate > TxnDate): full diagnostic + blockers CSV (non-blocking)
                    if is_6270 and payload:
                        try:
                            _diagnose_6270_and_report(payload, token_mgr, realm_id, config)
                        except Exception as diag_err:
                            print(f"[WARN] 6270 diagnostic failed (non-blocking): {diag_err}")
        
        # Include response text if JSON parsing failed
        if not body:
            error_msg += f"\nResponse: {response.text[:500]}"  # Limit length
        
        raise RuntimeError(error_msg)
    
    # Success - log inventory warning if present
    if inventory_warning and config and config.allow_negative_inventory:
        print(f"[WARN] Inventory warning detected but SalesReceipt accepted (negative inventory allowed)")
    
    return (True, inventory_warning, inventory_rejection)
    
    # Success - verify we got a SalesReceipt back
    if body:
        sales_receipt = body.get("SalesReceipt")
        if sales_receipt:
            receipt_id = sales_receipt.get("Id")
            doc_number = sales_receipt.get("DocNumber")
            if receipt_id:
                print(f"[OK] Sales Receipt created: ID={receipt_id}, DocNumber={doc_number}")
            else:
                print("[WARN] Sales Receipt response missing Id")
        else:
            print("[WARN] Sales Receipt response missing SalesReceipt object")


def main():
    # Parse command line arguments
    parser = argparse.ArgumentParser(
        description="Upload Sales Receipts to QuickBooks for a specific company."
    )
    parser.add_argument(
        "--company",
        required=True,
        choices=get_available_companies(),
        help="Company identifier (REQUIRED). Available: %(choices)s",
    )
    parser.add_argument(
        "--target-date",
        help="Target date in YYYY-MM-DD format (used when trading_day_enabled is True)",
    )
    args = parser.parse_args()
    
    # Load company configuration
    try:
        config = load_company_config(args.company)
    except Exception as e:
        print(f"Error: Failed to load company config for '{args.company}': {e}")
        sys.exit(1)
    
    # Safety check: verify realm_id matches tokens
    try:
        verify_realm_match(config.company_key, config.realm_id)
    except RuntimeError as e:
        print(f"Error: Realm ID safety check failed: {e}")
        sys.exit(1)
    
    # Log company info for safety
    print("=" * 60)
    print(f"COMPANY: {config.display_name} ({config.company_key})")
    print(f"REALM ID: {config.realm_id}")
    print(f"DEPOSIT ACCOUNT: {config.deposit_account}")
    print(f"TAX MODE: {config.tax_mode}")
    print("=" * 60)
    
    # Resolved target_date: CLI arg, or (when trading-day mode) default to yesterday for pre-flight/single-day behavior
    resolved_target_date: Optional[str] = args.target_date
    if not resolved_target_date and getattr(config, "trading_day_enabled", False):
        resolved_target_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

    # Initialize token manager (will refresh automatically on 401)
    token_mgr = TokenManager(config.company_key, config.realm_id)

    repo_root = get_repo_root()

    csv_path = find_latest_single_csv(repo_root, config)
    print(f"Using CSV: {csv_path}")

    df = pd.read_csv(csv_path)
    print(f"Loaded {len(df)} rows")

    grouped = df.groupby(GROUP_COL)
    print(f"Found {len(grouped)} distinct SalesReceiptNo groups")

    # Layer A: Load local ledger of uploaded DocNumbers (for reference/stats only)
    ledger_docnumbers = load_uploaded_docnumbers(repo_root, config)
    print(f"Loaded {len(ledger_docnumbers)} DocNumbers from local ledger")

    # Collect all DocNumbers to check
    all_docnumbers = list(grouped.groups.keys())
    
    # Layer B: Check QBO for existing DocNumbers (QBO is the source of truth)
    # Also check TxnDate if target_date is provided (for trading-day mode)
    print("Checking QBO for existing DocNumbers...")
    qbo_existing, date_mismatches = check_qbo_existing_docnumbers(
        all_docnumbers, token_mgr, config.realm_id, target_date=args.target_date
    )
    print(f"Found {len(qbo_existing)} existing DocNumbers in QBO with matching TxnDate")
    
    # Warn about date mismatches (receipts exist but with wrong TxnDate)
    if date_mismatches:
        print(f"\n[WARN] Found {len(date_mismatches)} receipt(s) in QBO with different TxnDate:")
        for docnum, wrong_date in sorted(date_mismatches.items()):
            print(f"  {docnum}: exists in QBO with TxnDate={wrong_date} (expected {args.target_date})")
        print(f"  These will be attempted for upload (will fail with duplicate DocNumber error)")
    
    # Detect stale ledger entries (in ledger but NOT in QBO with matching TxnDate)
    stale_ledger_entries = ledger_docnumbers - qbo_existing - set(date_mismatches.keys())
    stale_in_current_batch = stale_ledger_entries & set(all_docnumbers)
    
    if stale_in_current_batch:
        print(f"\n[WARN] Stale ledger entries detected: {len(stale_in_current_batch)} DocNumber(s) in ledger but not in QBO")
        for docnum in sorted(stale_in_current_batch):
            print(f"  Stale ledger entry detected: {docnum} is in {config.uploaded_docnumbers_file} but not in QBO; will attempt upload.")
    
    # Skip ONLY if DocNumber exists in QBO with matching TxnDate
    # Receipts with date mismatches will be attempted (and will fail, but that's expected)
    skip_docnumbers = qbo_existing
    if skip_docnumbers:
        print(f"Skipping {len(skip_docnumbers)} DocNumbers (confirmed existing in QBO with matching TxnDate)")

    item_cache: Dict[str, str] = {}
    department_cache: Dict[str, Optional[str]] = {}
    payment_method_cache: Dict[str, Optional[str]] = {}
    category_item_cache: Dict[str, str] = {}
    
    # Load category mapping and account cache if inventory enabled
    mapping_cache: Optional[Dict[str, Dict[str, str]]] = None
    account_cache: Dict[str, Optional[str]] = {}
    inventory_start_date_issues_count = 0
    inventory_start_date_report_path: Optional[str] = None
    if config.inventory_enabled:
        print(f"\n[INFO] Inventory mode ENABLED. QtyOnHand starts at {config.default_qty_on_hand}. QBO must allow negative inventory.")
        try:
            mapping_cache = load_category_account_mapping(config)
            print(f"[INFO] Loaded {len(mapping_cache)} category mappings from {config.product_mapping_file}")
        except Exception as e:
            print(f"[ERROR] Failed to load category mapping: {e}")
            raise
        # Pre-flight: find inventory items with InvStartDate > target_date (may cause QBO 6270)
        # Use resolved target_date so pre-flight runs for scheduled runs (e.g. default yesterday) even when CLI --target-date not set
        if resolved_target_date:
            print(f"\n[INFO] Running pre-flight inventory start-date check (target_date={resolved_target_date})")
            try:
                issues = find_inventory_items_with_future_start_date(token_mgr, config.realm_id, resolved_target_date)
                inventory_start_date_issues_count = len(issues)
                if issues:
                    print(f"[WARN] Found {inventory_start_date_issues_count} inventory items with InvStartDate AFTER target_date={resolved_target_date}. "
                          "These may fail with QBO error 6270.")
                    report_path = write_inventory_start_date_issues_report(
                        issues, config.company_key, resolved_target_date, out_dir=os.path.join(repo_root, "reports")
                    )
                    if report_path:
                        inventory_start_date_report_path = report_path
                        print(f"[INFO] Report written: {report_path}")
                else:
                    print(f"[INFO] Pre-flight: 0 issues found.")
            except Exception as e:
                print(f"[WARN] Pre-flight inventory start date check failed (non-blocking): {e}")
    
    # Pre-fetch tax code for Company B (tax_inclusive_composite mode) to validate it exists
    if config.tax_mode == "tax_inclusive_composite" and config.tax_code_name:
        tax_code_cache: Dict[str, Optional[str]] = {}
        tax_code_id = get_tax_code_id_by_name(
            config.tax_code_name,
            token_mgr,
            config.realm_id,
            tax_code_cache
        )
        if tax_code_id:
            print(f"[INFO] Found Tax Code '{config.tax_code_name}' with ID: {tax_code_id}")
            # Store in function-level cache for use in build_sales_receipt_payload
            if not hasattr(build_sales_receipt_payload, '_tax_code_cache'):
                build_sales_receipt_payload._tax_code_cache = {}
            build_sales_receipt_payload._tax_code_cache[config.tax_code_name] = tax_code_id
        else:
            print(f"[WARN] Tax Code '{config.tax_code_name}' not found in QBO.")
            print(f"       Receipts will be created without tax codes.")
            print(f"       You can add 'tax_code_id' to {config.company_key}.json to specify it directly.")
    
    stats = {
        "attempted": 0,
        "skipped": 0,
        "uploaded": 0,
        "failed": 0,
        "stale_ledger_entries_detected": len(stale_in_current_batch),
        "date_mismatches_detected": len(date_mismatches),
        "items_created_count": 0,
        "inventory_items_created_count": 0,
        "service_items_created_count": 0,
        "default_item_fallback_count": 0,
        "inventory_warnings_count": 0,
        "inventory_rejections_count": 0,
        "inventory_start_date_issues_count": inventory_start_date_issues_count,
        "inventory_start_date_report_path": inventory_start_date_report_path,
    }

    items_wrong_type: List[Dict[str, Any]] = []
    items_autofixed: List[Dict[str, Any]] = []
    items_patched_pricing_tax: List[Dict[str, Any]] = []

    for group_key, group_df in grouped:
        stats["attempted"] += 1
        
        # Skip ONLY if exists in QBO with matching TxnDate (QBO is source of truth)
        if group_key in skip_docnumbers:
            print(f"\nSkipping SalesReceiptNo: {group_key} (exists in QBO with matching TxnDate)")
            stats["skipped"] += 1
            # Add to ledger if confirmed in QBO (healing: sync ledger with QBO truth)
            if group_key not in ledger_docnumbers:
                save_uploaded_docnumber(repo_root, group_key, config)
                print(f"  Added {group_key} to ledger (confirmed in QBO)")
            continue
        
        # Check if this receipt has a date mismatch (exists but with wrong TxnDate)
        # We'll attempt upload anyway - it will fail with duplicate DocNumber error, but that's expected
        if group_key in date_mismatches:
            wrong_date = date_mismatches[group_key]
            print(f"\n[WARN] SalesReceiptNo: {group_key} exists in QBO with TxnDate={wrong_date} (expected {args.target_date})")
            print(f"       Attempting upload anyway (will fail with duplicate DocNumber error)")
        
        try:
            payload, inv_created, svc_created, default_fallback = build_sales_receipt_payload(
                group_df, token_mgr, config.realm_id, config, item_cache, department_cache, payment_method_cache,
                target_date=args.target_date,
                mapping_cache=mapping_cache,
                account_cache=account_cache,
                items_wrong_type=items_wrong_type,
                items_autofixed=items_autofixed,
                category_item_cache=category_item_cache,
                items_patched_pricing_tax=items_patched_pricing_tax,
            )
            stats["items_created_count"] += inv_created + svc_created
            stats["inventory_items_created_count"] += inv_created
            stats["service_items_created_count"] += svc_created
            stats["default_item_fallback_count"] += default_fallback
            print(f"\nSending SalesReceiptNo: {group_key}")
            success, inventory_warning, inventory_rejection = send_sales_receipt(payload, token_mgr, config.realm_id, config)
            # Track inventory stats (will be added to stats dict in main)
            if inventory_warning:
                stats.setdefault("inventory_warnings_count", 0)
                stats["inventory_warnings_count"] += 1
            if inventory_rejection:
                stats.setdefault("inventory_rejections_count", 0)
                stats["inventory_rejections_count"] += 1
            
            # Success - add to local ledger
            save_uploaded_docnumber(repo_root, group_key, config)
            stats["uploaded"] += 1
        except Exception as e:
            print(f"\n[ERROR] Failed to upload SalesReceiptNo {group_key}: {e}")
            stats["failed"] += 1
            # Don't add to ledger on failure

    if items_wrong_type:
        reports_dir = Path(repo_root) / "reports"
        reports_dir.mkdir(parents=True, exist_ok=True)
        target_date_str = args.target_date or "unknown"
        report_path = reports_dir / f"items_wrong_type_{config.company_key}_{target_date_str}.csv"
        seen_ids = set()
        rows = []
        for r in items_wrong_type:
            rid = r.get("Id", "")
            if rid and rid not in seen_ids:
                seen_ids.add(rid)
                rows.append(r)
        if rows:
            with open(report_path, "w", newline="", encoding="utf-8") as f:
                w = csv.DictWriter(f, fieldnames=["Name", "Id", "Type", "ExpectedType"])
                w.writeheader()
                w.writerows(rows)
            print(f"[INFO] Wrote {len(rows)} item(s) with wrong type to {report_path}")

    if items_autofixed:
        reports_dir = Path(repo_root) / "reports"
        reports_dir.mkdir(parents=True, exist_ok=True)
        target_date_str = args.target_date or "unknown"
        report_path = reports_dir / f"items_autofixed_{config.company_key}_{target_date_str}.csv"
        with open(report_path, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=["OriginalName", "OldItemId", "OldType", "OldActive", "NewName", "NewInventoryItemId", "TxnDate", "DocNumber"])
            w.writeheader()
            w.writerows(items_autofixed)
        print(f"[INFO] Wrote {len(items_autofixed)} item(s) auto-fixed (renamed/inactivated) to {report_path}")

    if items_patched_pricing_tax:
        reports_dir = Path(repo_root) / "reports"
        reports_dir.mkdir(parents=True, exist_ok=True)
        target_date_str = args.target_date or "unknown"
        report_path = reports_dir / f"items_patched_pricing_tax_{config.company_key}_{target_date_str}.csv"
        fieldnames = ["ItemId", "Name", "Category", "UnitPrice_old", "UnitPrice_new", "PurchaseCost_old", "PurchaseCost_new",
                      "SalesTaxIncluded_old/new", "PurchaseTaxIncluded_old/new", "Taxable_old/new", "TxnDate", "DocNumber"]
        with open(report_path, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
            w.writeheader()
            w.writerows(items_patched_pricing_tax)
        print(f"[INFO] Wrote {len(items_patched_pricing_tax)} item(s) patched (pricing/tax) to {report_path}")

    # Print summary
    print(f"\n=== Upload Summary ===")
    print(f"Attempted: {stats['attempted']}")
    print(f"Skipped (exists in QBO): {stats['skipped']}")
    print(f"Uploaded: {stats['uploaded']}")
    print(f"Failed: {stats['failed']}")
    if stats['stale_ledger_entries_detected'] > 0:
        print(f"Stale ledger entries detected: {stats['stale_ledger_entries_detected']} (healed by uploading)")
    if stats['date_mismatches_detected'] > 0:
        print(f"Date mismatches detected: {stats['date_mismatches_detected']} receipt(s) exist in QBO with wrong TxnDate")
        print(f"  These receipts need manual correction in QBO (change TxnDate to {args.target_date}) or delete and re-upload")
    print(f"\nLedger vs QBO sync:")
    print(f"  DocNumbers in ledger: {len(ledger_docnumbers)}")
    print(f"  DocNumbers confirmed in QBO (matching TxnDate): {len(qbo_existing)}")
    if date_mismatches:
        print(f"  Date mismatches (wrong TxnDate in QBO): {len(date_mismatches)}")
    if stale_in_current_batch:
        print(f"  Stale ledger entries (in ledger, not in QBO): {len(stale_in_current_batch)}")
    
    # Write stats to metadata for Slack notification
    metadata_path = os.path.join(repo_root, config.metadata_file)
    if os.path.exists(metadata_path):
        try:
            with open(metadata_path, "r") as f:
                metadata = json.load(f)
            metadata["upload_stats"] = stats
            with open(metadata_path, "w") as f:
                json.dump(metadata, f, indent=2)
        except Exception as e:
            print(f"[WARN] Failed to update metadata with upload stats: {e}")
    
    # Exit with error code if any uploads failed
    if stats['failed'] > 0:
        print(f"\n[ERROR] {stats['failed']} upload(s) failed. Exiting with error code.")
        sys.exit(1)
    
    # Exit with error code if no uploads succeeded (and there were attempts)
    if stats['attempted'] > 0 and stats['uploaded'] == 0 and stats['skipped'] == 0:
        print(f"\n[ERROR] All {stats['attempted']} upload attempt(s) failed. Exiting with error code.")
        sys.exit(1)


if __name__ == "__main__":
    main()