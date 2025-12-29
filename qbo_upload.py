from __future__ import annotations

import os
import glob
import json
import argparse
import sys
from typing import Optional, Dict, Callable, Any
from urllib.parse import quote
from datetime import datetime

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
    batch_size: int = 50
) -> set:
    """
    Check QBO for existing SalesReceipts by DocNumber.
    Returns set of DocNumbers that already exist in QBO.
    """
    existing = set()
    
    # Query in batches to avoid URL length limits
    for i in range(0, len(docnumbers), batch_size):
        batch = docnumbers[i:i + batch_size]
        # Build query: select Id, DocNumber from SalesReceipt where DocNumber in ('SR-...', 'SR-...', ...)
        docnumber_list = "', '".join(d.replace("'", "''") for d in batch)
        query = f"select Id, DocNumber from SalesReceipt where DocNumber in ('{docnumber_list}')"
        url = f"{BASE_URL}/v3/company/{realm_id}/query?query={quote(query)}&minorversion=70"
        
        resp = _make_qbo_request("GET", url, token_mgr)
        if resp.status_code == 200:
            data = resp.json()
            receipts = data.get("QueryResponse", {}).get("SalesReceipt", [])
            if not isinstance(receipts, list):
                receipts = [receipts] if receipts else []
            
            for receipt in receipts:
                doc_num = receipt.get("DocNumber")
                if doc_num:
                    existing.add(doc_num)
    
    return existing


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


def get_or_create_item_id(name: str, token_mgr: TokenManager, realm_id: str, config, cache: Dict[str, str]) -> str:
    """
    Resolve an Item name to an Item Id with simple caching.

    - If the name exists in cache, reuse its Id.
    - Otherwise, try a QBO query by Name.
    - Optionally auto-create a Service item if not found.
    - If all else fails, fall back to default_item_id from config.
    """
    default_item_id = config.get_qbo_config().get("default_item_id", "1")
    default_income_account_id = config.get_qbo_config().get("default_income_account_id", "1")
    auto_create_items = True  # Can be made configurable later
    
    if not name:
        return default_item_id

    if name in cache:
        return cache[name]

    safe_name = name.replace("'", "''")
    query = f"select Id from Item where Name = '{safe_name}'"
    url = f"{BASE_URL}/v3/company/{realm_id}/query?query={quote(query)}&minorversion=70"

    resp = _make_qbo_request("GET", url, token_mgr)
    item_id: Optional[str] = None
    if resp.status_code == 200:
        data = resp.json()
        items = data.get("QueryResponse", {}).get("Item", [])
        if items:
            item_id = items[0].get("Id")

    if not item_id and auto_create_items:
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
        else:
            print(f"[WARN] Failed to create Item '{name}': {create_resp.status_code}")
            try:
                print(create_resp.text)
            except Exception:
                pass

    if not item_id:
        item_id = default_item_id

    cache[name] = item_id
    return item_id


def build_sales_receipt_payload(
    group: pd.DataFrame,
    token_mgr: TokenManager,
    realm_id: str,
    config,
    item_cache: Dict[str, str],
    department_cache: Dict[str, Optional[str]],
    payment_method_cache: Dict[str, Optional[str]] = None,
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

    # Parse date from CSV using company's date_format and convert to ISO format (YYYY-MM-DD) for QBO
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

    for _, row in group.iterrows():
        # Product/Service
        item_name = str(row.get(ITEM_NAME_COL, "")).strip()
        item_ref_id = get_or_create_item_id(item_name, token_mgr, realm_id, config, item_cache)

        # Quantity (default to 1 if missing/NaN or <=0)
        try:
            qty_val = float(row.get(QTY_COL, 1) or 1)
            if qty_val <= 0:
                qty_val = 1.0
        except (TypeError, ValueError):
            qty_val = 1.0

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
        
        sales_item_detail = {
            "ItemRef": {"value": item_ref_id},
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
    return payload


def send_sales_receipt(payload: dict, token_mgr: TokenManager, realm_id: str):
    """
    Send a Sales Receipt to QuickBooks API.
    
    Raises RuntimeError if the API returns a non-2xx status code.
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
    
    # Validate response status - raise error if not successful
    if not (200 <= response.status_code < 300):
        error_msg = f"Failed to create Sales Receipt: HTTP {response.status_code}"
        
        # Extract error details from response if available
        if body:
            fault = body.get("fault")
            if fault:
                errors = fault.get("error", [])
                if errors:
                    error_details = []
                    for err in errors:
                        detail = err.get("message", err.get("detail", ""))
                        if detail:
                            error_details.append(detail)
                    if error_details:
                        error_msg += f"\nError details: {'; '.join(error_details)}"
        
        # Include response text if JSON parsing failed
        if not body:
            error_msg += f"\nResponse: {response.text[:500]}"  # Limit length
        
        raise RuntimeError(error_msg)
    
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
    
    # Initialize token manager (will refresh automatically on 401)
    token_mgr = TokenManager(config.company_key, config.realm_id)

    repo_root = get_repo_root()

    csv_path = find_latest_single_csv(repo_root, config)
    print(f"Using CSV: {csv_path}")

    df = pd.read_csv(csv_path)
    print(f"Loaded {len(df)} rows")

    grouped = df.groupby(GROUP_COL)
    print(f"Found {len(grouped)} distinct SalesReceiptNo groups")

    # Layer A: Load local ledger of uploaded DocNumbers
    uploaded_docnumbers = load_uploaded_docnumbers(repo_root, config)
    print(f"Loaded {len(uploaded_docnumbers)} DocNumbers from local ledger")

    # Collect all DocNumbers to check
    all_docnumbers = list(grouped.groups.keys())
    
    # Layer B: Check QBO for existing DocNumbers (optional safety check)
    print("Checking QBO for existing DocNumbers...")
    qbo_existing = check_qbo_existing_docnumbers(all_docnumbers, token_mgr, config.realm_id)
    print(f"Found {len(qbo_existing)} existing DocNumbers in QBO")
    
    # Combine both sources
    skip_docnumbers = uploaded_docnumbers | qbo_existing
    if skip_docnumbers:
        print(f"Skipping {len(skip_docnumbers)} DocNumbers (already uploaded or exist in QBO)")

    item_cache: Dict[str, str] = {}
    department_cache: Dict[str, Optional[str]] = {}
    payment_method_cache: Dict[str, Optional[str]] = {}
    
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
    }

    for group_key, group_df in grouped:
        stats["attempted"] += 1
        
        # Skip if already uploaded or exists in QBO
        if group_key in skip_docnumbers:
            print(f"\nSkipping SalesReceiptNo: {group_key} (already uploaded or exists)")
            stats["skipped"] += 1
            continue
        
        try:
            payload = build_sales_receipt_payload(group_df, token_mgr, config.realm_id, config, item_cache, department_cache, payment_method_cache)
            print(f"\nSending SalesReceiptNo: {group_key}")
            send_sales_receipt(payload, token_mgr, config.realm_id)
            
            # Success - add to local ledger
            save_uploaded_docnumber(repo_root, group_key, config)
            stats["uploaded"] += 1
        except Exception as e:
            print(f"\n[ERROR] Failed to upload SalesReceiptNo {group_key}: {e}")
            stats["failed"] += 1
            # Don't add to ledger on failure
    
    # Print summary
    print(f"\n=== Upload Summary ===")
    print(f"Attempted: {stats['attempted']}")
    print(f"Skipped (duplicates): {stats['skipped']}")
    print(f"Uploaded: {stats['uploaded']}")
    print(f"Failed: {stats['failed']}")
    
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