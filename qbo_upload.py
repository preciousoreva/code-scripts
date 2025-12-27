from __future__ import annotations

import os
import glob
import json
from typing import Optional, Dict, Callable, Any
from urllib.parse import quote
from datetime import datetime

import pandas as pd
import requests
from qbo_auth import get_access_token, refresh_access_token, load_tokens, save_tokens
from load_env import load_env_file

# Load .env if present so QBO_* vars are available
load_env_file()

# === CONFIG: QBO company info (auth is handled in qbo_auth.py) ===
try:
    REALM_ID = os.environ["QBO_REALM_ID"]  # QBO Company ID as string
except KeyError:
    raise RuntimeError(
        "QBO_REALM_ID environment variable is not set. "
        "Set it in your environment or .env file."
    )
BASE_URL = "https://quickbooks.api.intuit.com"

# Tax code id for your 7.5% VAT ("7.5% S")
TAX_CODE_ID = "2"

# Map our tender/memo text to QBO PaymentMethod IDs (from your latest query)
PAYMENT_METHOD_BY_NAME = {
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


def load_uploaded_docnumbers(repo_root: str) -> set:
    """Load set of DocNumbers that have been successfully uploaded."""
    ledger_path = os.path.join(repo_root, "uploaded_docnumbers.json")
    if not os.path.exists(ledger_path):
        return set()
    
    try:
        with open(ledger_path, "r") as f:
            data = json.load(f)
            return set(data.get("docnumbers", []))
    except Exception as e:
        print(f"[WARN] Failed to load uploaded_docnumbers.json: {e}")
        return set()


def save_uploaded_docnumber(repo_root: str, docnumber: str) -> None:
    """Add a DocNumber to the uploaded ledger."""
    ledger_path = os.path.join(repo_root, "uploaded_docnumbers.json")
    
    # Load existing
    docnumbers = load_uploaded_docnumbers(repo_root)
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
        print(f"[WARN] Failed to save uploaded_docnumbers.json: {e}")


def check_qbo_existing_docnumbers(
    docnumbers: list[str],
    token_mgr: TokenManager,
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
        url = f"{BASE_URL}/v3/company/{REALM_ID}/query?query={quote(query)}&minorversion=70"
        
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


def find_latest_single_csv(repo_root: str) -> str:
    """
    Find the most recently modified single_sales_receipts_*.csv file
    in repo root.
    """
    pattern = os.path.join(repo_root, "single_sales_receipts_*.csv")
    files = glob.glob(pattern)
    if not files:
        raise FileNotFoundError(
            f"No single_sales_receipts_*.csv files found in {repo_root}"
        )
    return max(files, key=os.path.getmtime)


def infer_payment_method_id(memo: str) -> Optional[str]:
    """
    Try to map the memo text (tender type) to a QBO PaymentMethod Id.
    We use exact matches: 'Cash', 'Card', 'Card/Transfer', etc.
    """
    if not memo:
        return None
    memo_clean = memo.strip()
    return PAYMENT_METHOD_BY_NAME.get(memo_clean)


def _qbo_headers(access_token: str) -> dict:
    return {
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Content-Type": "application/json",
    }


def _refresh_token_and_get_new_access_token() -> str:
    """Refresh the access token and return the new one."""
    tokens = load_tokens()
    if not tokens:
        raise RuntimeError("Cannot refresh token: qbo_tokens.json not found or empty")
    tokens = refresh_access_token(tokens)
    return tokens["access_token"]


class TokenManager:
    """
    Manages QBO access token state during a run.
    Automatically refreshes token on 401 errors.
    """
    def __init__(self):
        self.access_token = get_access_token()
    
    def get(self) -> str:
        """Get the current access token."""
        return self.access_token
    
    def refresh(self) -> str:
        """Refresh the access token and update internal state."""
        self.access_token = _refresh_token_and_get_new_access_token()
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


def get_department_id(name: str, token_mgr: TokenManager, cache: Dict[str, Optional[str]]) -> Optional[str]:
    """
    Resolve a Department (shown as "Location" in the QBO UI) name to a Department Id with simple caching.

    - If the name exists in cache, reuse its Id.
    - Otherwise, try a QBO query by Name.
    - Returns None if department not found or name is empty.
    """
    if not name or not name.strip():
        return None
    
    name_clean = name.strip()
    if name_clean in cache:
        return cache[name_clean]
    
    safe_name = name_clean.replace("'", "''")
    query = f"select Id from Department where Name = '{safe_name}'"
    url = f"{BASE_URL}/v3/company/{REALM_ID}/query?query={quote(query)}&minorversion=70"
    
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


def get_or_create_item_id(name: str, token_mgr: TokenManager, cache: Dict[str, str]) -> str:
    """
    Resolve an Item name to an Item Id with simple caching.

    - If the name exists in cache, reuse its Id.
    - Otherwise, try a QBO query by Name.
    - Optionally auto-create a Service item if not found.
    - If all else fails, fall back to DEFAULT_ITEM_ID.
    """
    if not name:
        return DEFAULT_ITEM_ID

    if name in cache:
        return cache[name]

    safe_name = name.replace("'", "''")
    query = f"select Id from Item where Name = '{safe_name}'"
    url = f"{BASE_URL}/v3/company/{REALM_ID}/query?query={quote(query)}&minorversion=70"

    resp = _make_qbo_request("GET", url, token_mgr)
    item_id: Optional[str] = None
    if resp.status_code == 200:
        data = resp.json()
        items = data.get("QueryResponse", {}).get("Item", [])
        if items:
            item_id = items[0].get("Id")

    if not item_id and AUTO_CREATE_ITEMS:
        create_url = f"{BASE_URL}/v3/company/{REALM_ID}/item?minorversion=70"
        payload = {
            "Name": name,
            "Type": "Service",
            "IncomeAccountRef": {"value": DEFAULT_INCOME_ACCOUNT_ID},
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
        item_id = DEFAULT_ITEM_ID

    cache[name] = item_id
    return item_id


def build_sales_receipt_payload(
    group: pd.DataFrame,
    token_mgr: TokenManager,
    item_cache: Dict[str, str],
    department_cache: Dict[str, Optional[str]],
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

    txn_date = str(first_row[DATE_COL])
    memo = str(first_row[MEMO_COL])
    doc_number = str(first_row[DOCNUM_COL])
    location_name = str(first_row.get(LOCATION_COL, "")).strip()

    lines = []
    gross_total = 0.0
    net_total = 0.0

    for _, row in group.iterrows():
        # Product/Service
        item_name = str(row.get(ITEM_NAME_COL, "")).strip()
        item_ref_id = get_or_create_item_id(item_name, token_mgr, item_cache)

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

        # Service date: fall back to TxnDate if missing
        service_date = str(row.get(SERVICE_DATE_COL, txn_date))

        # Description: prefer ItemDescription, fall back to memo
        description = str(row.get(ITEM_DESC_COL, memo))

        sales_item_detail = {
            "ItemRef": {"value": item_ref_id},
            "Qty": qty_val,
            # UnitPrice will be set after we compute the net amount below
            "UnitPrice": None,
            "ServiceDate": service_date,
            "TaxCodeRef": {"value": TAX_CODE_ID},  # 7.5% S
        }

        # To match QBO's "good" behaviour, we store line Amount as NET (exclusive of VAT)
        # and provide TaxInclusiveAmt as the original gross from EPOS.
        raw_amount_net = amount_gross - tax_amount
        if raw_amount_net < 0:
            raw_amount_net = 0.0

        # Net unit price so that Amount == UnitPrice * Qty holds for QBO validation.
        unit_price_net = round(raw_amount_net / qty_val, 2) if qty_val else raw_amount_net
        amount_net = round(unit_price_net * qty_val, 2)
        sales_item_detail["UnitPrice"] = unit_price_net

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
        # Tell QBO these line amounts are tax-inclusive (like the manual CSV import)
        "GlobalTaxCalculation": "TaxInclusive",
        "Line": lines,
    }

    # Explicit tax summary so QBO keeps the overall total equal to our gross_total
    # and only backs out the VAT portion for display, mirroring "good" receipts.
    try:
        # If we have a sensible net_total (from the per-line calculations), use that;
        # otherwise fall back to deriving from the configured VAT rate.
        tax_rate = 0.075  # 7.5%
        net_base = round(net_total or (gross_total / (1 + tax_rate)), 2)
        total_tax = round(gross_total - net_base, 2)

        payload["TxnTaxDetail"] = {
            "TotalTax": total_tax,
            "TaxLine": [
                {
                    "Amount": total_tax,
                    "DetailType": "TaxLineDetail",
                    "TaxLineDetail": {
                        "TaxRateRef": {"value": TAX_CODE_ID},
                        "PercentBased": True,
                        "TaxPercent": 7.5,
                        "NetAmountTaxable": net_base,
                    },
                }
            ],
        }
    except Exception:
        # If anything goes wrong with our explicit tax calc, fall back to letting QBO compute.
        pass

    # Payment method (tender type) from memo
    payment_method_id = infer_payment_method_id(memo)
    if payment_method_id:
        payload["PaymentMethodRef"] = {"value": payment_method_id}

    # Location from CSV -> QBO Department (Location tracking)
    if location_name:
        department_id = get_department_id(location_name, token_mgr, department_cache)
        if department_id:
            payload["DepartmentRef"] = {"value": department_id}
        else:
            print(f"[WARN] Department/Location '{location_name}' not found in QBO, skipping DepartmentRef")

    # No CustomerRef -> customer left blank (as desired)
    return payload


def send_sales_receipt(payload: dict, token_mgr: TokenManager):
    """
    Send a Sales Receipt to QuickBooks API.
    
    Raises RuntimeError if the API returns a non-2xx status code.
    """
    url = f"{BASE_URL}/v3/company/{REALM_ID}/salesreceipt?minorversion=70"

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
    # Initialize token manager once (will refresh automatically on 401)
    token_mgr = TokenManager()

    repo_root = get_repo_root()

    csv_path = find_latest_single_csv(repo_root)
    print(f"Using CSV: {csv_path}")

    df = pd.read_csv(csv_path)
    print(f"Loaded {len(df)} rows")

    grouped = df.groupby(GROUP_COL)
    print(f"Found {len(grouped)} distinct SalesReceiptNo groups")

    # Layer A: Load local ledger of uploaded DocNumbers
    uploaded_docnumbers = load_uploaded_docnumbers(repo_root)
    print(f"Loaded {len(uploaded_docnumbers)} DocNumbers from local ledger")

    # Collect all DocNumbers to check
    all_docnumbers = list(grouped.groups.keys())
    
    # Layer B: Check QBO for existing DocNumbers (optional safety check)
    print("Checking QBO for existing DocNumbers...")
    qbo_existing = check_qbo_existing_docnumbers(all_docnumbers, token_mgr)
    print(f"Found {len(qbo_existing)} existing DocNumbers in QBO")
    
    # Combine both sources
    skip_docnumbers = uploaded_docnumbers | qbo_existing
    if skip_docnumbers:
        print(f"Skipping {len(skip_docnumbers)} DocNumbers (already uploaded or exist in QBO)")

    item_cache: Dict[str, str] = {}
    department_cache: Dict[str, Optional[str]] = {}
    
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
            payload = build_sales_receipt_payload(group_df, token_mgr, item_cache, department_cache)
            print(f"\nSending SalesReceiptNo: {group_key}")
            send_sales_receipt(payload, token_mgr)
            
            # Success - add to local ledger
            save_uploaded_docnumber(repo_root, group_key)
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
    metadata_path = os.path.join(repo_root, "last_epos_transform.json")
    if os.path.exists(metadata_path):
        try:
            with open(metadata_path, "r") as f:
                metadata = json.load(f)
            metadata["upload_stats"] = stats
            with open(metadata_path, "w") as f:
                json.dump(metadata, f, indent=2)
        except Exception as e:
            print(f"[WARN] Failed to update metadata with upload stats: {e}")


if __name__ == "__main__":
    main()