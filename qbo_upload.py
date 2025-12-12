import os
import glob
import json
from typing import Optional, Dict
from urllib.parse import quote

import pandas as pd
import requests
from qbo_auth import get_access_token

# === CONFIG: QBO company info (auth is handled in qbo_auth.py) ===
REALM_ID = "9341455406194328"  # QBO Company ID as string
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

# Detail columns
ITEM_NAME_COL = "Item(Product/Service)"  # Product/Service name in QBO
ITEM_DESC_COL = "ItemDescription"        # Line description
QTY_COL = "ItemQuantity"                 # Quantity sold
RATE_COL = "ItemRate"                    # Unit price (can be NaN)
SERVICE_DATE_COL = "Service Date"        # Per-line service date

# Item mapping / creation behaviour
DEFAULT_ITEM_ID = "1"           # Fallback generic item
DEFAULT_INCOME_ACCOUNT_ID = "1" # For auto-created items
AUTO_CREATE_ITEMS = False       # Flip to True if you ever want auto item creation


def get_repo_root() -> str:
    """Return the directory this script lives in (the repo root for our purposes)."""
    return os.path.dirname(os.path.abspath(__file__))


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


def get_or_create_item_id(name: str, access_token: str, cache: Dict[str, str]) -> str:
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

    resp = requests.get(url, headers=_qbo_headers(access_token))
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
        create_resp = requests.post(
            create_url, headers=_qbo_headers(access_token), json=payload
        )
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
    access_token: str,
    item_cache: Dict[str, str],
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

    lines = []

    for _, row in group.iterrows():
        # Product/Service
        item_name = str(row.get(ITEM_NAME_COL, "")).strip()
        item_ref_id = get_or_create_item_id(item_name, access_token, item_cache)

        # Quantity (default to 1 if missing/NaN or <=0)
        try:
            qty_val = float(row.get(QTY_COL, 1) or 1)
            if qty_val <= 0:
                qty_val = 1.0
        except (TypeError, ValueError):
            qty_val = 1.0

        # Authoritative gross amount from CSV (*ItemAmount)
        try:
            amount_csv = float(row[AMOUNT_COL])
        except (TypeError, ValueError, KeyError):
            amount_csv = 0.0

        # Prefer ItemRate directly from CSV when valid and consistent
        rate_raw = row.get(RATE_COL, None)
        unit_price_gross: float

        try:
            if rate_raw is not None and rate_raw == rate_raw and float(rate_raw) > 0:
                rate_val = float(rate_raw)
                # If CSV is internally consistent, keep it exactly
                if qty_val and abs(rate_val * qty_val - amount_csv) < 0.01:
                    unit_price_gross = rate_val
                    amount_gross = amount_csv
                else:
                    # Derive a clean pair that satisfies QBO’s rule: Amount = UnitPrice * Qty
                    unit_price_gross = round(amount_csv / qty_val, 2) if qty_val else amount_csv
                    amount_gross = round(unit_price_gross * qty_val, 2)
            else:
                # No usable rate: derive from amount
                unit_price_gross = round(amount_csv / qty_val, 2) if qty_val else amount_csv
                amount_gross = round(unit_price_gross * qty_val, 2)
        except (TypeError, ValueError):
            # Fallback if something is really broken
            unit_price_gross = amount_csv
            qty_val = 1.0
            amount_gross = amount_csv

        # Service date: fall back to TxnDate if missing
        service_date = str(row.get(SERVICE_DATE_COL, txn_date))

        # Description: prefer ItemDescription, fall back to memo
        description = str(row.get(ITEM_DESC_COL, memo))

        sales_item_detail = {
            "ItemRef": {"value": item_ref_id},
            "Qty": qty_val,
            "UnitPrice": unit_price_gross,   # this matches CSV ItemRate when valid
            "ServiceDate": service_date,
            "TaxCodeRef": {"value": TAX_CODE_ID},  # 7.5% S
        }

        lines.append(
            {
                "DetailType": "SalesItemLineDetail",
                "Amount": amount_gross,  # gross per line; must equal UnitPrice * Qty
                "Description": description,
                "SalesItemLineDetail": sales_item_detail,
            }
        )

    payload: dict = {
        "TxnDate": txn_date,
        "PrivateNote": memo,
        "DocNumber": doc_number,
        # Tell QBO these line amounts are tax-inclusive (like the manual CSV import)
        "GlobalTaxCalculation": "TaxInclusive",
        "Line": lines,
    }

    # Payment method (tender type) from memo
    payment_method_id = infer_payment_method_id(memo)
    if payment_method_id:
        payload["PaymentMethodRef"] = {"value": payment_method_id}

    # No CustomerRef -> customer left blank (as desired)
    return payload


def send_sales_receipt(payload: dict, access_token: str):
    url = f"{BASE_URL}/v3/company/{REALM_ID}/salesreceipt?minorversion=70"

    response = requests.post(
        url,
        headers=_qbo_headers(access_token),
        data=json.dumps(payload),
    )

    print("Status:", response.status_code)
    try:
        body = response.json()
        print(json.dumps(body, indent=2))
    except Exception:
        print(response.text)


def main():
    # Get a valid access token from qbo_auth (will refresh if needed)
    access_token = get_access_token()

    repo_root = get_repo_root()

    csv_path = find_latest_single_csv(repo_root)
    print(f"Using CSV: {csv_path}")

    df = pd.read_csv(csv_path)
    print(f"Loaded {len(df)} rows")

    grouped = df.groupby(GROUP_COL)
    print(f"Found {len(grouped)} distinct SalesReceiptNo groups")

    item_cache: Dict[str, str] = {}

    for group_key, group_df in grouped:
        payload = build_sales_receipt_payload(group_df, access_token, item_cache)
        print(f"\nSending SalesReceiptNo: {group_key}")
        send_sales_receipt(payload, access_token)


if __name__ == "__main__":
    main()