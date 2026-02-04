"""
QBO Item helpers: get-or-create Service item for bypass mode, load blocker item IDs from CSV.
Uses qbo_upload for BASE_URL and _make_qbo_request (imported at call site to avoid circular import).
"""
from __future__ import annotations

import csv
from pathlib import Path
from typing import Any, Dict, Optional, Set, Tuple, Union

# TokenManager type: use Any to avoid circular import; caller passes instance from qbo_upload
TokenManager = Any


def get_or_create_service_item(
    token_mgr: TokenManager,
    realm_id: str,
    name: str,
    income_account_id: str,
    tax_code_id: Optional[str] = None,
    base_url: str = "https://quickbooks.api.intuit.com",
    make_request: Any = None,
) -> Tuple[str, Dict[str, Any]]:
    """
    Look up QBO Item by exact Name; if not found, create a Service item with IncomeAccountRef.

    Args:
        token_mgr: TokenManager instance (from qbo_upload)
        realm_id: QBO Realm ID
        name: Item name (exact match for lookup; used as Name on create)
        income_account_id: Required IncomeAccountRef value for new Service item
        tax_code_id: Optional TaxCodeRef value for new item
        base_url: QBO API base URL
        make_request: _make_qbo_request function from qbo_upload

    Returns:
        Tuple of (item_id: str, metadata: dict with keys like Name, Type, IncomeAccountRef)

    Raises:
        RuntimeError: If income_account_id is missing or create fails
    """
    if not income_account_id or not str(income_account_id).strip():
        raise RuntimeError(
            "Bypass income account ID is required for get_or_create_service_item. "
            "Set bypass_income_account_id in company config or COMPANY_X_BYPASS_INCOME_ACCOUNT_ID in env."
        )
    if make_request is None:
        raise RuntimeError("make_request (e.g. _make_qbo_request from qbo_upload) is required")

    from urllib.parse import quote

    safe_name = (name or "").strip().replace("'", "''")
    if not safe_name:
        raise RuntimeError("Service item name cannot be blank")

    # Lookup by Name
    query = (
        "select Id, Name, Type, IncomeAccountRef from Item "
        f"where Name = '{safe_name}' maxresults 5"
    )
    url = f"{base_url}/v3/company/{realm_id}/query?query={quote(query)}&minorversion=70"
    resp = make_request("GET", url, token_mgr)
    if resp.status_code == 200:
        data = resp.json()
        items = data.get("QueryResponse", {}).get("Item", [])
        if not isinstance(items, list):
            items = [items] if items else []
        for it in items:
            if (it.get("Name") or "").strip() == (name or "").strip():
                item_id = it.get("Id", "")
                if item_id:
                    return (str(item_id), {"Name": it.get("Name"), "Type": it.get("Type"), "IncomeAccountRef": it.get("IncomeAccountRef")})

    # Create Service item
    create_url = f"{base_url}/v3/company/{realm_id}/item?minorversion=70"
    payload = {
        "Name": name.strip(),
        "Type": "Service",
        "Active": True,
        "IncomeAccountRef": {"value": str(income_account_id).strip()},
    }
    if tax_code_id:
        payload["Taxable"] = True
        payload["SalesTaxIncluded"] = False
        payload["TaxCodeRef"] = {"value": str(tax_code_id).strip()}
    create_resp = make_request("POST", create_url, token_mgr, json=payload)
    if create_resp.status_code not in (200, 201):
        try:
            body = create_resp.json()
            fault = body.get("Fault") or body.get("fault")
            errors = (fault or {}).get("Error") or (fault or {}).get("error") or []
            details = "; ".join(
                str(e.get("message", e.get("detail", ""))) for e in errors
            )
        except Exception:
            details = create_resp.text[:500] if create_resp.text else ""
        raise RuntimeError(
            f"Failed to create Service item '{name.strip()}': HTTP {create_resp.status_code}. {details}"
        )
    created = create_resp.json().get("Item")
    if not created:
        raise RuntimeError(f"No Item in response when creating Service '{name.strip()}'")
    item_id = created.get("Id")
    if not item_id:
        raise RuntimeError(f"Created Service item has no Id: {created}")
    return (str(item_id), {"Name": created.get("Name"), "Type": created.get("Type"), "IncomeAccountRef": created.get("IncomeAccountRef")})


def load_blocker_item_ids_from_csv(csv_path: Union[str, Path]) -> Set[str]:
    """
    Load set of QBO Item IDs that are inventory start-date blockers from a CSV.

    Expected CSV columns: ItemId (and optionally DocNumber, TxnDate, ItemName, etc.).
    Returns the set of unique ItemId values (as strings).
    """
    path = Path(csv_path)
    if not path.exists():
        return set()
    ids: Set[str] = set()
    with open(path, "r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        if "ItemId" not in (reader.fieldnames or []):
            return set()
        for row in reader:
            item_id = (row.get("ItemId") or "").strip()
            if item_id:
                ids.add(item_id)
    return ids
