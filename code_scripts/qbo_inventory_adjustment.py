"""
QuickBooks Online: Inventory quantity adjustments via InventoryAdjustment.

This uses the QBO v3 accounting API entity ``InventoryAdjustment`` (not the older
name ``InventoryQtyAdjustment``). The XSD defines:

- ``AdjustAccountRef`` (required): inventory adjustment (expense/OBE) account
- ``Line`` entries with ``DetailType == "ItemAdjustmentLineDetail"`` and
  ``ItemAdjustmentLineDetail`` containing ``ItemRef`` plus either ``QtyDiff`` or
  ``NewQty``.

See Intuit ``Finance.xsd`` in the official QuickBooks V3 PHP SDK repo.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from code_scripts.company_config import get_qbo_api_base_url
from code_scripts.qbo_upload import TokenManager, _make_qbo_request

MINORVERSION = "70"


def build_inventory_adjustment_payload(
    *,
    adjust_account_id: str,
    txn_date: str,
    private_note: str,
    lines: List[Dict[str, Any]],
    doc_number: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Build the JSON body for POST /v3/company/{realmId}/inventoryadjustment

    Each line dict must include:
    - item_id: str
    - qty_diff: float | int  (positive increases, negative decreases)

    Optional:
    - sales_price: decimal as float/str
    """
    if not adjust_account_id or not str(adjust_account_id).strip():
        raise ValueError("adjust_account_id is required")
    if not txn_date or not str(txn_date).strip():
        raise ValueError("txn_date is required")
    if not lines:
        raise ValueError("lines must be non-empty")

    qbo_lines: List[Dict[str, Any]] = []
    for raw in lines:
        item_id = str(raw.get("item_id", "")).strip()
        if not item_id:
            raise ValueError("Each line must include item_id")

        detail: Dict[str, Any] = {
            "ItemRef": {"value": item_id},
        }
        if raw.get("sales_price") is not None and str(raw.get("sales_price")).strip() != "":
            detail["SalesPrice"] = raw["sales_price"]

        if "qty_diff" not in raw:
            raise ValueError("Each line must include qty_diff")
        detail["QtyDiff"] = raw["qty_diff"]

        qbo_lines.append(
            {
                "DetailType": "ItemAdjustmentLineDetail",
                "ItemAdjustmentLineDetail": detail,
            }
        )

    payload_obj: Dict[str, Any] = {
        "TxnDate": str(txn_date).strip()[:10],
        "PrivateNote": private_note,
        "AdjustAccountRef": {"value": str(adjust_account_id).strip()},
        "Line": qbo_lines,
    }
    if doc_number is not None and str(doc_number).strip():
        payload_obj["DocNumber"] = str(doc_number).strip()

    return payload_obj


def post_inventory_adjustment(
    token_mgr: TokenManager,
    realm_id: str,
    payload: Dict[str, Any],
) -> Dict[str, Any]:
    """POST InventoryAdjustment; returns parsed JSON body."""
    base_url = get_qbo_api_base_url()
    url = f"{base_url}/v3/company/{realm_id}/inventoryadjustment?minorversion={MINORVERSION}"
    resp = _make_qbo_request("POST", url, token_mgr, json=payload)
    if resp.status_code not in (200, 201):
        try:
            body = resp.json()
            detail = body
        except Exception:
            detail = resp.text[:2000]
        raise RuntimeError(f"InventoryAdjustment failed: HTTP {resp.status_code}: {detail}")
    try:
        return resp.json()
    except Exception as exc:
        raise RuntimeError(f"InventoryAdjustment: invalid JSON response: {exc}") from exc
