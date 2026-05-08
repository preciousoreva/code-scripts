"""
QuickBooks Online InventoryAdjustment helpers.

InventoryAdjustment POST support is intentionally disabled for the forward
inventory sync workflow. Public QBO InventoryAdjustment quantity changes use an
adjustment account and previously polluted P&L when routed through COGS /
Inventory Shrinkage. Keep payload construction only for historical tests and
remediation diagnostics; do not post new inventory adjustments from code.

The QBO v3 accounting API entity ``InventoryAdjustment`` (not the older name
``InventoryQtyAdjustment``) defines:

- ``AdjustAccountRef`` (required): inventory adjustment (expense/OBE) account
- ``Line`` entries with ``DetailType == "ItemAdjustmentLineDetail"`` and
  ``ItemAdjustmentLineDetail`` containing ``ItemRef`` plus either ``QtyDiff`` or
  ``NewQty``.

See Intuit ``Finance.xsd`` in the official QuickBooks V3 PHP SDK repo.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

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
    token_mgr: Any,
    realm_id: str,
    payload: Dict[str, Any],
) -> Dict[str, Any]:
    """Fail closed: automated QBO InventoryAdjustment posting has been removed."""
    raise RuntimeError(
        "QBO InventoryAdjustment posting has been removed. "
        "Use preview reports and manual QBO Adjust starting value corrections instead."
    )
