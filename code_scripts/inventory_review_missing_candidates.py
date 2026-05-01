"""Pure helpers for Inventory Review \"missing from QuickBooks\" classification.

Used by the Django preview/queue flow and by ``inventory_pipeline`` when
executing review-triggered missing-item creation. Keeps guardrails aligned
without importing Django models in the pipeline.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

from apps.epos_qbo.services.inventory_review import (
    is_inventory_summary_or_invalid_product_name,
    normalize_inventory_review_key,
    parse_inventory_review_csv,
)

REASON_GROUP_MISSING = "missing_from_quickbooks"

PACK_SUFFIX_RE = re.compile(r"(?P<base>.+?)\s*\*\s*(?P<count>\d+)\s*$")
INVALID_NAME_FRAGMENTS = (
    "total:",
    "totals:",
    "grand total",
    "subtotal",
    "report total",
    "summary",
)


def _normalize_base_name(value: str) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    match = PACK_SUFFIX_RE.match(text)
    if match:
        text = match.group("base").strip()
    return re.sub(r"\s+", " ", text)


def _name_key(value: str) -> str:
    return _normalize_base_name(value).lower()


def _is_pack_variant(name: str) -> bool:
    return bool(PACK_SUFFIX_RE.match(str(name or "").strip()))


def _looks_invalid(name: str) -> bool:
    raw = str(name or "").strip()
    if not raw:
        return True
    if is_inventory_summary_or_invalid_product_name(raw):
        return True
    key = normalize_inventory_review_key(raw)
    return any(fragment in key for fragment in INVALID_NAME_FRAGMENTS)


def load_category_mapping_for_company_key(company_key: str) -> tuple[dict[str, dict[str, str]], str]:
    try:
        from code_scripts.company_config import load_company_config
        from code_scripts.qbo_upload import load_category_account_mapping
    except Exception as exc:  # pragma: no cover
        return {}, f"Could not import inventory mapping helpers: {exc}"
    try:
        cfg = load_company_config(company_key)
        mapping = load_category_account_mapping(cfg)
    except FileNotFoundError as exc:
        return {}, str(exc)
    except ValueError as exc:
        return {}, str(exc)
    except Exception as exc:  # pragma: no cover
        return {}, f"Could not load Product.Mapping.csv: {exc}"
    if not isinstance(mapping, dict):
        return {}, "Product.Mapping.csv produced no mapping."
    return mapping, ""


def load_qbo_base_name_keys_for_company_key(company_key: str) -> tuple[set[str], str]:
    try:
        from code_scripts.inventory_sync import load_qbo_inventory_item_rows
        from code_scripts.qbo_snapshot_cache import get_qbo_snapshot_path
    except Exception as exc:  # pragma: no cover
        return set(), f"Could not import QBO snapshot helpers: {exc}"
    try:
        snapshot_path = get_qbo_snapshot_path(company_key)
    except Exception as exc:
        return set(), f"Could not resolve QBO snapshot path: {exc}"
    if not snapshot_path or not Path(snapshot_path).exists():
        return set(), "QBO inventory snapshot not found; refresh inventory data."
    try:
        rows = load_qbo_inventory_item_rows(str(snapshot_path))
    except Exception as exc:  # pragma: no cover
        return set(), f"Could not load QBO snapshot: {exc}"
    keys: set[str] = set()
    if rows is None:
        return keys, ""
    try:
        for _, row in rows.iterrows():
            active = row.get("Active") if "Active" in rows.columns else True
            if isinstance(active, str):
                active_flag = active.strip().lower() not in {"false", "0", "no"}
            else:
                active_flag = bool(active) if active is not None else True
            if not active_flag:
                continue
            base = str(row.get("base_name") or row.get("Name") or "").strip()
            if not base:
                continue
            if _is_pack_variant(base):
                continue
            keys.add(_name_key(base))
    except Exception:
        return keys, "Could not iterate QBO snapshot rows; treating QBO bases as unknown."
    return keys, ""


def _classify_missing_row_dict(
    row: dict[str, Any],
    *,
    mapping: dict[str, dict[str, str]],
    mapping_loaded: bool,
    qbo_base_keys: set[str],
    qbo_base_keys_loaded: bool,
    seen_keys: set[str],
) -> dict[str, Any]:
    product = str(row.get("product") or "").strip()
    category = str(row.get("category") or "").strip()
    if normalize_inventory_review_key(category) in {"nan", "none", "null"}:
        category = ""
    epos_qty = str(row.get("epos_expected_qty") or "").strip()
    base_name = _normalize_base_name(product)
    base_key = _name_key(product)

    inventory_account = ""
    revenue_account = ""
    cogs_account = ""

    if _looks_invalid(product):
        return {
            "product": product or "(blank)",
            "base_name": base_name,
            "suggested_qbo_name": base_name or product or "(blank)",
            "category": category,
            "epos_expected_qty": epos_qty,
            "inventory_account": inventory_account,
            "revenue_account": revenue_account,
            "cogs_account": cogs_account,
            "safety_status": "Invalid row",
            "block_reason": "Row looks like a CSV summary or empty product (e.g. 'Total:').",
            "is_safe": False,
        }

    is_pack = _is_pack_variant(product)
    base_exists_in_qbo = qbo_base_keys_loaded and base_key in qbo_base_keys
    if is_pack and base_exists_in_qbo:
        return {
            "product": product,
            "base_name": base_name,
            "suggested_qbo_name": base_name,
            "category": category,
            "epos_expected_qty": epos_qty,
            "inventory_account": inventory_account,
            "revenue_account": revenue_account,
            "cogs_account": cogs_account,
            "safety_status": "Pack variant of existing base",
            "block_reason": "Do not create pack variant; base item exists.",
            "is_safe": False,
        }

    if base_key in seen_keys:
        return {
            "product": product,
            "base_name": base_name,
            "suggested_qbo_name": base_name,
            "category": category,
            "epos_expected_qty": epos_qty,
            "inventory_account": inventory_account,
            "revenue_account": revenue_account,
            "cogs_account": cogs_account,
            "safety_status": "Duplicate candidate",
            "block_reason": (
                "Another missing row already maps to this base name; review the "
                "EPOS source before creating duplicates."
            ),
            "is_safe": False,
        }
    seen_keys.add(base_key)

    if not mapping_loaded:
        return {
            "product": product,
            "base_name": base_name,
            "suggested_qbo_name": base_name,
            "category": category,
            "epos_expected_qty": epos_qty,
            "inventory_account": inventory_account,
            "revenue_account": revenue_account,
            "cogs_account": cogs_account,
            "safety_status": "Mapping unavailable",
            "block_reason": "Product.Mapping.csv could not be loaded; cannot verify accounts.",
            "is_safe": False,
        }

    category_normalized = re.sub(r"\s+", " ", category).strip()
    if not category_normalized:
        return {
            "product": product,
            "base_name": base_name,
            "suggested_qbo_name": base_name,
            "category": category,
            "epos_expected_qty": epos_qty,
            "inventory_account": inventory_account,
            "revenue_account": revenue_account,
            "cogs_account": cogs_account,
            "safety_status": "Missing category",
            "block_reason": "EPOS row has no category; cannot resolve account mapping.",
            "is_safe": False,
        }
    if category_normalized not in mapping:
        return {
            "product": product,
            "base_name": base_name,
            "suggested_qbo_name": base_name,
            "category": category,
            "epos_expected_qty": epos_qty,
            "inventory_account": inventory_account,
            "revenue_account": revenue_account,
            "cogs_account": cogs_account,
            "safety_status": "Category not in mapping",
            "block_reason": (
                f"Category '{category_normalized}' is not in Product.Mapping.csv; "
                "add it before creating items."
            ),
            "is_safe": False,
        }

    accounts = mapping[category_normalized]
    inventory_account = str(accounts.get("asset") or "").strip()
    revenue_account = str(accounts.get("income") or "").strip()
    cogs_account = str(accounts.get("expense") or "").strip()
    missing_accounts = [
        label
        for label, value in (
            ("Inventory", inventory_account),
            ("Revenue", revenue_account),
            ("COGS", cogs_account),
        )
        if not value
    ]
    if missing_accounts:
        return {
            "product": product,
            "base_name": base_name,
            "suggested_qbo_name": base_name,
            "category": category,
            "epos_expected_qty": epos_qty,
            "inventory_account": inventory_account,
            "revenue_account": revenue_account,
            "cogs_account": cogs_account,
            "safety_status": "Incomplete account mapping",
            "block_reason": (
                "Mapping is missing: "
                + ", ".join(missing_accounts)
                + ". Fill in Product.Mapping.csv."
            ),
            "is_safe": False,
        }

    return {
        "product": product,
        "base_name": base_name,
        "suggested_qbo_name": base_name,
        "category": category,
        "epos_expected_qty": epos_qty,
        "inventory_account": inventory_account,
        "revenue_account": revenue_account,
        "cogs_account": cogs_account,
        "safety_status": "Safe candidate",
        "block_reason": "",
        "is_safe": True,
    }


def classify_missing_items_for_audit_file(company_key: str, audit_path: Path) -> dict[str, Any]:
    """Parse final audit CSV and classify missing-from-QBO rows."""

    parsed = parse_inventory_review_csv(audit_path)
    mapping, mapping_error = load_category_mapping_for_company_key(company_key)
    mapping_loaded = bool(mapping) and not mapping_error
    qbo_base_keys, qbo_base_keys_error = load_qbo_base_name_keys_for_company_key(company_key)
    qbo_base_keys_loaded = not qbo_base_keys_error

    rows_in = [r for r in parsed.rows if str(r.get("reason_group_slug") or "") == REASON_GROUP_MISSING]
    seen_keys: set[str] = set()
    classified: list[dict[str, Any]] = []
    safe_count = 0
    blocked_count = 0
    for row in rows_in:
        result = _classify_missing_row_dict(
            row,
            mapping=mapping,
            mapping_loaded=mapping_loaded,
            qbo_base_keys=qbo_base_keys,
            qbo_base_keys_loaded=qbo_base_keys_loaded,
            seen_keys=seen_keys,
        )
        classified.append(result)
        if result["is_safe"]:
            safe_count += 1
        else:
            blocked_count += 1

    return {
        "parse_error": parsed.error or "",
        "rows": classified,
        "safe_count": safe_count,
        "blocked_count": blocked_count,
        "mapping_loaded": mapping_loaded,
        "mapping_error": mapping_error,
        "qbo_base_names_loaded": qbo_base_keys_loaded,
        "qbo_base_names_error": qbo_base_keys_error,
    }


def parse_epos_qty_for_item_create(text: str) -> float:
    raw = str(text or "").strip().replace(",", "")
    if not raw:
        return 0.0
    try:
        return float(raw)
    except ValueError:
        return 0.0
