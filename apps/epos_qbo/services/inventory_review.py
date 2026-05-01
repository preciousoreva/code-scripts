from __future__ import annotations

import csv
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any


PRODUCT_ALIASES = (
    "product_name",
    "product",
    "name",
    "item_name",
    "epos_product_name",
    "base_name",
    "epos_base_name",
)
STATUS_ALIASES = (
    "final_status",
    "status",
    "review_status",
    "audit_status",
)
REASON_ALIASES = (
    "blocking_reason",
    "blocked_reason",
    "reason",
    "catalog_issue_detail",
    "issue_reason",
    "final_reason",
)
ISSUE_TYPE_ALIASES = (
    "catalog_issue_type",
    "issue_type",
    "blocking_status",
)
EPOS_QTY_ALIASES = (
    "epos_expected_qty",
    "expected_qty",
    "epos_qty",
    "epos_single_units",
    "epos_expected_units",
)
QBO_QTY_ALIASES = (
    "qbo_qty",
    "qty_on_hand",
    "qbo_qty_on_hand",
    "qbo_final_qty",
)
DELTA_ALIASES = ("delta", "difference", "qty_delta")
CATEGORY_ALIASES = ("category", "categories", "epos_categories", "item_category")

HEALTHY_KEYS = {"in_sync", "synced", "ok", "matched"}
NON_BLOCKER_REASON_KEYS = {
    "",
    "exact_name_match",
    "in_sync",
    "synced",
    "ok",
    "matched",
    "none",
    "no_issue",
    "no_issues",
}

REASON_GROUPS = {
    "missing_from_quickbooks": "Missing from QuickBooks",
    "duplicate_base_conflicts": "Duplicate/base conflicts",
    "pack_base_variants": "Pack/base variant issues",
    "needs_adjustment": "Needs adjustment",
    "other": "Other/Unknown",
}


@dataclass(frozen=True)
class InventoryReviewParseResult:
    rows: list[dict[str, Any]]
    total_rows: int
    healthy_rows: int
    malformed_rows: int = 0
    error: str = ""


def normalize_inventory_review_key(value: object) -> str:
    text = str(value or "").strip().lower()
    text = re.sub(r"[^a-z0-9]+", "_", text)
    return re.sub(r"_+", "_", text).strip("_")


def is_inventory_summary_or_invalid_product_name(value: object) -> bool:
    """Return True for summary/footer rows or unusable product identifiers."""

    raw = str(value or "").strip()
    if not raw:
        return True
    key = normalize_inventory_review_key(raw)
    if not key:
        return True
    if key == "total":
        return True
    if key.startswith(("total", "totals")):
        return True
    return any(fragment in key for fragment in ("grand_total", "subtotal", "report_total", "summary"))


def _clean_display_value(value: object) -> str:
    text = str(value or "").strip()
    if normalize_inventory_review_key(text) in {"nan", "none", "null"}:
        return ""
    return text


def _normalize_row(raw_row: dict[Any, Any]) -> dict[str, str]:
    normalized: dict[str, str] = {}
    for raw_key, raw_value in raw_row.items():
        if raw_key is None:
            continue
        key = normalize_inventory_review_key(raw_key)
        if not key:
            continue
        value = "" if raw_value is None else str(raw_value).strip()
        if key not in normalized or (not normalized[key] and value):
            normalized[key] = value
    return normalized


def _pick(row: dict[str, str], aliases: tuple[str, ...]) -> str:
    for alias in aliases:
        value = row.get(alias)
        if value not in (None, ""):
            return str(value).strip()
    return ""


def _key(value: object) -> str:
    return normalize_inventory_review_key(value)


def _is_non_blocker(value: object) -> bool:
    return _key(value) in NON_BLOCKER_REASON_KEYS


def _text_indicates_blocker(value: object) -> bool:
    raw = str(value or "").strip().lower()
    key = _key(raw)
    if not raw:
        return False
    blocker_keys = {
        "missing_from_qbo",
        "missing_in_qbo",
        "only_pack_variant_exists",
        "base_with_pack_variants",
        "multiple_active_base_items",
        "needs_adjustment",
        "ambiguous_in_qbo",
        "blocked",
        "needs_review",
    }
    if key in blocker_keys:
        return True
    blocker_fragments = (
        "product_not_found",
        "not_found_in_quickbooks",
        "missing",
        "duplicate",
        "multiple_active",
        "pack_variant",
        "pack_variants",
        "needs_adjustment",
        "adjustment",
        "blocked",
        "review",
        "conflict",
    )
    return any(fragment in key for fragment in blocker_fragments)


def _row_needs_review(*, status: str, reason: str, issue_type: str) -> bool:
    status_key = _key(status)
    if status_key in HEALTHY_KEYS and not (
        _text_indicates_blocker(reason) or _text_indicates_blocker(issue_type)
    ):
        return False
    if status_key and status_key not in HEALTHY_KEYS:
        return True
    if reason and not _is_non_blocker(reason):
        return True
    if issue_type and not _is_non_blocker(issue_type):
        return True
    return False


def _humanize_key(value: object) -> str:
    raw = str(value or "").strip()
    key = _key(raw)
    labels = {
        "missing_from_qbo": "Missing from QBO",
        "missing_in_qbo": "Missing in QBO",
        "needs_adjustment": "Needs adjustment",
        "ambiguous_in_qbo": "Ambiguous in QBO",
        "multiple_active_base_items": "Multiple active base items",
        "only_pack_variant_exists": "Only pack variant exists",
        "base_with_pack_variants": "Base with pack variants",
        "in_sync": "In sync",
    }
    if key in labels:
        return labels[key]
    return raw


def inventory_review_reason_group(status: str, reason: str, issue_type: str = "") -> dict[str, str]:
    key = " ".join(_key(part) for part in (status, reason, issue_type) if str(part or "").strip())
    raw = " ".join(str(part or "").lower() for part in (status, reason, issue_type))
    if (
        "missing_from_qbo" in key
        or "missing_in_qbo" in key
        or "product_not_found" in key
        or "not_found_in_quickbooks" in key
        or "product not found in quickbooks" in raw
    ):
        slug = "missing_from_quickbooks"
    elif "multiple_active_base_items" in key or "ambiguous_in_qbo" in key or "duplicate" in key:
        slug = "duplicate_base_conflicts"
    elif "only_pack_variant_exists" in key or "base_with_pack_variants" in key or "pack_variant" in key:
        slug = "pack_base_variants"
    elif "needs_adjustment" in key or "adjustment" in key:
        slug = "needs_adjustment"
    else:
        slug = "other"
    return {"slug": slug, "label": REASON_GROUPS[slug]}


def inventory_review_suggested_next_step(status: str, reason: str, issue_type: str = "") -> str:
    key = " ".join(_key(part) for part in (status, reason, issue_type) if str(part or "").strip())
    raw = " ".join(str(part or "").lower() for part in (status, reason, issue_type))
    if (
        "missing_from_qbo" in key
        or "missing_in_qbo" in key
        or "product_not_found" in key
        or "not_found_in_quickbooks" in key
        or "product not found in quickbooks" in raw
    ):
        return "Create the QBO inventory item or map this EPOS product to an existing QBO item."
    if "multiple_active_base_items" in key or "ambiguous_in_qbo" in key or "duplicate" in key:
        return "Choose the canonical QBO item and clean up duplicates."
    if "only_pack_variant_exists" in key:
        return "Create or confirm the missing base item."
    if "base_with_pack_variants" in key or "pack_variant" in key:
        return "Review base item and pack variants before cleanup."
    if "needs_adjustment" in key or "adjustment" in key:
        return "Retry inventory sync for this product after verifying QBO item state."
    return "Review final audit row and resolve in QBO or mapping config."


def parse_inventory_review_csv(path: Path) -> InventoryReviewParseResult:
    rows: list[dict[str, Any]] = []
    total_rows = 0
    healthy_rows = 0
    malformed_rows = 0

    try:
        with path.open("r", encoding="utf-8-sig", newline="") as handle:
            reader = csv.DictReader(handle, strict=True)
            if not reader.fieldnames:
                return InventoryReviewParseResult(
                    rows=[],
                    total_rows=0,
                    healthy_rows=0,
                    error="The final audit CSV is missing a header row.",
                )
            for raw_row in reader:
                if None in raw_row:
                    malformed_rows += 1
                    continue
                total_rows += 1
                normalized = _normalize_row(raw_row)
                product = _pick(normalized, PRODUCT_ALIASES)
                if is_inventory_summary_or_invalid_product_name(product):
                    continue
                status = _pick(normalized, STATUS_ALIASES)
                reason = _pick(normalized, REASON_ALIASES)
                issue_type = _pick(normalized, ISSUE_TYPE_ALIASES)
                if not _row_needs_review(status=status, reason=reason, issue_type=issue_type):
                    healthy_rows += 1
                    continue

                reason_label_source = reason or issue_type or status or "Needs review"
                group = inventory_review_reason_group(status, reason, issue_type)
                rows.append(
                    {
                        "product": product or "(Unnamed product)",
                        "status": status,
                        "status_label": _humanize_key(status or "Needs review"),
                        "reason": reason or issue_type or status,
                        "reason_label": _humanize_key(reason_label_source),
                        "issue_type": issue_type,
                        "reason_group": group["label"],
                        "reason_group_slug": group["slug"],
                        "epos_expected_qty": _pick(normalized, EPOS_QTY_ALIASES),
                        "qbo_qty": _pick(normalized, QBO_QTY_ALIASES),
                        "delta": _pick(normalized, DELTA_ALIASES),
                        "category": _clean_display_value(_pick(normalized, CATEGORY_ALIASES)),
                        "suggested_next_step": inventory_review_suggested_next_step(
                            status,
                            reason,
                            issue_type,
                        ),
                        "search_text": " ".join(
                            part
                            for part in (product, status, reason, issue_type, _pick(normalized, CATEGORY_ALIASES))
                            if part
                        ).lower(),
                        "raw": {str(k): v for k, v in raw_row.items() if k is not None},
                    }
                )
    except csv.Error as exc:
        return InventoryReviewParseResult(
            rows=[],
            total_rows=total_rows,
            healthy_rows=healthy_rows,
            malformed_rows=malformed_rows,
            error=f"The final audit CSV could not be parsed: {exc}",
        )
    except (OSError, UnicodeDecodeError) as exc:
        return InventoryReviewParseResult(
            rows=[],
            total_rows=total_rows,
            healthy_rows=healthy_rows,
            malformed_rows=malformed_rows,
            error=f"The final audit CSV could not be read: {exc}",
        )

    error = ""
    if malformed_rows:
        error = f"{malformed_rows} malformed final audit row(s) were skipped."
    return InventoryReviewParseResult(
        rows=rows,
        total_rows=total_rows,
        healthy_rows=healthy_rows,
        malformed_rows=malformed_rows,
        error=error,
    )
