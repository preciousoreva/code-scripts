from __future__ import annotations

import csv
import re
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

from django.db import transaction
from django.utils import timezone

from apps.epos_qbo.models import InventoryReviewItem, RunArtifact


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
                        "category": _pick(normalized, CATEGORY_ALIASES),
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


def normalize_product_name(value: object) -> str:
    return normalize_inventory_review_key(value)


def _parse_decimal(value: object) -> Decimal | None:
    raw = str(value or "").strip()
    if raw in {"", "—", "-"}:
        return None
    try:
        return Decimal(raw)
    except (InvalidOperation, ValueError):
        return None


def _map_reason_group(*, status: str, reason: str, issue_type: str = "") -> str:
    """Stable group slugs for DB-backed review items."""
    key = " ".join(_key(part) for part in (status, reason, issue_type) if str(part or "").strip())
    raw = " ".join(str(part or "").lower() for part in (status, reason, issue_type))

    if (
        "missing_from_qbo" in key
        or "missing_in_qbo" in key
        or "product_not_found" in key
        or "not_found_in_quickbooks" in key
        or "missing_from_quickbooks" in key
        or "product not found in quickbooks" in raw
        or "missing from quickbooks" in raw
    ):
        return "missing_from_qbo"

    if (
        "multiple_active_base_items" in key
        or "ambiguous_in_qbo" in key
        or "duplicate" in key
        or "base item and pack variants both exist" in raw
    ):
        return "duplicate_base_conflict"

    if (
        "only_pack_variant_exists" in key
        or "base_with_pack_variants" in key
        or "pack_variant" in key
        or "pack variants" in raw
    ):
        return "pack_variant_issue"

    if "needs_adjustment" in key or "adjustment" in key:
        return "needs_adjustment"

    return "other"


def ingest_inventory_review_items(*, artifact: RunArtifact, final_audit_path: Path) -> dict[str, int]:
    """Idempotently ingest blocked rows from a final audit CSV into InventoryReviewItem."""
    parsed = parse_inventory_review_csv(final_audit_path)
    blocked_rows = parsed.rows
    now = timezone.now()

    # Build desired set keyed by practical dedupe key.
    desired: dict[tuple[str, str, str, str], dict[str, Any]] = {}
    for row in blocked_rows:
        product = str(row.get("product") or "").strip() or "(Unnamed product)"
        normalized = normalize_product_name(product)
        reason_group = _map_reason_group(
            status=str(row.get("status") or ""),
            reason=str(row.get("reason") or ""),
            issue_type=str(row.get("issue_type") or ""),
        )
        status_code = _key(row.get("status") or row.get("issue_type") or "")
        desired[(artifact.company_key, normalized, reason_group, status_code)] = {
            "product_name": product,
            "normalized_product_name": normalized,
            "category": str(row.get("category") or "").strip(),
            "status_code": status_code,
            "reason": str(row.get("reason_label") or row.get("reason") or "").strip(),
            "reason_group": reason_group,
            "epos_expected_qty": _parse_decimal(row.get("epos_expected_qty")),
            "qbo_qty": _parse_decimal(row.get("qbo_qty")),
            "delta": _parse_decimal(row.get("delta")),
            "source_row_json": row.get("raw") or {},
            "suggested_next_step": str(row.get("suggested_next_step") or "").strip(),
        }

    created = 0
    updated = 0
    reopened = 0
    resolved_by_rerun = 0

    run = artifact.run_job if getattr(artifact, "run_job_id", None) else None
    run_label = run.friendly_id if run else ""

    with transaction.atomic():
        existing_qs = InventoryReviewItem.objects.select_for_update().filter(
            company_key=artifact.company_key,
            normalized_product_name__in=[k[1] for k in desired.keys()] or [""],
        )
        existing_map: dict[tuple[str, str, str, str], InventoryReviewItem] = {
            (i.company_key, i.normalized_product_name, i.reason_group, i.status_code): i for i in existing_qs
        }

        seen_keys = set()
        for key, payload in desired.items():
            seen_keys.add(key)
            item = existing_map.get(key)
            if item is None:
                InventoryReviewItem.objects.create(
                    company_key=artifact.company_key,
                    run_job=run,
                    artifact=artifact,
                    product_name=payload["product_name"],
                    normalized_product_name=payload["normalized_product_name"],
                    category=payload["category"],
                    status_code=payload["status_code"],
                    reason=payload["reason"],
                    reason_group=payload["reason_group"],
                    epos_expected_qty=payload["epos_expected_qty"],
                    qbo_qty=payload["qbo_qty"],
                    delta=payload["delta"],
                    source_row_json=payload["source_row_json"],
                    suggested_next_step=payload["suggested_next_step"],
                    first_seen_at=now,
                    last_seen_at=now,
                    last_seen_run_label=run_label,
                    occurrence_count=1,
                    is_active=True,
                    review_status=InventoryReviewItem.REVIEW_OPEN,
                )
                created += 1
                continue

            was_inactive = not item.is_active
            prior_status = item.review_status
            item.run_job = run
            item.artifact = artifact
            item.product_name = payload["product_name"]
            item.category = payload["category"]
            item.reason = payload["reason"]
            item.epos_expected_qty = payload["epos_expected_qty"]
            item.qbo_qty = payload["qbo_qty"]
            item.delta = payload["delta"]
            item.source_row_json = payload["source_row_json"]
            item.suggested_next_step = payload["suggested_next_step"]
            item.last_seen_at = now
            item.last_seen_run_label = run_label
            item.occurrence_count = (item.occurrence_count or 0) + 1
            item.is_active = True

            # If it reappears after being marked resolved, reopen to open for now.
            if prior_status in {
                InventoryReviewItem.REVIEW_MANUALLY_RESOLVED,
                InventoryReviewItem.REVIEW_RESOLVED_BY_RERUN,
            }:
                item.review_status = InventoryReviewItem.REVIEW_OPEN
                item.resolved_at = None
                item.resolution_type = ""
                item.resolved_by = None
                reopened += 1
            elif was_inactive and prior_status in {InventoryReviewItem.REVIEW_OPEN, InventoryReviewItem.REVIEW_ACKNOWLEDGED}:
                reopened += 1

            item.save(update_fields=[
                "run_job",
                "artifact",
                "product_name",
                "category",
                "reason",
                "epos_expected_qty",
                "qbo_qty",
                "delta",
                "source_row_json",
                "suggested_next_step",
                "last_seen_at",
                "last_seen_run_label",
                "occurrence_count",
                "is_active",
                "review_status",
                "resolved_at",
                "resolution_type",
                "resolved_by",
                "updated_at",
            ])
            updated += 1

        # Mark previously-active open/ack items as resolved_by_rerun if not present anymore.
        active_to_check = InventoryReviewItem.objects.select_for_update().filter(
            company_key=artifact.company_key,
            is_active=True,
        )
        for item in active_to_check:
            key = (item.company_key, item.normalized_product_name, item.reason_group, item.status_code)
            if key in seen_keys:
                continue

            # Disappeared from latest blocked set.
            if item.review_status in {InventoryReviewItem.REVIEW_OPEN, InventoryReviewItem.REVIEW_ACKNOWLEDGED}:
                item.review_status = InventoryReviewItem.REVIEW_RESOLVED_BY_RERUN
                item.resolution_type = "resolved_by_future_sync"
                if item.resolved_at is None:
                    item.resolved_at = now
                item.is_active = False
                item.save(update_fields=["review_status", "resolution_type", "resolved_at", "is_active", "updated_at"])
                resolved_by_rerun += 1
            elif item.review_status in {InventoryReviewItem.REVIEW_IGNORED}:
                # Keep operator decision, but it's no longer active for the latest run.
                item.is_active = False
                item.save(update_fields=["is_active", "updated_at"])

    return {
        "created": created,
        "updated": updated,
        "reopened": reopened,
        "resolved_by_rerun": resolved_by_rerun,
        "active_open_count": InventoryReviewItem.objects.filter(
            company_key=artifact.company_key,
            is_active=True,
            review_status__in=[InventoryReviewItem.REVIEW_OPEN, InventoryReviewItem.REVIEW_ACKNOWLEDGED],
        ).count(),
        "total_blocked_from_artifact": len(blocked_rows),
    }
