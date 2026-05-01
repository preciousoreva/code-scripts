"""Service layer for the Inventory Review Center remediation actions.

The Inventory Review page (v1) is artifact-driven: it parses the latest final
audit CSV for a company and renders read-only review rows.

This module adds Phase 1 remediation actions that work on top of that artifact:

* ``retry_catalog_cleanup_for_review`` queues an inventory pipeline run scoped to
  duplicate/base conflicts that the user wants to retry. The inventory pipeline
  already does catalog cleanup as one of its phases — we record a retry intent
  and reuse that path so we don't fork the supported sequence.

* ``retry_quantity_adjustments_for_review`` queues an inventory pipeline run for
  exact-match ``needs_adjustment`` rows. Same reasoning: the unified pipeline
  already posts inventory adjustments for exact-name matches; we just record a
  scoped retry.

* ``build_missing_item_creation_preview`` is a read-only classifier for
  ``missing_from_qbo`` rows. It does **not** create QBO items. Phase 2 will turn
  this preview into a guarded write workflow.

Critical guardrail: missing-item creation must never re-introduce a pack
variant when a base item already exists. The classifier in this module is the
first place that guard lands; the future write path will reuse the same
classifier.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable

from django.http import Http404

from ..models import CompanyConfigRecord, RunArtifact, RunJob
from .inventory_review import (
    InventoryReviewParseResult,
    is_inventory_summary_or_invalid_product_name,
    normalize_inventory_review_key,
    parse_inventory_review_csv,
)


REASON_GROUP_DUPLICATE_BASE = "duplicate_base_conflicts"
REASON_GROUP_PACK_BASE_VARIANTS = "pack_base_variants"
REASON_GROUP_NEEDS_ADJUSTMENT = "needs_adjustment"
REASON_GROUP_MISSING = "missing_from_quickbooks"

CATALOG_CLEANUP_REASON_GROUPS = {
    REASON_GROUP_DUPLICATE_BASE,
    REASON_GROUP_PACK_BASE_VARIANTS,
}
CATALOG_CLEANUP_ISSUE_TYPES = {
    "base_with_pack_variants",
    "multiple_active_base_items",
    "only_pack_variant_exists",
}

QUANTITY_ADJUSTMENT_REASON_GROUPS = {REASON_GROUP_NEEDS_ADJUSTMENT}
QUANTITY_ADJUSTMENT_ISSUE_TYPES = {"exact_name_match"}

# Retry-intent slugs we stamp into RunJob.inventory_options_json so logs/UX
# can attribute a retry run back to the review action that triggered it.
RETRY_INTENT_CATALOG = "review_retry_catalog_cleanup"
RETRY_INTENT_QUANTITY = "review_retry_quantity_adjustments"


PACK_SUFFIX_RE = re.compile(r"(?P<base>.+?)\s*\*\s*(?P<count>\d+)\s*$")
INVALID_NAME_FRAGMENTS = (
    "total:",
    "totals:",
    "grand total",
    "subtotal",
    "report total",
    "summary",
)


@dataclass(frozen=True)
class ReviewContext:
    """Bundle of trusted artifact + parsed rows used by every action view."""

    company: CompanyConfigRecord
    artifact: RunArtifact
    final_audit_path: Path
    parse_result: InventoryReviewParseResult

    @property
    def rows(self) -> list[dict[str, Any]]:
        return list(self.parse_result.rows)


@dataclass
class MissingPreviewRow:
    product: str
    base_name: str
    suggested_qbo_name: str
    category: str
    epos_expected_qty: str
    inventory_account: str
    revenue_account: str
    cogs_account: str
    safety_status: str
    block_reason: str
    is_safe: bool


@dataclass
class MissingPreview:
    rows: list[MissingPreviewRow] = field(default_factory=list)
    safe_count: int = 0
    blocked_count: int = 0
    mapping_loaded: bool = False
    mapping_error: str = ""
    qbo_base_names_loaded: bool = False
    qbo_base_names_error: str = ""


def _normalize_base_name(value: str) -> str:
    """Strip a trailing ``*N`` pack-size suffix and collapse whitespace."""

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
    return is_inventory_summary_or_invalid_product_name(name)


def get_review_rows_by_reason(
    rows: Iterable[dict[str, Any]],
    reason_group_slug: str,
) -> list[dict[str, Any]]:
    """Filter parsed final-audit rows to a single reason group.

    Action endpoints only ever scope work to rows the parser already produced,
    so the user never gets to inject arbitrary product names.
    """

    target = str(reason_group_slug or "").strip()
    if not target:
        return []
    return [row for row in rows if str(row.get("reason_group_slug") or "") == target]


def get_quantity_adjustment_rows(rows: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    """Return rows that ``retry_quantity_adjustments`` would act on."""

    out: list[dict[str, Any]] = []
    for row in rows:
        if str(row.get("reason_group_slug") or "") not in QUANTITY_ADJUSTMENT_REASON_GROUPS:
            continue
        issue = normalize_inventory_review_key(row.get("issue_type"))
        if issue and issue not in QUANTITY_ADJUSTMENT_ISSUE_TYPES:
            continue
        out.append(row)
    return out


def get_catalog_cleanup_rows(rows: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    """Return rows that ``retry_catalog_cleanup`` would act on."""

    out: list[dict[str, Any]] = []
    for row in rows:
        slug = str(row.get("reason_group_slug") or "")
        if slug in CATALOG_CLEANUP_REASON_GROUPS:
            out.append(row)
            continue
        issue = normalize_inventory_review_key(row.get("issue_type"))
        if issue in CATALOG_CLEANUP_ISSUE_TYPES:
            out.append(row)
    return out


def load_review_context(
    *,
    company: CompanyConfigRecord,
    artifact: RunArtifact | None,
    final_audit_path_resolver,
) -> ReviewContext | None:
    """Resolve the trusted final-audit artifact + parsed rows for a company.

    ``final_audit_path_resolver`` is the existing view-side path resolver; we
    inject it instead of importing from views.py to keep this module free of
    circular imports.
    """

    if artifact is None:
        return None
    try:
        final_audit_path = final_audit_path_resolver(artifact, "final_audit")
    except Http404:
        return None
    if not isinstance(final_audit_path, Path):
        final_audit_path = Path(str(final_audit_path))
    parsed = parse_inventory_review_csv(final_audit_path)
    return ReviewContext(
        company=company,
        artifact=artifact,
        final_audit_path=final_audit_path,
        parse_result=parsed,
    )


def _affected_base_names(rows: Iterable[dict[str, Any]]) -> list[str]:
    seen: dict[str, str] = {}
    for row in rows:
        product = str(row.get("product") or "").strip()
        if not product:
            continue
        base = _normalize_base_name(product) or product
        key = base.lower()
        if key not in seen:
            seen[key] = base
    return list(seen.values())


def _build_retry_inventory_options(
    *,
    intent: str,
    artifact: RunArtifact,
    rows: list[dict[str, Any]],
) -> dict[str, Any]:
    """Build inventory_options_json for a review-triggered retry RunJob.

    We DON'T pass user-supplied product names through to the pipeline CLI as a
    free-text filter — the inventory pipeline filter is a single substring
    match, not a list, so a multi-product retry would be ambiguous. Instead we
    queue a full pipeline run and stamp retry context into ``review_retry`` so
    the run can be attributed back to the user action that started it.
    """

    return {
        "review_retry": {
            "intent": str(intent),
            "source_artifact_id": int(artifact.id) if artifact.id else None,
            "source_final_audit": str(artifact.source_path or ""),
            "affected_base_names": _affected_base_names(rows),
            "row_count": int(len(rows)),
        }
    }


def queue_retry_run_job(
    *,
    company: CompanyConfigRecord,
    intent: str,
    rows: list[dict[str, Any]],
    artifact: RunArtifact,
    requested_by,
) -> RunJob:
    """Create a queued RunJob for a review-triggered retry."""

    inventory_options = _build_retry_inventory_options(
        intent=intent, artifact=artifact, rows=rows
    )
    return RunJob.objects.create(
        scope=RunJob.SCOPE_INVENTORY_PIPELINE,
        company_key=company.company_key,
        inventory_options_json=inventory_options,
        requested_by=requested_by,
        status=RunJob.STATUS_QUEUED,
    )


def retry_catalog_cleanup_for_review(
    *,
    context: ReviewContext,
    requested_by,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Phase 1: queue an inventory pipeline retry for catalog conflicts.

    Returns a dict describing what was queued (or what would be queued in
    dry-run mode). Raises no exceptions for empty input — the caller is
    expected to gate on the count.
    """

    rows = get_catalog_cleanup_rows(context.rows)
    if dry_run or not rows:
        return {
            "queued": False,
            "row_count": len(rows),
            "rows": rows,
            "intent": RETRY_INTENT_CATALOG,
        }
    job = queue_retry_run_job(
        company=context.company,
        intent=RETRY_INTENT_CATALOG,
        rows=rows,
        artifact=context.artifact,
        requested_by=requested_by,
    )
    return {
        "queued": True,
        "row_count": len(rows),
        "rows": rows,
        "intent": RETRY_INTENT_CATALOG,
        "job_id": job.id,
    }


def retry_quantity_adjustments_for_review(
    *,
    context: ReviewContext,
    requested_by,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Phase 1: queue an inventory pipeline retry for exact-match adjustments."""

    rows = get_quantity_adjustment_rows(context.rows)
    if dry_run or not rows:
        return {
            "queued": False,
            "row_count": len(rows),
            "rows": rows,
            "intent": RETRY_INTENT_QUANTITY,
        }
    job = queue_retry_run_job(
        company=context.company,
        intent=RETRY_INTENT_QUANTITY,
        rows=rows,
        artifact=context.artifact,
        requested_by=requested_by,
    )
    return {
        "queued": True,
        "row_count": len(rows),
        "rows": rows,
        "intent": RETRY_INTENT_QUANTITY,
        "job_id": job.id,
    }


# ---------------------------------------------------------------------------
# Missing from QuickBooks: read-only preview
# ---------------------------------------------------------------------------


def _load_category_mapping_safe(company: CompanyConfigRecord) -> tuple[dict[str, dict[str, str]], str]:
    """Load Product.Mapping.csv for the company; return (mapping, error)."""

    try:
        from code_scripts.company_config import load_company_config
        from code_scripts.qbo_upload import load_category_account_mapping
    except Exception as exc:  # pragma: no cover - defensive
        return {}, f"Could not import inventory mapping helpers: {exc}"
    try:
        cfg = load_company_config(company.company_key)
    except Exception as exc:
        return {}, f"Could not load company config: {exc}"
    try:
        mapping = load_category_account_mapping(cfg)
    except FileNotFoundError as exc:
        return {}, str(exc)
    except ValueError as exc:
        return {}, str(exc)
    except Exception as exc:  # pragma: no cover - defensive
        return {}, f"Could not load Product.Mapping.csv: {exc}"
    if not isinstance(mapping, dict):
        return {}, "Product.Mapping.csv produced no mapping."
    return mapping, ""


def _load_qbo_base_name_keys(company: CompanyConfigRecord) -> tuple[set[str], str]:
    """Best-effort load of normalized QBO base names from the cached snapshot.

    Used to detect "would create a pack variant when base exists" in the
    preview. If the snapshot can't be read, we still render the preview but
    flag mapping-only safety.
    """

    try:
        from code_scripts.inventory_sync import load_qbo_inventory_item_rows
        from code_scripts.qbo_snapshot_cache import get_qbo_snapshot_path
    except Exception as exc:  # pragma: no cover - defensive
        return set(), f"Could not import QBO snapshot helpers: {exc}"
    try:
        snapshot_path = get_qbo_snapshot_path(company.company_key)
    except Exception as exc:
        return set(), f"Could not resolve QBO snapshot path: {exc}"
    if not snapshot_path or not Path(snapshot_path).exists():
        return set(), "QBO inventory snapshot not found; refresh inventory data."
    try:
        rows = load_qbo_inventory_item_rows(str(snapshot_path))
    except Exception as exc:  # pragma: no cover - defensive
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
        # Pandas iteration issues shouldn't crash the preview.
        return keys, "Could not iterate QBO snapshot rows; treating QBO bases as unknown."
    return keys, ""


def _classify_missing_row(
    row: dict[str, Any],
    *,
    mapping: dict[str, dict[str, str]],
    mapping_loaded: bool,
    qbo_base_keys: set[str],
    qbo_base_keys_loaded: bool,
    seen_keys: set[str],
) -> MissingPreviewRow:
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
        return MissingPreviewRow(
            product=product or "(blank)",
            base_name=base_name,
            suggested_qbo_name=base_name or product or "(blank)",
            category=category,
            epos_expected_qty=epos_qty,
            inventory_account=inventory_account,
            revenue_account=revenue_account,
            cogs_account=cogs_account,
            safety_status="Invalid row",
            block_reason="Row looks like a CSV summary or empty product (e.g. 'Total:').",
            is_safe=False,
        )

    is_pack = _is_pack_variant(product)
    base_exists_in_qbo = qbo_base_keys_loaded and base_key in qbo_base_keys
    if is_pack and base_exists_in_qbo:
        return MissingPreviewRow(
            product=product,
            base_name=base_name,
            suggested_qbo_name=base_name,
            category=category,
            epos_expected_qty=epos_qty,
            inventory_account=inventory_account,
            revenue_account=revenue_account,
            cogs_account=cogs_account,
            safety_status="Pack variant of existing base",
            block_reason="Do not create pack variant; base item exists.",
            is_safe=False,
        )

    if base_key in seen_keys:
        return MissingPreviewRow(
            product=product,
            base_name=base_name,
            suggested_qbo_name=base_name,
            category=category,
            epos_expected_qty=epos_qty,
            inventory_account=inventory_account,
            revenue_account=revenue_account,
            cogs_account=cogs_account,
            safety_status="Duplicate candidate",
            block_reason=(
                "Another missing row already maps to this base name; review the "
                "EPOS source before creating duplicates."
            ),
            is_safe=False,
        )
    seen_keys.add(base_key)

    if not mapping_loaded:
        return MissingPreviewRow(
            product=product,
            base_name=base_name,
            suggested_qbo_name=base_name,
            category=category,
            epos_expected_qty=epos_qty,
            inventory_account=inventory_account,
            revenue_account=revenue_account,
            cogs_account=cogs_account,
            safety_status="Mapping unavailable",
            block_reason="Product.Mapping.csv could not be loaded; cannot verify accounts.",
            is_safe=False,
        )

    category_normalized = re.sub(r"\s+", " ", category).strip()
    if not category_normalized:
        return MissingPreviewRow(
            product=product,
            base_name=base_name,
            suggested_qbo_name=base_name,
            category=category,
            epos_expected_qty=epos_qty,
            inventory_account=inventory_account,
            revenue_account=revenue_account,
            cogs_account=cogs_account,
            safety_status="Missing category",
            block_reason="EPOS row has no category; cannot resolve account mapping.",
            is_safe=False,
        )
    if category_normalized not in mapping:
        return MissingPreviewRow(
            product=product,
            base_name=base_name,
            suggested_qbo_name=base_name,
            category=category,
            epos_expected_qty=epos_qty,
            inventory_account=inventory_account,
            revenue_account=revenue_account,
            cogs_account=cogs_account,
            safety_status="Category not in mapping",
            block_reason=(
                f"Category '{category_normalized}' is not in Product.Mapping.csv; "
                "add it before creating items."
            ),
            is_safe=False,
        )

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
        return MissingPreviewRow(
            product=product,
            base_name=base_name,
            suggested_qbo_name=base_name,
            category=category,
            epos_expected_qty=epos_qty,
            inventory_account=inventory_account,
            revenue_account=revenue_account,
            cogs_account=cogs_account,
            safety_status="Incomplete account mapping",
            block_reason=(
                "Mapping is missing: "
                + ", ".join(missing_accounts)
                + ". Fill in Product.Mapping.csv."
            ),
            is_safe=False,
        )

    return MissingPreviewRow(
        product=product,
        base_name=base_name,
        suggested_qbo_name=base_name,
        category=category,
        epos_expected_qty=epos_qty,
        inventory_account=inventory_account,
        revenue_account=revenue_account,
        cogs_account=cogs_account,
        safety_status="Safe candidate",
        block_reason="",
        is_safe=True,
    )


def build_missing_item_creation_preview(
    *,
    context: ReviewContext,
    mapping_loader=_load_category_mapping_safe,
    qbo_base_loader=_load_qbo_base_name_keys,
) -> MissingPreview:
    """Classify missing-from-QBO rows for the read-only preview page.

    The classifier is the place where the "do not create pack variant when base
    exists" guardrail lives. The future Phase 2 write path will gate on
    ``MissingPreviewRow.is_safe`` from this same classifier.
    """

    rows = get_review_rows_by_reason(context.rows, REASON_GROUP_MISSING)
    mapping, mapping_error = mapping_loader(context.company)
    mapping_loaded = bool(mapping) and not mapping_error
    qbo_base_keys, qbo_base_keys_error = qbo_base_loader(context.company)
    qbo_base_keys_loaded = not qbo_base_keys_error

    seen_keys: set[str] = set()
    classified: list[MissingPreviewRow] = []
    safe_count = 0
    blocked_count = 0
    for row in rows:
        result = _classify_missing_row(
            row,
            mapping=mapping,
            mapping_loaded=mapping_loaded,
            qbo_base_keys=qbo_base_keys,
            qbo_base_keys_loaded=qbo_base_keys_loaded,
            seen_keys=seen_keys,
        )
        classified.append(result)
        if result.is_safe:
            safe_count += 1
        else:
            blocked_count += 1

    return MissingPreview(
        rows=classified,
        safe_count=safe_count,
        blocked_count=blocked_count,
        mapping_loaded=mapping_loaded,
        mapping_error=mapping_error,
        qbo_base_names_loaded=qbo_base_keys_loaded,
        qbo_base_names_error=qbo_base_keys_error,
    )
