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
  ``missing_from_qbo`` rows (shared logic lives in
  ``code_scripts.inventory_review_missing_candidates``).

* ``queue_missing_item_creation_job`` queues a pipeline run that creates only
  server-classified safe missing Inventory items (see
  ``inventory_pipeline --review-create-missing-items``).

Critical guardrail: missing-item creation must never re-introduce a pack
variant when a base item already exists. The classifier in this module is the
first place that guard lands; the future write path will reuse the same
classifier.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable

from django.http import Http404

from code_scripts.inventory_review_missing_candidates import (
    _normalize_base_name,
    classify_missing_items_for_audit_file,
)

from ..business_date import get_target_trading_date
from ..models import CompanyConfigRecord, RunArtifact, RunJob
from .inventory_review import (
    InventoryReviewParseResult,
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
REVIEW_CREATE_MISSING_INTENT = "review_create_missing_items"

SNAPSHOT_PACK_GUARD_MESSAGE = (
    "QBO snapshot could not be loaded; pack-variant safety checks may be incomplete. "
    "Do not proceed until the snapshot is refreshed."
)


def resolve_txn_date_for_review_missing_item_creation(
    *, company_key: str, artifact: RunArtifact
) -> tuple[str, str]:
    """Return (YYYY-MM-DD, source_label) for InvStartDate and inventory_pipeline --txn-date."""

    def _is_iso(d: str) -> bool:
        return len(d) == 10 and d[4] == "-" and d[7] == "-"

    if artifact.target_date:
        return artifact.target_date.isoformat(), "artifact.target_date"
    job = getattr(artifact, "run_job", None)
    if job is not None and job.target_date:
        return job.target_date.isoformat(), "source_run.target_date"
    stats = artifact.upload_stats_json if isinstance(artifact.upload_stats_json, dict) else {}
    for key, label in (
        ("inv_txn_date", "summary.inv_txn_date"),
        ("txn_date", "summary.txn_date"),
    ):
        raw = str(stats.get(key) or "").strip()[:10]
        if _is_iso(raw):
            return raw, label
    qas = stats.get("quantity_adjustment_stats")
    if isinstance(qas, dict):
        raw = str(qas.get("txn_date") or "").strip()[:10]
        if _is_iso(raw):
            return raw, "summary.quantity_adjustment_stats.txn_date"
    try:
        from code_scripts.company_config import load_company_config

        cfg = load_company_config(company_key)
        floor = str(cfg.inv_start_date_floor or "").strip()[:10]
        if _is_iso(floor):
            return floor, "company_config.inv_start_date_floor"
    except Exception:
        pass
    return get_target_trading_date().isoformat(), "business_date.get_target_trading_date"


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
    free-text filter. Instead, we derive exact base names from the trusted
    final audit and emit them via ``base_names`` so the inventory pipeline can
    scope work without relying on ambiguous substring matching.
    """

    affected_base_names = _affected_base_names(rows)
    options: dict[str, Any] = {
        # Scoped execution inputs for the unified inventory pipeline runner.
        # These are derived from the trusted latest final-audit artifact (not user input).
        "base_names": affected_base_names,
        # Attribution/UX metadata (does not drive selection).
        "review_retry": {
            "intent": str(intent),
            "source_artifact_id": int(artifact.id) if artifact.id else None,
            "source_final_audit": str(artifact.source_path or ""),
            "affected_base_names": affected_base_names,
            "row_count": int(len(rows)),
        },
    }

    # Phase caps: ensure the queued retry doesn't accidentally run unrelated write phases.
    # Job runner supports 0 as "disable phase".
    if intent == RETRY_INTENT_CATALOG:
        options["max_catalog_fixes"] = int(len(rows))
        options["max_quantity_adjustments"] = 0
    elif intent == RETRY_INTENT_QUANTITY:
        options["max_catalog_fixes"] = 0
        options["max_quantity_adjustments"] = int(len(rows))

    return options


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
# Missing from QuickBooks: read-only preview + queued item creation
# ---------------------------------------------------------------------------


def build_missing_item_creation_preview(
    *,
    context: ReviewContext,
) -> MissingPreview:
    """Classify missing-from-QBO rows for the read-only preview page."""

    data = classify_missing_items_for_audit_file(context.company.company_key, context.final_audit_path)
    classified = [MissingPreviewRow(**row) for row in data["rows"]]
    mapping_error = str(data.get("mapping_error") or "").strip()
    parse_error = str(data.get("parse_error") or "").strip()
    combined_mapping_error = mapping_error or parse_error
    return MissingPreview(
        rows=classified,
        safe_count=int(data.get("safe_count") or 0),
        blocked_count=int(data.get("blocked_count") or 0),
        mapping_loaded=bool(data.get("mapping_loaded")),
        mapping_error=combined_mapping_error,
        qbo_base_names_loaded=bool(data.get("qbo_base_names_loaded")),
        qbo_base_names_error=str(data.get("qbo_base_names_error") or ""),
    )


def _build_missing_create_inventory_options(
    *,
    company_key: str,
    artifact: RunArtifact,
    final_audit_path: Path,
    preview: MissingPreview,
) -> dict[str, Any]:
    safe_rows = [r for r in preview.rows if r.is_safe]
    base_names = [str(r.suggested_qbo_name or "").strip() for r in safe_rows if str(r.suggested_qbo_name or "").strip()]
    txn_date, txn_source = resolve_txn_date_for_review_missing_item_creation(
        company_key=company_key, artifact=artifact
    )
    return {
        "base_names": base_names,
        "max_catalog_fixes": 0,
        "max_quantity_adjustments": 0,
        "txn_date": txn_date,
        "review_create_missing_items": {
            "intent": REVIEW_CREATE_MISSING_INTENT,
            "source_artifact_id": int(artifact.id) if artifact.id else None,
            "source_final_audit": str(final_audit_path),
            "affected_base_names": base_names,
            "row_count": int(len(preview.rows)),
            "safe_count": int(preview.safe_count),
            "blocked_count": int(preview.blocked_count),
            "create_qty_policy": "initial_qty_from_epos",
            "mapping_source": "Product.Mapping.csv",
            "item_inv_start_date": txn_date,
            "txn_date_source": txn_source,
        },
    }


def queue_missing_item_creation_job(
    *,
    company: CompanyConfigRecord,
    artifact: RunArtifact,
    final_audit_path: Path,
    preview: MissingPreview,
    requested_by,
) -> RunJob | None:
    """Queue inventory pipeline run that only creates safe missing Inventory items."""

    if preview.safe_count <= 0:
        return None
    inventory_options = _build_missing_create_inventory_options(
        company_key=company.company_key,
        artifact=artifact,
        final_audit_path=final_audit_path,
        preview=preview,
    )
    if not inventory_options.get("base_names"):
        return None
    return RunJob.objects.create(
        scope=RunJob.SCOPE_INVENTORY_PIPELINE,
        company_key=company.company_key,
        inventory_options_json=inventory_options,
        requested_by=requested_by,
        status=RunJob.STATUS_QUEUED,
    )
