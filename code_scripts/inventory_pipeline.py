"""
Unified Inventory pipeline.

This command is the operator-facing orchestration path for inventory. It keeps
the lower-level audit and catalog cleanup tools available, but coordinates the
safe sequence in one run:

1. EPOS stock snapshot
2. fresh QBO item snapshot
3. inventory audit
4. safe existing-base pack cleanup
5. exact-name quantity adjustments
6. final summary report and Slack summary
"""

from __future__ import annotations

import argparse
import csv
import json
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

import pandas as pd

from code_scripts.artifact_paths import (
    inventory_audit_reports_dir,
    inventory_catalog_cleanup_reports_dir,
    inventory_pipeline_reports_dir,
)
from code_scripts.company_config import (
    ensure_company_runtime_compatible,
    get_available_companies,
    load_company_config,
)
from code_scripts.inventory_catalog_cleanup import (
    _run_apply_for_existing_base_pack_variants,
    _write_csv as _write_catalog_csv,
    plan_catalog_cleanup,
)
from code_scripts.inventory_notifications import format_scope
from code_scripts.inventory_sync import (
    EPOS_NEGATIVE_STOCK_POLICY,
    TokenManager,
    _auto_download_stock_csv,
    _normalize_name_key,
    _time_stamp,
    _write_audit_metadata,
    _write_csv as _write_audit_csv,
    build_audit_report,
    build_inventory_adjustment_doc_number,
    build_inventory_adjustment_payload,
    choose_canonical_qbo_item_row,
    fetch_qbo_inventory_items_snapshot,
    load_epos_stock_snapshot,
    load_qbo_inventory_item_rows,
    load_qbo_inventory_snapshot,
    mark_qbo_snapshot_stale,
    post_inventory_adjustment,
)
from code_scripts.qbo_snapshot_cache import get_qbo_snapshot_path
from code_scripts.run_lock import GlobalRunLock
from code_scripts.slack_notify import notify_inventory_pipeline_start, send_slack_success
from code_scripts.token_manager import verify_realm_match


SAFE_CATALOG_ISSUE_TYPES = {
    "base_with_pack_variants",
    "only_pack_variant_exists",
    "multiple_active_base_items",
}
UNSUPPORTED_CATALOG_ISSUE_TYPES = {
    "only_pack_variant_exists",
    "missing_from_qbo",
    "multiple_active_base_items",
}


@dataclass(frozen=True)
class AuditResult:
    phase: str
    report: pd.DataFrame
    report_path: Path
    qbo_path: Path


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run the unified safe Inventory pipeline for one company."
    )
    parser.add_argument("--company", required=True, choices=get_available_companies())
    parser.add_argument("--stock-csv", default=None)
    parser.add_argument("--auto-download", action="store_true")
    parser.add_argument("--download-headful", action="store_true")
    parser.add_argument("--download-timeout-ms", type=int, default=None)
    parser.add_argument("--download-output-dir", default=None)
    parser.add_argument("--qbo-csv", default=None)
    parser.add_argument("--auto-fetch-qbo", action="store_true")
    parser.add_argument("--qbo-force-refresh", action="store_true")
    parser.add_argument("--qbo-cache-max-age-hours", type=int, default=24)
    parser.add_argument("--qbo-export-path", default=None)
    parser.add_argument("--category", dest="categories", action="append", default=[])
    parser.add_argument("--product", dest="product_filter", default=None)
    parser.add_argument("--max-catalog-fixes", type=int, default=None)
    parser.add_argument("--max-quantity-adjustments", type=int, default=None)
    parser.add_argument("--max-qty-delta", type=float, default=None)
    parser.add_argument("--adjust-account-id", default=None)
    parser.add_argument("--txn-date", default=None)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--no-slack", action="store_true")
    parser.add_argument(
        "--summary-output-dir",
        default=None,
        help="Override the inventory pipeline summary report directory.",
    )
    return parser


def _now_utc_iso() -> str:
    return datetime.now(tz=timezone.utc).isoformat()


def _optional_non_negative_int(value: int | None) -> int | None:
    if value is None:
        return None
    return max(0, int(value))


def _resolve_stock_path(args: argparse.Namespace, cfg) -> Path:
    if args.stock_csv:
        stock_path = Path(args.stock_csv).expanduser()
    elif args.auto_download:
        stock_path = _auto_download_stock_csv(
            cfg,
            output_dir=args.download_output_dir,
            download_timeout_ms=args.download_timeout_ms,
            headful=bool(args.download_headful),
        )
        print(f"[INFO] Downloaded stock CSV: {stock_path}")
    else:
        raise ValueError("Provide --stock-csv or --auto-download.")

    if not stock_path.exists():
        raise FileNotFoundError(f"stock csv not found: {stock_path}")
    return stock_path


def _resolve_qbo_snapshot(args: argparse.Namespace, cfg, *, force_refresh: bool) -> Path:
    if args.auto_fetch_qbo:
        output_path = (
            Path(args.qbo_export_path).expanduser()
            if args.qbo_export_path
            else Path(args.qbo_csv).expanduser()
            if args.qbo_csv
            else get_qbo_snapshot_path(cfg.company_key)
        )
        return fetch_qbo_inventory_items_snapshot(
            company_key=cfg.company_key,
            realm_id=cfg.realm_id,
            output_path=output_path,
            cache_max_age_hours=int(args.qbo_cache_max_age_hours),
            force_refresh=bool(force_refresh or args.qbo_force_refresh),
        )

    if args.qbo_csv:
        qbo_path = Path(args.qbo_csv).expanduser()
    else:
        qbo_path = get_qbo_snapshot_path(cfg.company_key)

    if not qbo_path.exists():
        raise FileNotFoundError(
            "QBO snapshot not found. Use --auto-fetch-qbo or pass --qbo-csv."
        )
    return qbo_path


def _audit_output_path(company_key: str, phase: str, *, now: datetime | None = None) -> Path:
    clock = now or datetime.now()
    return (
        inventory_audit_reports_dir(clock)
        / f"inventory_audit_{company_key}_{phase}_{_time_stamp(clock)}.csv"
    )


def _catalog_output_path(company_key: str, *, now: datetime | None = None) -> Path:
    clock = now or datetime.now()
    return (
        inventory_catalog_cleanup_reports_dir(clock)
        / f"inventory_catalog_cleanup_{company_key}_pipeline_{_time_stamp(clock)}.csv"
    )


def _run_audit_phase(
    *,
    cfg,
    stock_path: Path,
    qbo_path: Path,
    phase: str,
    categories: list[str],
    product_filter: str | None,
    quantity_apply_stats: dict[str, Any] | None = None,
) -> AuditResult:
    epos = load_epos_stock_snapshot(
        str(stock_path),
        product_filter=product_filter,
        categories=categories,
    )
    qbo = load_qbo_inventory_snapshot(str(qbo_path))
    report = build_audit_report(epos, qbo, tolerance=0.0)
    out_path = _audit_output_path(cfg.company_key, phase)
    _write_audit_csv(out_path, report)

    counts = report["status"].value_counts().to_dict() if "status" in report.columns else {}
    _write_audit_metadata(
        out_path,
        company_key=cfg.company_key,
        display_name=cfg.display_name,
        stock_csv=str(stock_path),
        qbo_csv=str(qbo_path),
        status_counts=counts,
        total_groups=len(report),
        apply_stats=quantity_apply_stats or {"mode": phase, "posted": 0, "skipped": 0},
    )
    print(f"[INFO] Wrote {phase} inventory audit: {out_path}")
    return AuditResult(phase=phase, report=report, report_path=out_path, qbo_path=qbo_path)


def _filter_catalog_plan_for_pipeline(plan_df: pd.DataFrame) -> pd.DataFrame:
    if plan_df.empty or "catalog_issue_type" not in plan_df.columns:
        return plan_df.copy()
    issue = plan_df["catalog_issue_type"].astype(str).str.strip()
    return plan_df[(issue != "") & (issue != "exact_name_match") & (issue != "nan")].copy()


def _catalog_counts(plan_df: pd.DataFrame) -> dict[str, int]:
    if plan_df.empty or "catalog_issue_type" not in plan_df.columns:
        return {}
    counts = plan_df["catalog_issue_type"].value_counts().to_dict()
    return {str(k): int(v) for k, v in counts.items()}


def _print_unsupported_catalog_rows(plan_df: pd.DataFrame) -> None:
    if plan_df.empty:
        return
    for _, row in plan_df.iterrows():
        print(
            f"[SKIP] base={str(row.get('base_name') or '')!r} "
            f"planned_action={str(row.get('planned_action') or '')} "
            f"reason={str(row.get('block_reason') or 'unsupported_catalog_cleanup_row')}"
        )


def _supported_catalog_rows(plan_df: pd.DataFrame) -> pd.DataFrame:
    if plan_df.empty:
        return plan_df.copy()
    supported_actions = {
        "consolidate_existing_base_pack_variants",
        "create_base_then_consolidate_pack_variant",
        "resolve_duplicate_base_items",
    }
    return plan_df[
        (plan_df["catalog_issue_type"].astype(str).isin(SAFE_CATALOG_ISSUE_TYPES))
        & (plan_df["planned_action"].astype(str).isin(supported_actions))
        & (plan_df["action_eligible"] == True)  # noqa: E712
    ].copy()


def _apply_catalog_cleanup(
    *,
    cfg,
    plan_df: pd.DataFrame,
    qbo_item_rows: pd.DataFrame,
    txn_date: str,
    max_catalog_fixes: int | None,
    dry_run: bool,
) -> dict[str, Any]:
    action_plan = _filter_catalog_plan_for_pipeline(plan_df)
    catalog_report_path = _catalog_output_path(cfg.company_key)
    _write_catalog_csv(catalog_report_path, action_plan)
    print(f"[INFO] Wrote catalog cleanup report: {catalog_report_path}")

    supported = _supported_catalog_rows(action_plan)
    unsupported = (
        action_plan.drop(index=supported.index).copy() if not action_plan.empty else action_plan.copy()
    )
    skipped_due_to_cap = 0
    if max_catalog_fixes is not None:
        skipped_due_to_cap = max(0, len(supported) - int(max_catalog_fixes))

    result = {
        "report_path": str(catalog_report_path),
        "supported_available": int(len(supported)),
        "unsupported_counts": _catalog_counts(unsupported),
        "skipped_due_to_cap": int(skipped_due_to_cap),
        "applied": 0,
        "base_items_created": 0,
        "duplicate_base_items_resolved": 0,
        "created_base_details": [],
        "changed_qbo": False,
        "exit_code": 0,
    }

    if supported.empty or (max_catalog_fixes is not None and max_catalog_fixes <= 0):
        if unsupported.empty:
            print("[INFO] No catalog cleanup rows require action.")
        else:
            _print_unsupported_catalog_rows(unsupported)
            print("[INFO] Catalog rows needing manual review were reported; none were applied.")
        return result

    apply_limit = len(supported) if max_catalog_fixes is None else int(max_catalog_fixes)

    apply_result = _run_apply_for_existing_base_pack_variants(
        cfg=cfg,
        plan_df=action_plan,
        qbo_item_rows=qbo_item_rows,
        txn_date=txn_date,
        max_products=int(apply_limit),
        dry_run=bool(dry_run),
        return_stats=True,
    )
    if isinstance(apply_result, dict):
        exit_code = int(apply_result.get("exit_code", 0))
        consolidated = int(apply_result.get("consolidated", 0))
        cleaned_up = int(apply_result.get("cleaned_up", 0))
        result["base_items_created"] = int(apply_result.get("base_items_created", 0))
        result["duplicate_base_items_resolved"] = int(apply_result.get("duplicate_base_items_resolved", 0))
        result["created_base_details"] = list(apply_result.get("created_base_details") or [])
    else:
        exit_code = int(apply_result)
        consolidated = min(int(apply_limit), len(supported))
        cleaned_up = consolidated
    result["exit_code"] = int(exit_code)
    if exit_code != 0:
        return result

    applied = min(consolidated, cleaned_up) if cleaned_up else consolidated
    result["applied"] = 0 if dry_run else int(applied)
    result["changed_qbo"] = bool(applied and not dry_run)
    return result


def _apply_exact_match_quantity_adjustments(
    *,
    cfg,
    audit_df: pd.DataFrame,
    qbo_item_rows: pd.DataFrame,
    max_quantity_adjustments: int | None,
    max_qty_delta: float | None,
    adjust_account_id: str | None,
    txn_date: str,
    dry_run: bool,
) -> dict[str, Any]:
    result = {
        "posted": 0,
        "planned": 0,
        "skipped": 0,
        "skipped_due_to_cap": 0,
        "skipped_non_exact": 0,
        "changed_qbo": False,
        "details": [],
    }

    if audit_df.empty:
        return result

    candidates = audit_df[
        (audit_df["status"].astype(str) == "needs_adjustment")
        & (audit_df["catalog_issue_type"].astype(str) == "exact_name_match")
    ].copy()
    if candidates.empty:
        print("[INFO] No exact-match quantity adjustments required.")
        return result

    if max_quantity_adjustments is not None:
        if max_quantity_adjustments <= 0:
            result["skipped_due_to_cap"] = int(len(candidates))
            return result
        result["skipped_due_to_cap"] = max(0, len(candidates) - int(max_quantity_adjustments))
        candidates = candidates.head(int(max_quantity_adjustments)).copy()

    account_id = (adjust_account_id or "").strip() or str(
        getattr(cfg, "inventory_adjustment_account_id", "") or ""
    ).strip()
    if not account_id:
        raise RuntimeError(
            "missing inventory adjustment account id for quantity adjustments."
        )

    effective_max_delta = max_qty_delta
    if effective_max_delta is None:
        effective_max_delta = getattr(cfg, "inventory_max_qty_delta", None)
    if effective_max_delta is not None and effective_max_delta <= 0:
        effective_max_delta = None

    token_mgr: Optional[TokenManager] = None
    run_lock: Optional[GlobalRunLock] = None
    if not dry_run:
        verify_realm_match(cfg.company_key, cfg.realm_id)
        token_mgr = TokenManager(cfg.company_key, cfg.realm_id)
        run_lock = GlobalRunLock(holder=f"inventory_pipeline:{cfg.company_key}")
        lock_result = run_lock.acquire()
        if not lock_result.acquired:
            raise RuntimeError(
                f"another pipeline run is active ({lock_result.reason}); refusing quantity adjustments."
            )

    try:
        for _, row in candidates.iterrows():
            base = str(row.get("base_name") or "").strip()
            base_norm = _normalize_name_key(base)
            if "base_name_norm" in qbo_item_rows.columns:
                group = qbo_item_rows[qbo_item_rows["base_name_norm"] == base_norm]
            else:
                group = qbo_item_rows[qbo_item_rows["base_name"].map(_normalize_name_key) == base_norm]
            chosen, reason = choose_canonical_qbo_item_row(group, base_name=base)
            if chosen is None or reason != "exact_name_match":
                print(f"[SKIP] {base!r}: quantity sync requires exact QBO name match.")
                result["skipped"] += 1
                result["skipped_non_exact"] += 1
                continue

            item_id = str(chosen.get("Id", "") or "").strip()
            current_qty = float(chosen.get("qbo_qty_on_hand", 0.0) or 0.0)
            epos_target = float(row.get("epos_single_units", 0.0) or 0.0)
            qty_diff = epos_target - current_qty
            if abs(qty_diff) <= 0:
                continue
            if effective_max_delta is not None and abs(qty_diff) > float(effective_max_delta):
                print(
                    f"[SKIP] {base!r}: quantity change {abs(qty_diff)} exceeds cap "
                    f"{effective_max_delta}."
                )
                result["skipped"] += 1
                continue

            doc_number = build_inventory_adjustment_doc_number(txn_date=txn_date, item_id=item_id)
            payload = build_inventory_adjustment_payload(
                adjust_account_id=account_id,
                txn_date=txn_date,
                doc_number=doc_number,
                private_note=(
                    "OIAT inventory pipeline | "
                    f"base={base!r} | epos_single_units={epos_target} | "
                    f"qbo_item_qty={current_qty} | delta={qty_diff}"
                )[:950],
                lines=[{"item_id": item_id, "qty_diff": qty_diff}],
            )

            if dry_run:
                print(f"[DRY-RUN] would post quantity adjustment for {base!r}: {payload}")
                result["planned"] += 1
                result["details"].append(
                    {
                        "base_name": base,
                        "item_id": item_id,
                        "epos_expected_qty": epos_target,
                        "qbo_start_qty": current_qty,
                        "delta": qty_diff,
                        "applied": False,
                        "dry_run": True,
                    }
                )
                continue

            assert token_mgr is not None
            post_inventory_adjustment(token_mgr, cfg.realm_id, payload)
            print(f"[OK] Posted quantity adjustment for {base!r} item_id={item_id}")
            result["posted"] += 1
            result["details"].append(
                {
                    "base_name": base,
                    "item_id": item_id,
                    "epos_expected_qty": epos_target,
                    "qbo_start_qty": current_qty,
                    "delta": qty_diff,
                    "applied": True,
                    "dry_run": False,
                }
            )
    finally:
        if run_lock is not None:
            run_lock.release()

    if result["posted"] > 0:
        mark_qbo_snapshot_stale(cfg.company_key, reason="inventory_pipeline_quantity_adjustments_posted")
        result["changed_qbo"] = True
    return result


def _write_summary_reports(summary: dict[str, Any], *, output_dir: str | None = None) -> tuple[Path, Path]:
    directory = Path(output_dir).expanduser() if output_dir else inventory_pipeline_reports_dir()
    directory.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%H%M%S")
    company = str(summary.get("company_key") or "company")
    json_path = directory / f"inventory_pipeline_{company}_{stamp}.json"
    csv_path = directory / f"inventory_pipeline_{company}_{stamp}.csv"
    payload = _stable_summary_payload(summary)
    payload["summary_json"] = str(json_path)
    payload["summary_csv"] = str(csv_path)
    payload = _stable_summary_payload(payload)

    with open(json_path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, sort_keys=True)
        handle.write("\n")

    final_audit = str((payload.get("child_reports") or {}).get("final_audit") or "")
    row = {
        "run_type": payload.get("run_type"),
        "company_key": payload.get("company_key"),
        "scope": payload.get("scope"),
        "products_checked": payload.get("products_checked"),
        "in_sync": payload.get("in_sync"),
        "catalog_fixes_applied": payload.get("catalog_fixes_applied"),
        "base_items_created": payload.get("base_items_created"),
        "duplicate_base_items_resolved": payload.get("duplicate_base_items_resolved"),
        "quantity_updates_applied": payload.get("quantity_updates_applied"),
        "blocked_items": payload.get("blocked_items"),
        "missing_base_item_in_qbo": payload.get("missing_base_item_in_qbo"),
        "duplicate_base_items_in_qbo": payload.get("duplicate_base_items_in_qbo"),
        "epos_negative_rows_clamped": payload.get("epos_negative_rows_clamped"),
        "epos_negative_units_clamped": payload.get("epos_negative_units_clamped"),
        "epos_negative_stock_policy": payload.get("epos_negative_stock_policy"),
        "still_needs_review": payload.get("still_needs_review"),
        "final_audit": final_audit,
        "stock_csv": payload.get("stock_csv"),
        "qbo_csv": payload.get("qbo_csv"),
        "run_job_id": payload.get("run_job_id"),
        "summary_json": str(json_path),
    }
    with open(csv_path, "w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(row.keys()))
        writer.writeheader()
        writer.writerow(row)

    return json_path, csv_path


def _final_counts(report: pd.DataFrame) -> dict[str, int]:
    if report.empty or "status" not in report.columns:
        return {}
    return {str(k): int(v) for k, v in report["status"].value_counts().to_dict().items()}


def _catalog_issue_counts(report: pd.DataFrame) -> dict[str, int]:
    if report.empty or "catalog_issue_type" not in report.columns:
        return {}
    return {str(k): int(v) for k, v in report["catalog_issue_type"].value_counts().to_dict().items()}


def _numeric(value: Any, default: float = 0.0) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return float(default)
    if pd.isna(parsed):
        return float(default)
    return parsed


def _epos_negative_clamp_totals(report: pd.DataFrame) -> dict[str, Any]:
    if report.empty:
        return {
            "epos_negative_rows_clamped": 0,
            "epos_negative_units_clamped": 0.0,
            "epos_negative_stock_policy": EPOS_NEGATIVE_STOCK_POLICY,
        }
    rows = 0
    units = 0.0
    if "epos_negative_rows_clamped" in report.columns:
        rows = int(pd.to_numeric(report["epos_negative_rows_clamped"], errors="coerce").fillna(0).sum())
    if "epos_negative_units_clamped" in report.columns:
        units = float(pd.to_numeric(report["epos_negative_units_clamped"], errors="coerce").fillna(0.0).sum())
    return {
        "epos_negative_rows_clamped": rows,
        "epos_negative_units_clamped": units,
        "epos_negative_stock_policy": EPOS_NEGATIVE_STOCK_POLICY,
    }


def _stable_count_map(values: dict[str, Any], keys: list[str]) -> dict[str, int]:
    out = {key: 0 for key in keys}
    for key, value in (values or {}).items():
        try:
            out[str(key)] = int(value)
        except (TypeError, ValueError):
            out[str(key)] = 0
    return out


def _build_run_detail_url(run_job_id: str | None = None) -> str:
    explicit = os.environ.get("OIAT_RUN_URL", "").strip()
    if explicit:
        return explicit
    job_id = (run_job_id or os.environ.get("OIAT_RUN_JOB_ID", "")).strip()
    base = os.environ.get("OIAT_PORTAL_BASE_URL", "").strip().rstrip("/")
    if not job_id or not base:
        return ""
    return f"{base}/epos-qbo/runs/{job_id}/"


def _completion_status(*, blocked_items: int, final_status_counts: dict[str, int], final_catalog_issue_counts: dict[str, int]) -> str:
    if int(blocked_items) > 0:
        return "completed_with_blocked_items"
    if any(int(v) > 0 for k, v in final_status_counts.items() if str(k) != "in_sync"):
        return "completed_with_blocked_items"
    blocking_issue_keys = {
        "missing_from_qbo",
        "only_pack_variant_exists",
        "multiple_active_base_items",
        "base_with_pack_variants",
    }
    if any(int(final_catalog_issue_counts.get(k, 0) or 0) > 0 for k in blocking_issue_keys):
        return "completed_with_blocked_items"
    return "clean"


def _completion_heading(status: str) -> str:
    if status == "failed":
        return "Inventory sync failed"
    if status == "completed_with_blocked_items":
        return "Inventory sync completed with blocked items"
    return "Inventory sync completed successfully"


def _write_final_audit_alias(
    *,
    cfg,
    source: AuditResult,
    stock_path: Path,
    qbo_path: Path,
    quantity_apply_stats: dict[str, Any],
) -> AuditResult:
    if source.phase == "final":
        return source
    out_path = _audit_output_path(cfg.company_key, "final")
    _write_audit_csv(out_path, source.report)
    counts = source.report["status"].value_counts().to_dict() if "status" in source.report.columns else {}
    _write_audit_metadata(
        out_path,
        company_key=cfg.company_key,
        display_name=cfg.display_name,
        stock_csv=str(stock_path),
        qbo_csv=str(qbo_path),
        status_counts=counts,
        total_groups=len(source.report),
        apply_stats=quantity_apply_stats,
    )
    print(f"[INFO] Wrote final inventory audit: {out_path}")
    return AuditResult(phase="final", report=source.report.copy(), report_path=out_path, qbo_path=qbo_path)


def _quantity_adjusted_bases(quantity_result: dict[str, Any]) -> set[str]:
    details = quantity_result.get("details") if isinstance(quantity_result, dict) else []
    if not isinstance(details, list):
        return set()
    return {str(d.get("base_name") or "").strip() for d in details if isinstance(d, dict) and d.get("applied")}


def _collect_product_details(
    *,
    final_report: pd.DataFrame,
    product_filter: str | None,
    catalog_plan: pd.DataFrame,
    catalog_result: dict[str, Any],
    quantity_result: dict[str, Any],
    threshold: int = 5,
) -> list[dict[str, Any]]:
    if not product_filter or final_report.empty or len(final_report) > int(threshold):
        return []
    created_bases = {
        str(d.get("base_name") or "").strip()
        for d in (catalog_result.get("created_base_details") or [])
        if isinstance(d, dict)
    }
    duplicate_plan_bases: set[str] = set()
    catalog_fix_bases: set[str] = set()
    if catalog_plan is not None and not catalog_plan.empty:
        for _, row in catalog_plan.iterrows():
            base = str(row.get("base_name") or "").strip()
            action = str(row.get("planned_action") or "").strip()
            if bool(row.get("action_eligible")):
                catalog_fix_bases.add(base)
            if action == "resolve_duplicate_base_items":
                duplicate_plan_bases.add(base)
    adjusted_bases = _quantity_adjusted_bases(quantity_result)

    details: list[dict[str, Any]] = []
    for _, row in final_report.iterrows():
        base = str(row.get("base_name") or "").strip()
        status = str(row.get("status") or "").strip()
        blocked_reason = ""
        if status != "in_sync":
            blocked_reason = str(row.get("catalog_issue_detail") or row.get("status") or "").strip()
        details.append(
            {
                "base_name": base,
                "epos_expected_qty": _numeric(row.get("epos_single_units", 0.0)),
                "qbo_final_qty": _numeric(row.get("qbo_qty_on_hand", 0.0)),
                "delta": _numeric(row.get("delta", 0.0)),
                "epos_negative_rows_clamped": int(_numeric(row.get("epos_negative_rows_clamped", 0), 0.0)),
                "epos_negative_units_clamped": _numeric(row.get("epos_negative_units_clamped", 0.0)),
                "epos_negative_stock_policy": str(
                    row.get("epos_negative_stock_policy") or EPOS_NEGATIVE_STOCK_POLICY
                ),
                "epos_negative_clamped_row_names": str(row.get("epos_negative_clamped_row_names") or ""),
                "catalog_fix_applied": base in catalog_fix_bases and int(catalog_result.get("applied", 0) or 0) > 0,
                "base_item_created": base in created_bases,
                "duplicate_base_resolved": base in duplicate_plan_bases and int(catalog_result.get("duplicate_base_items_resolved", 0) or 0) > 0,
                "quantity_adjustment_applied": base in adjusted_bases,
                "final_status": status,
                "blocked_reason": blocked_reason,
            }
        )
    return details


def _stable_summary_payload(summary: dict[str, Any]) -> dict[str, Any]:
    payload = dict(summary)
    child_reports = payload.get("child_reports") if isinstance(payload.get("child_reports"), dict) else {}
    payload["child_reports"] = {str(k): str(v) for k, v in child_reports.items()}
    final_status_counts = _stable_count_map(
        payload.get("final_status_counts") if isinstance(payload.get("final_status_counts"), dict) else {},
        ["in_sync", "needs_adjustment", "ambiguous_in_qbo", "missing_in_qbo"],
    )
    final_catalog_issue_counts = _stable_count_map(
        payload.get("final_catalog_issue_counts") if isinstance(payload.get("final_catalog_issue_counts"), dict) else {},
        [
            "exact_name_match",
            "base_with_pack_variants",
            "only_pack_variant_exists",
            "multiple_active_base_items",
            "missing_from_qbo",
        ],
    )
    unsupported = _stable_count_map(
        payload.get("unsupported_catalog_issues") if isinstance(payload.get("unsupported_catalog_issues"), dict) else {},
        [
            "base_with_pack_variants",
            "only_pack_variant_exists",
            "multiple_active_base_items",
            "missing_from_qbo",
        ],
    )
    payload["final_status_counts"] = final_status_counts
    payload["final_catalog_issue_counts"] = final_catalog_issue_counts
    payload["unsupported_catalog_issues"] = unsupported

    numeric_defaults = {
        "products_checked": 0,
        "already_correct": 0,
        "in_sync": int(payload.get("already_correct", payload.get("in_sync", 0)) or 0),
        "catalog_fixes_applied": 0,
        "base_items_created": 0,
        "duplicate_base_items_resolved": 0,
        "quantity_updates_applied": 0,
        "blocked_items": 0,
        "missing_base_item_in_qbo": 0,
        "duplicate_base_items_in_qbo": 0,
        "epos_negative_rows_clamped": 0,
        "still_needs_review": 0,
        "skipped_unsupported": 0,
        "skipped_safely": 0,
    }
    for key, default in numeric_defaults.items():
        try:
            payload[key] = int(payload.get(key, default) or 0)
        except (TypeError, ValueError):
            payload[key] = int(default)
    payload["already_correct"] = int(payload.get("already_correct", 0) or 0)
    payload["in_sync"] = int(payload.get("in_sync", payload["already_correct"]) or 0)
    payload["epos_negative_units_clamped"] = _numeric(payload.get("epos_negative_units_clamped", 0.0))
    payload["epos_negative_stock_policy"] = str(
        payload.get("epos_negative_stock_policy") or EPOS_NEGATIVE_STOCK_POLICY
    )
    text_defaults = {
        "run_type": "inventory_pipeline",
        "company_key": "",
        "display_name": "",
        "scope": "",
        "started_at": "",
        "finished_at": "",
        "run_job_id": "",
        "stock_csv": "",
        "qbo_csv": "",
        "summary_json": "",
        "summary_csv": "",
    }
    for key, default in text_defaults.items():
        payload[key] = str(payload.get(key) or "")
        if not payload[key]:
            payload[key] = default
    payload["dry_run"] = bool(payload.get("dry_run", False))
    payload["blocked_catalog_examples"] = list(payload.get("blocked_catalog_examples") or [])
    payload["created_base_details"] = list(payload.get("created_base_details") or [])
    payload["duplicate_resolution_details"] = list(payload.get("duplicate_resolution_details") or [])
    payload["product_details"] = list(payload.get("product_details") or [])
    if not isinstance(payload.get("quantity_adjustment_stats"), dict):
        payload["quantity_adjustment_stats"] = {}
    payload["completion_status"] = str(payload.get("completion_status") or "clean")
    payload["run_url"] = str(payload.get("run_url") or "")
    return payload


def _format_final_summary(summary: dict[str, Any]) -> str:
    summary = _stable_summary_payload(summary)
    lines = [
        f"{_completion_heading(summary.get('completion_status'))} for {summary['display_name']} ({summary['company_key']})",
        f"Scope: {summary['scope'] or 'all products'}",
        f"Products checked: {summary['products_checked']}",
        f"In sync / Products clean: {summary['in_sync']}",
        f"Catalog fixes applied: {summary['catalog_fixes_applied']}",
        f"Base items created: {int(summary.get('base_items_created', 0) or 0)}",
        f"Duplicate base items resolved: {int(summary.get('duplicate_base_items_resolved', 0) or 0)}",
        f"Quantity updates applied: {summary['quantity_updates_applied']}",
        f"Blocked items: {summary['blocked_items']}",
        f"Missing base item in QBO: {summary['missing_base_item_in_qbo']}",
        f"Duplicate base items in QBO: {summary['duplicate_base_items_in_qbo']}",
        f"Still needs review: {summary['still_needs_review']}",
    ]
    if int(summary.get("epos_negative_rows_clamped", 0) or 0) > 0:
        lines.append(
            "EPOS negative rows clamped to zero: "
            f"{summary['epos_negative_rows_clamped']} "
            f"({summary['epos_negative_units_clamped']} units)"
        )
    product_details = [d for d in (summary.get("product_details") or []) if isinstance(d, dict)]
    if product_details:
        lines.append("Product details:")
        for detail in product_details:
            line = (
                f"- {detail.get('base_name')}: EPOS={detail.get('epos_expected_qty')} "
                f"QBO={detail.get('qbo_final_qty')} delta={detail.get('delta')} "
                f"status={detail.get('final_status')}"
            )
            reason = str(detail.get("blocked_reason") or "").strip()
            if reason:
                line += f" reason={reason}"
            clamped_rows = int(_numeric(detail.get("epos_negative_rows_clamped", 0), 0.0))
            if clamped_rows > 0:
                line += (
                    f" negative_rows_clamped={clamped_rows}"
                    f" negative_units_clamped={_numeric(detail.get('epos_negative_units_clamped', 0.0))}"
                )
            lines.append(line)
    blocked_examples = [str(x) for x in (summary.get("blocked_catalog_examples") or []) if str(x).strip()]
    if blocked_examples and int(summary.get("blocked_items", 0) or 0) <= 10:
        lines.append("Blocked examples:")
        for example in blocked_examples:
            lines.append(f"- {example}")
    if summary.get("max_catalog_fixes") is not None:
        lines.append(f"Catalog fixes limit: {summary['max_catalog_fixes']}")
    if summary.get("max_quantity_adjustments") is not None:
        lines.append(f"Quantity updates limit: {summary['max_quantity_adjustments']}")
    if summary.get("run_url"):
        lines.append(f"Run: {summary['run_url']}")
    lines.append(f"Report path: {summary['summary_json']}")
    return "\n".join(lines)


def _format_operator_number(value: Any) -> str:
    number = _numeric(value, 0.0)
    if abs(number - int(number)) <= 0.0000001:
        return str(int(number))
    return f"{number:.2f}".rstrip("0").rstrip(".")


def _format_operator_int_commas(value: Any) -> str:
    number = _numeric(value, 0.0)
    if abs(number - int(number)) <= 0.0000001:
        return f"{int(number):,}"
    return f"{number:,.2f}".rstrip("0").rstrip(".")


def _scope_part(scope: str, key: str) -> str:
    prefix = f"{key}="
    for part in str(scope or "").split(";"):
        chunk = part.strip()
        if chunk.startswith(prefix):
            return chunk.removeprefix(prefix).strip()
    return ""


def _first_product_detail(summary: dict[str, Any]) -> dict[str, Any]:
    details = [d for d in (summary.get("product_details") or []) if isinstance(d, dict)]
    return details[0] if details else {}


def _is_product_scope(summary: dict[str, Any]) -> bool:
    if _first_product_detail(summary):
        return True
    return bool(_scope_part(str(summary.get("scope") or ""), "product"))


def _slack_scope_title(summary: dict[str, Any]) -> str:
    detail = _first_product_detail(summary)
    if detail.get("base_name"):
        return str(detail.get("base_name"))
    scope = str(summary.get("scope") or "")
    product = _scope_part(scope, "product")
    if product:
        return product
    category = _scope_part(scope, "category")
    if category:
        return category
    return "all products"


def _compact_catalog_fix_count(summary: dict[str, Any]) -> int:
    applied = int(summary.get("catalog_fixes_applied", 0) or 0)
    subcounts = (
        int(summary.get("base_items_created", 0) or 0)
        + int(summary.get("duplicate_base_items_resolved", 0) or 0)
    )
    return max(applied, subcounts)


def _negative_epos_note(summary: dict[str, Any]) -> str:
    rows = int(summary.get("epos_negative_rows_clamped", 0) or 0)
    if rows <= 0:
        return ""
    units = _format_operator_int_commas(summary.get("epos_negative_units_clamped", 0.0))
    return (
        f"Negative EPOS stock: {rows} row(s) treated as 0 for sync safety ({units} units)."
    )


def _short_report_path(raw: str) -> str:
    text = str(raw or "").strip()
    if not text:
        return ""
    p = Path(text)
    parts = list(p.parts)
    if "reports" in parts:
        idx = parts.index("reports")
        tail = "/".join(parts[idx:])
        return tail or p.name
    if len(parts) >= 2:
        return "/".join(parts[-2:])
    return p.name


def _append_report_paths(lines: list[str], summary: dict[str, Any]) -> None:
    bullets: list[str] = []
    summary_csv = _short_report_path(str(summary.get("summary_csv") or ""))
    if summary_csv:
        bullets.append(f"• Summary CSV: `{summary_csv}`")
    child_reports = summary.get("child_reports") if isinstance(summary.get("child_reports"), dict) else {}
    final_audit = _short_report_path(str(child_reports.get("final_audit") or ""))
    if final_audit:
        bullets.append(f"• Final audit CSV: `{final_audit}`")
    initial_audit = _short_report_path(str(child_reports.get("initial_audit") or ""))
    if initial_audit:
        bullets.append(f"• Initial audit CSV: `{initial_audit}`")
    catalog_cleanup = _short_report_path(str(child_reports.get("catalog_cleanup") or ""))
    if catalog_cleanup:
        bullets.append(f"• Catalog cleanup CSV: `{catalog_cleanup}`")
    if not bullets:
        return
    lines.append("")
    lines.append("Reports:")
    lines.extend(bullets)


def _blocked_review_examples(summary: dict[str, Any], *, max_examples: int = 5) -> list[str]:
    out: list[str] = []
    details = [d for d in (summary.get("product_details") or []) if isinstance(d, dict)]
    for d in details:
        status = str(d.get("final_status") or "")
        if status == "in_sync":
            continue
        base = str(d.get("base_name") or "").strip()
        if not base:
            continue
        reason = _short_block_reason(str(d.get("blocked_reason") or status))
        out.append(f"{base} — {reason}")
        if len(out) >= max_examples:
            return out
    examples = [str(x).strip() for x in (summary.get("blocked_catalog_examples") or []) if str(x).strip()]
    for line in examples:
        if len(out) >= max_examples:
            break
        if "—" in line:
            base, reason = [p.strip() for p in line.split("—", 1)]
            if base:
                out.append(f"{base} — {_short_block_reason(reason)}")
        else:
            out.append(f"{_slack_scope_title(summary)} — {_short_block_reason(line)}")
    return out


def _short_block_reason(raw: str) -> str:
    text = str(raw or "").strip()
    lowered = text.lower()
    normalized = lowered.replace("_", " ")
    if "duplicate" in normalized or "multiple active" in normalized:
        return "duplicate base items in QBO"
    if "only pack variant" in normalized:
        return "only pack variant exists in QBO"
    if "missing" in normalized or "not found" in normalized:
        return "product not found in QuickBooks"
    if "needs adjustment" in normalized:
        return "quantity mismatch needs review"
    if "ambiguous" in normalized:
        return "ambiguous QBO match"
    return text or "see final audit"


def _short_failure_reason(raw: str) -> str:
    text = " ".join(str(raw or "").strip().split())
    if not text:
        return "see run log"
    lowered = text.lower()
    if "qbo query failed" in lowered or "queryvalidationerror" in lowered:
        return "QBO query failed"
    if len(text) > 120:
        return text[:117].rstrip() + "..."
    return text


def _first_blocked_item(summary: dict[str, Any]) -> tuple[str, str]:
    details = [d for d in (summary.get("product_details") or []) if isinstance(d, dict)]
    for detail in details:
        if str(detail.get("final_status") or "") != "in_sync":
            return (
                str(detail.get("base_name") or _slack_scope_title(summary)),
                _short_block_reason(str(detail.get("blocked_reason") or detail.get("final_status") or "")),
            )
    examples = [str(x).strip() for x in (summary.get("blocked_catalog_examples") or []) if str(x).strip()]
    if examples:
        if "—" in examples[0]:
            base, reason = [p.strip() for p in examples[0].split("—", 1)]
            return base, _short_block_reason(reason)
        return _slack_scope_title(summary), _short_block_reason(examples[0])
    return _slack_scope_title(summary), "see final audit"


def _append_run_or_report(lines: list[str], summary: dict[str, Any]) -> None:
    run_url = str(summary.get("run_url") or "").strip()
    if run_url:
        run_id = _operator_run_id(
            scope="inventory_pipeline",
            run_job_id=str(summary.get("run_job_id") or "").strip(),
            started_at=str(summary.get("started_at") or os.environ.get("OIAT_RUN_STARTED_AT", "")).strip(),
        )
        label = f"Inventory Run {run_id}" if run_id else "Inventory Run"
        lines.append(f"Run: <{run_url}|{label}>")
        return
    report = Path(str(summary.get("summary_json") or "")).name
    if report:
        lines.append(f"Report: {report}")


def _operator_run_id(*, scope: str, run_job_id: str, started_at: str) -> str:
    prefix = "INV" if scope == "inventory_pipeline" else "RUN"
    if scope in {"single_company", "all_companies"}:
        prefix = "SAL"
    if scope == "inventory_sync":
        prefix = "AUD"

    suffix = (run_job_id.split("-", 1)[0][:4] if run_job_id else "").upper()
    if not suffix:
        return ""

    # started_at is expected to be ISO 8601; be forgiving.
    mmdd = hhmm = ""
    raw = (started_at or "").strip()
    if raw:
        try:
            # datetime.fromisoformat handles both "Z" missing and offset-aware strings in py3.11+
            dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
            mmdd = dt.strftime("%m%d")
            hhmm = dt.strftime("%H%M")
        except Exception:
            mmdd = hhmm = ""
    if not (mmdd and hhmm):
        return f"{prefix}-????-????-{suffix}"
    return f"{prefix}-{mmdd}-{hhmm}-{suffix}"


def _format_slack_summary(summary: dict[str, Any]) -> str:
    summary = _stable_summary_payload(summary)
    title = _slack_scope_title(summary)
    blocked = int(summary.get("blocked_items", 0) or 0) > 0 or int(summary.get("still_needs_review", 0) or 0) > 0
    failed = str(summary.get("completion_status") or "") == "failed"
    is_product = _is_product_scope(summary)

    if failed:
        lines = [f"❌ Inventory sync failed — {title}", ""]
        lines.append(
            f"Reason: {_short_failure_reason(str(summary.get('error') or summary.get('failure_reason') or 'see run log'))}"
        )
        _append_run_or_report(lines, summary)
        return "\n".join(lines)

    if blocked:
        lines = [f"⚠️ *Inventory Sync needs review* — {summary.get('display_name', '').strip() or title}", ""]
        scope_label = _slack_scope_title(summary)
        if scope_label:
            lines.append(f"• Scope: {scope_label}")
        detail = _first_product_detail(summary)
        if is_product and detail:
            lines.append(f"• EPOS: {_format_operator_number(detail.get('epos_expected_qty', 0))}")
            lines.append(f"• QBO: {_format_operator_number(detail.get('qbo_final_qty', 0))}")
            lines.append(f"• Difference: {_format_operator_number(detail.get('delta', 0))}")
            lines.append("")
        else:
            lines.append(f"• In sync: {_format_operator_int_commas(summary['in_sync'])} / {_format_operator_int_commas(summary['products_checked'])}")
        lines.append(f"• Blocked: {_format_operator_int_commas(summary['blocked_items'])}")
        note = _negative_epos_note(summary)
        if note:
            lines.extend(["", f"• {note}"])
        examples = _blocked_review_examples(summary, max_examples=5)
        if examples and not is_product:
            lines.append("")
            lines.append("Needs review example:" if len(examples) == 1 else "Needs review examples:")
            for ex in examples[:5]:
                lines.append(f"• {ex}")
            if int(summary.get("blocked_items", 0) or 0) > len(examples):
                lines.append("• See report for full list.")
        else:
            item, reason = _first_blocked_item(summary)
            if reason:
                lines.extend(["", f"Reason: {reason}"])
        _append_report_paths(lines, summary)
        lines.append("")
        _append_run_or_report(lines, summary)
        return "\n".join(lines)

    lines = [f"✅ Inventory synced — {title}", ""]
    if is_product:
        detail = _first_product_detail(summary)
        lines.append(f"• EPOS: {_format_operator_number(detail.get('epos_expected_qty', 0))}")
        lines.append(f"• QBO: {_format_operator_number(detail.get('qbo_final_qty', 0))}")
        lines.append(f"• Difference: {_format_operator_number(detail.get('delta', 0))}")
        lines.append("")
        lines.append(f"• Updated in QBO: {'Yes' if int(summary.get('quantity_updates_applied', 0) or 0) > 0 else 'No'}")
        lines.append(f"• Catalog fixes: {_format_operator_int_commas(_compact_catalog_fix_count(summary))}")
        lines.append(f"• Blocked: {_format_operator_int_commas(summary['blocked_items'])}")
    else:
        lines.append(f"• Scope: {_slack_scope_title(summary)}")
        lines.append(f"• In sync: {_format_operator_int_commas(summary['in_sync'])} / {_format_operator_int_commas(summary['products_checked'])}")
        lines.append(f"• Catalog fixes: {_format_operator_int_commas(_compact_catalog_fix_count(summary))}")
        lines.append(f"• Quantity updates: {_format_operator_int_commas(summary['quantity_updates_applied'])}")
        lines.append(f"• Blocked: {_format_operator_int_commas(summary['blocked_items'])}")

    note = _negative_epos_note(summary)
    if note:
        lines.extend(["", f"• {note}"])
    _append_report_paths(lines, summary)
    lines.append("")
    _append_run_or_report(lines, summary)
    return "\n".join(lines)


def _format_unsupported_breakdown(unsupported_counts: dict[str, Any]) -> str:
    labels = {
        "base_with_pack_variants": "Blocked pack-variant action",
        "only_pack_variant_exists": "Missing base item in QBO",
        "missing_from_qbo": "Missing base item in QBO",
        "multiple_active_base_items": "Duplicate base items in QBO",
    }
    parts: list[str] = []
    for key, raw_value in unsupported_counts.items():
        try:
            value = int(raw_value)
        except (TypeError, ValueError):
            continue
        if value <= 0:
            continue
        parts.append(f"{labels.get(str(key), str(key))}: {value}")
    return "; ".join(parts)


def _blocked_example_reason(row: pd.Series) -> str:
    issue = str(row.get("catalog_issue_type") or "").strip()
    if issue == "only_pack_variant_exists":
        pack = str(row.get("qbo_pack_variant_names_for_base") or "").strip()
        return f"QBO only has pack variant {pack}" if pack else "QBO only has pack variants"
    if issue == "multiple_active_base_items":
        return "multiple active base items in QBO"
    return str(row.get("catalog_issue_detail") or "").strip() or "requires manual review"


def _collect_blocked_catalog_examples(report: pd.DataFrame, *, max_examples: int = 10) -> list[str]:
    if report.empty or "catalog_issue_type" not in report.columns:
        return []
    blocked_types = {"only_pack_variant_exists", "multiple_active_base_items", "missing_from_qbo"}
    blocked = report[report["catalog_issue_type"].astype(str).isin(blocked_types)]
    examples: list[str] = []
    for _, row in blocked.iterrows():
        if len(examples) >= max_examples:
            break
        base_name = str(row.get("base_name") or "").strip()
        if not base_name:
            continue
        reason = _blocked_example_reason(row)
        examples.append(f"{base_name} — {reason}")
    return examples


def run_inventory_pipeline(args: argparse.Namespace) -> dict[str, Any]:
    started_at = _now_utc_iso()
    cfg = load_company_config(args.company)
    ensure_company_runtime_compatible(cfg)

    max_catalog_fixes = _optional_non_negative_int(args.max_catalog_fixes)
    max_quantity_adjustments = _optional_non_negative_int(args.max_quantity_adjustments)
    categories = [str(c).strip() for c in list(args.categories or []) if str(c).strip()]
    product_filter = (args.product_filter or "").strip() or None
    txn_date = (args.txn_date or datetime.now().strftime("%Y-%m-%d")).strip()
    child_reports: dict[str, str] = {}

    print("=" * 68)
    print(f"Inventory pipeline: {cfg.display_name} ({cfg.company_key})")
    scope_text = format_scope(category=categories, product=product_filter)
    if scope_text:
        print(f"Scope: {scope_text}")
    print(
        "Catalog fixes limit: "
        + ("unlimited" if max_catalog_fixes is None else str(max_catalog_fixes))
    )
    print(
        "Quantity updates limit: "
        + ("unlimited" if max_quantity_adjustments is None else str(max_quantity_adjustments))
    )
    print("=" * 68)

    webhook = getattr(cfg, "slack_webhook_url", None)
    if webhook and not args.no_slack:
        notify_inventory_pipeline_start(
            company_name=cfg.display_name,
            company_key=cfg.company_key,
            categories=categories,
            product_filter=product_filter,
            dry_run=bool(args.dry_run),
            webhook_url=webhook,
            metadata={
                "scope": "inventory_pipeline",
                "run_job_id": os.environ.get("OIAT_RUN_JOB_ID", "").strip(),
                "run_url": _build_run_detail_url(os.environ.get("OIAT_RUN_JOB_ID", "").strip()),
            },
        )

    stock_path = _resolve_stock_path(args, cfg)
    qbo_path = _resolve_qbo_snapshot(args, cfg, force_refresh=True)
    qbo_item_rows = load_qbo_inventory_item_rows(str(qbo_path))

    initial = _run_audit_phase(
        cfg=cfg,
        stock_path=stock_path,
        qbo_path=qbo_path,
        phase="initial",
        categories=categories,
        product_filter=product_filter,
    )
    child_reports["initial_audit"] = str(initial.report_path)

    catalog_plan = plan_catalog_cleanup(
        company_key=cfg.company_key,
        audit_df=initial.report,
        qbo_item_rows=qbo_item_rows,
        source_inventory_report=str(initial.report_path),
    )
    catalog_result = _apply_catalog_cleanup(
        cfg=cfg,
        plan_df=catalog_plan,
        qbo_item_rows=qbo_item_rows,
        txn_date=txn_date,
        max_catalog_fixes=max_catalog_fixes,
        dry_run=bool(args.dry_run),
    )
    child_reports["catalog_cleanup"] = catalog_result["report_path"]
    if catalog_result["exit_code"] != 0:
        raise RuntimeError(
            f"catalog cleanup failed with exit code {catalog_result['exit_code']}"
        )

    if catalog_result["changed_qbo"]:
        qbo_path = _resolve_qbo_snapshot(args, cfg, force_refresh=True)
        qbo_item_rows = load_qbo_inventory_item_rows(str(qbo_path))

    post_catalog = _run_audit_phase(
        cfg=cfg,
        stock_path=stock_path,
        qbo_path=qbo_path,
        phase="post_catalog",
        categories=categories,
        product_filter=product_filter,
    )
    child_reports["post_catalog_audit"] = str(post_catalog.report_path)

    quantity_result = _apply_exact_match_quantity_adjustments(
        cfg=cfg,
        audit_df=post_catalog.report,
        qbo_item_rows=qbo_item_rows,
        max_quantity_adjustments=max_quantity_adjustments,
        max_qty_delta=args.max_qty_delta,
        adjust_account_id=args.adjust_account_id,
        txn_date=txn_date,
        dry_run=bool(args.dry_run),
    )

    final = post_catalog
    if quantity_result["changed_qbo"]:
        qbo_path = _resolve_qbo_snapshot(args, cfg, force_refresh=True)
        final = _run_audit_phase(
            cfg=cfg,
            stock_path=stock_path,
            qbo_path=qbo_path,
            phase="final",
            categories=categories,
            product_filter=product_filter,
            quantity_apply_stats={
                "mode": "apply",
                "posted": int(quantity_result["posted"]),
                "skipped": int(quantity_result["skipped"]),
                "txn_date": txn_date,
            },
        )
    final = _write_final_audit_alias(
        cfg=cfg,
        source=final,
        stock_path=stock_path,
        qbo_path=qbo_path,
        quantity_apply_stats={
            "mode": "final",
            "posted": int(quantity_result.get("posted", 0) or 0),
            "skipped": int(quantity_result.get("skipped", 0) or 0),
            "txn_date": txn_date,
        },
    )
    child_reports["final_audit"] = str(final.report_path)

    counts = _final_counts(final.report)
    catalog_issue_counts = _catalog_issue_counts(final.report)
    stable_status_counts = _stable_count_map(counts, ["in_sync", "needs_adjustment", "ambiguous_in_qbo", "missing_in_qbo"])
    stable_catalog_issue_counts = _stable_count_map(
        catalog_issue_counts,
        [
            "exact_name_match",
            "base_with_pack_variants",
            "only_pack_variant_exists",
            "multiple_active_base_items",
            "missing_from_qbo",
        ],
    )
    stable_unsupported_counts = _stable_count_map(
        catalog_result["unsupported_counts"],
        [
            "base_with_pack_variants",
            "only_pack_variant_exists",
            "multiple_active_base_items",
            "missing_from_qbo",
        ],
    )
    epos_negative_clamp_totals = _epos_negative_clamp_totals(final.report)
    unsupported_total = sum(int(v) for v in stable_unsupported_counts.values())
    skipped_safely = (
        int(unsupported_total)
        + int(catalog_result["skipped_due_to_cap"])
        + int(quantity_result["skipped"])
        + int(quantity_result["skipped_due_to_cap"])
    )
    products_checked = int(len(final.report))
    in_sync = int(stable_status_counts.get("in_sync", 0) or 0)
    blocked_items = max(0, products_checked - in_sync)
    still_needs_review = (
        int(stable_status_counts.get("ambiguous_in_qbo", 0) or 0)
        + int(stable_status_counts.get("missing_in_qbo", 0) or 0)
        + int(stable_status_counts.get("needs_adjustment", 0) or 0)
    )
    missing_base_item_in_qbo = (
        int(stable_catalog_issue_counts.get("missing_from_qbo", 0) or 0)
        + int(stable_catalog_issue_counts.get("only_pack_variant_exists", 0) or 0)
    )
    duplicate_base_items_in_qbo = int(stable_catalog_issue_counts.get("multiple_active_base_items", 0) or 0)
    blocked_examples = _collect_blocked_catalog_examples(final.report, max_examples=10)
    completion_status = _completion_status(
        blocked_items=blocked_items,
        final_status_counts=stable_status_counts,
        final_catalog_issue_counts=stable_catalog_issue_counts,
    )
    product_details = _collect_product_details(
        final_report=final.report,
        product_filter=product_filter,
        catalog_plan=catalog_plan,
        catalog_result=catalog_result,
        quantity_result=quantity_result,
    )
    run_job_id = os.environ.get("OIAT_RUN_JOB_ID", "").strip()
    run_url = _build_run_detail_url(run_job_id)

    summary: dict[str, Any] = {
        "run_type": "inventory_pipeline",
        "started_at": started_at,
        "company_key": cfg.company_key,
        "display_name": cfg.display_name,
        "scope": scope_text,
        "dry_run": bool(args.dry_run),
        "stock_csv": str(stock_path),
        "qbo_csv": str(qbo_path),
        "max_catalog_fixes": max_catalog_fixes,
        "max_quantity_adjustments": max_quantity_adjustments,
        "products_checked": products_checked,
        "already_correct": in_sync,
        "in_sync": in_sync,
        "catalog_fixes_applied": int(catalog_result["applied"]),
        "base_items_created": int(catalog_result.get("base_items_created", 0) or 0),
        "duplicate_base_items_resolved": int(catalog_result.get("duplicate_base_items_resolved", 0) or 0),
        "quantity_updates_applied": int(quantity_result["posted"]),
        "blocked_items": int(blocked_items),
        "missing_base_item_in_qbo": int(missing_base_item_in_qbo),
        "duplicate_base_items_in_qbo": int(duplicate_base_items_in_qbo),
        "epos_negative_rows_clamped": int(epos_negative_clamp_totals["epos_negative_rows_clamped"]),
        "epos_negative_units_clamped": float(epos_negative_clamp_totals["epos_negative_units_clamped"]),
        "epos_negative_stock_policy": str(epos_negative_clamp_totals["epos_negative_stock_policy"]),
        "skipped_unsupported": int(unsupported_total),
        "skipped_safely": int(skipped_safely),
        "still_needs_review": int(still_needs_review),
        "final_status_counts": stable_status_counts,
        "final_catalog_issue_counts": stable_catalog_issue_counts,
        "unsupported_catalog_issues": stable_unsupported_counts,
        "blocked_catalog_examples": blocked_examples,
        "created_base_details": list(catalog_result.get("created_base_details") or []),
        "duplicate_resolution_details": list(catalog_result.get("duplicate_resolution_details") or []),
        "quantity_adjustment_stats": quantity_result,
        "child_reports": child_reports,
        "completion_status": completion_status,
        "product_details": product_details,
        "run_job_id": run_job_id,
        "run_url": run_url,
        "finished_at": _now_utc_iso(),
    }
    summary = _stable_summary_payload(summary)
    summary_json, summary_csv = _write_summary_reports(
        summary,
        output_dir=args.summary_output_dir,
    )
    summary["summary_json"] = str(summary_json)
    summary["summary_csv"] = str(summary_csv)
    summary = _stable_summary_payload(summary)

    print("=" * 68)
    print(_format_final_summary(summary))
    print("=" * 68)

    if webhook and not args.no_slack:
        send_slack_success(_format_slack_summary(summary), webhook)

    return summary


def main(argv: Optional[list[str]] = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        run_inventory_pipeline(args)
    except Exception as exc:  # noqa: BLE001
        print(f"Error: inventory pipeline failed: {exc}")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
