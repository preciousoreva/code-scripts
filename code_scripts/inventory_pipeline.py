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
    TokenManager,
    _auto_download_stock_csv,
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
from code_scripts.slack_notify import send_slack_success
from code_scripts.token_manager import verify_realm_match


SAFE_CATALOG_ISSUE_TYPES = {"base_with_pack_variants"}
UNSUPPORTED_CATALOG_ISSUE_TYPES = {
    "only_pack_variant_exists",
    "missing_from_qbo",
    "multiple_active_base_items",
}
PIPELINE_CATALOG_ISSUE_TYPES = SAFE_CATALOG_ISSUE_TYPES | UNSUPPORTED_CATALOG_ISSUE_TYPES


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
    parser.add_argument("--max-catalog-fixes", type=int, default=5)
    parser.add_argument("--max-quantity-adjustments", type=int, default=10)
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


def _non_negative_int(value: int | None, *, default: int) -> int:
    if value is None:
        return default
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
    return plan_df[
        plan_df["catalog_issue_type"].astype(str).isin(PIPELINE_CATALOG_ISSUE_TYPES)
    ].copy()


def _catalog_counts(plan_df: pd.DataFrame) -> dict[str, int]:
    if plan_df.empty or "catalog_issue_type" not in plan_df.columns:
        return {}
    counts = plan_df["catalog_issue_type"].value_counts().to_dict()
    return {str(k): int(v) for k, v in counts.items()}


def _supported_catalog_rows(plan_df: pd.DataFrame) -> pd.DataFrame:
    if plan_df.empty:
        return plan_df.copy()
    return plan_df[
        (plan_df["catalog_issue_type"].astype(str).isin(SAFE_CATALOG_ISSUE_TYPES))
        & (plan_df["planned_action"].astype(str) == "consolidate_existing_base_pack_variants")
        & (plan_df["action_eligible"] == True)  # noqa: E712
    ].copy()


def _apply_catalog_cleanup(
    *,
    cfg,
    plan_df: pd.DataFrame,
    qbo_item_rows: pd.DataFrame,
    txn_date: str,
    max_catalog_fixes: int,
    dry_run: bool,
) -> dict[str, Any]:
    action_plan = _filter_catalog_plan_for_pipeline(plan_df)
    catalog_report_path = _catalog_output_path(cfg.company_key)
    _write_catalog_csv(catalog_report_path, action_plan)
    print(f"[INFO] Wrote catalog cleanup report: {catalog_report_path}")

    supported = _supported_catalog_rows(action_plan)
    unsupported = action_plan[
        action_plan["catalog_issue_type"].astype(str).isin(UNSUPPORTED_CATALOG_ISSUE_TYPES)
    ].copy() if not action_plan.empty else action_plan.copy()
    skipped_due_to_cap = max(0, len(supported) - int(max_catalog_fixes))

    result = {
        "report_path": str(catalog_report_path),
        "supported_available": int(len(supported)),
        "unsupported_counts": _catalog_counts(unsupported),
        "skipped_due_to_cap": int(skipped_due_to_cap),
        "applied": 0,
        "changed_qbo": False,
        "exit_code": 0,
    }

    if supported.empty or max_catalog_fixes <= 0:
        if unsupported.empty:
            print("[INFO] No catalog cleanup rows require action.")
        else:
            print("[INFO] Catalog rows needing manual review were reported; none were applied.")
        return result

    apply_result = _run_apply_for_existing_base_pack_variants(
        cfg=cfg,
        plan_df=action_plan,
        qbo_item_rows=qbo_item_rows,
        txn_date=txn_date,
        max_products=int(max_catalog_fixes),
        dry_run=bool(dry_run),
        return_stats=True,
    )
    if isinstance(apply_result, dict):
        exit_code = int(apply_result.get("exit_code", 0))
        consolidated = int(apply_result.get("consolidated", 0))
        cleaned_up = int(apply_result.get("cleaned_up", 0))
    else:
        exit_code = int(apply_result)
        consolidated = min(int(max_catalog_fixes), len(supported))
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
    max_quantity_adjustments: int,
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
    }

    if audit_df.empty or max_quantity_adjustments <= 0:
        return result

    candidates = audit_df[
        (audit_df["status"].astype(str) == "needs_adjustment")
        & (audit_df["catalog_issue_type"].astype(str) == "exact_name_match")
    ].copy()
    if candidates.empty:
        print("[INFO] No exact-match quantity adjustments required.")
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
            group = qbo_item_rows[qbo_item_rows["base_name"] == base]
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
                continue

            assert token_mgr is not None
            post_inventory_adjustment(token_mgr, cfg.realm_id, payload)
            print(f"[OK] Posted quantity adjustment for {base!r} item_id={item_id}")
            result["posted"] += 1
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
    payload = dict(summary)
    payload["summary_json"] = str(json_path)
    payload["summary_csv"] = str(csv_path)

    with open(json_path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, sort_keys=True)
        handle.write("\n")

    row = {
        "company_key": payload.get("company_key"),
        "products_checked": payload.get("products_checked"),
        "already_correct": payload.get("already_correct"),
        "catalog_fixes_applied": payload.get("catalog_fixes_applied"),
        "quantity_updates_applied": payload.get("quantity_updates_applied"),
        "skipped_safely": payload.get("skipped_safely"),
        "still_needs_review": payload.get("still_needs_review"),
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


def _format_final_summary(summary: dict[str, Any]) -> str:
    return "\n".join(
        [
            f"Inventory sync finished for {summary['display_name']} ({summary['company_key']})",
            f"Scope: {summary['scope'] or 'all products'}",
            f"Products checked: {summary['products_checked']}",
            f"Already correct: {summary['already_correct']}",
            f"Catalog fixes applied: {summary['catalog_fixes_applied']}",
            f"Quantity updates applied: {summary['quantity_updates_applied']}",
            f"Skipped safely: {summary['skipped_safely']}",
            f"Still needs review: {summary['still_needs_review']}",
            f"Report path: {summary['summary_json']}",
        ]
    )


def run_inventory_pipeline(args: argparse.Namespace) -> dict[str, Any]:
    started_at = _now_utc_iso()
    cfg = load_company_config(args.company)
    ensure_company_runtime_compatible(cfg)

    max_catalog_fixes = _non_negative_int(args.max_catalog_fixes, default=5)
    max_quantity_adjustments = _non_negative_int(args.max_quantity_adjustments, default=10)
    categories = [str(c).strip() for c in list(args.categories or []) if str(c).strip()]
    product_filter = (args.product_filter or "").strip() or None
    txn_date = (args.txn_date or datetime.now().strftime("%Y-%m-%d")).strip()
    child_reports: dict[str, str] = {}

    print("=" * 68)
    print(f"Inventory pipeline: {cfg.display_name} ({cfg.company_key})")
    scope_text = format_scope(category=categories, product=product_filter)
    if scope_text:
        print(f"Scope: {scope_text}")
    print("=" * 68)

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
        child_reports["final_audit"] = str(final.report_path)

    counts = _final_counts(final.report)
    unsupported_total = sum(int(v) for v in catalog_result["unsupported_counts"].values())
    skipped_safely = (
        int(unsupported_total)
        + int(catalog_result["skipped_due_to_cap"])
        + int(quantity_result["skipped"])
        + int(quantity_result["skipped_due_to_cap"])
    )
    still_needs_review = int(counts.get("ambiguous_in_qbo", 0)) + int(counts.get("missing_in_qbo", 0))

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
        "products_checked": int(len(final.report)),
        "already_correct": int(counts.get("in_sync", 0)),
        "catalog_fixes_applied": int(catalog_result["applied"]),
        "quantity_updates_applied": int(quantity_result["posted"]),
        "skipped_safely": int(skipped_safely),
        "still_needs_review": int(still_needs_review),
        "final_status_counts": counts,
        "unsupported_catalog_issues": catalog_result["unsupported_counts"],
        "quantity_adjustment_stats": quantity_result,
        "child_reports": child_reports,
        "finished_at": _now_utc_iso(),
    }
    summary_json, summary_csv = _write_summary_reports(
        summary,
        output_dir=args.summary_output_dir,
    )
    summary["summary_json"] = str(summary_json)
    summary["summary_csv"] = str(summary_csv)

    print("=" * 68)
    print(_format_final_summary(summary))
    print("=" * 68)

    webhook = getattr(cfg, "slack_webhook_url", None)
    if webhook and not args.no_slack:
        send_slack_success(_format_final_summary(summary), webhook)

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
