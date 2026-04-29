from __future__ import annotations

import argparse
import csv
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

import pandas as pd

from code_scripts.artifact_paths import inventory_catalog_cleanup_reports_dir
from code_scripts.company_config import (
    ensure_company_runtime_compatible,
    get_available_companies,
    load_company_config,
)
from code_scripts.inventory_sync import (
    _auto_download_stock_csv,
    _collapse_spaces,
    _normalize_name_key,
    _time_stamp,
    fetch_qbo_inventory_items_snapshot,
    literal_product_filter_mask,
    load_epos_stock_snapshot,
    load_qbo_inventory_item_rows,
    load_qbo_inventory_snapshot,
    build_audit_report,
)
from code_scripts.qbo_inventory_adjustment import build_inventory_adjustment_payload, post_inventory_adjustment
from code_scripts.qbo_pack_variant_cleanup import (
    _fetch_item_with_sync_token,
    _post_inactivate,
    build_inactivate_payload,
)
from code_scripts.qbo_pack_variant_consolidation import (
    build_doc_number as build_consolidation_doc_number,
    is_duplicate_doc_number_error,
)
from code_scripts.qbo_snapshot_cache import get_qbo_snapshot_path, mark_qbo_snapshot_stale
from code_scripts.qbo_upload import TokenManager
from code_scripts.qbo_upload import (
    create_inventory_item,
    get_or_create_item_category_id,
    load_category_account_mapping,
)
from code_scripts.run_lock import GlobalRunLock
from code_scripts.token_manager import verify_realm_match


_PLANNER_COLUMNS = [
    "company_key",
    "base_name",
    "epos_single_units",
    "epos_categories",
    "catalog_issue_type",
    "planned_action",
    "action_eligible",
    "block_reason",
    "qbo_item_ids",
    "qbo_item_names",
    "qbo_base_item_ids",
    "qbo_base_item_names",
    "qbo_pack_variant_item_ids",
    "qbo_pack_variant_names",
    "suggested_next_action",
    "source_inventory_report",
]

_ACTIONABLE_CATALOG_ISSUE_TYPES = {
    "base_with_pack_variants",
    "only_pack_variant_exists",
    "multiple_active_base_items",
    "missing_from_qbo",
}


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Inventory catalog cleanup planner (audit-only). No QBO writes."
    )
    p.add_argument(
        "--company",
        required=True,
        choices=get_available_companies(),
        help="Company key (REQUIRED).",
    )
    p.add_argument(
        "--stock-csv",
        default=None,
        help="Path to EPOS StockReport/StockHistory CSV export.",
    )
    p.add_argument(
        "--auto-download",
        action="store_true",
        help="Auto-download a fresh EPOS StockReport CSV (Playwright).",
    )
    p.add_argument(
        "--category",
        dest="categories",
        action="append",
        default=[],
        help="Optional EPOS category filter (repeatable).",
    )
    p.add_argument(
        "--product",
        dest="product_filter",
        default=None,
        help="Optional substring filter on EPOS/QBO base product name (case-insensitive).",
    )
    p.add_argument(
        "--qbo-csv",
        default=None,
        help="Optional path to QBO Item export CSV (defaults to the standard snapshot path if present).",
    )
    p.add_argument(
        "--auto-fetch-qbo",
        action="store_true",
        help="Fetch/update the QBO Items snapshot before loading (writes the standard snapshot path unless --qbo-csv is provided).",
    )
    p.add_argument(
        "--qbo-force-refresh",
        action="store_true",
        help="When using --auto-fetch-qbo, ignore any cached snapshot and always re-query QBO.",
    )
    p.add_argument(
        "--from-report",
        default=None,
        help="Use an existing inventory audit CSV as the source instead of regenerating.",
    )
    p.add_argument(
        "--apply",
        action="store_true",
        help="Apply catalog cleanup for eligible rows. CLI-only; supports only existing-base pack consolidation.",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview what --apply would do (no QBO writes).",
    )
    p.add_argument(
        "--max-products",
        type=int,
        default=None,
        help="Required with --apply. Hard cap on number of base products to process.",
    )
    p.add_argument(
        "--txn-date",
        default=None,
        help="TxnDate for InventoryAdjustments (YYYY-MM-DD). Defaults to today.",
    )
    p.add_argument(
        "--include-no-action",
        action="store_true",
        help="Include exact-match / no_action rows in the report (default: exclude).",
    )
    p.add_argument(
        "--output",
        default=None,
        help="Optional output CSV path. Defaults to reports/inventory_catalog_cleanup/YYYY-MM-DD/...",
    )
    return p


def _default_output_path(company_key: str, *, now: datetime | None = None) -> Path:
    clock = now or datetime.now()
    return (
        inventory_catalog_cleanup_reports_dir(clock)
        / f"inventory_catalog_cleanup_{company_key}_{_time_stamp(clock)}.csv"
    )


def _default_qbo_snapshot_path(company_key: str) -> Optional[Path]:
    # Keep this planner intentionally read-only; reuse the snapshot file if it exists.
    path = get_qbo_snapshot_path(company_key)
    return path if path.exists() else None


def _read_inventory_report(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path)
    if "base_name" not in df.columns:
        raise ValueError("Inventory audit report missing required column: base_name")
    if "catalog_issue_type" not in df.columns:
        raise ValueError(
            "Inventory audit report missing required column: catalog_issue_type "
            "(regenerate audit with an updated inventory_sync)."
        )
    return df


@dataclass(frozen=True)
class PlannedAction:
    planned_action: str
    action_eligible: bool
    block_reason: str


def _map_planned_action(issue_type: str) -> PlannedAction:
    t = (issue_type or "").strip()
    if t == "base_with_pack_variants":
        return PlannedAction(
            planned_action="consolidate_existing_base_pack_variants",
            action_eligible=True,
            block_reason="",
        )
    if t == "only_pack_variant_exists":
        return PlannedAction(
            planned_action="create_base_then_consolidate_pack_variant",
            action_eligible=True,
            block_reason="",
        )
    if t == "multiple_active_base_items":
        return PlannedAction(
            planned_action="manual_review_duplicate_base_items",
            action_eligible=False,
            block_reason="duplicate_base_items_require_manual_review",
        )
    if t == "missing_from_qbo":
        return PlannedAction(
            planned_action="create_inventory_item",
            action_eligible=False,
            block_reason="missing_in_qbo_requires_item_creation",
        )
    return PlannedAction(
        planned_action="no_action",
        action_eligible=False,
        block_reason="not_a_catalog_cleanup_candidate",
    )


def plan_catalog_cleanup(
    *,
    company_key: str,
    audit_df: pd.DataFrame,
    qbo_item_rows: Optional[pd.DataFrame],
    source_inventory_report: str,
) -> pd.DataFrame:
    out_rows: list[dict[str, Any]] = []

    by_base = {}
    if qbo_item_rows is not None and not qbo_item_rows.empty:
        group_col = "base_name_norm" if "base_name_norm" in qbo_item_rows.columns else "base_name"
        for base, g in qbo_item_rows.groupby(group_col):
            base = _normalize_name_key(base)
            ids_all = [str(x).strip() for x in g["Id"].tolist() if str(x).strip()]
            names_all = [str(x).strip() for x in g["Name"].tolist() if str(x).strip()]
            pack = g[g["qbo_has_pack"] == True]  # noqa: E712
            base_rows = g[g["qbo_has_pack"] == False]  # noqa: E712
            by_base[base] = {
                "qbo_item_ids": ",".join(ids_all[:50]),
                "qbo_item_names": " | ".join(names_all[:10]),
                "qbo_base_item_ids": ",".join([str(x).strip() for x in base_rows["Id"].tolist() if str(x).strip()][:50]),
                "qbo_base_item_names": " | ".join([str(x).strip() for x in base_rows["Name"].tolist() if str(x).strip()][:10]),
                "qbo_pack_variant_item_ids": ",".join([str(x).strip() for x in pack["Id"].tolist() if str(x).strip()][:50]),
                "qbo_pack_variant_names": " | ".join([str(x).strip() for x in pack["Name"].tolist() if str(x).strip()][:10]),
            }

    for _, row in audit_df.iterrows():
        base_name = _collapse_spaces(str(row.get("base_name") or ""))
        base_name_norm = _normalize_name_key(base_name)
        issue = str(row.get("catalog_issue_type") or "").strip()
        planned = _map_planned_action(issue)

        details = by_base.get(base_name_norm, {})
        out_rows.append(
            {
                "company_key": company_key,
                "base_name": base_name,
                "epos_single_units": float(row.get("epos_single_units") or 0.0),
                "epos_categories": str(row.get("epos_categories") or ""),
                "catalog_issue_type": issue,
                "planned_action": planned.planned_action,
                "action_eligible": bool(planned.action_eligible),
                "block_reason": planned.block_reason,
                "qbo_item_ids": details.get("qbo_item_ids") or str(row.get("qbo_base_item_ids") or ""),
                "qbo_item_names": details.get("qbo_item_names") or str(row.get("qbo_item_names_for_base") or ""),
                "qbo_base_item_ids": details.get("qbo_base_item_ids") or str(row.get("qbo_base_item_ids") or ""),
                "qbo_base_item_names": details.get("qbo_base_item_names") or str(row.get("qbo_base_item_names_for_base") or ""),
                "qbo_pack_variant_item_ids": details.get("qbo_pack_variant_item_ids") or "",
                "qbo_pack_variant_names": details.get("qbo_pack_variant_names") or str(row.get("qbo_pack_variant_names_for_base") or ""),
                "suggested_next_action": str(row.get("suggested_next_action") or ""),
                "source_inventory_report": source_inventory_report,
            }
        )

    plan_df = pd.DataFrame(out_rows)
    for col in _PLANNER_COLUMNS:
        if col not in plan_df.columns:
            plan_df[col] = ""
    return plan_df[_PLANNER_COLUMNS]


def _write_csv(path: Path, df: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False, quoting=csv.QUOTE_MINIMAL)


def _plan_summary_counts(plan_df: pd.DataFrame) -> dict[str, int]:
    vc = plan_df["planned_action"].value_counts().to_dict() if not plan_df.empty else {}
    return {str(k): int(v) for k, v in vc.items()}


def _run_apply_for_existing_base_pack_variants(
    *,
    cfg,
    plan_df: pd.DataFrame,
    qbo_item_rows: pd.DataFrame,
    txn_date: str,
    max_products: int,
    dry_run: bool,
    return_stats: bool = False,
) -> int | dict[str, int]:
    """
    Apply-mode runner for:
    - consolidate_existing_base_pack_variants
    - create_base_then_consolidate_pack_variant
    """
    attempted = consolidated = cleaned_up = skipped = failed = 0
    base_items_created = 0
    partial_failures: list[str] = []
    created_base_details: list[dict[str, Any]] = []

    def _result(exit_code: int) -> int | dict[str, int]:
        if return_stats:
            return {
                "exit_code": int(exit_code),
                "attempted": int(attempted),
                "consolidated": int(consolidated),
                "cleaned_up": int(cleaned_up),
                "skipped": int(skipped),
                "failed": int(failed),
                "base_items_created": int(base_items_created),
                "created_base_details": list(created_base_details),
            }
        return int(exit_code)

    supported_actions = {
        "consolidate_existing_base_pack_variants",
        "create_base_then_consolidate_pack_variant",
    }
    supported = plan_df[plan_df["planned_action"].isin(supported_actions)].copy()
    unsupported = plan_df[~plan_df["planned_action"].isin(supported_actions)].copy()
    if not unsupported.empty:
        for _, r in unsupported.iterrows():
            skipped += 1
            print(
                f"[SKIP] base={str(r.get('base_name') or '')!r} planned_action={r.get('planned_action')} "
                "reason=unsupported_planned_action"
            )

    eligible = supported[supported["action_eligible"] == True].copy()  # noqa: E712
    ineligible = supported[supported["action_eligible"] != True].copy()  # noqa: E712
    if not ineligible.empty:
        for _, r in ineligible.iterrows():
            skipped += 1
            print(
                f"[SKIP] base={str(r.get('base_name') or '')!r} planned_action={r.get('planned_action')} "
                f"reason={str(r.get('block_reason') or 'not_eligible')}"
            )

    if eligible.empty:
        print("[INFO] No eligible rows to apply for existing-base pack consolidation.")
        print(f"Apply summary: attempted={attempted} consolidated={consolidated} cleaned_up={cleaned_up} skipped={skipped} failed={failed}")
        return _result(0)

    capped = eligible.head(int(max_products)).copy()
    skipped_due_to_cap = max(0, len(eligible) - len(capped))
    skipped += skipped_due_to_cap
    if skipped_due_to_cap:
        print(f"[INFO] Cap active: skipped_due_to_cap={skipped_due_to_cap}")

    mapping_cache: dict[str, dict[str, str]] | None = None
    mapping_load_error: str | None = None
    if not capped[capped["planned_action"] == "create_base_then_consolidate_pack_variant"].empty:
        try:
            mapping_cache = load_category_account_mapping(cfg)
        except Exception as exc:  # noqa: BLE001
            mapping_load_error = str(exc)

    token_mgr = None
    if not dry_run:
        verify_realm_match(cfg.company_key, cfg.realm_id)
        token_mgr = TokenManager(cfg.company_key, cfg.realm_id)

    run_lock = None
    if not dry_run:
        run_lock = GlobalRunLock(holder=f"inventory_catalog_cleanup:{cfg.company_key}")
        lock_result = run_lock.acquire()
        if not lock_result.acquired:
            print(f"Error: another pipeline run is active ({lock_result.reason}); refusing to --apply.", flush=True)
            return _result(2)

    try:
        qbo_rows_df = qbo_item_rows.copy()
        if "base_name_norm" not in qbo_rows_df.columns and "base_name" in qbo_rows_df.columns:
            qbo_rows_df["base_name_norm"] = qbo_rows_df["base_name"].map(_normalize_name_key)
        for _, planned_row in capped.iterrows():
            attempted += 1
            base_name = str(planned_row.get("base_name") or "").strip()
            action = str(planned_row.get("planned_action") or "").strip()
            base_norm = _normalize_name_key(base_name)
            group = qbo_rows_df[qbo_rows_df["base_name_norm"] == base_norm].copy()
            base_rows = group[group["qbo_has_pack"] == False]  # noqa: E712
            pack_rows = group[group["qbo_has_pack"] == True]  # noqa: E712

            if action == "create_base_then_consolidate_pack_variant":
                if len(base_rows) > 0:
                    skipped += 1
                    print(f"[SKIP] base={base_name!r} reason=active_base_already_exists")
                    continue
                if pack_rows.empty:
                    skipped += 1
                    print(f"[SKIP] base={base_name!r} reason=no_pack_variant_found")
                    continue
                if mapping_load_error:
                    skipped += 1
                    print(f"[SKIP] base={base_name!r} reason=missing_account_mapping detail={mapping_load_error}")
                    continue
                assert mapping_cache is not None

                category_text = str(planned_row.get("epos_categories") or "").strip()
                category = category_text.split("|")[0].strip() if category_text else ""
                if not category:
                    skipped += 1
                    print(f"[SKIP] base={base_name!r} reason=missing_epos_category_for_item_creation")
                    continue
                if category not in mapping_cache:
                    skipped += 1
                    print(f"[SKIP] base={base_name!r} reason=missing_account_mapping_for_category category={category!r}")
                    continue

                source_pack_id = str(pack_rows.iloc[0].get("Id") or "").strip()
                source_pack_name = str(pack_rows.iloc[0].get("Name") or "").strip()
                unit_sales_price = 0.0
                unit_purchase_cost = 0.0
                category_item_id_val: str | None = None

                if dry_run:
                    print(
                        f"[DRY-RUN] would create base inventory item base={base_name!r} "
                        f"category={category!r} from_pack={source_pack_name!r}"
                    )
                    base_item_id = f"DRYRUN:{base_name}"
                    base_qty_live = float(getattr(cfg, "default_qty_on_hand", 0) or 0)
                    base_items_created += 1
                else:
                    assert token_mgr is not None
                    try:
                        source_pack_live = _fetch_item_with_sync_token(token_mgr, cfg.realm_id, source_pack_id)
                        unit_sales_price = float(source_pack_live.get("UnitPrice", 0) or 0)
                        unit_purchase_cost = float(source_pack_live.get("PurchaseCost", 0) or 0)
                        category_item_id_val = get_or_create_item_category_id(
                            token_mgr, cfg.realm_id, category
                        )
                    except Exception as exc:  # noqa: BLE001
                        failed += 1
                        print(f"[FAIL] base={base_name!r} pre-create lookup failed: {exc}")
                        continue

                    try:
                        base_item_id = create_inventory_item(
                            base_name,
                            category,
                            unit_sales_price,
                            unit_purchase_cost,
                            cfg,
                            token_mgr,
                            cfg.realm_id,
                            mapping_cache,
                            {},
                            target_date=txn_date,
                            category_item_id=category_item_id_val,
                        )
                    except Exception as exc:  # noqa: BLE001
                        failed += 1
                        print(f"[FAIL] base={base_name!r} create_base_item failed: {exc}")
                        continue

                    base_items_created += 1
                    created_base_details.append(
                        {
                            "base_name": base_name,
                            "base_item_id": str(base_item_id),
                            "created": True,
                            "source_pack_variant": source_pack_name,
                        }
                    )
                    try:
                        base_live = _fetch_item_with_sync_token(token_mgr, cfg.realm_id, str(base_item_id))
                        base_qty_live = float(base_live.get("QtyOnHand", 0) or 0)
                    except Exception as exc:  # noqa: BLE001
                        failed += 1
                        print(f"[FAIL] base={base_name!r} fetch_created_base failed: {exc}")
                        continue

                qbo_rows_df = pd.concat(
                    [
                        qbo_rows_df,
                        pd.DataFrame(
                            [
                                {
                                    "Id": str(base_item_id),
                                    "Name": base_name,
                                    "base_name": base_name,
                                    "base_name_norm": base_norm,
                                    "qbo_name_raw": base_name,
                                    "qbo_has_pack": False,
                                    "qbo_qty_on_hand": base_qty_live,
                                }
                            ]
                        ),
                    ],
                    ignore_index=True,
                )
                group = qbo_rows_df[qbo_rows_df["base_name_norm"] == base_norm].copy()
                base_rows = group[group["qbo_has_pack"] == False]  # noqa: E712
                pack_rows = group[group["qbo_has_pack"] == True]  # noqa: E712

            if base_rows.empty:
                skipped += 1
                print(f"[SKIP] base={base_name!r} reason=no_base_item_for_consolidation")
                continue
            if len(base_rows) > 1:
                skipped += 1
                print(f"[SKIP] base={base_name!r} reason=multiple_active_base_items")
                continue

            base_item_id = str(base_rows.iloc[0].get("Id") or "").strip()
            base_qty = float(base_rows.iloc[0].get("qbo_qty_on_hand", 0) or 0)
            epos_target = float(planned_row.get("epos_single_units") or 0.0)
            lines = []
            base_diff = epos_target - base_qty
            if base_diff != 0:
                lines.append({"item_id": base_item_id, "qty_diff": base_diff})
            pack_ids_for_cleanup: list[str] = []
            for _, pr in pack_rows.iterrows():
                pid = str(pr.get("Id") or "").strip()
                if not pid:
                    continue
                pack_ids_for_cleanup.append(pid)
                pack_qty = float(pr.get("qbo_qty_on_hand", 0) or 0)
                if pack_qty != 0:
                    lines.append({"item_id": pid, "qty_diff": -pack_qty})

            if not lines:
                skipped += 1
                print(f"[SKIP] base={base_name!r} reason=no_nonzero_qty_diffs")
                continue

            doc_number = build_consolidation_doc_number(txn_date=txn_date, base_item_id=base_item_id)
            payload = build_inventory_adjustment_payload(
                adjust_account_id=str((cfg.inventory_adjustment_account_id or "")).strip(),
                txn_date=txn_date,
                private_note=f"OIAT catalog cleanup | base={base_name!r} | action={action}",
                lines=lines,
                doc_number=doc_number,
            )

            if dry_run:
                print("[DRY-RUN] would post InventoryAdjustment payload=" + str(payload))
                consolidated += 1
                cleaned_up += 1 if pack_ids_for_cleanup else 0
                continue

            assert token_mgr is not None
            try:
                resp = post_inventory_adjustment(token_mgr, cfg.realm_id, payload)
                consolidated += 1
                inv_id = ""
                try:
                    inv_id = str(((resp or {}).get("InventoryAdjustment") or {}).get("Id") or "").strip()
                except Exception:  # noqa: BLE001
                    inv_id = ""
                suffix = f"id={inv_id}" if inv_id else f"doc={doc_number}"
                print(f"[OK] Posted InventoryAdjustment {suffix} for base={base_name!r}")
            except Exception as exc:  # noqa: BLE001
                failed += 1
                if is_duplicate_doc_number_error(exc):
                    print(f"[FAIL] base={base_name!r} duplicate DocNumber={doc_number}: {exc}")
                else:
                    print(f"[FAIL] base={base_name!r} consolidation failed: {exc}")
                continue

            cleanup_failed = False
            for pid in pack_ids_for_cleanup:
                try:
                    live = _fetch_item_with_sync_token(token_mgr, cfg.realm_id, pid)
                    qty = float(live.get("QtyOnHand", 0) or 0)
                    if qty != 0:
                        print(f"[SKIP] pack_variant_id={pid} qty_on_hand={qty} reason=nonzero_qty_on_hand")
                        continue
                    sync_token = str(live.get("SyncToken", "")).strip()
                    payload_inactivate = build_inactivate_payload(
                        item_id=pid,
                        sync_token=sync_token,
                        original_name=str(live.get("Name", "") or "").strip(),
                    )
                    _post_inactivate(token_mgr, cfg.realm_id, payload_inactivate)
                    nm = str(live.get("Name", "") or "").strip()
                    print(f"[OK] Inactivated pack_variant_id={pid} {nm!r}")
                except Exception as exc:  # noqa: BLE001
                    cleanup_failed = True
                    failed += 1
                    partial_failures.append(f"base={base_name} cleanup_failed pack_id={pid}: {exc}")
                    print(f"[FAIL] base={base_name!r} cleanup failed for pack_id={pid}: {exc}")
                    break
            if not cleanup_failed:
                cleaned_up += 1 if pack_ids_for_cleanup else 0

            if cleanup_failed:
                print(f"[WARN] base={base_name!r} consolidation succeeded but cleanup failed (partial).")

    finally:
        if not dry_run:
            if consolidated > 0 or cleaned_up > 0:
                mark_qbo_snapshot_stale(cfg.company_key, reason="inventory_catalog_cleanup_applied")
                print("[INFO] Marked cached QBO snapshot stale after catalog cleanup apply.")
            if run_lock is not None:
                run_lock.release()

    print(
        "Apply summary: "
        f"attempted={attempted} consolidated={consolidated} cleaned_up={cleaned_up} "
        f"base_items_created={base_items_created} skipped={skipped} failed={failed}"
    )
    if partial_failures:
        for line in partial_failures[:10]:
            print("  partial_failure: " + line)
    return _result(0 if failed == 0 else 1)


def main(argv: Optional[list[str]] = None) -> int:
    args = build_parser().parse_args(argv)
    cfg = load_company_config(args.company)
    ensure_company_runtime_compatible(cfg)

    if args.apply and args.dry_run:
        raise SystemExit("Error: pass either --apply or --dry-run, not both.")
    if args.apply:
        if args.max_products is None:
            raise SystemExit("Error: --apply requires --max-products.")
        if int(args.max_products) <= 0:
            raise SystemExit("Error: --max-products must be > 0.")
        adjust_account_id = str(getattr(cfg, "inventory_adjustment_account_id", "") or "").strip()
        if not adjust_account_id:
            raise SystemExit(
                "Error: qbo.inventory_adjustment_account_id is not configured; "
                "apply mode refuses to post without an adjust account."
            )

    report_path = Path(args.from_report).expanduser() if args.from_report else None
    stock_path = Path(args.stock_csv).expanduser() if args.stock_csv else None

    if report_path is None:
        if stock_path is None and not args.auto_download:
            raise SystemExit("Must supply --from-report or (--stock-csv / --auto-download).")
        if stock_path is None:
            stock_path = _auto_download_stock_csv(cfg)
        qbo_path = Path(args.qbo_csv).expanduser() if args.qbo_csv else _default_qbo_snapshot_path(cfg.company_key)
        if args.auto_fetch_qbo and not args.qbo_csv:
            qbo_path = fetch_qbo_inventory_items_snapshot(
                company_key=cfg.company_key,
                realm_id=cfg.realm_id,
                output_path=get_qbo_snapshot_path(cfg.company_key),
                force_refresh=bool(args.qbo_force_refresh),
            )
        if qbo_path is None or not qbo_path.exists():
            raise SystemExit(
                "QBO snapshot not found. Pass --auto-fetch-qbo (optionally with --qbo-force-refresh) "
                "or pass --qbo-csv."
            )

        epos = load_epos_stock_snapshot(
            str(stock_path),
            categories=list(args.categories or []),
            product_filter=args.product_filter,
        )
        qbo_grouped = load_qbo_inventory_snapshot(str(qbo_path))
        audit_df = build_audit_report(epos, qbo_grouped, tolerance=0.0)
        source_inventory_report = ""
        print(f"[INFO] QBO snapshot: {qbo_path}")
        qbo_item_rows = load_qbo_inventory_item_rows(str(qbo_path))
    else:
        audit_df = _read_inventory_report(report_path)
        source_inventory_report = str(report_path)
        if args.product_filter:
            audit_df = audit_df[literal_product_filter_mask(audit_df, args.product_filter)].copy()
        qbo_path = Path(args.qbo_csv).expanduser() if args.qbo_csv else _default_qbo_snapshot_path(cfg.company_key)
        if args.auto_fetch_qbo and not args.qbo_csv:
            qbo_path = fetch_qbo_inventory_items_snapshot(
                company_key=cfg.company_key,
                realm_id=cfg.realm_id,
                output_path=get_qbo_snapshot_path(cfg.company_key),
                force_refresh=bool(args.qbo_force_refresh),
            )
        if qbo_path and qbo_path.exists():
            print(f"[INFO] QBO snapshot: {qbo_path}")
        qbo_item_rows = load_qbo_inventory_item_rows(str(qbo_path)) if qbo_path and qbo_path.exists() else None

    plan_df = plan_catalog_cleanup(
        company_key=cfg.company_key,
        audit_df=audit_df,
        qbo_item_rows=qbo_item_rows,
        source_inventory_report=source_inventory_report,
    )

    if not args.include_no_action:
        plan_df = plan_df[plan_df["catalog_issue_type"].isin(_ACTIONABLE_CATALOG_ISSUE_TYPES)].copy()

    out_path = Path(args.output).expanduser() if args.output else _default_output_path(cfg.company_key)
    _write_csv(out_path, plan_df)

    counts = _plan_summary_counts(plan_df)
    print("=" * 68)
    print(f"Catalog cleanup plan: {cfg.display_name} ({cfg.company_key})")
    print(f"Rows planned: {len(plan_df)}")
    print(f"Wrote report: {out_path}")
    print("-" * 68)
    for key in [
        "consolidate_existing_base_pack_variants",
        "create_base_then_consolidate_pack_variant",
        "manual_review_duplicate_base_items",
        "create_inventory_item",
    ]:
        if key in counts:
            print(f"{key}: {int(counts[key])}")
    if args.include_no_action and "no_action" in counts:
        print(f"no_action: {int(counts['no_action'])}")
    print("=" * 68)

    if not args.apply and not args.dry_run:
        return 0

    if qbo_item_rows is None or qbo_item_rows.empty:
        raise SystemExit(
            "QBO snapshot not found. Run inventory_sync with --auto-fetch-qbo first, or pass --qbo-csv."
        )

    txn_date = (args.txn_date or datetime.now().strftime("%Y-%m-%d")).strip()
    return _run_apply_for_existing_base_pack_variants(
        cfg=cfg,
        plan_df=plan_df,
        qbo_item_rows=qbo_item_rows,
        txn_date=txn_date,
        max_products=int(args.max_products or 0),
        dry_run=bool(args.dry_run),
    )


if __name__ == "__main__":
    raise SystemExit(main())
