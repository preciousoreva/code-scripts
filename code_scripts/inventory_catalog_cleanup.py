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
    _time_stamp,
    load_epos_stock_snapshot,
    load_qbo_inventory_item_rows,
    load_qbo_inventory_snapshot,
    build_audit_report,
)


_PLANNER_COLUMNS = [
    "company_key",
    "base_name",
    "epos_single_units",
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
        "--qbo-csv",
        default=None,
        help="Optional path to QBO Item export CSV (defaults to the standard snapshot path if present).",
    )
    p.add_argument(
        "--from-report",
        default=None,
        help="Use an existing inventory audit CSV as the source instead of regenerating.",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="No-op mode; still writes the planner CSV and prints summary.",
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
    from code_scripts.qbo_snapshot_cache import get_qbo_snapshot_path

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
        for base, g in qbo_item_rows.groupby("base_name"):
            base = _collapse_spaces(str(base))
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
        issue = str(row.get("catalog_issue_type") or "").strip()
        planned = _map_planned_action(issue)

        details = by_base.get(base_name, {})
        out_rows.append(
            {
                "company_key": company_key,
                "base_name": base_name,
                "epos_single_units": float(row.get("epos_single_units") or 0.0),
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


def main(argv: Optional[list[str]] = None) -> int:
    args = build_parser().parse_args(argv)
    cfg = load_company_config(args.company)
    ensure_company_runtime_compatible(cfg)

    report_path = Path(args.from_report).expanduser() if args.from_report else None
    stock_path = Path(args.stock_csv).expanduser() if args.stock_csv else None

    if report_path is None:
        if stock_path is None and not args.auto_download:
            raise SystemExit("Must supply --from-report or (--stock-csv / --auto-download).")
        if stock_path is None:
            stock_path = _auto_download_stock_csv(cfg)
        qbo_path = Path(args.qbo_csv).expanduser() if args.qbo_csv else _default_qbo_snapshot_path(cfg.company_key)
        if qbo_path is None or not qbo_path.exists():
            raise SystemExit(
                "QBO snapshot not found. Run inventory_sync with --auto-fetch-qbo first, or pass --qbo-csv."
            )

        epos = load_epos_stock_snapshot(
            str(stock_path),
            categories=list(args.categories or []),
        )
        qbo_grouped = load_qbo_inventory_snapshot(str(qbo_path))
        audit_df = build_audit_report(epos, qbo_grouped, tolerance=0.0)
        source_inventory_report = ""
        qbo_item_rows = load_qbo_inventory_item_rows(str(qbo_path))
    else:
        audit_df = _read_inventory_report(report_path)
        source_inventory_report = str(report_path)
        qbo_path = Path(args.qbo_csv).expanduser() if args.qbo_csv else _default_qbo_snapshot_path(cfg.company_key)
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

    counts = plan_df["planned_action"].value_counts().to_dict()
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

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

