"""Audit-only QBO pack-variant consolidation planner (target-based).

Why this is different from ``qbo_pack_variant_migration``
=========================================================
The migration planner assumed the right end-state was *base + sum(packs in
single units)*.  That's wrong when QBO base + pack-variant qtys overlap or
contradict each other (e.g. base shows -295, packs show extra positive
stock).  Production data showed this would severely overstate final QBO
stock.

This planner instead uses **EPOS as the inventory target of record**.  For
each base product it proposes one logical InventoryAdjustment that:

* sets the exact-base item to the EPOS single-unit target, and
* zeros every active pack-variant item under that base.

Concretely, for the TROPHY example::

    QBO  9364 TROPHY LAGER CAN 500ML       qty=-295    diff=+631 (target 336)
    QBO  9365 TROPHY LAGER CAN 500ML*12    qty=  3     diff=  -3
    QBO  9366 TROPHY LAGER CAN 500ML*24    qty= 52     diff= -52
    EPOS               14 packs of *24                target = 14 * 24 = 336

Strictly audit-only.  No QBO writes.  No InventoryAdjustment payloads are
constructed here.  An apply-side follow-up will live in a separate task.

Reuses without duplication:
* :func:`code_scripts.inventory_sync.load_epos_stock_snapshot` for the
  EPOS base-unit target calculation (it already strips ``*N`` and sums).
* :func:`code_scripts.inventory_sync.load_qbo_inventory_item_rows` for
  per-item QBO rows with ``base_name`` / ``qbo_has_pack`` already derived.
* :func:`code_scripts.qbo_pack_variant_cleanup._resolve_qbo_csv` for the
  ``--qbo-csv``/``--auto-fetch-qbo`` plumbing.
* :func:`code_scripts.transform.strip_pack_multiplier` is reached
  transitively via the helpers above — no separate import needed.
"""

from __future__ import annotations

import argparse
import csv
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

from code_scripts.artifact_paths import qbo_pack_variant_consolidation_reports_dir
from code_scripts.company_config import (
    ensure_company_runtime_compatible,
    get_available_companies,
    load_company_config,
)
from code_scripts.inventory_sync import (
    load_epos_stock_snapshot,
    load_qbo_inventory_item_rows,
)
from code_scripts.qbo_pack_variant_cleanup import _resolve_qbo_csv


_REPORT_FIELDS = [
    "company_key",
    "base_name",
    "epos_single_units_target",
    "base_qbo_item_id",
    "base_qbo_name",
    "base_qbo_qty_on_hand",
    "base_qty_diff_to_target",
    "pack_variant_item_ids",
    "pack_variant_names",
    "pack_variant_qtys_on_hand",
    "pack_variant_qty_diffs_to_zero",
    "total_qbo_qty_before_simple_sum",
    "planned_line_count",
    "consolidation_recommended_action",
    "risk_reason",
]

# Multi-value cell delimiter — '|' avoids needing CSV-quoting for commas.
_LIST_DELIMITER = "|"


# ---------------------------------------------------------------------------
# Plan
# ---------------------------------------------------------------------------


def _norm(value: Any) -> str:
    return str(value or "").strip().lower()


def _to_float(value: Any) -> float:
    if value in (None, ""):
        return 0.0
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _format_float(value: float) -> str:
    """Render a float without unnecessary trailing zeros for CSV/console output."""
    if value == int(value):
        return str(int(value))
    return str(value)


def build_consolidation_plan(
    qbo_rows: list[dict[str, Any]],
    epos_targets: dict[str, float],
    *,
    company_key: str,
    in_scope_bases: Optional[set[str]] = None,
) -> list[dict[str, Any]]:
    """Build per-base consolidation plan rows.

    Parameters
    ----------
    qbo_rows
        Iterable of dicts with at least ``Id``, ``Name``, ``base_name``,
        ``qbo_has_pack``, ``qbo_qty_on_hand`` (the columns
        :func:`load_qbo_inventory_item_rows` produces).  The function does
        not need pandas — pass ``df.to_dict(orient='records')`` from a
        pandas DataFrame, or a hand-built list in tests.
    epos_targets
        Lookup of ``{base_name_lower: epos_single_units}``.  Bases not in
        the dict are flagged as ``no_epos_target`` only when
        ``in_scope_bases`` is ``None``; when ``in_scope_bases`` is supplied,
        out-of-scope bases are skipped entirely.
    company_key
        Surfaced on every row of the report.
    in_scope_bases
        Optional set of normalised base names to include.  Used to scope
        the report when ``--category`` and/or ``--product`` filters apply.

    Bases that have a clean exact base item but **no** pack variants are
    not in this report — there's nothing to consolidate.
    """
    # Group QBO items by normalised base_name. ``base_name`` already comes
    # from inventory_sync's strip_pack_multiplier+_collapse_spaces.
    groups: dict[str, list[dict[str, Any]]] = {}
    display_for: dict[str, str] = {}
    for row in qbo_rows:
        base = _norm(row.get("base_name"))
        if not base:
            continue
        groups.setdefault(base, []).append(row)
        display_for.setdefault(base, str(row.get("base_name") or "").strip())

    plan: list[dict[str, Any]] = []
    for base_norm in sorted(groups):
        if in_scope_bases is not None and base_norm not in in_scope_bases:
            continue

        items = groups[base_norm]
        bases = [it for it in items if not bool(it.get("qbo_has_pack"))]
        packs = [it for it in items if bool(it.get("qbo_has_pack"))]

        # No pack variants -> no consolidation needed.
        if not packs:
            continue

        target = epos_targets.get(base_norm)

        # Pack diagnostics (assembled regardless of action so the report is
        # always informative).
        pack_qtys = [_to_float(p.get("qbo_qty_on_hand")) for p in packs]
        pack_diffs = [-q for q in pack_qtys]

        pack_ids_str = _LIST_DELIMITER.join(str(p.get("Id") or "").strip() for p in packs)
        pack_names_str = _LIST_DELIMITER.join(str(p.get("Name") or "").strip() for p in packs)
        pack_qtys_str = _LIST_DELIMITER.join(_format_float(q) for q in pack_qtys)
        pack_diffs_str = _LIST_DELIMITER.join(_format_float(d) for d in pack_diffs)

        all_qtys = pack_qtys + [_to_float(b.get("qbo_qty_on_hand")) for b in bases]
        total_simple = sum(all_qtys)

        if len(bases) == 0:
            action = "needs_manual_review"
            risk = "no_active_exact_base_in_qbo"
            base_id_out: Any = ""
            base_name_disp: Any = ""
            base_qty_out: Any = ""
            base_diff_out: Any = ""
            planned_line_count = 0
            target_out: Any = "" if target is None else _format_float(float(target))
        elif len(bases) > 1:
            action = "needs_manual_review"
            risk = "multiple_active_exact_base_in_qbo"
            base_id_out = _LIST_DELIMITER.join(
                str(b.get("Id") or "").strip() for b in bases
            )
            base_name_disp = _LIST_DELIMITER.join(
                str(b.get("Name") or "").strip() for b in bases
            )
            base_qty_out = _LIST_DELIMITER.join(
                _format_float(_to_float(b.get("qbo_qty_on_hand"))) for b in bases
            )
            base_diff_out = ""
            planned_line_count = 0
            target_out = "" if target is None else _format_float(float(target))
        elif target is None:
            base = bases[0]
            action = "needs_manual_review"
            risk = "no_epos_target"
            base_id_out = str(base.get("Id") or "").strip()
            base_name_disp = str(base.get("Name") or "").strip()
            base_qty_out = _to_float(base.get("qbo_qty_on_hand"))
            base_diff_out = ""
            planned_line_count = 0
            target_out = ""
        else:
            base = bases[0]
            action = "consolidation_plan_available"
            risk = ""
            base_id_out = str(base.get("Id") or "").strip()
            base_name_disp = str(base.get("Name") or "").strip()
            base_qty_out = _to_float(base.get("qbo_qty_on_hand"))
            base_diff_out = float(target) - base_qty_out
            planned_line_count = 1 + len(packs)
            target_out = _format_float(float(target))

        plan.append({
            "company_key": company_key,
            "base_name": display_for.get(base_norm, base_norm),
            "epos_single_units_target": target_out,
            "base_qbo_item_id": base_id_out,
            "base_qbo_name": base_name_disp,
            "base_qbo_qty_on_hand": base_qty_out,
            "base_qty_diff_to_target": base_diff_out,
            "pack_variant_item_ids": pack_ids_str,
            "pack_variant_names": pack_names_str,
            "pack_variant_qtys_on_hand": pack_qtys_str,
            "pack_variant_qty_diffs_to_zero": pack_diffs_str,
            "total_qbo_qty_before_simple_sum": total_simple,
            "planned_line_count": planned_line_count,
            "consolidation_recommended_action": action,
            "risk_reason": risk,
        })

    return plan


def write_report(plan: list[dict[str, Any]], output_path: Path) -> Path:
    output_path = output_path.expanduser()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=_REPORT_FIELDS)
        writer.writeheader()
        for row in plan:
            writer.writerow({k: row.get(k, "") for k in _REPORT_FIELDS})
    return output_path


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Audit-only target-based QBO pack-variant consolidation planner. "
            "Plans one logical InventoryAdjustment per base product such that "
            "the base item ends at the EPOS single-unit target and every "
            "pack variant ends at zero. Does NOT call QBO update."
        ),
    )
    available = get_available_companies()
    parser.add_argument("--company", required=True, choices=available)
    parser.add_argument(
        "--stock-csv",
        required=True,
        help="EPOS StockReport CSV (the source of truth for the base-unit target).",
    )
    parser.add_argument(
        "--qbo-csv",
        default=None,
        help="Pre-existing QBO Items snapshot CSV. If omitted, --auto-fetch-qbo "
             "is required so we have something to plan against.",
    )
    parser.add_argument(
        "--auto-fetch-qbo",
        action="store_true",
        help="Query QBO for the active inventory items snapshot before planning.",
    )
    parser.add_argument(
        "--qbo-cache-max-age-hours",
        type=int,
        default=24,
        help="When auto-fetching, reuse a cached QBO snapshot if younger than "
             "this many hours (default: 24).",
    )
    parser.add_argument(
        "--qbo-force-refresh",
        action="store_true",
        help="When auto-fetching, ignore the cached snapshot and re-query QBO.",
    )
    parser.add_argument(
        "--qbo-export-path",
        default=None,
        help="Override the snapshot output path for --auto-fetch-qbo.",
    )
    parser.add_argument(
        "--category",
        action="append",
        default=[],
        help="Restrict to base names whose EPOS rows include this CategoryName "
             "(case-insensitive, exact match). May be supplied multiple times.",
    )
    parser.add_argument(
        "--product",
        default=None,
        help="Substring filter (case-insensitive) on base_name; useful for "
             "piloting on a single product like 'TROPHY'.",
    )
    parser.add_argument(
        "--output",
        default=None,
        help="Override the report CSV output path.",
    )
    parser.add_argument(
        "--top",
        type=int,
        default=10,
        help="Number of rows printed under the 'top by |base_qty_diff_to_target|' "
             "summary section (default: 10).",
    )
    return parser


def _print_summary(
    config_display_name: str,
    company_key: str,
    plan: list[dict[str, Any]],
    report_path: Path,
    top_n: int,
) -> None:
    by_action: dict[str, int] = {}
    for row in plan:
        a = row["consolidation_recommended_action"]
        by_action[a] = by_action.get(a, 0) + 1

    eligible = [r for r in plan if r["consolidation_recommended_action"] == "consolidation_plan_available"]
    eligible_sorted = sorted(
        eligible,
        key=lambda r: abs(_to_float(r["base_qty_diff_to_target"])),
        reverse=True,
    )[:top_n]

    bar = "=" * 78
    print(bar)
    print(f"QBO pack-variant consolidation plan: {config_display_name} ({company_key})")
    print(f"Total base products scanned: {len(plan)}")
    for action in ("consolidation_plan_available", "needs_manual_review"):
        print(f"  {action}: {by_action.get(action, 0)}")
    if eligible_sorted:
        print("-" * 78)
        print(f"Top {len(eligible_sorted)} by |base_qty_diff_to_target|:")
        print(
            f"  {'base_id':>10}  {'cur_qty':>9}  {'target':>9}  "
            f"{'diff':>9}  base_name"
        )
        for r in eligible_sorted:
            print(
                f"  {r['base_qbo_item_id']:>10}  "
                f"{_format_float(_to_float(r['base_qbo_qty_on_hand'])):>9}  "
                f"{_format_float(_to_float(r['epos_single_units_target'])):>9}  "
                f"{_format_float(_to_float(r['base_qty_diff_to_target'])):>9}  "
                f"{r['base_name']}"
            )
    print("-" * 78)
    print(f"Wrote report: {report_path}")
    print(bar)


def main(argv: Optional[list[str]] = None) -> int:
    args = build_parser().parse_args(argv)

    if not args.qbo_csv and not args.auto_fetch_qbo:
        print(
            "Error: pass either --qbo-csv <path> or --auto-fetch-qbo so we have "
            "QBO data to plan against.",
            file=sys.stderr,
        )
        return 2

    config = load_company_config(args.company)
    ensure_company_runtime_compatible(config)

    qbo_path = _resolve_qbo_csv(args, config)
    qbo_df = load_qbo_inventory_item_rows(str(qbo_path))
    qbo_rows = qbo_df.to_dict(orient="records")

    epos_df_full = load_epos_stock_snapshot(args.stock_csv)
    epos_targets: dict[str, float] = {}
    for _, row in epos_df_full.iterrows():
        key = _norm(row.get("base_name"))
        if not key:
            continue
        epos_targets[key] = float(row.get("epos_single_units") or 0)

    in_scope_bases: Optional[set[str]] = None
    if args.category:
        epos_df_cat = load_epos_stock_snapshot(args.stock_csv, categories=args.category)
        in_scope_bases = {
            _norm(r.get("base_name")) for _, r in epos_df_cat.iterrows() if _norm(r.get("base_name"))
        }
    if args.product:
        needle = args.product.strip().lower()
        # Restrict to base names containing the substring. If --category is
        # also active, intersect; otherwise consider every QBO base.
        if in_scope_bases is None:
            qbo_bases = {_norm(r.get("base_name")) for r in qbo_rows if _norm(r.get("base_name"))}
            in_scope_bases = {b for b in qbo_bases if needle in b}
        else:
            in_scope_bases = {b for b in in_scope_bases if needle in b}

    plan = build_consolidation_plan(
        qbo_rows,
        epos_targets,
        company_key=config.company_key,
        in_scope_bases=in_scope_bases,
    )

    if args.output:
        report_path = Path(args.output).expanduser()
    else:
        ts = datetime.now().strftime("%H%M%S")
        report_path = (
            qbo_pack_variant_consolidation_reports_dir()
            / f"qbo_pack_variant_consolidation_{config.company_key}_{ts}.csv"
        )
    write_report(plan, report_path)
    _print_summary(
        config_display_name=config.display_name,
        company_key=config.company_key,
        plan=plan,
        report_path=report_path,
        top_n=args.top,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
