"""Audit-only QBO pack-variant quantity migration planner.

When a pack-variant item like ``WIDGET 330ml*12`` still holds non-zero
``QtyOnHand`` in QBO, the existing :mod:`code_scripts.qbo_pack_variant_cleanup`
tool refuses to inactivate it (we'd lose stock).  Before we build any "move
the qty into the base item then inactivate" apply behaviour, we want to see
**what that migration would actually do**: how many units would shift from
each pack variant into the corresponding base item, in single-unit terms.

This module is a thin, audit-only planner on top of
:func:`code_scripts.qbo_pack_variant_cleanup.audit_pack_variants` — the
**same** classification logic the cleanup tool uses.  No QBO writes happen
here.  No InventoryAdjustment payloads are constructed here.  Apply
behaviour for migration is intentionally out of scope and will be a follow
-up.

Per-row math
============
For every active QBO pack-variant item with ``QtyOnHand != 0``::

    multiplier                       = trailing *N from the variant name
    proposed_base_qty_delta          = pack_qty_on_hand * multiplier
    proposed_pack_variant_qty_delta  = -pack_qty_on_hand

These deltas are *signed* — applying them would zero the variant's QtyOnHand
and add the equivalent number of single units to the base item.

Recommended action
==================
* exactly one active exact-base item exists
    -> ``migration_plan_available``
* zero active base items
    -> ``needs_manual_review`` (``risk_reason=no_active_exact_base_in_qbo``)
* multiple active base items
    -> ``needs_manual_review`` (``risk_reason=multiple_active_exact_base_in_qbo``)

Pack variants with ``QtyOnHand == 0`` are **not** in this report — they're
the cleanup tool's territory and have nothing to migrate.
"""

from __future__ import annotations

import argparse
import csv
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

from code_scripts.artifact_paths import qbo_pack_variant_migration_reports_dir
from code_scripts.company_config import (
    ensure_company_runtime_compatible,
    get_available_companies,
    load_company_config,
)
from code_scripts.qbo_pack_variant_cleanup import (
    audit_pack_variants,
    _filter_by_category,
    _filter_by_product,
    _resolve_qbo_csv,
)
from code_scripts.inventory_sync import load_qbo_inventory_item_rows
from code_scripts.transform import strip_pack_multiplier


_REPORT_FIELDS = [
    "company_key",
    "base_name",
    "base_qbo_item_id",
    "base_qbo_name",
    "pack_variant_item_id",
    "pack_variant_name",
    "multiplier",
    "pack_variant_qty_on_hand",
    "proposed_base_qty_delta",
    "proposed_pack_variant_qty_delta",
    "migration_recommended_action",
    "risk_reason",
]


# ---------------------------------------------------------------------------
# Plan
# ---------------------------------------------------------------------------


def build_migration_plan(audit_records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Convert cleanup audit records into migration plan rows.

    Only rows where ``pack_variant_qty_on_hand != 0`` are included — the
    rest have nothing to migrate (and are handled by the cleanup tool).
    """
    plan: list[dict[str, Any]] = []
    for record in audit_records:
        try:
            qty = float(record.get("pack_variant_qty_on_hand") or 0)
        except (TypeError, ValueError):
            qty = 0.0
        if qty == 0:
            continue

        pack_name = str(record.get("pack_variant_name") or "")
        _, multiplier = strip_pack_multiplier(pack_name)
        if multiplier <= 1:
            # Defensive: audit_pack_variants only emits records for true pack
            # variants, so this branch shouldn't trigger. Skip just in case.
            continue

        risk = str(record.get("risk_reason") or "")
        if risk == "no_active_exact_base_in_qbo":
            action = "needs_manual_review"
            base_delta: Any = ""
            pack_delta: Any = ""
            risk_out = "no_active_exact_base_in_qbo"
        elif risk == "multiple_active_exact_base_in_qbo":
            action = "needs_manual_review"
            base_delta = ""
            pack_delta = ""
            risk_out = "multiple_active_exact_base_in_qbo"
        elif risk == "pack_variant_has_nonzero_qty_on_hand":
            # exactly one active base + nonzero qty -> compute deltas
            action = "migration_plan_available"
            base_delta = qty * multiplier
            pack_delta = -qty
            risk_out = ""
        else:
            # E.g. cleanup classified this row as safe_to_inactivate (qty=0)
            # but qty != 0 here — defensive skip rather than guess.
            continue

        plan.append({
            "company_key": record.get("company_key", ""),
            "base_name": record.get("base_name", ""),
            "base_qbo_item_id": record.get("base_qbo_item_id", ""),
            "base_qbo_name": record.get("base_qbo_name", ""),
            "pack_variant_item_id": record.get("pack_variant_item_id", ""),
            "pack_variant_name": pack_name,
            "multiplier": multiplier,
            "pack_variant_qty_on_hand": qty,
            "proposed_base_qty_delta": base_delta,
            "proposed_pack_variant_qty_delta": pack_delta,
            "migration_recommended_action": action,
            "risk_reason": risk_out,
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
            "Audit-only QBO pack-variant qty migration planner. Reports how "
            "non-zero pack-variant QtyOnHand would migrate into the base item. "
            "Does NOT call QBO update — apply behaviour is intentionally out "
            "of scope."
        ),
    )
    available = get_available_companies()
    parser.add_argument("--company", required=True, choices=available)
    parser.add_argument(
        "--qbo-csv",
        default=None,
        help="Pre-existing QBO Items snapshot CSV. If omitted, --auto-fetch-qbo "
             "is used to query QBO directly.",
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
        help="Restrict the plan to base names that appear in EPOS rows with "
             "this CategoryName (case-insensitive, exact match). Requires "
             "--stock-csv. May be supplied multiple times.",
    )
    parser.add_argument(
        "--stock-csv",
        default=None,
        help="EPOS StockReport CSV; required when --category is used.",
    )
    parser.add_argument(
        "--product",
        default=None,
        help="Substring filter (case-insensitive) on pack_variant_name or "
             "base_name.",
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
        help="Number of rows to print under the 'top by proposed_base_qty_delta' "
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
        a = row["migration_recommended_action"]
        by_action[a] = by_action.get(a, 0) + 1

    eligible = [r for r in plan if r["migration_recommended_action"] == "migration_plan_available"]
    eligible_sorted = sorted(
        eligible,
        key=lambda r: abs(float(r["proposed_base_qty_delta"] or 0)),
        reverse=True,
    )[:top_n]

    bar = "=" * 78
    print(bar)
    print(f"QBO pack-variant migration plan: {config_display_name} ({company_key})")
    print(f"Rows scanned (non-zero pack qty): {len(plan)}")
    for action in ("migration_plan_available", "needs_manual_review"):
        print(f"  {action}: {by_action.get(action, 0)}")
    if eligible_sorted:
        print("-" * 78)
        print(f"Top {len(eligible_sorted)} by |proposed_base_qty_delta|:")
        print(
            f"  {'item_id':>10}  {'mult':>5}  {'pack_qty':>9}  "
            f"{'base_delta':>11}  base_name"
        )
        for r in eligible_sorted:
            print(
                f"  {r['pack_variant_item_id']:>10}  "
                f"{r['multiplier']:>5}  "
                f"{r['pack_variant_qty_on_hand']:>9}  "
                f"{r['proposed_base_qty_delta']:>11}  "
                f"{r['base_name']}"
            )
    print("-" * 78)
    print(f"Wrote report: {report_path}")
    print(bar)


def main(argv: Optional[list[str]] = None) -> int:
    args = build_parser().parse_args(argv)

    config = load_company_config(args.company)
    ensure_company_runtime_compatible(config)

    qbo_path = _resolve_qbo_csv(args, config)
    qbo_df = load_qbo_inventory_item_rows(str(qbo_path))
    qbo_rows = [
        {
            "Id": str(r.get("Id") or "").strip(),
            "Name": str(r.get("Name") or "").strip(),
            "Type": str(r.get("Type") or "").strip(),
            "QtyOnHand": r.get("qbo_qty_on_hand") if "qbo_qty_on_hand" in r else r.get("QtyOnHand", ""),
        }
        for r in qbo_df.to_dict(orient="records")
    ]

    audit_records = audit_pack_variants(qbo_rows, company_key=config.company_key)
    audit_records = _filter_by_product(audit_records, args.product or "")
    audit_records = _filter_by_category(
        audit_records,
        stock_csv=args.stock_csv,
        categories=args.category,
    )

    plan = build_migration_plan(audit_records)

    if args.output:
        report_path = Path(args.output).expanduser()
    else:
        ts = datetime.now().strftime("%H%M%S")
        report_path = (
            qbo_pack_variant_migration_reports_dir()
            / f"qbo_pack_variant_migration_{config.company_key}_{ts}.csv"
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
