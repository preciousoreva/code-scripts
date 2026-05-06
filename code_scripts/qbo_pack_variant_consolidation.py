"""QBO pack-variant consolidation planner (target-based) with audit / dry-run / apply.

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

Default invocation is **audit-only** — no QBO writes, no payloads built,
just the report CSV.

``--dry-run`` builds the exact ``InventoryAdjustment`` payloads that
``--apply`` would POST and prints them, but does **not** call QBO.

``--apply`` POSTs one ``InventoryAdjustment`` per consolidation_plan_available
row using ``code_scripts.qbo_inventory_adjustment.post_inventory_adjustment``,
under strict safety guards:

* ``--apply`` is mutually exclusive with ``--dry-run``;
* ``--apply`` requires ``--max-products`` (> 0);
* ``--apply`` requires either ``--product`` or ``--category`` to scope the
  run (no whole-catalog applies);
* ``--apply`` requires ``qbo.inventory_adjustment_account_id`` to be
  configured for the company;
* rows whose ``|base_qty_diff_to_target|`` exceeds ``--max-abs-base-diff``
  (default 1000) are blocked;
* rows whose ``planned_line_count`` exceeds ``--max-lines`` (default 10)
  are blocked;
* a ``GlobalRunLock`` is held for the duration of the apply;
* the cached QBO snapshot is marked stale on any successful POST so that
  the next inventory_sync / consolidation run refreshes from QBO;
* pack-variant items are **not** inactivated by this command — that
  remains the cleanup tool's responsibility once their QtyOnHand has
  been driven to zero by a successful apply here.

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
import json
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
from code_scripts.inventory_safety import assert_inventory_apply_allowed
from code_scripts.inventory_sync import (
    load_epos_stock_snapshot,
    load_qbo_inventory_item_rows,
)
from code_scripts.qbo_inventory_adjustment import (
    build_inventory_adjustment_payload,
    post_inventory_adjustment,
)
from code_scripts.qbo_pack_variant_cleanup import _resolve_qbo_csv
from code_scripts.qbo_snapshot_cache import mark_qbo_snapshot_stale
from code_scripts.qbo_upload import TokenManager
from code_scripts.run_lock import GlobalRunLock
from code_scripts.token_manager import verify_realm_match


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
# Apply / dry-run payload helpers
# ---------------------------------------------------------------------------


def build_doc_number(txn_date: str, base_item_id: str) -> str:
    """Deterministic InventoryAdjustment DocNumber for a given (date, base item).

    QBO requires a non-null ``DocNumber`` on the ``InventoryAdjustment``
    entity (rejects the POST with HTTP 400 / code 2010 otherwise).  We
    derive a stable, idempotent value from the operation's ``TxnDate``
    and the base QBO Item id::

        INVCON-{YYYYMMDD}-{base_item_id}

    Same company, same date, same base -> same DocNumber.  Re-runs of the
    same consolidation against the same item on the same day will collide
    on DocNumber and QBO will surface that as a duplicate, which is the
    behaviour we want (no accidental double-posting).
    """
    date_compact = str(txn_date or "").strip()[:10].replace("-", "")
    item_str = str(base_item_id or "").strip()
    if not date_compact or not item_str:
        return ""
    return f"INVCON-{date_compact}-{item_str}"


def build_lines_from_plan_row(row: dict[str, Any]) -> list[dict[str, Any]]:
    """Convert one consolidation_plan_available row into InventoryAdjustment lines.

    Skips zero-diff entries (no point posting a no-op line).  Returns the
    list ready for :func:`code_scripts.qbo_inventory_adjustment.build_inventory_adjustment_payload`.
    """
    lines: list[dict[str, Any]] = []

    base_id = str(row.get("base_qbo_item_id") or "").strip()
    base_diff = _to_float(row.get("base_qty_diff_to_target"))
    if base_id and base_diff != 0:
        lines.append({"item_id": base_id, "qty_diff": base_diff})

    pack_ids = [
        p.strip()
        for p in str(row.get("pack_variant_item_ids") or "").split(_LIST_DELIMITER)
        if p.strip()
    ]
    pack_diffs = [
        _to_float(d)
        for d in str(row.get("pack_variant_qty_diffs_to_zero") or "").split(_LIST_DELIMITER)
        if d.strip() != ""
    ]
    for pid, pdiff in zip(pack_ids, pack_diffs):
        if pid and pdiff != 0:
            lines.append({"item_id": pid, "qty_diff": pdiff})

    return lines


def _scope_description(args: argparse.Namespace) -> str:
    parts: list[str] = []
    if args.category:
        parts.append("category=" + ", ".join(args.category))
    if args.product:
        parts.append(f"product={args.product}")
    return "; ".join(parts)


def build_private_note(row: dict[str, Any], scope_description: str = "") -> str:
    """Compose the PrivateNote string for a consolidation InventoryAdjustment."""
    pack_ids = str(row.get("pack_variant_item_ids") or "").replace(_LIST_DELIMITER, ", ")
    parts = [
        "OIAT pack variant consolidation",
        f"base: {row.get('base_name', '')}",
        f"base item id: {row.get('base_qbo_item_id', '')}",
        f"EPOS single-unit target: {row.get('epos_single_units_target', '')}",
    ]
    if pack_ids:
        parts.append(f"pack item ids: {pack_ids}")
    if scope_description:
        parts.append(f"scope: {scope_description}")
    return "\n".join(parts)


def is_duplicate_doc_number_error(exc: BaseException) -> bool:
    """Detect QBO's "Duplicate Document Number" rejection by error string.

    QBO returns this as ``ValidationFault`` ``code=6240`` with message text
    that contains the phrase ``Duplicate Document Number``. We compare on
    the stringified exception so this works regardless of whether the
    caller wrapped the original ``RuntimeError`` from
    :func:`code_scripts.qbo_inventory_adjustment.post_inventory_adjustment`.

    Returns ``True`` for both the explicit code (``"6240"``) and the
    English phrase, so a future minor-version change in either field
    still trips the check.
    """
    text = str(exc).lower()
    return ("6240" in text) or ("duplicate document number" in text)


def _classify_for_apply(
    plan: list[dict[str, Any]],
    *,
    max_abs_base_diff: float,
    max_lines: int,
) -> tuple[list[dict[str, Any]], list[tuple[dict[str, Any], str]]]:
    """Split eligible rows into (postable, blocked-by-safety-cap).

    Only rows with ``consolidation_recommended_action == 'consolidation_plan_available'``
    are considered eligible.  Of those, rows that exceed ``max_abs_base_diff``
    or ``max_lines`` are returned in the blocked list with a human-readable
    reason string.
    """
    postable: list[dict[str, Any]] = []
    blocked: list[tuple[dict[str, Any], str]] = []
    for row in plan:
        if row["consolidation_recommended_action"] != "consolidation_plan_available":
            continue
        diff = abs(_to_float(row.get("base_qty_diff_to_target")))
        if diff > max_abs_base_diff:
            blocked.append((row, f"base_qty_diff_to_target {_format_float(diff)} > --max-abs-base-diff {_format_float(max_abs_base_diff)}"))
            continue
        try:
            line_count = int(row.get("planned_line_count") or 0)
        except (TypeError, ValueError):
            line_count = 0
        if line_count > max_lines:
            blocked.append((row, f"planned_line_count {line_count} > --max-lines {max_lines}"))
            continue
        postable.append(row)
    return postable, blocked


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Target-based QBO pack-variant consolidation planner. Default mode "
            "is audit-only (writes a report CSV). Pass --dry-run to preview "
            "InventoryAdjustment payloads without posting, or --apply (with "
            "scoping + safety caps) to post them to QBO. Pack variants are "
            "NOT inactivated by this command — that remains the cleanup tool's "
            "responsibility once their QtyOnHand has been driven to zero here."
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

    # --- dry-run / apply ------------------------------------------------
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Build the exact InventoryAdjustment payloads that --apply would "
             "POST and print them, but do NOT call QBO.",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="POST one InventoryAdjustment per consolidation_plan_available row. "
             "Mutually exclusive with --dry-run. Requires --max-products plus "
             "either --product or --category to scope the run, and "
             "qbo.inventory_adjustment_account_id configured for the company.",
    )
    parser.add_argument(
        "--max-products",
        type=int,
        default=None,
        help="Hard cap on how many consolidation_plan_available rows to "
             "post in --apply mode (and to preview in --dry-run).",
    )
    parser.add_argument(
        "--max-abs-base-diff",
        type=float,
        default=1000.0,
        help="Block any row whose |base_qty_diff_to_target| exceeds this "
             "magnitude (default: 1000). Raise explicitly if you really mean "
             "to post a larger single-item adjustment.",
    )
    parser.add_argument(
        "--max-lines",
        type=int,
        default=10,
        help="Block any row whose planned_line_count (1 + active pack "
             "variants) exceeds this (default: 10).",
    )
    parser.add_argument(
        "--txn-date",
        default=None,
        help="TxnDate for the InventoryAdjustment (YYYY-MM-DD). Defaults to today.",
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

    # --- Mode validation (before we touch anything) -----------------------
    if args.apply and args.dry_run:
        print("Error: pass either --apply or --dry-run, not both.", file=sys.stderr)
        return 2
    if args.max_products is not None and args.max_products <= 0:
        print("Error: --max-products must be > 0.", file=sys.stderr)
        return 2
    if args.apply:
        if args.max_products is None:
            print("Error: --apply requires --max-products.", file=sys.stderr)
            return 2
        if not args.product and not args.category:
            print(
                "Error: --apply requires --product or --category to scope the run; "
                "whole-catalog applies are intentionally not allowed.",
                file=sys.stderr,
            )
            return 2

    if not args.qbo_csv and not args.auto_fetch_qbo:
        print(
            "Error: pass either --qbo-csv <path> or --auto-fetch-qbo so we have "
            "QBO data to plan against.",
            file=sys.stderr,
        )
        return 2

    config = load_company_config(args.company)
    ensure_company_runtime_compatible(config)

    if args.apply:
        assert_inventory_apply_allowed(config, action="pack_variant_consolidation_apply")
        adjust_account_id = (config.inventory_adjustment_account_id or "").strip()
        if not adjust_account_id:
            print(
                "Error: qbo.inventory_adjustment_account_id is not configured for "
                f"company '{config.company_key}'. Apply mode refuses to post "
                "without an adjust account.",
                file=sys.stderr,
            )
            return 2
    else:
        adjust_account_id = (config.inventory_adjustment_account_id or "").strip()

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

    if not args.apply and not args.dry_run:
        return 0

    # ----------------------- dry-run / apply -----------------------------
    postable, blocked = _classify_for_apply(
        plan,
        max_abs_base_diff=args.max_abs_base_diff,
        max_lines=args.max_lines,
    )
    if args.max_products is not None:
        capped = postable[: args.max_products]
        skipped_due_to_cap = postable[args.max_products :]
    else:
        capped = postable
        skipped_due_to_cap = []

    txn_date = (args.txn_date or datetime.now().strftime("%Y-%m-%d")).strip()
    scope_desc = _scope_description(args)
    mode_label = "APPLY" if args.apply else "DRY-RUN"

    print()
    print(f"Mode: {mode_label}")
    print(f"Eligible (consolidation_plan_available): {sum(1 for r in plan if r['consolidation_recommended_action'] == 'consolidation_plan_available')}")
    print(
        f"After safety caps (--max-abs-base-diff={_format_float(args.max_abs_base_diff)}, "
        f"--max-lines={args.max_lines}): postable={len(postable)} blocked={len(blocked)}"
    )
    if blocked:
        for row, reason in blocked:
            print(f"  [BLOCKED] base={row['base_name']!r}: {reason}")
    if args.max_products is not None:
        print(f"After --max-products cap: will {'post' if args.apply else 'preview'} {len(capped)} (skipped={len(skipped_due_to_cap)})")
    else:
        print(f"Will {'post' if args.apply else 'preview'}: {len(capped)}")

    # Build payloads (and either print them, or post them).
    attempted = succeeded = failed = no_op = 0
    failures: list[tuple[str, str]] = []
    token_mgr: Optional[TokenManager] = None
    run_lock: Optional[GlobalRunLock] = None

    if args.apply:
        verify_realm_match(config.company_key, config.realm_id)
        token_mgr = TokenManager(config.company_key, config.realm_id)
        run_lock = GlobalRunLock(holder=f"qbo_pack_variant_consolidation:{config.company_key}")
        lock_result = run_lock.acquire()
        if not lock_result.acquired:
            print(
                f"Error: another pipeline run is active ({lock_result.reason}); "
                "refusing to --apply consolidation.",
                file=sys.stderr,
            )
            return 2

    try:
        for row in capped:
            attempted += 1
            lines = build_lines_from_plan_row(row)
            if not lines:
                no_op += 1
                print(f"[SKIP] base={row['base_name']!r} has no non-zero diffs; nothing to post.")
                continue
            doc_number = build_doc_number(
                txn_date=txn_date,
                base_item_id=str(row.get("base_qbo_item_id", "")),
            )
            payload = build_inventory_adjustment_payload(
                adjust_account_id=adjust_account_id or "",
                txn_date=txn_date,
                private_note=build_private_note(row, scope_description=scope_desc),
                lines=lines,
                doc_number=doc_number,
            )
            print(
                f"[{mode_label}-PLAN] base={row['base_name']!r} "
                f"item_id={row['base_qbo_item_id']} target={row['epos_single_units_target']} "
                f"base_diff={row['base_qty_diff_to_target']} packs={row['pack_variant_item_ids']}"
            )
            print("              payload=" + json.dumps(payload, separators=(",", ":")))
            if args.dry_run:
                continue

            # Apply path
            try:
                resp = post_inventory_adjustment(token_mgr, config.realm_id, payload)
                inv_adj = (resp or {}).get("InventoryAdjustment", {})
                doc = inv_adj.get("DocNumber") or inv_adj.get("Id")
                print(f"[OK] Posted InventoryAdjustment doc/id={doc} for base={row['base_name']!r}")
                succeeded += 1
            except Exception as exc:  # noqa: BLE001
                failed += 1
                failures.append((str(row.get("base_qbo_item_id", "")), str(exc)))
                if is_duplicate_doc_number_error(exc):
                    # Friendly hint when QBO rejects on DocNumber collision.
                    # Our DocNumbers are deterministic per (date, base item),
                    # so a duplicate here usually means we already posted this
                    # exact consolidation today.  We do NOT silently treat
                    # the duplicate as success — the operator should verify
                    # the resulting QBO state before retrying.
                    print(
                        f"[DUPLICATE] base={row['base_name']!r} "
                        f"item_id={row['base_qbo_item_id']} "
                        f"DocNumber={doc_number}: QBO rejected as a duplicate. "
                        f"This consolidation may have already been applied for "
                        f"this base item on {txn_date}. Verify base / pack "
                        f"QtyOnHand in QBO before re-running with --txn-date "
                        f"set to a different date.",
                        file=sys.stderr,
                    )
                else:
                    print(
                        f"[FAIL] base={row['base_name']!r} item_id={row['base_qbo_item_id']}: {exc}",
                        file=sys.stderr,
                    )
    finally:
        if args.apply:
            if succeeded > 0:
                mark_qbo_snapshot_stale(
                    config.company_key, reason="pack_variant_consolidation_applied"
                )
                print("[INFO] Marked cached QBO snapshot stale after consolidation apply.")
            if run_lock is not None:
                run_lock.release()

    print("-" * 78)
    print(
        f"{mode_label} summary: attempted={attempted} succeeded={succeeded} "
        f"failed={failed} no_op={no_op} blocked={len(blocked)} "
        f"skipped_due_to_cap={len(skipped_due_to_cap)}"
    )
    if failures:
        for item_id, err in failures:
            print(f"  fail: id={item_id} -> {err}", file=sys.stderr)

    # Optional, non-blocking Slack notify — only on real apply runs (the
    # dry-run already prints the planned payloads to stdout).
    if args.apply:
        webhook = getattr(config, "slack_webhook_url", None)
        if webhook:
            try:
                from code_scripts.inventory_notifications import (
                    format_pack_variant_apply_summary,
                )
                from code_scripts.slack_notify import send_slack_success

                send_slack_success(
                    format_pack_variant_apply_summary(
                        kind="pack_variant_consolidation",
                        company_display_name=config.display_name,
                        company_key=config.company_key,
                        mode="apply",
                        scope=scope_desc,
                        counts={
                            "attempted": attempted,
                            "succeeded": succeeded,
                            "failed": failed,
                            "no_op": no_op,
                            "blocked": len(blocked),
                            "skipped_due_to_cap": len(skipped_due_to_cap),
                        },
                        report_path=str(report_path),
                    ),
                    webhook,
                )
            except Exception as notify_exc:  # noqa: BLE001 — never fail the run
                print(f"[WARN] Slack notify failed (ignored): {notify_exc}", file=sys.stderr)

    return 0 if failed == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
