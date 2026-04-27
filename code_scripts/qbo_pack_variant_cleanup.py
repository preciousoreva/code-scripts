"""Audit + (optional) cleanup of QBO inventory pack-size variants.

Background
==========
The sales-receipt pipeline normalises product names to a "base unit" by
stripping a trailing ``*N`` pack multiplier (see
``code_scripts.transform.strip_pack_multiplier``).  When operators have
historically created **separate** QBO Inventory items per pack size
(``PRODUCT*6``, ``PRODUCT*12``, ``PRODUCT*24``), the inventory audit ends up
with many ``ambiguous_in_qbo`` rows because each EPOS base name resolves to
multiple active QBO items.

This tool walks the active QBO Inventory catalogue and proposes inactivation
of the pack-variant items, **only** when:

* an active exact-base item (same name, no ``*N``) already exists, *and*
* the pack variant's ``QtyOnHand`` is zero (so we don't lose stock by
  inactivating).

Outputs a CSV report.  ``--apply`` performs sparse QBO updates that rename
the variant to ``"{original_name} (old-{item_id})"`` and set
``Active=false`` in a single ``POST /v3/company/{realm}/item`` call.

This module is deliberately a thin layer on top of helpers already shipping
on the inventory-sync-focus branch — it reuses
:func:`code_scripts.transform.strip_pack_multiplier`,
:func:`code_scripts.inventory_sync.fetch_qbo_inventory_items_snapshot` and
:func:`load_qbo_inventory_item_rows`,
:func:`code_scripts.qbo_snapshot_cache.mark_qbo_snapshot_stale`,
:class:`code_scripts.qbo_upload.TokenManager`,
:func:`code_scripts.qbo_upload._make_qbo_request`, and the existing run-lock
+ realm-match guards.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Optional
from urllib.parse import quote

from code_scripts.artifact_paths import qbo_pack_variant_reports_dir
from code_scripts.company_config import (
    ensure_company_runtime_compatible,
    get_available_companies,
    get_qbo_api_base_url,
    load_company_config,
)
from code_scripts.inventory_sync import (
    fetch_qbo_inventory_items_snapshot,
    load_qbo_inventory_item_rows,
)
from code_scripts.qbo_snapshot_cache import get_qbo_snapshot_path, mark_qbo_snapshot_stale
from code_scripts.qbo_upload import TokenManager, _make_qbo_request
from code_scripts.run_lock import GlobalRunLock
from code_scripts.token_manager import verify_realm_match
from code_scripts.transform import strip_pack_multiplier


_QBO_MINOR_VERSION = "75"
_REPORT_FIELDS = [
    "company_key",
    "base_name",
    "base_qbo_item_id",
    "base_qbo_name",
    "pack_variant_item_id",
    "pack_variant_name",
    "pack_variant_qty_on_hand",
    "pack_variant_active",
    "base_qbo_active",
    "pack_variant_sync_token",
    "recommended_action",
    "risk_reason",
    "apply_eligible",
    "apply_block_reason",
]


# ---------------------------------------------------------------------------
# Audit
# ---------------------------------------------------------------------------


def _norm_key(value: Any) -> str:
    return str(value or "").strip().lower()


def _build_active_base_index(qbo_rows: Iterable[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    """Index active QBO items keyed on **the literal item name (lowercased)**.

    A "base item" is any active item whose own name has no trailing pack
    multiplier — i.e. ``strip_pack_multiplier(name)[1] == 1``.  Pack-variant
    items go into the index under their own (variant) name; lookup of a base
    only finds true base items because the index key is the literal name.
    """
    index: dict[str, list[dict[str, Any]]] = {}
    for row in qbo_rows:
        name = str(row.get("Name") or "").strip()
        if not name:
            continue
        _, multiplier = strip_pack_multiplier(name)
        if multiplier != 1:
            continue  # not a base candidate
        index.setdefault(_norm_key(name), []).append(row)
    return index


def _qty_on_hand(row: dict[str, Any]) -> float:
    raw = row.get("QtyOnHand", "")
    if raw in (None, ""):
        return 0.0
    try:
        return float(raw)
    except (TypeError, ValueError):
        return 0.0


def audit_pack_variants(
    qbo_rows: list[dict[str, Any]],
    *,
    company_key: str,
) -> list[dict[str, Any]]:
    """Walk all active QBO inventory items and classify each pack variant.

    Returns one record per pack-variant item.  Items whose name has no ``*N``
    suffix are not pack variants and are skipped.
    """
    base_index = _build_active_base_index(qbo_rows)
    records: list[dict[str, Any]] = []

    for row in qbo_rows:
        name = str(row.get("Name") or "").strip()
        if not name:
            continue
        base_name, multiplier = strip_pack_multiplier(name)
        base_name = base_name.strip()
        if multiplier == 1 or not base_name or base_name.lower() == name.lower():
            continue  # not a pack variant

        candidates = base_index.get(_norm_key(base_name), [])
        pack_qty = _qty_on_hand(row)

        if len(candidates) == 0:
            recommended = "needs_manual_review"
            risk = "no_active_exact_base_in_qbo"
        elif len(candidates) > 1:
            recommended = "needs_manual_review"
            risk = "multiple_active_exact_base_in_qbo"
        elif pack_qty != 0:
            recommended = "needs_manual_review"
            risk = "pack_variant_has_nonzero_qty_on_hand"
        else:
            recommended = "safe_to_inactivate_pack_variant"
            risk = ""

        base_id = candidates[0].get("Id", "") if len(candidates) == 1 else ""
        base_name_actual = candidates[0].get("Name", "") if len(candidates) == 1 else ""

        records.append({
            "company_key": company_key,
            "base_name": base_name,
            "base_qbo_item_id": str(base_id or "").strip(),
            "base_qbo_name": str(base_name_actual or "").strip(),
            "pack_variant_item_id": str(row.get("Id", "") or "").strip(),
            "pack_variant_name": name,
            "pack_variant_qty_on_hand": pack_qty,
            # All rows in the snapshot are filtered to Active=true at fetch
            # time; surface as constants so the report is self-describing.
            "pack_variant_active": True,
            "base_qbo_active": True if len(candidates) == 1 else "",
            "pack_variant_sync_token": "",  # populated only at apply time
            "recommended_action": recommended,
            "risk_reason": risk,
            "apply_eligible": recommended == "safe_to_inactivate_pack_variant",
            "apply_block_reason": "" if recommended == "safe_to_inactivate_pack_variant" else risk,
        })

    return records


# ---------------------------------------------------------------------------
# Filters
# ---------------------------------------------------------------------------


def _filter_by_product(records: list[dict[str, Any]], product_filter: str) -> list[dict[str, Any]]:
    needle = (product_filter or "").strip().lower()
    if not needle:
        return records
    return [r for r in records if needle in r["pack_variant_name"].lower()
            or needle in r["base_name"].lower()]


def _load_epos_categories_for_bases(
    stock_csv_path: str,
    bases: set[str],
    categories: list[str],
) -> set[str]:
    """Return the subset of ``bases`` whose EPOS category matches ``categories``.

    Reuses ``code_scripts.inventory_sync.load_epos_stock_snapshot``'s column
    conventions implicitly via direct CSV read so we don't pull in pandas
    aggregation just for a filter.
    """
    import pandas as pd

    df = pd.read_csv(stock_csv_path)
    name_col = "Name" if "Name" in df.columns else None
    cat_col = "CategoryName" if "CategoryName" in df.columns else (
        "Category" if "Category" in df.columns else None
    )
    if not name_col or not cat_col:
        raise ValueError(
            f"--category requires both name + category columns in --stock-csv. "
            f"Got columns: {list(df.columns)}"
        )

    requested = {c.strip().lower() for c in categories if c and c.strip()}
    if not requested:
        return bases

    df_cat = df[df[cat_col].astype(str).str.strip().str.lower().isin(requested)]
    epos_bases: set[str] = set()
    for raw_name in df_cat[name_col].astype(str).tolist():
        base, _ = strip_pack_multiplier(raw_name)
        epos_bases.add(base.strip().lower())

    return {b for b in bases if b.lower() in epos_bases}


def _filter_by_category(
    records: list[dict[str, Any]],
    *,
    stock_csv: Optional[str],
    categories: list[str],
) -> list[dict[str, Any]]:
    if not categories:
        return records
    if not stock_csv:
        raise SystemExit(
            "--category requires --stock-csv to map QBO base names to EPOS categories. "
            "Re-run with --stock-csv pointing at an EPOS StockReport CSV that contains "
            "the products you want to scope the cleanup to."
        )
    bases = {r["base_name"] for r in records}
    keep = _load_epos_categories_for_bases(stock_csv, bases, categories)
    keep_lower = {b.lower() for b in keep}
    return [r for r in records if r["base_name"].lower() in keep_lower]


# ---------------------------------------------------------------------------
# Report
# ---------------------------------------------------------------------------


def write_report(records: list[dict[str, Any]], output_path: Path) -> Path:
    output_path = output_path.expanduser()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=_REPORT_FIELDS)
        writer.writeheader()
        for record in records:
            writer.writerow({k: record.get(k, "") for k in _REPORT_FIELDS})
    return output_path


# ---------------------------------------------------------------------------
# QBO sparse update
# ---------------------------------------------------------------------------


def _fetch_item_with_sync_token(token_mgr: TokenManager, realm_id: str, item_id: str) -> dict[str, Any]:
    base_url = get_qbo_api_base_url()
    url = f"{base_url}/v3/company/{realm_id}/item/{quote(str(item_id))}?minorversion={_QBO_MINOR_VERSION}"
    resp = _make_qbo_request("GET", url, token_mgr)
    if resp.status_code != 200:
        raise RuntimeError(
            f"QBO Item GET failed: HTTP {resp.status_code} {resp.text[:500] if resp.text else ''}"
        )
    payload = resp.json() or {}
    item = payload.get("Item")
    if not isinstance(item, dict):
        raise RuntimeError(f"QBO Item GET returned no Item payload for id={item_id}")
    return item


def build_inactivate_payload(item_id: str, sync_token: str, original_name: str) -> dict[str, Any]:
    return {
        "Id": str(item_id),
        "SyncToken": str(sync_token),
        "sparse": True,
        "Name": f"{original_name} (old-{item_id})",
        "Active": False,
    }


def _post_inactivate(token_mgr: TokenManager, realm_id: str, payload: dict[str, Any]) -> dict[str, Any]:
    base_url = get_qbo_api_base_url()
    url = f"{base_url}/v3/company/{realm_id}/item?minorversion={_QBO_MINOR_VERSION}"
    resp = _make_qbo_request("POST", url, token_mgr, json=payload)
    if resp.status_code not in (200, 201):
        raise RuntimeError(
            f"QBO sparse update failed: HTTP {resp.status_code} {resp.text[:500] if resp.text else ''}"
        )
    return resp.json() or {}


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Audit / inactivate QBO inventory pack-variant items.",
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
        help="Query QBO for the active inventory items snapshot before auditing.",
    )
    parser.add_argument(
        "--qbo-cache-max-age-hours",
        type=int,
        default=24,
        help="When auto-fetching, reuse a cached QBO snapshot if it is younger "
             "than this many hours (default: 24).",
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
        help="Restrict the audit to base names that appear in EPOS rows with "
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
        help="Substring filter (case-insensitive) on either pack_variant_name "
             "or base_name; useful for piloting on a single item.",
    )
    parser.add_argument(
        "--output",
        default=None,
        help="Override the report CSV output path.",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="POST sparse updates to QBO (rename + Active=false) for rows whose "
             "recommended_action is safe_to_inactivate_pack_variant.",
    )
    parser.add_argument(
        "--max-items",
        type=int,
        default=None,
        help="Required with --apply. Hard cap on the number of items to update.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the planned QBO sparse update payloads without calling QBO.",
    )
    return parser


def _resolve_qbo_csv(args, config) -> Path:
    if args.qbo_csv:
        path = Path(args.qbo_csv).expanduser()
        if not path.exists():
            raise SystemExit(f"--qbo-csv path does not exist: {path}")
        return path

    if not args.auto_fetch_qbo:
        raise SystemExit(
            "Need either --qbo-csv <path> or --auto-fetch-qbo. Without one of "
            "those there is no QBO inventory data to audit."
        )

    snapshot_path = (
        Path(args.qbo_export_path).expanduser()
        if args.qbo_export_path
        else get_qbo_snapshot_path(config.company_key)
    )
    return fetch_qbo_inventory_items_snapshot(
        company_key=config.company_key,
        realm_id=config.realm_id,
        output_path=snapshot_path,
        cache_max_age_hours=args.qbo_cache_max_age_hours,
        force_refresh=args.qbo_force_refresh,
    )


def _print_apply_intent(record: dict[str, Any]) -> None:
    print(
        f"[APPLY-PLAN] item_id={record['pack_variant_item_id']} "
        f"name={record['pack_variant_name']!r} -> rename + Active=false; "
        f"base={record['base_qbo_name']!r} (id={record['base_qbo_item_id']})"
    )


def main(argv: Optional[list[str]] = None) -> int:
    args = build_parser().parse_args(argv)

    if args.apply and args.dry_run:
        print("Error: pass either --apply or --dry-run, not both.", file=sys.stderr)
        return 2
    if args.apply:
        if args.max_items is None:
            print("Error: --apply requires --max-items.", file=sys.stderr)
            return 2
        if args.max_items <= 0:
            print("Error: --max-items must be > 0.", file=sys.stderr)
            return 2

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

    records = audit_pack_variants(qbo_rows, company_key=config.company_key)
    records = _filter_by_product(records, args.product or "")
    records = _filter_by_category(records, stock_csv=args.stock_csv, categories=args.category)

    if args.output:
        report_path = Path(args.output).expanduser()
    else:
        ts = datetime.now().strftime("%H%M%S")
        report_path = qbo_pack_variant_reports_dir() / f"qbo_pack_variant_audit_{config.company_key}_{ts}.csv"

    write_report(records, report_path)

    by_action: dict[str, int] = {}
    for r in records:
        by_action[r["recommended_action"]] = by_action.get(r["recommended_action"], 0) + 1
    print("=" * 68)
    print(f"QBO pack-variant audit: {config.display_name} ({config.company_key})")
    print(f"Pack variants found:  {len(records)}")
    for action, count in sorted(by_action.items()):
        print(f"  {action}: {count}")
    print(f"Wrote report: {report_path}")
    print("=" * 68)

    if not args.apply and not args.dry_run:
        return 0

    safe_records = [r for r in records if r["apply_eligible"]]
    if args.dry_run:
        print(f"Dry-run: {len(safe_records)} rows are eligible for inactivation.")
        for r in safe_records:
            _print_apply_intent(r)
            payload = build_inactivate_payload(
                item_id=r["pack_variant_item_id"],
                sync_token="<would-fetch-at-apply-time>",
                original_name=r["pack_variant_name"],
            )
            print(f"          payload={json.dumps(payload, separators=(',', ':'))}")
        return 0

    # --- apply mode ---
    if not safe_records:
        print("Nothing to apply: no rows have recommended_action=safe_to_inactivate_pack_variant.")
        return 0

    capped = safe_records[: args.max_items]
    print(f"Apply: {len(capped)} of {len(safe_records)} eligible rows (cap={args.max_items}).")
    for r in capped:
        _print_apply_intent(r)

    verify_realm_match(config.company_key, config.realm_id)
    token_mgr = TokenManager(config.company_key, config.realm_id)

    run_lock = GlobalRunLock(holder=f"qbo_pack_variant_cleanup:{config.company_key}")
    lock_result = run_lock.acquire()
    if not lock_result.acquired:
        print(
            f"Error: another pipeline run is active ({lock_result.reason}); "
            "refusing to --apply pack-variant cleanup.",
            file=sys.stderr,
        )
        return 2

    attempted = succeeded = failed = 0
    failures: list[tuple[str, str]] = []
    try:
        for record in capped:
            attempted += 1
            item_id = record["pack_variant_item_id"]
            try:
                live = _fetch_item_with_sync_token(token_mgr, config.realm_id, item_id)
                sync_token = str(live.get("SyncToken", "")).strip()
                if not sync_token and sync_token != "0":
                    raise RuntimeError(f"QBO returned no SyncToken for item id={item_id}")
                payload = build_inactivate_payload(
                    item_id=item_id,
                    sync_token=sync_token,
                    original_name=record["pack_variant_name"],
                )
                _post_inactivate(token_mgr, config.realm_id, payload)
                succeeded += 1
                print(f"[OK] Inactivated item_id={item_id} {record['pack_variant_name']!r}")
            except Exception as exc:  # noqa: BLE001 — surface and continue
                failed += 1
                failures.append((item_id, str(exc)))
                print(f"[FAIL] item_id={item_id} {record['pack_variant_name']!r}: {exc}", file=sys.stderr)
    finally:
        if succeeded > 0:
            mark_qbo_snapshot_stale(
                config.company_key, reason="pack_variant_cleanup_applied"
            )
            print("[INFO] Marked cached QBO snapshot stale after pack-variant cleanup.")
        run_lock.release()

    skipped = len(safe_records) - len(capped)
    print("-" * 68)
    print(
        f"Apply summary: attempted={attempted} succeeded={succeeded} "
        f"failed={failed} skipped_due_to_cap={skipped}"
    )
    if failures:
        for item_id, err in failures:
            print(f"  fail: id={item_id} -> {err}", file=sys.stderr)

    _maybe_notify_slack(config, attempted, succeeded, failed, skipped, report_path)

    return 0 if failed == 0 else 1


def _maybe_notify_slack(
    config,
    attempted: int,
    succeeded: int,
    failed: int,
    skipped: int,
    report_path: Path,
) -> None:
    """Send a Slack notification iff the company has a webhook configured.

    Failures here must NOT fail the cleanup command — Slack is a non-blocking
    side-channel.
    """
    webhook = getattr(config, "slack_webhook_url", None)
    if not webhook:
        return
    try:
        from code_scripts.slack_notify import send_slack_success

        emoji = "✅" if failed == 0 else "⚠️"
        send_slack_success(
            (
                f"{emoji} *QBO pack-variant cleanup* — {config.display_name} ({config.company_key})\n"
                f"• Time: {datetime.now(timezone.utc).isoformat(timespec='seconds')}\n"
                f"• Inactivated: {succeeded}  |  Failed: {failed}  |  Skipped (cap): {skipped}\n"
                f"• Report: `{report_path}`"
            ),
            webhook,
        )
    except Exception as exc:  # noqa: BLE001 — never fail the run on notify
        print(f"[WARN] Slack notify failed (ignored): {exc}", file=sys.stderr)


if __name__ == "__main__":
    raise SystemExit(main())
