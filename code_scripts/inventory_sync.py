"""
Inventory Sync (EPOS -> QBO) - Audit-first

This module builds an inventory reconciliation report by comparing an EPOS Stock
Report export (StockHistory / StockReport CSV) against a QBO Item export.

Current scope:
- Normalize EPOS product names by stripping trailing *N pack multipliers.
- Convert EPOS on-hand into "single unit" quantities: stock * multiplier.
- Compare to QBO QtyOnHand for Inventory items (TrackQtyOnHand = true).
- Write a CSV report and print a small summary.

Optional write-back:
- Create a QBO ``InventoryAdjustment`` (quantity difference per item) for rows that
  are safe to apply. Ambiguous QBO mappings (multiple inventory SKUs for the same
  base name) are skipped unless explicitly allowed.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, Optional, Tuple

import pandas as pd

from code_scripts.artifact_paths import inventory_audit_reports_dir
from code_scripts.company_config import (
    ensure_company_runtime_compatible,
    get_available_companies,
    get_qbo_api_base_url,
    load_company_config,
)
from code_scripts.paths import OPS_ROOT, REPO_ROOT
from code_scripts.qbo_inventory_adjustment import build_inventory_adjustment_payload, post_inventory_adjustment
from code_scripts.qbo_snapshot_cache import (
    clear_qbo_snapshot_stale_marker,
    get_qbo_snapshot_path,
    get_qbo_snapshot_stale_reason,
    mark_qbo_snapshot_stale,
)
from code_scripts.inventory_notifications import (
    format_inventory_audit_summary,
    format_scope,
)
from code_scripts.run_lock import GlobalRunLock
from code_scripts.slack_notify import send_slack_success
from code_scripts.qbo_upload import TokenManager, _make_qbo_request, get_repo_root
from code_scripts.token_manager import verify_realm_match
from code_scripts.transform import strip_pack_multiplier


_DEFAULT_STOCK_NAME_COL = "Name"
_DEFAULT_STOCK_QTY_COL = "MeasuredCurrentStock"
_DEFAULT_STOCK_CATEGORY_COL = "CategoryName"
_QBO_MINOR_VERSION = "70"


def _safe_float(value: Any, default: float = 0.0) -> float:
    if value is None:
        return default
    try:
        if isinstance(value, str):
            value = value.replace(",", "").strip()
        if value == "" or (isinstance(value, float) and pd.isna(value)):
            return default
        return float(value)
    except Exception:
        return default


def _safe_bool_str(value: Any) -> bool:
    s = str(value or "").strip().lower()
    return s in {"true", "1", "yes", "y", "on"}


def _stamp() -> str:
    return datetime.now().strftime("%Y-%m-%d_%H%M%S")


def _time_stamp(now: datetime | None = None) -> str:
    return (now or datetime.now()).strftime("%H%M%S")


def _collapse_spaces(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())


def build_inventory_adjustment_doc_number(txn_date: str, item_id: str | int) -> str:
    compact_date = str(txn_date or "").strip().replace("-", "")
    if not re.fullmatch(r"\d{8}", compact_date):
        parsed = datetime.fromisoformat(str(txn_date).strip()).date()
        compact_date = parsed.strftime("%Y%m%d")
    return f"INVADJ-{compact_date}-{str(item_id).strip()}"


def _normalize_category_value(value: Any) -> str:
    return _collapse_spaces(str(value or ""))


def _join_unique_non_blank(series: Iterable[Any]) -> str:
    seen: list[str] = []
    for raw in series:
        value = _collapse_spaces(str(raw or ""))
        if value and value not in seen:
            seen.append(value)
    return " | ".join(seen)


@dataclass(frozen=True)
class AuditRow:
    base_name: str
    epos_single_units: float
    qbo_qty_on_hand: float
    delta: float
    status: str
    epos_raw_rows: int
    epos_has_pack: bool
    qbo_item_count_for_base: int
    qbo_has_pack_variants: bool
    qbo_base_item_ids: str


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Audit EPOS stock report vs QBO QtyOnHand (company-config aware)."
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
        help=(
            "Path to EPOS StockReport/StockHistory CSV export. Required unless "
            "--auto-download is used."
        ),
    )
    p.add_argument(
        "--auto-download",
        action="store_true",
        help=(
            "Use Playwright to log into EPOS Now and download a fresh "
            "StockReport CSV before running the audit. Mutually exclusive with "
            "--stock-csv (if both are given, --stock-csv wins and the download "
            "is skipped)."
        ),
    )
    p.add_argument(
        "--download-headful",
        action="store_true",
        help="When using --auto-download, run Playwright with a visible browser (debugging).",
    )
    p.add_argument(
        "--download-timeout-ms",
        type=int,
        default=None,
        help="When using --auto-download, override the download wait timeout (default: 90000 ms).",
    )
    p.add_argument(
        "--download-output-dir",
        default=None,
        help=(
            "When using --auto-download, override where the StockReport CSV is "
            "saved (default: $STATE_ROOT/code_scripts/exports/stock_reports/YYYY-MM-DD/)."
        ),
    )
    p.add_argument(
        "--qbo-csv",
        default=None,
        help=(
            "Optional path to QBO Item export CSV. If omitted, defaults to "
            "$STATE_ROOT/code_scripts/exports/qbo_snapshots/<company>_products.csv "
            "(falls back to older export locations when present)."
        ),
    )
    p.add_argument(
        "--auto-fetch-qbo",
        action="store_true",
        help=(
            "When --qbo-csv is omitted, query QBO for Inventory items (TrackQtyOnHand=true) "
            "and write a snapshot CSV to "
            "$STATE_ROOT/code_scripts/exports/qbo_snapshots/<company>_products.csv. "
            "By default this reuses a fresh cache unless --qbo-force-refresh is used."
        ),
    )
    p.add_argument(
        "--qbo-cache-max-age-hours",
        type=int,
        default=24,
        help="When using --auto-fetch-qbo, reuse the cached snapshot if it is newer than this many hours (default: 24).",
    )
    p.add_argument(
        "--qbo-force-refresh",
        action="store_true",
        help="When using --auto-fetch-qbo, ignore any cached snapshot and always re-query QBO.",
    )
    p.add_argument(
        "--qbo-export-path",
        default=None,
        help="When using --auto-fetch-qbo, override where the QBO snapshot CSV is written.",
    )
    p.add_argument(
        "--print-qbo-company-info",
        action="store_true",
        help="When querying QBO (auto-fetch), also print basic CompanyInfo (name + realm) for sanity checking.",
    )
    p.add_argument(
        "--output",
        default=None,
        help=(
            "Optional output report CSV path. Defaults to "
            "reports/inventory_sync/YYYY-MM-DD/inventory_audit_<company>_<time>.csv"
        ),
    )
    p.add_argument(
        "--product",
        dest="product_filter",
        default=None,
        help="Optional substring filter on EPOS base product name (case-insensitive) for a single-product test run.",
    )
    p.add_argument(
        "--category",
        dest="categories",
        action="append",
        default=[],
        help=(
            "Optional EPOS category filter (exact category name, case-insensitive). "
            "Repeat to include multiple categories."
        ),
    )
    p.add_argument(
        "--tolerance",
        type=float,
        default=0.0,
        help="Numeric tolerance within which a product is considered in sync (default: 0).",
    )
    p.add_argument(
        "--apply",
        action="store_true",
        help="Post InventoryAdjustment transactions to QBO for applicable rows (requires tokens + account id).",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Print InventoryAdjustment JSON payloads for applicable rows but do not POST (no tokens required).",
    )
    p.add_argument(
        "--notify-slack",
        action="store_true",
        help=(
            "Send an audit-only Slack summary when a company webhook is configured. "
            "Apply runs still notify automatically; normal CLI audits are quiet by default."
        ),
    )
    p.add_argument(
        "--txn-date",
        dest="txn_date",
        default=None,
        help="TxnDate for InventoryAdjustment (YYYY-MM-DD). Defaults to today (local date).",
    )
    p.add_argument(
        "--adjust-account-id",
        dest="adjust_account_id",
        default=None,
        help=(
            "QBO Account Id for AdjustAccountRef (inventory adjustment account). "
            "If omitted, uses qbo.inventory_adjustment_account_id or "
            "{COMPANY}_INVENTORY_ADJUSTMENT_ACCOUNT_ID."
        ),
    )
    p.add_argument(
        "--allow-ambiguous",
        action="store_true",
        help=(
            "Allow applying adjustments when multiple QBO inventory rows map to the same base name. "
            "This picks a canonical row (exact base-name match preferred)."
        ),
    )
    p.add_argument(
        "--allow-fallback-picks",
        action="store_true",
        help=(
            "In --apply mode, allow posting adjustments when the selected QBO item was chosen by a "
            "non-exact pick method (e.g. fallback_largest_qty). This is for CLI power-users only; "
            "the dashboard should remain exact-match-only."
        ),
    )
    p.add_argument(
        "--max-adjustments",
        type=int,
        default=25,
        help="Safety cap on number of InventoryAdjustment POSTs in one run (default: 25).",
    )
    p.add_argument(
        "--max-qty-delta",
        type=float,
        default=None,
        help=(
            "Per-item absolute qty-delta safety cap. Any row whose |QtyDiff| exceeds "
            "this is skipped and logged. Falls back to qbo.inventory_max_qty_delta in "
            "company config (or {COMPANY}_INVENTORY_MAX_QTY_DELTA env). 0 or negative "
            "disables the cap."
        ),
    )
    return p


def _auto_download_stock_csv(
    config,
    *,
    output_dir: Optional[str] = None,
    download_timeout_ms: Optional[int] = None,
    headful: bool = False,
) -> Path:
    """Run the EPOS StockReport Playwright downloader and return the saved path.

    Imports are lazy because Playwright is a heavy dependency and audit-only
    runs (operator already has a CSV) shouldn't have to load it.
    """
    # Lazy imports to keep the audit-only path Playwright-free.
    from playwright.sync_api import sync_playwright

    from code_scripts import epos_stocklevels_playwright

    with sync_playwright() as playwright:
        saved = epos_stocklevels_playwright.run(
            playwright,
            config,
            output_dir=output_dir,
            download_timeout_ms=download_timeout_ms,
            headful=headful,
        )
    return Path(saved).expanduser()


def _default_qbo_export_path(company_key: str) -> Optional[Path]:
    primary = get_qbo_snapshot_path(company_key)
    if primary.exists():
        return primary
    filename = f"{company_key}_products.csv"
    old_primary = OPS_ROOT / "exports" / filename
    if old_primary.exists():
        return old_primary
    legacy = REPO_ROOT / "code_scripts" / "exports" / filename
    return legacy if legacy.exists() else None


def _default_qbo_export_write_path(company_key: str) -> Path:
    """Canonical snapshot path under STATE_ROOT for generated QBO exports."""
    return get_qbo_snapshot_path(company_key)


def _default_inventory_audit_output_path(company_key: str, *, now: datetime | None = None) -> Path:
    clock = now or datetime.now()
    return inventory_audit_reports_dir(clock) / f"inventory_audit_{company_key}_{_time_stamp(clock)}.csv"


def _resolve_qbo_export_path_for_run(args: argparse.Namespace) -> Optional[Path]:
    if args.qbo_csv:
        return Path(args.qbo_csv).expanduser()
    if args.auto_fetch_qbo:
        return Path(args.qbo_export_path).expanduser() if args.qbo_export_path else _default_qbo_export_write_path(args.company)
    return _default_qbo_export_path(args.company)


def _is_cache_fresh(path: Path, *, max_age_hours: int) -> bool:
    if not path.exists():
        return False
    if max_age_hours <= 0:
        return False
    age_s = (datetime.now(timezone.utc) - datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc)).total_seconds()
    return age_s <= float(max_age_hours) * 3600.0


def _qbo_query_items_page(
    token_mgr: TokenManager,
    *,
    realm_id: str,
    start_position: int,
    max_results: int,
) -> list[dict[str, Any]]:
    """Fetch a single page of Inventory Items from QBO via the query endpoint."""
    from urllib.parse import quote

    query = (
        "select Id, Name, Type, TrackQtyOnHand, QtyOnHand "
        "from Item "
        "where Active = true and Type = 'Inventory' "
        f"startposition {int(start_position)} maxresults {int(max_results)}"
    )
    base_url = get_qbo_api_base_url()
    url = f"{base_url}/v3/company/{realm_id}/query?query={quote(query)}&minorversion={_QBO_MINOR_VERSION}"
    resp = _make_qbo_request("GET", url, token_mgr)
    if resp.status_code != 200:
        raise RuntimeError(f"QBO query failed: HTTP {resp.status_code}: {resp.text[:2000] if resp.text else ''}")
    payload = resp.json()
    items = payload.get("QueryResponse", {}).get("Item", [])
    if not items:
        return []
    if isinstance(items, dict):
        return [items]
    if isinstance(items, list):
        return [it for it in items if isinstance(it, dict)]
    return []


def fetch_qbo_inventory_items_snapshot(
    *,
    company_key: str,
    realm_id: str,
    token_mgr: Optional[TokenManager] = None,
    output_path: Path,
    cache_max_age_hours: int = 24,
    force_refresh: bool = False,
    print_company_info: bool = False,
) -> Path:
    """Query QBO for Inventory items and write a snapshot CSV, optionally reusing a fresh cache."""
    output_path = output_path.expanduser()
    output_path.parent.mkdir(parents=True, exist_ok=True)

    stale_reason = get_qbo_snapshot_stale_reason(company_key, output_path)
    if stale_reason:
        print(f"[INFO] Ignoring cached QBO snapshot: invalidated ({stale_reason})")

    if not force_refresh and stale_reason is None and _is_cache_fresh(output_path, max_age_hours=cache_max_age_hours):
        print(f"[INFO] Reusing fresh QBO snapshot: {output_path}")
        return output_path

    verify_realm_match(company_key, realm_id)
    if token_mgr is None:
        token_mgr = TokenManager(company_key, realm_id)

    if print_company_info:
        try:
            from code_scripts.qbo_company_info import fetch_company_info_summary

            summary = fetch_company_info_summary(token_mgr=token_mgr, realm_id=realm_id)
            if summary:
                print(
                    f"[INFO] QBO CompanyInfo: realm={realm_id} "
                    f"name={summary.get('CompanyName') or summary.get('LegalName') or 'unknown'}"
                )
        except Exception as exc:
            print(f"[WARN] Failed to fetch CompanyInfo: {exc}")

    print(f"[INFO] Fetching QBO Inventory items snapshot -> {output_path}")

    rows: list[dict[str, Any]] = []
    start = 1
    page_size = 1000
    while True:
        page = _qbo_query_items_page(
            token_mgr,
            realm_id=realm_id,
            start_position=start,
            max_results=page_size,
        )
        if not page:
            break
        rows.extend(page)
        if len(page) < page_size:
            break
        start += page_size

    with open(output_path, "w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=["Id", "Name", "Type", "TrackQtyOnHand", "QtyOnHand"],
        )
        writer.writeheader()
        for it in rows:
            writer.writerow(
                {
                    "Id": str(it.get("Id", "")).strip(),
                    "Name": str(it.get("Name", "")).strip(),
                    "Type": str(it.get("Type", "")).strip(),
                    "TrackQtyOnHand": str(it.get("TrackQtyOnHand", "")).strip(),
                    "QtyOnHand": str(it.get("QtyOnHand", "")).strip(),
                }
            )

    clear_qbo_snapshot_stale_marker(company_key)
    print(f"[INFO] Wrote QBO snapshot rows: {len(rows)}")
    return output_path


def load_epos_stock_snapshot(
    stock_csv_path: str,
    name_col: str = _DEFAULT_STOCK_NAME_COL,
    qty_col: str = _DEFAULT_STOCK_QTY_COL,
    product_filter: Optional[str] = None,
    categories: Optional[list[str]] = None,
    category_col: str = _DEFAULT_STOCK_CATEGORY_COL,
) -> pd.DataFrame:
    df = pd.read_csv(stock_csv_path)
    if name_col not in df.columns:
        raise ValueError(f"Missing column {name_col!r} in EPOS stock CSV. Present: {list(df.columns)}")
    if qty_col not in df.columns:
        raise ValueError(f"Missing column {qty_col!r} in EPOS stock CSV. Present: {list(df.columns)}")

    names = df[name_col].astype(str).map(_collapse_spaces)
    qty_raw = df[qty_col].map(_safe_float)
    if category_col in df.columns:
        category_values = df[category_col].map(_normalize_category_value)
    else:
        category_values = pd.Series([""] * len(df))

    base_names = []
    multipliers = []
    for n in names.tolist():
        base, mult = strip_pack_multiplier(n)
        base_names.append(_collapse_spaces(base))
        multipliers.append(int(mult))

    out = pd.DataFrame(
        {
            "raw_name": names,
            "base_name": base_names,
            "multiplier": multipliers,
            "epos_qty_raw": qty_raw,
            "category_name": category_values,
        }
    )
    out["epos_single_units"] = out["epos_qty_raw"] * out["multiplier"]
    out["epos_has_pack"] = out["multiplier"] > 1

    if categories:
        requested = {_normalize_category_value(v).lower() for v in categories if _normalize_category_value(v)}
        out = out[out["category_name"].str.lower().isin(requested)].copy()

    if product_filter:
        needle = product_filter.strip().lower()
        out = out[out["base_name"].str.lower().str.contains(needle, na=False)].copy()

    # Group to base_name (base-only, single units)
    grouped = (
        out.groupby("base_name", as_index=False)
        .agg(
            epos_single_units=("epos_single_units", "sum"),
            epos_raw_rows=("raw_name", "count"),
            epos_has_pack=("epos_has_pack", "max"),
            epos_categories=("category_name", _join_unique_non_blank),
            epos_category_count=("category_name", lambda s: len({v for v in s if str(v).strip()})),
        )
        .sort_values("base_name")
        .reset_index(drop=True)
    )
    return grouped


def load_qbo_inventory_snapshot(qbo_csv_path: str) -> pd.DataFrame:
    df = pd.read_csv(qbo_csv_path)
    if "Name" not in df.columns:
        raise ValueError(f"Missing column 'Name' in QBO export CSV. Present: {list(df.columns)}")
    if "Type" not in df.columns:
        raise ValueError(f"Missing column 'Type' in QBO export CSV. Present: {list(df.columns)}")

    # Filter to Inventory items that track QtyOnHand
    is_inventory = df["Type"].astype(str).str.strip().str.lower() == "inventory"
    if "TrackQtyOnHand" in df.columns:
        tracks = df["TrackQtyOnHand"].map(_safe_bool_str)
    else:
        # Some exports omit it; assume inventory items track on hand
        tracks = True

    inv = df[is_inventory & tracks].copy()

    names = inv["Name"].astype(str).map(_collapse_spaces)
    base_names = []
    had_pack = []
    for n in names.tolist():
        base, mult = strip_pack_multiplier(n)
        base_names.append(_collapse_spaces(base))
        had_pack.append(mult > 1)
    inv["base_name"] = base_names
    inv["qbo_name_raw"] = names
    inv["qbo_has_pack"] = had_pack
    inv["qbo_qty_on_hand"] = inv.get("QtyOnHand", 0).map(_safe_float)
    inv["qbo_is_base_item"] = [not v for v in had_pack]

    # Group by base_name to detect ambiguity; keep ids list for base-names
    def _join_ids(series: Iterable[Any]) -> str:
        ids = [str(x).strip() for x in series if str(x).strip()]
        return ",".join(ids[:50])  # cap to avoid huge cells

    def _join_names(series: Iterable[Any]) -> str:
        names_out = [str(x).strip() for x in series if str(x).strip()]
        return " | ".join(names_out[:10])

    def _join_pack_names(df_group: pd.DataFrame) -> str:
        items = df_group[df_group["qbo_has_pack"] == True]  # noqa: E712
        return _join_names(items["qbo_name_raw"].tolist())

    def _join_base_names(df_group: pd.DataFrame) -> str:
        items = df_group[df_group["qbo_has_pack"] == False]  # noqa: E712
        return _join_names(items["qbo_name_raw"].tolist())

    grouped = (
        inv.groupby("base_name", as_index=False)
        .agg(
            qbo_qty_on_hand=("qbo_qty_on_hand", "sum"),
            qbo_item_count_for_base=("Id", "count"),
            qbo_has_pack_variants=("qbo_has_pack", "max"),
            qbo_base_item_count=("qbo_is_base_item", "sum"),
            qbo_base_item_names=("qbo_name_raw", _join_names),
            qbo_pack_variant_names=("qbo_name_raw", _join_names),
            qbo_base_item_ids=("Id", _join_ids),
        )
        .sort_values("base_name")
        .reset_index(drop=True)
    )
    # Replace the naive name joins with base/pack-specific joins.
    if not grouped.empty:
        by_base = {}
        for _, g in inv.groupby("base_name"):
            by_base[str(g.iloc[0]["base_name"])] = {
                "base_names": _join_base_names(g),
                "pack_names": _join_pack_names(g),
            }
        grouped["qbo_base_item_names"] = grouped["base_name"].map(lambda k: by_base.get(str(k), {}).get("base_names", ""))
        grouped["qbo_pack_variant_names"] = grouped["base_name"].map(lambda k: by_base.get(str(k), {}).get("pack_names", ""))
    return grouped


def load_qbo_inventory_item_rows(qbo_csv_path: str) -> pd.DataFrame:
    """
    Load per-QBO-item rows (not grouped) for Inventory + TrackQtyOnHand.

    Columns returned (best-effort):
    - Id, Name, base_name, qbo_name_raw, qbo_has_pack, qbo_qty_on_hand
    """
    df = pd.read_csv(qbo_csv_path)
    if "Name" not in df.columns:
        raise ValueError(f"Missing column 'Name' in QBO export CSV. Present: {list(df.columns)}")
    if "Type" not in df.columns:
        raise ValueError(f"Missing column 'Type' in QBO export CSV. Present: {list(df.columns)}")

    is_inventory = df["Type"].astype(str).str.strip().str.lower() == "inventory"
    if "TrackQtyOnHand" in df.columns:
        tracks = df["TrackQtyOnHand"].map(_safe_bool_str)
    else:
        tracks = True

    inv = df[is_inventory & tracks].copy()
    names = inv["Name"].astype(str).map(_collapse_spaces)
    base_names = []
    had_pack = []
    for n in names.tolist():
        base, mult = strip_pack_multiplier(n)
        base_names.append(_collapse_spaces(base))
        had_pack.append(mult > 1)

    out = pd.DataFrame(
        {
            "Id": inv.get("Id", pd.Series([""] * len(inv))),
            "Name": names,
            "base_name": base_names,
            "qbo_name_raw": names,
            "qbo_has_pack": had_pack,
            "qbo_qty_on_hand": inv.get("QtyOnHand", 0).map(_safe_float),
        }
    )
    out["Id"] = out["Id"].map(lambda x: str(x).strip())
    return out


def choose_canonical_qbo_item_row(rows: pd.DataFrame, *, base_name: str) -> Tuple[Optional[pd.Series], str]:
    """
    Pick a single QBO item row to adjust for a given base_name group.

    Preference order:
    1) Exact name match to base_name (case/space normalized)
    2) Non-pack variant (no *N suffix) if unique
    3) Largest QtyOnHand
    """
    if rows is None or rows.empty:
        return None, "no_rows"

    bn = _collapse_spaces(base_name)
    exact = rows[rows["Name"].map(_collapse_spaces).str.lower() == bn.lower()]
    if len(exact) == 1:
        return exact.iloc[0], "exact_name_match"
    if len(exact) > 1:
        exact = exact.sort_values("qbo_qty_on_hand", ascending=False)
        return exact.iloc[0], "exact_name_match_multi_pick_largest_qty"

    non_pack = rows[rows["qbo_has_pack"] == False]  # noqa: E712
    if len(non_pack) == 1:
        return non_pack.iloc[0], "unique_non_pack"

    ranked = rows.sort_values(["qbo_qty_on_hand", "Id"], ascending=[False, True])
    return ranked.iloc[0], "fallback_largest_qty"


def build_audit_report(
    epos_by_base: pd.DataFrame,
    qbo_by_base: pd.DataFrame,
    tolerance: float = 0.0,
) -> pd.DataFrame:
    merged = epos_by_base.merge(qbo_by_base, on="base_name", how="left")
    merged["qbo_qty_on_hand"] = merged["qbo_qty_on_hand"].fillna(0.0)
    merged["qbo_item_count_for_base"] = merged["qbo_item_count_for_base"].fillna(0).astype(int)
    merged["qbo_has_pack_variants"] = (
        merged["qbo_has_pack_variants"].astype("boolean").fillna(False).astype(bool)
    )
    merged["qbo_base_item_count"] = merged.get("qbo_base_item_count", 0).fillna(0).astype(int)
    merged["qbo_base_item_ids"] = merged["qbo_base_item_ids"].fillna("")
    merged["qbo_base_item_names"] = merged.get("qbo_base_item_names", "").fillna("")
    merged["qbo_pack_variant_names"] = merged.get("qbo_pack_variant_names", "").fillna("")

    merged["delta"] = merged["epos_single_units"] - merged["qbo_qty_on_hand"]

    def classify(row: pd.Series) -> str:
        if row["qbo_item_count_for_base"] <= 0:
            return "missing_in_qbo"
        if abs(float(row["delta"])) <= float(tolerance):
            return "in_sync"
        if row["qbo_item_count_for_base"] > 1:
            # Multiple QBO inventory items share the same base_name; adjusting automatically is risky.
            return "ambiguous_in_qbo"
        return "needs_adjustment"

    merged["status"] = merged.apply(classify, axis=1)

    def _catalog_type(row: pd.Series) -> str:
        if row["qbo_item_count_for_base"] <= 0:
            return "missing_from_qbo"
        if int(row.get("qbo_base_item_count", 0) or 0) > 1:
            return "multiple_active_base_items"
        if bool(row.get("qbo_has_pack_variants")) and int(row.get("qbo_base_item_count", 0) or 0) == 0:
            return "only_pack_variant_exists"
        if bool(row.get("qbo_has_pack_variants")) and int(row.get("qbo_base_item_count", 0) or 0) == 1:
            return "base_with_pack_variants"
        return "exact_name_match"

    merged["catalog_issue_type"] = merged.apply(_catalog_type, axis=1)

    def _catalog_detail(row: pd.Series) -> str:
        t = str(row.get("catalog_issue_type") or "")
        if t == "only_pack_variant_exists":
            pack = str(row.get("qbo_pack_variant_names") or "").strip()
            return f"only pack variant exists in QuickBooks: {pack}" if pack else "only pack variants exist in QuickBooks"
        if t == "base_with_pack_variants":
            return "base item and pack variants both exist; pack variant consolidation needed"
        if t == "multiple_active_base_items":
            return "multiple active matching QuickBooks items found"
        if t == "missing_from_qbo":
            return "product not found in QuickBooks"
        return ""

    def _suggested_action(row: pd.Series) -> str:
        t = str(row.get("catalog_issue_type") or "")
        if t == "only_pack_variant_exists":
            return "create base item, consolidate pack variant quantity, then inactivate pack variant"
        if t == "base_with_pack_variants":
            return "run pack variant consolidation and cleanup"
        if t == "multiple_active_base_items":
            return "manually merge/inactivate duplicate base items"
        if t == "missing_from_qbo":
            return "create inventory item in QuickBooks using standard item creation logic"
        return ""

    merged["catalog_issue_detail"] = merged.apply(_catalog_detail, axis=1)
    merged["suggested_next_action"] = merged.apply(_suggested_action, axis=1)

    cols = [
        "base_name",
        "epos_single_units",
        "qbo_qty_on_hand",
        "delta",
        "status",
        "catalog_issue_type",
        "catalog_issue_detail",
        "suggested_next_action",
        "epos_raw_rows",
        "epos_has_pack",
        "epos_categories",
        "epos_category_count",
        "qbo_item_count_for_base",
        "qbo_has_pack_variants",
        "qbo_base_item_count",
        "qbo_base_item_names",
        "qbo_pack_variant_names",
        "qbo_base_item_ids",
    ]
    return merged[cols].sort_values(["status", "base_name"]).reset_index(drop=True)


def _write_csv(path: Path, df: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False, quoting=csv.QUOTE_MINIMAL)


def _write_audit_metadata(
    report_csv_path: Path,
    *,
    company_key: str,
    display_name: str,
    stock_csv: str,
    qbo_csv: str,
    status_counts: Dict[str, int],
    total_groups: int,
    apply_stats: Dict[str, Any],
) -> Path:
    """Emit a sidecar JSON next to the report CSV for portal ingestion.

    Fields are intentionally stable — the portal's artifact_ingestion reads this.
    """
    meta_path = report_csv_path.with_suffix(".json")
    payload: Dict[str, Any] = {
        "company_key": company_key,
        "display_name": display_name,
        "processed_at": datetime.now(tz=timezone.utc).isoformat(),
        "report_csv": str(report_csv_path),
        "stock_csv": stock_csv,
        "qbo_csv": qbo_csv,
        "total_groups": int(total_groups),
        "status_counts": {k: int(v) for k, v in status_counts.items()},
        "apply": apply_stats,
    }
    run_job_id = os.environ.get("OIAT_RUN_JOB_ID", "").strip()
    if run_job_id:
        payload["run_job_id"] = run_job_id
    meta_path.parent.mkdir(parents=True, exist_ok=True)
    with open(meta_path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, sort_keys=True)
    return meta_path


def _should_notify_audit_only(args: argparse.Namespace) -> bool:
    """Keep exploratory CLI audits quiet unless explicitly opted in.

    Portal/job-triggered audits set OIAT_RUN_JOB_ID in the subprocess
    environment, so those runs can still emit the operational summary.
    """
    return bool(args.notify_slack or os.environ.get("OIAT_RUN_JOB_ID", "").strip())


def main(argv: Optional[list[str]] = None) -> int:
    args = build_parser().parse_args(argv)

    # Load config for future extensions (auth/realm sanity, per-company defaults)
    try:
        config = load_company_config(args.company)
    except Exception as exc:
        print(f"Error: Failed to load company config for '{args.company}': {exc}")
        return 1

    try:
        ensure_company_runtime_compatible(config)
    except RuntimeError as exc:
        print(f"Error: {exc}")
        return 2

    if not args.stock_csv and not args.auto_download:
        print(
            "Error: must provide --stock-csv (path to an EPOS StockReport CSV) "
            "or --auto-download (fetch a fresh CSV via Playwright)."
        )
        return 1

    if args.stock_csv:
        stock_path = Path(args.stock_csv).expanduser()
    else:
        try:
            stock_path = _auto_download_stock_csv(
                config,
                output_dir=args.download_output_dir,
                download_timeout_ms=args.download_timeout_ms,
                headful=bool(args.download_headful),
            )
        except Exception as exc:
            print(f"Error: auto-download of EPOS StockReport CSV failed: {exc}")
            return 1
        print(f"[INFO] Downloaded stock CSV: {stock_path}")

    if not stock_path.exists():
        print(f"Error: stock csv not found: {stock_path}")
        return 1

    qbo_path = _resolve_qbo_export_path_for_run(args)
    if args.auto_fetch_qbo and not args.qbo_csv:
        try:
            qbo_path = fetch_qbo_inventory_items_snapshot(
                company_key=config.company_key,
                realm_id=config.realm_id,
                output_path=qbo_path or _default_qbo_export_write_path(config.company_key),
                cache_max_age_hours=int(args.qbo_cache_max_age_hours),
                force_refresh=bool(args.qbo_force_refresh),
                print_company_info=bool(args.print_qbo_company_info),
            )
        except Exception as exc:
            print(f"Error: auto-fetch of QBO Items snapshot failed: {exc}")
            return 1
    if not qbo_path or not qbo_path.exists():
        print(
            "Error: QBO export csv not found. Provide --qbo-csv, or use --auto-fetch-qbo, "
            "or place export at "
            f"{OPS_ROOT / 'exports'}/<company>_products.csv"
        )
        return 1

    epos = load_epos_stock_snapshot(
        str(stock_path),
        product_filter=args.product_filter,
        categories=list(args.categories or []),
    )
    qbo = load_qbo_inventory_snapshot(str(qbo_path))
    report = build_audit_report(epos, qbo, tolerance=float(args.tolerance))

    out_path = Path(args.output).expanduser() if args.output else _default_inventory_audit_output_path(config.company_key)
    _write_csv(out_path, report)

    # Console summary
    counts = report["status"].value_counts().to_dict()
    total_groups = int(len(report))
    in_sync = int(counts.get("in_sync", 0) or 0)
    needs_adjustment = int(counts.get("needs_adjustment", 0) or 0)
    ambiguous_in_qbo = int(counts.get("ambiguous_in_qbo", 0) or 0)
    missing_in_qbo = int(counts.get("missing_in_qbo", 0) or 0)

    def _manual_review_examples_for_audit() -> list[str]:
        examples: list[str] = []
        if "catalog_issue_detail" not in report.columns:
            return examples
        subset = report[report["catalog_issue_detail"].astype(str).str.strip() != ""]
        for _, r in subset.iterrows():
            if len(examples) >= 10:
                break
            base = str(r.get("base_name") or "").strip()
            detail = str(r.get("catalog_issue_detail") or "").strip()
            if base and detail:
                examples.append(f"{base} — {detail}")
        return examples

    def _emit_metadata(apply_stats: Dict[str, Any]) -> None:
        try:
            _write_audit_metadata(
                out_path,
                company_key=config.company_key,
                display_name=config.display_name,
                stock_csv=str(stock_path),
                qbo_csv=str(qbo_path),
                status_counts=counts,
                total_groups=len(report),
                apply_stats=apply_stats,
            )
        except Exception as exc:  # pragma: no cover - best-effort metadata write
            print(f"[WARN] Could not write audit metadata sidecar: {exc}")
    print("=" * 68)
    print(f"Inventory audit: {config.display_name} ({config.company_key})")
    print(f"EPOS stock: {stock_path.name}")
    print(f"QBO export: {qbo_path.name}")
    if args.product_filter:
        print(f"Filter: {args.product_filter!r}")
    if args.categories:
        print(f"Categories: {', '.join(args.categories)}")
    print("-" * 68)
    print(f"Total products (base_name groups): {len(report)}")
    for key in ["in_sync", "needs_adjustment", "ambiguous_in_qbo", "missing_in_qbo"]:
        if key in counts:
            print(f"{key}: {counts[key]}")
    print("-" * 68)
    print(f"Wrote report: {out_path}")
    print("=" * 68)

    if not args.apply and not args.dry_run:
        _emit_metadata({"mode": "audit_only", "posted": 0, "skipped": 0})
        webhook = config.slack_webhook_url if _should_notify_audit_only(args) else None
        if webhook:
            try:
                manual_review = ambiguous_in_qbo + missing_in_qbo
                send_slack_success(
                    format_inventory_audit_summary(
                        company_display_name=config.display_name,
                        company_key=config.company_key,
                        mode="audit",
                        scope=format_scope(category=list(args.categories or []), product=args.product_filter),
                        counts={
                            "total_groups": total_groups,
                            "in_sync": in_sync,
                            "needs_adjustment": needs_adjustment,
                            "ambiguous_in_qbo": ambiguous_in_qbo,
                            "missing_in_qbo": missing_in_qbo,
                        },
                        report_path=str(out_path),
                        warnings_count=manual_review,
                        manual_review_examples=_manual_review_examples_for_audit(),
                    ),
                    webhook,
                )
            except Exception as notify_exc:  # noqa: BLE001 — never fail the run on notify
                print(f"[WARN] Slack notify failed (ignored): {notify_exc}")
        return 0

    adjust_account_id = (args.adjust_account_id or "").strip() or (config.inventory_adjustment_account_id or "")
    if not adjust_account_id:
        print(
            "Error: missing inventory adjustment account id. "
            "Set --adjust-account-id, or qbo.inventory_adjustment_account_id, "
            "or {COMPANY}_INVENTORY_ADJUSTMENT_ACCOUNT_ID."
        )
        return 2

    txn_date = (args.txn_date or "").strip()
    if not txn_date:
        txn_date = datetime.now().strftime("%Y-%m-%d")

    if args.apply:
        try:
            verify_realm_match(config.company_key, config.realm_id)
        except RuntimeError as exc:
            print(f"Error: Realm ID safety check failed: {exc}")
            return 2

    token_mgr: Optional[TokenManager] = TokenManager(config.company_key, config.realm_id) if args.apply else None

    qbo_rows = load_qbo_inventory_item_rows(str(qbo_path))

    if args.allow_ambiguous:
        candidates = report[report["status"].isin(["needs_adjustment", "ambiguous_in_qbo"])].copy()
    else:
        candidates = report[report["status"] == "needs_adjustment"].copy()
    if args.product_filter:
        needle = args.product_filter.strip().lower()
        candidates = candidates[candidates["base_name"].str.lower().str.contains(needle, na=False)].copy()

    if candidates.empty:
        print("[INFO] No applicable rows to adjust (needs_adjustment/ambiguous with --allow-ambiguous).")
        _emit_metadata({
            "mode": "dry_run" if args.dry_run else ("apply" if args.apply else "audit_only"),
            "posted": 0,
            "skipped": 0,
            "reason": "no_candidates",
        })
        return 0

    run_lock: Optional[GlobalRunLock] = None
    if args.apply:
        run_lock = GlobalRunLock(holder=f"inventory_sync:{config.company_key}")
        lock_result = run_lock.acquire()
        if not lock_result.acquired:
            print(
                f"Error: another pipeline run is active ({lock_result.reason}); "
                "refusing to --apply inventory adjustments."
            )
            return 2

    max_qty_delta: Optional[float] = args.max_qty_delta
    if max_qty_delta is None:
        max_qty_delta = config.inventory_max_qty_delta
    if max_qty_delta is not None and max_qty_delta <= 0:
        max_qty_delta = None
    if max_qty_delta is not None:
        print(f"[INFO] Per-item qty-delta cap active: |QtyDiff| <= {max_qty_delta}")

    posted = 0
    skipped = 0
    skipped_non_exact_pick = 0
    apply_error: Optional[str] = None
    manual_review_examples: list[str] = []
    try:
        for _, row in candidates.iterrows():
            if posted >= int(args.max_adjustments):
                print(f"[WARN] Hit --max-adjustments={args.max_adjustments}; stopping.")
                break

            base = str(row["base_name"])
            epos_target = float(row["epos_single_units"])
            group = qbo_rows[qbo_rows["base_name"] == base]

            chosen, reason = choose_canonical_qbo_item_row(group, base_name=base)
            if chosen is None or not str(chosen.get("Id", "")).strip():
                print(f"[SKIP] {base!r}: could not pick QBO item ({reason})")
                skipped += 1
                continue

            item_id = str(chosen["Id"]).strip()
            current_qty = float(chosen.get("qbo_qty_on_hand", 0.0) or 0.0)
            qty_diff = float(epos_target) - current_qty

            if abs(qty_diff) <= float(args.tolerance):
                continue

            if max_qty_delta is not None and abs(qty_diff) > max_qty_delta:
                print(
                    f"[SKIP] {base!r}: |qty_diff|={abs(qty_diff)} exceeds cap={max_qty_delta} "
                    f"(epos_single_units={epos_target}, qbo_item_qty={current_qty}); "
                    "review manually."
                )
                skipped += 1
                continue

            if args.apply and not args.dry_run:
                if reason != "exact_name_match":
                    if args.allow_fallback_picks and reason == "fallback_largest_qty":
                        pass
                    else:
                        print(
                            f"[SKIP] base={base!r} item_id={item_id} pick={reason} "
                            "reason=non_exact_pick_not_allowed"
                        )
                        skipped += 1
                        skipped_non_exact_pick += 1
                        if len(manual_review_examples) < 10:
                            chosen_name = str(chosen.get("Name", "") or "").strip()
                            if reason == "fallback_largest_qty" and str(chosen.get("qbo_has_pack", False)):
                                manual_review_examples.append(
                                    f"{base} — only pack variant exists in QuickBooks: {chosen_name}"
                                )
                            else:
                                manual_review_examples.append(
                                    f"{base} — no exact QuickBooks item match found"
                                )
                        continue

            memo = (
                f"OIAT inventory sync | base={base!r} | pick={reason} | "
                f"epos_single_units={epos_target} | qbo_item_qty={current_qty} | delta={qty_diff}"
            )
            doc_number = build_inventory_adjustment_doc_number(txn_date=str(txn_date), item_id=item_id)
            payload = build_inventory_adjustment_payload(
                adjust_account_id=str(adjust_account_id),
                txn_date=str(txn_date),
                doc_number=doc_number,
                private_note=memo[:950],
                lines=[{"item_id": item_id, "qty_diff": qty_diff}],
            )

            print("-" * 68)
            print(f"{'DRY-RUN ' if args.dry_run else ''}InventoryAdjustment → item_id={item_id} base={base!r} QtyDiff={qty_diff}")

            if args.dry_run:
                print(json.dumps(payload, indent=2, sort_keys=True))
                posted += 1
                continue

            assert token_mgr is not None
            try:
                resp = post_inventory_adjustment(token_mgr, config.realm_id, payload)
            except Exception as exc:
                apply_error = f"{base!r} (item_id={item_id}): {exc}"
                print(f"[FAIL] Posting InventoryAdjustment failed for {apply_error}")
                raise
            inv_adj = (resp or {}).get("InventoryAdjustment") or {}
            doc = inv_adj.get("DocNumber") or inv_adj.get("Id")
            print(f"[OK] Posted InventoryAdjustment doc/id={doc}")
            posted += 1
    except Exception as exc:
        if args.apply:
            if not apply_error:
                apply_error = str(exc)
            webhook = config.slack_webhook_url
            if webhook:
                try:
                    send_slack_success(
                        format_inventory_audit_summary(
                            company_display_name=config.display_name,
                            company_key=config.company_key,
                            mode="apply",
                            scope=format_scope(category=list(args.categories or []), product=args.product_filter),
                            counts={
                                "total_groups": total_groups,
                                "in_sync": in_sync,
                                "needs_adjustment": needs_adjustment,
                                "ambiguous_in_qbo": ambiguous_in_qbo,
                                "missing_in_qbo": missing_in_qbo,
                                "posted": posted,
                                "skipped": skipped,
                            },
                            report_path=str(out_path),
                            error=apply_error,
                            warnings_count=(ambiguous_in_qbo + missing_in_qbo + skipped_non_exact_pick),
                            manual_review_examples=manual_review_examples or _manual_review_examples_for_audit(),
                        ),
                        webhook,
                    )
                except Exception as notify_exc:  # noqa: BLE001 — never fail the run on notify
                    print(f"[WARN] Slack notify failed (ignored): {notify_exc}")
        _emit_metadata({
            "mode": "dry_run" if args.dry_run else ("apply" if args.apply else "audit_only"),
            "posted": posted,
            "skipped": skipped,
            "error": apply_error or str(exc),
        })
        raise
    finally:
        if run_lock is not None:
            run_lock.release()

    print("=" * 68)
    print(f"Adjustments {'planned' if args.dry_run else 'posted'}: {posted} | skipped: {skipped}")

    if args.apply and not args.dry_run:
        if posted > 0:
            mark_qbo_snapshot_stale(config.company_key, reason="inventory_adjustments_posted")
            print("[INFO] Marked cached QBO snapshot stale after posting adjustments.")
        webhook = config.slack_webhook_url
        if webhook:
            try:
                send_slack_success(
                    format_inventory_audit_summary(
                        company_display_name=config.display_name,
                        company_key=config.company_key,
                        mode="apply",
                        scope=format_scope(category=list(args.categories or []), product=args.product_filter),
                        counts={
                            "total_groups": total_groups,
                            "in_sync": in_sync,
                            "needs_adjustment": needs_adjustment,
                            "ambiguous_in_qbo": ambiguous_in_qbo,
                            "missing_in_qbo": missing_in_qbo,
                            "posted": posted,
                            "skipped": skipped,
                            "txn_date": txn_date,
                        },
                        report_path=str(out_path),
                        warnings_count=(ambiguous_in_qbo + missing_in_qbo + skipped_non_exact_pick),
                        manual_review_examples=manual_review_examples or _manual_review_examples_for_audit(),
                    ),
                    webhook,
                )
            except Exception as notify_exc:  # noqa: BLE001 — never fail the run on notify
                print(f"[WARN] Slack notify failed (ignored): {notify_exc}")

    _emit_metadata({
        "mode": "dry_run" if args.dry_run else "apply",
        "posted": posted,
        "skipped": skipped,
        "txn_date": txn_date,
    })
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
