"""
Inventory Sync (EPOS -> QBO) - Audit-first

This module builds an inventory reconciliation report by comparing an EPOS Stock
Report export (StockHistory / StockReport CSV) against a QBO Item export.

Current scope:
- Normalize EPOS product names by stripping trailing *N pack multipliers.
- Convert EPOS on-hand into "single unit" quantities: stock * multiplier.
- Compare to QBO QtyOnHand for Inventory items (TrackQtyOnHand = true).
- Write a CSV report and print a small summary.

Preview workflow:
- ``--dry-run`` prints manual QBO starting-value correction rows for scoped,
  exact-match products. ``--apply`` is intentionally disabled; the public QBO
  InventoryAdjustment API must not be used for this forward inventory sync.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import re
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, Optional, Tuple
from urllib.parse import urlencode

import pandas as pd

from code_scripts.artifact_paths import inventory_audit_reports_dir
from code_scripts.company_config import (
    ensure_company_runtime_compatible,
    get_available_companies,
    get_qbo_api_base_url,
    load_company_config,
)
from code_scripts.paths import OPS_ROOT, REPO_ROOT
from code_scripts.qbo_snapshot_cache import (
    clear_qbo_snapshot_stale_marker,
    get_qbo_snapshot_path,
    get_qbo_snapshot_stale_reason,
)
from code_scripts.inventory_notifications import (
    format_inventory_audit_summary,
    format_scope,
)
from code_scripts.slack_notify import send_slack_success
from code_scripts.qbo_upload import TokenManager, _make_qbo_request, get_repo_root
from code_scripts.token_manager import verify_realm_match
from code_scripts.transform import strip_pack_multiplier


_DEFAULT_STOCK_NAME_COL = "Name"
_DEFAULT_STOCK_QTY_COL = "MeasuredCurrentStock"
_DEFAULT_STOCK_CATEGORY_COL = "CategoryName"
EPOS_NEGATIVE_STOCK_POLICY = "clamp_to_zero"
_QBO_MINOR_VERSION = "75"
_QBO_STARTING_QTY_FALLBACK_BATCH_SIZE = 100
_QBO_SNAPSHOT_COLUMNS = [
    "Id",
    "Name",
    "Type",
    "TrackQtyOnHand",
    "QtyOnHand",
    "Active",
    "InvStartDate",
    "ParentRef",
    "UnitPrice",
    "PurchaseCost",
    "qbo_current_starting_qty",
    "qbo_starting_qty_rate",
    "qbo_starting_inventory_cost",
    "qbo_starting_asset_value",
    "qbo_starting_qty_source",
    "qbo_starting_qty_status",
    "qbo_name_original",
    "qbo_name_raw",
    "qbo_name_display",
]
_QBO_ITEM_SAFE_SELECT_FIELDS = [
    "Id",
    "Name",
    "Type",
    "TrackQtyOnHand",
    "QtyOnHand",
    "Active",
]
_QBO_ITEM_DIAGNOSTIC_SELECT_FIELDS = [
    *_QBO_ITEM_SAFE_SELECT_FIELDS,
    "InvStartDate",
    "ParentRef",
    "UnitPrice",
    "PurchaseCost",
]
_QBO_STARTING_QTY_SNAPSHOT_COLUMNS = {
    "qbo_current_starting_qty",
    "qbo_starting_qty_source",
    "qbo_starting_qty_status",
}


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


def _safe_float_optional(value: Any) -> float | None:
    """Parse a float or return None when the value is missing/unparseable."""
    if value is None:
        return None
    try:
        if isinstance(value, str):
            value = value.replace(",", "").strip()
        if value == "" or (isinstance(value, float) and pd.isna(value)):
            return None
        return float(value)
    except Exception:
        return None


def _format_optional_qty(value: float | None) -> str:
    if value is None:
        return ""
    if float(value).is_integer():
        return str(int(value))
    return f"{float(value):.4f}".rstrip("0").rstrip(".")


def _safe_bool_str(value: Any) -> bool:
    s = str(value or "").strip().lower()
    return s in {"true", "1", "yes", "y", "on"}


def _safe_active_bool(value: Any) -> bool:
    if value is None or value == "":
        return True
    if isinstance(value, float) and pd.isna(value):
        return True
    return _safe_bool_str(value)


def _stamp() -> str:
    return datetime.now().strftime("%Y-%m-%d_%H%M%S")


def _time_stamp(now: datetime | None = None) -> str:
    return (now or datetime.now()).strftime("%H%M%S")


def _collapse_spaces(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())


def _original_qbo_names(series: pd.Series) -> pd.Series:
    """Preserve QBO item names exactly enough for catalog decisions."""
    return series.where(series.notna(), "").astype(str)


def _qbo_parent_ref_value(value: Any) -> str:
    if isinstance(value, dict):
        return str(value.get("value") or value.get("name") or "").strip()
    return str(value or "").strip()


def _normalize_name_key(value: Any) -> str:
    """Canonical key for case-insensitive product-name matching/grouping."""
    return _collapse_spaces(str(value or "")).lower()


_CURRENT_VOLUME_LOOSE_EACH_RE = re.compile(
    r"^\s*(?P<loose>[-+]?\d+(?:\.\d+)?)\s+of\s+\d+(?:\.\d+)?\s+Each\s*$",
    re.IGNORECASE,
)


def _parse_current_volume_loose_units(value: Any) -> float | None:
    """Parse EPOS "Current Volume" like "23 of 24 Each" -> loose units (23).

    Returns None for missing/unparseable values.
    """
    if value is None:
        return None
    s = str(value).strip()
    if not s:
        return None
    m = _CURRENT_VOLUME_LOOSE_EACH_RE.match(s)
    if not m:
        return None
    try:
        return float(m.group("loose"))
    except Exception:
        return None


def _apply_epos_negative_stock_policy(rows: pd.DataFrame) -> pd.DataFrame:
    """Apply the inventory-sync policy for negative EPOS row quantities.

    EPOS can report a negative stock/volume on one pack row while sibling pack
    rows for the same product are positive. Inventory sync treats each negative
    row as zero before grouping so the negative row cannot subtract from the
    product-level expected quantity.
    """
    out = rows.copy()
    computed_units = pd.to_numeric(out["epos_single_units"], errors="coerce").fillna(0.0)
    negative_mask = computed_units < 0
    out["epos_single_units_before_negative_policy"] = computed_units
    out["epos_negative_rows_clamped"] = negative_mask.astype(int)
    out["epos_negative_units_clamped"] = computed_units.where(negative_mask, 0.0).abs()
    out["epos_negative_stock_policy"] = EPOS_NEGATIVE_STOCK_POLICY
    out["epos_negative_clamped_row_names"] = out["raw_name"].where(negative_mask, "")
    out["epos_single_units"] = computed_units.where(~negative_mask, 0.0)
    return out


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


def _dedupe_qbo_rows_by_id(df: pd.DataFrame) -> pd.DataFrame:
    """Drop duplicate QBO rows by item Id while preserving first-seen order."""
    if "Id" not in df.columns or df.empty:
        return df
    out = df.copy()
    out["Id"] = out["Id"].map(lambda x: str(x or "").strip())
    with_id = out[out["Id"] != ""].drop_duplicates(subset=["Id"], keep="first")
    without_id = out[out["Id"] == ""]
    return pd.concat([with_id, without_id], ignore_index=True)


def literal_product_filter_mask(
    df: pd.DataFrame,
    product_filter: str | None,
    *,
    base_col: str = "base_name",
    raw_col: str | None = None,
) -> pd.Series:
    """Match operator product filters as literal text, not regex patterns."""
    if not product_filter:
        return pd.Series([True] * len(df), index=df.index)

    raw_needle = _collapse_spaces(str(product_filter).strip()).lower()
    needles = [raw_needle] if raw_needle else []
    base_needle, _ = strip_pack_multiplier(raw_needle)
    base_needle = _collapse_spaces(base_needle).lower()
    if base_needle and base_needle not in needles:
        needles.append(base_needle)

    mask = pd.Series([False] * len(df), index=df.index)
    columns = [base_col]
    if raw_col:
        columns.append(raw_col)
    for column in columns:
        if column not in df.columns:
            continue
        values = df[column].fillna("").astype(str).map(_collapse_spaces).str.lower()
        for needle in needles:
            mask = mask | values.str.contains(needle, na=False, regex=False)
    return mask


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
        help="Removed: QBO quantity-apply posting is disabled; use preview/manual starting-value correction workflow.",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview rows that would need manual QBO starting-value correction; do not POST to QBO.",
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
        help="Date label for manual correction preview rows (YYYY-MM-DD). Defaults to today.",
    )
    p.add_argument(
        "--adjust-account-id",
        dest="adjust_account_id",
        default=None,
        help=(
            "Deprecated compatibility option. QBO quantity adjustment posting is disabled; "
            "manual corrections should use the configured Opening Balance Equity account in QBO UI."
        ),
    )
    p.add_argument(
        "--allow-ambiguous",
        action="store_true",
        help=(
            "Include ambiguous QBO mappings in the preview. Exact base-name matches are still preferred."
        ),
    )
    p.add_argument(
        "--allow-fallback-picks",
        action="store_true",
        help=(
            "In preview mode, include rows where the selected QBO item was chosen by a non-exact "
            "fallback method (e.g. fallback_largest_qty)."
        ),
    )
    p.add_argument(
        "--max-adjustments",
        type=int,
        default=25,
        help="Safety cap on number of manual correction preview rows (default: 25).",
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


def _snapshot_has_required_columns(path: Path, required_columns: Iterable[str]) -> bool:
    try:
        with path.open("r", encoding="utf-8-sig", newline="") as handle:
            reader = csv.reader(handle)
            header = next(reader, [])
    except (OSError, StopIteration):
        return False
    present = {str(col).strip() for col in header}
    return set(required_columns).issubset(present)


class QBOItemQueryValidationError(RuntimeError):
    """Raised when QBO rejects an Item query SELECT field."""


def _is_query_validation_error_response(status_code: int, text: str | None) -> bool:
    body = str(text or "").lower()
    return int(status_code) == 400 and "queryvalidationerror" in body


def _report_rows(container: Any) -> list[dict[str, Any]]:
    if not container:
        return []
    if isinstance(container, list):
        return [row for row in container if isinstance(row, dict)]
    if not isinstance(container, dict):
        return []
    rows = container.get("Row")
    if rows is None:
        rows = container.get("Rows")
    if isinstance(rows, dict):
        return _report_rows(rows)
    if isinstance(rows, list):
        return [row for row in rows if isinstance(row, dict)]
    return []


def _col_value(row: dict[str, Any], index: int) -> str:
    cols = row.get("ColData")
    if not isinstance(cols, list) or index >= len(cols):
        return ""
    cell = cols[index]
    if not isinstance(cell, dict):
        return ""
    return str(cell.get("value") or "").strip()


def _header_item(section: dict[str, Any]) -> tuple[str, str]:
    header = section.get("Header")
    if not isinstance(header, dict):
        return "", ""
    cols = header.get("ColData")
    if not isinstance(cols, list) or not cols or not isinstance(cols[0], dict):
        return "", ""
    item_name = str(cols[0].get("value") or "").strip()
    item_id = str(cols[0].get("id") or "").strip()
    return item_id, item_name


def parse_inventory_valuation_starting_quantities(payload: dict[str, Any]) -> dict[str, dict[str, Any]]:
    """Parse QBO InventoryValuationDetail item sections into starting-quantity rows keyed by Item.Id."""

    results: dict[str, dict[str, Any]] = {}

    def walk(rows: list[dict[str, Any]]) -> None:
        for section in rows:
            item_id, item_name = _header_item(section)
            child_rows = _report_rows(section.get("Rows"))
            if item_id or item_name:
                start_rows: list[dict[str, Any]] = []
                has_beginning_balance = False
                for child in child_rows:
                    txn_type = _col_value(child, 1).lower()
                    doc_no = _col_value(child, 2).lower()
                    if txn_type == "inventory starting value" and doc_no == "start":
                        start_rows.append(child)
                    elif txn_type == "beginning balance":
                        has_beginning_balance = True
                key = item_id or f"name:{_normalize_name_key(item_name)}"
                if start_rows:
                    qty_values = [_safe_float_optional(_col_value(row, 4)) for row in start_rows]
                    qty_values = [value for value in qty_values if value is not None]
                    qty = sum(qty_values) if qty_values else None
                    latest = start_rows[-1]
                    status = "found" if len(start_rows) == 1 else "found_multiple_start_rows"
                    results[key] = {
                        "item_id": item_id,
                        "item_name": item_name,
                        "current_starting_qty": qty,
                        "rate": _safe_float_optional(_col_value(latest, 5)),
                        "inventory_cost": _safe_float_optional(_col_value(latest, 6)),
                        "qty_on_hand_at_start": _safe_float_optional(_col_value(latest, 7)),
                        "asset_value": _safe_float_optional(_col_value(latest, 8)),
                        "source": "inventory_valuation_detail_start_row",
                        "status": status,
                        "start_row_count": len(start_rows),
                    }
                elif has_beginning_balance:
                    results[key] = {
                        "item_id": item_id,
                        "item_name": item_name,
                        "current_starting_qty": None,
                        "source": "inventory_valuation_detail",
                        "status": "beginning_balance_only",
                        "start_row_count": 0,
                    }
            walk(child_rows)

    walk(_report_rows(payload.get("Rows")))
    return results


def fetch_qbo_inventory_starting_quantities(
    *,
    token_mgr: TokenManager,
    realm_id: str,
    item_ids: Iterable[str] | None = None,
    start_date: str = "1900-01-01",
) -> dict[str, dict[str, Any]]:
    base_url = get_qbo_api_base_url()
    query_params = {
        "minorversion": _QBO_MINOR_VERSION,
        "start_date": start_date,
        "end_date": date.today().isoformat(),
    }
    ids = [str(item_id).strip() for item_id in (item_ids or []) if str(item_id).strip()]
    if ids:
        query_params["item"] = ",".join(ids)
    params = urlencode(query_params)
    url = f"{base_url}/v3/company/{realm_id}/reports/InventoryValuationDetail?{params}"
    resp = _make_qbo_request("GET", url, token_mgr)
    if resp.status_code != 200:
        raise RuntimeError(
            f"QBO InventoryValuationDetail failed: HTTP {resp.status_code}: {resp.text[:2000] if resp.text else ''}"
        )
    return parse_inventory_valuation_starting_quantities(resp.json())


def _chunked(values: list[str], size: int) -> Iterable[list[str]]:
    for idx in range(0, len(values), size):
        yield values[idx : idx + size]


def _fetch_missing_qbo_inventory_starting_quantities(
    *,
    token_mgr: TokenManager,
    realm_id: str,
    item_ids: Iterable[str],
    existing: dict[str, dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    missing = [
        str(item_id).strip()
        for item_id in item_ids
        if str(item_id).strip()
        and existing.get(str(item_id).strip(), {}).get("current_starting_qty") is None
    ]
    if not missing:
        return {}

    found: dict[str, dict[str, Any]] = {}
    for batch in _chunked(missing, _QBO_STARTING_QTY_FALLBACK_BATCH_SIZE):
        try:
            batch_rows = fetch_qbo_inventory_starting_quantities(
                token_mgr=token_mgr,
                realm_id=realm_id,
                item_ids=batch,
            )
        except Exception as exc:
            print(f"[WARN] Failed targeted QBO START-row lookup for {len(batch)} item(s): {exc}")
            continue
        for item_id, row in batch_rows.items():
            if row.get("current_starting_qty") is not None:
                row = dict(row)
                row["source"] = "inventory_valuation_detail_item_filter_start_row"
                found[item_id] = row
    return found


def _qbo_query_items_page(
    token_mgr: TokenManager,
    *,
    realm_id: str,
    start_position: int,
    max_results: int,
    select_fields: list[str] | None = None,
) -> list[dict[str, Any]]:
    """Fetch a single page of Inventory Items from QBO via the query endpoint."""
    from urllib.parse import quote

    fields = list(select_fields or _QBO_ITEM_DIAGNOSTIC_SELECT_FIELDS)
    query = (
        f"select {', '.join(fields)} "
        "from Item "
        "where Active = true and Type = 'Inventory' "
        f"startposition {int(start_position)} maxresults {int(max_results)}"
    )
    base_url = get_qbo_api_base_url()
    url = f"{base_url}/v3/company/{realm_id}/query?query={quote(query)}&minorversion={_QBO_MINOR_VERSION}"
    resp = _make_qbo_request("GET", url, token_mgr)
    if resp.status_code != 200:
        if _is_query_validation_error_response(resp.status_code, resp.text):
            raise QBOItemQueryValidationError(
                f"QBO Item query rejected select fields {fields}: {resp.text[:2000] if resp.text else ''}"
            )
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
    enrich_starting_quantities: bool = True,
) -> Path:
    """Query QBO for Inventory items and write a snapshot CSV, optionally reusing a fresh cache."""
    output_path = output_path.expanduser()
    output_path.parent.mkdir(parents=True, exist_ok=True)

    stale_reason = get_qbo_snapshot_stale_reason(company_key, output_path)
    if stale_reason:
        print(f"[INFO] Ignoring cached QBO snapshot: invalidated ({stale_reason})")

    missing_starting_columns = bool(
        enrich_starting_quantities
        and output_path.exists()
        and not _snapshot_has_required_columns(output_path, _QBO_STARTING_QTY_SNAPSHOT_COLUMNS)
    )
    if missing_starting_columns:
        print("[INFO] Ignoring cached QBO snapshot: missing starting-quantity columns")

    if (
        not force_refresh
        and stale_reason is None
        and not missing_starting_columns
        and _is_cache_fresh(output_path, max_age_hours=cache_max_age_hours)
    ):
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

    def _fetch_all_pages(select_fields: list[str]) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        start = 1
        page_size = 1000
        while True:
            page = _qbo_query_items_page(
                token_mgr,
                realm_id=realm_id,
                start_position=start,
                max_results=page_size,
                select_fields=select_fields,
            )
            if not page:
                break
            out.extend(page)
            if len(page) < page_size:
                break
            start += page_size
        return out

    try:
        rows = _fetch_all_pages(_QBO_ITEM_DIAGNOSTIC_SELECT_FIELDS)
    except QBOItemQueryValidationError as exc:
        print(
            "[WARN] QBO rejected optional Item diagnostic fields; "
            f"retrying with safe baseline fields: {', '.join(_QBO_ITEM_SAFE_SELECT_FIELDS)}. "
            f"Reason: {exc}"
        )
        rows = _fetch_all_pages(_QBO_ITEM_SAFE_SELECT_FIELDS)

    starting_by_id: dict[str, dict[str, Any]] = {}
    if enrich_starting_quantities:
        try:
            starting_by_id = fetch_qbo_inventory_starting_quantities(token_mgr=token_mgr, realm_id=realm_id)
            found = sum(1 for value in starting_by_id.values() if value.get("current_starting_qty") is not None)
            print(f"[INFO] QBO InventoryValuationDetail START rows discovered: {found}")
            targeted = _fetch_missing_qbo_inventory_starting_quantities(
                token_mgr=token_mgr,
                realm_id=realm_id,
                item_ids=[str(it.get("Id") or "") for it in rows],
                existing=starting_by_id,
            )
            if targeted:
                starting_by_id.update(targeted)
                print(f"[INFO] QBO targeted START-row lookup discovered: {len(targeted)}")
        except Exception as exc:
            print(f"[WARN] Failed to fetch QBO inventory starting quantities: {exc}")

    with open(output_path, "w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=_QBO_SNAPSHOT_COLUMNS,
        )
        writer.writeheader()
        for it in rows:
            name_original = str(it.get("Name", ""))
            name_display = _collapse_spaces(name_original)
            item_id = str(it.get("Id", "")).strip()
            start = starting_by_id.get(item_id) or starting_by_id.get(f"name:{_normalize_name_key(name_display)}") or {}
            writer.writerow(
                {
                    "Id": item_id,
                    "Name": name_original,
                    "Type": str(it.get("Type", "")).strip(),
                    "TrackQtyOnHand": str(it.get("TrackQtyOnHand", "")).strip(),
                    "QtyOnHand": str(it.get("QtyOnHand", "")).strip(),
                    "Active": str(it.get("Active", True)).strip(),
                    "InvStartDate": str(it.get("InvStartDate", "")).strip(),
                    "ParentRef": _qbo_parent_ref_value(it.get("ParentRef")),
                    "UnitPrice": str(it.get("UnitPrice", "")).strip(),
                    "PurchaseCost": str(it.get("PurchaseCost", "")).strip(),
                    "qbo_current_starting_qty": _format_optional_qty(start.get("current_starting_qty")),
                    "qbo_starting_qty_rate": _format_optional_qty(start.get("rate")),
                    "qbo_starting_inventory_cost": _format_optional_qty(start.get("inventory_cost")),
                    "qbo_starting_asset_value": _format_optional_qty(start.get("asset_value")),
                    "qbo_starting_qty_source": str(start.get("source") or "").strip(),
                    "qbo_starting_qty_status": str(start.get("status") or "not_found").strip(),
                    "qbo_name_original": name_original,
                    "qbo_name_raw": name_original,
                    "qbo_name_display": name_display,
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
    base_names: Optional[list[str]] = None,
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

    requested_base_names = list(base_names or [])
    derived_base_names = []
    multipliers = []
    for n in names.tolist():
        base, mult = strip_pack_multiplier(n)
        derived_base_names.append(_collapse_spaces(base))
        multipliers.append(int(mult))

    out = pd.DataFrame(
        {
            "raw_name": names,
            "base_name": derived_base_names,
            "base_name_norm": [_normalize_name_key(v) for v in derived_base_names],
            "multiplier": multipliers,
            "epos_qty_raw": qty_raw,
            "category_name": category_values,
        }
    )
    out["epos_single_units"] = out["epos_qty_raw"] * out["multiplier"]
    out["epos_has_pack"] = out["multiplier"] > 1

    # Optional pack-row columns — older exports/tests may omit them.
    # EPOS values often look like "23 of 24 Each" for Current Volume and a
    # fractional pack count for Total Stock (e.g. 25.958 packs of *24).
    cols_lc = {str(c).strip().lower(): c for c in df.columns}
    current_volume_col = (
        cols_lc.get("current volume")
        or cols_lc.get("measuredcurrentvolume")
        or cols_lc.get("currentvolume")
        or cols_lc.get("measured current volume")
    )
    total_stock_col = (
        cols_lc.get("total stock")
        or cols_lc.get("measuredtotalstock")
        or cols_lc.get("totalstock")
        or cols_lc.get("measured total stock")
    )

    mask_pack = out["epos_has_pack"]
    mask_loose_parsed = pd.Series([False] * len(out), index=out.index)

    # Preferred: Current Volume provides the loose-unit remainder for the
    # active pack size (e.g. +23 loose each for "23 of 24 Each").
    if current_volume_col and mask_pack.any():
        loose_units = df[current_volume_col].map(_parse_current_volume_loose_units)
        loose_units = pd.Series(loose_units, index=out.index, dtype="float64")
        mask_loose_parsed = mask_pack & loose_units.notna()
        if mask_loose_parsed.any():
            out.loc[mask_loose_parsed, "epos_single_units"] = (
                out.loc[mask_loose_parsed, "epos_qty_raw"] * out.loc[mask_loose_parsed, "multiplier"]
                + loose_units.loc[mask_loose_parsed]
            )

    # Fallback: when Current Volume is missing/unparseable, Total Stock is
    # treated as a fractional pack count; normalize via:
    #   round(Total Stock * pack_multiplier)
    if total_stock_col and mask_pack.any():
        total_stock = df[total_stock_col].map(_safe_float_optional)
        total_stock = pd.Series(total_stock, index=out.index, dtype="float64")
        mask_total_available = mask_pack & (~mask_loose_parsed) & total_stock.notna()
        if mask_total_available.any():
            raw = total_stock.loc[mask_total_available] * out.loc[mask_total_available, "multiplier"]
            out.loc[mask_total_available, "epos_single_units"] = raw.map(lambda v: float(int(round(v))))

    out = _apply_epos_negative_stock_policy(out)

    if categories:
        requested = {_normalize_category_value(v).lower() for v in categories if _normalize_category_value(v)}
        out = out[out["category_name"].str.lower().isin(requested)].copy()

    if product_filter:
        out = out[literal_product_filter_mask(out, product_filter, raw_col="raw_name")].copy()

    if requested_base_names:
        requested = {
            _normalize_name_key(_collapse_spaces(v))
            for v in requested_base_names
            if str(v).strip()
        }
        if requested:
            out = out[out["base_name_norm"].isin(requested)].copy()

    # Group to normalized base_name (case-insensitive matching), while
    # preserving original display casing for reports.
    grouped = (
        out.groupby("base_name_norm", as_index=False)
        .agg(
            base_name=("base_name", "first"),
            epos_single_units=("epos_single_units", "sum"),
            epos_raw_rows=("raw_name", "count"),
            epos_has_pack=("epos_has_pack", "max"),
            epos_categories=("category_name", _join_unique_non_blank),
            epos_category_count=("category_name", lambda s: len({v for v in s if str(v).strip()})),
            epos_negative_rows_clamped=("epos_negative_rows_clamped", "sum"),
            epos_negative_units_clamped=("epos_negative_units_clamped", "sum"),
            epos_negative_stock_policy=("epos_negative_stock_policy", "first"),
            epos_negative_clamped_row_names=("epos_negative_clamped_row_names", _join_unique_non_blank),
        )
        .sort_values("base_name")
        .reset_index(drop=True)
    )
    return grouped


def load_qbo_inventory_snapshot(qbo_csv_path: str, *, base_names: Optional[list[str]] = None) -> pd.DataFrame:
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
    inv = _dedupe_qbo_rows_by_id(inv)
    if "Active" in inv.columns:
        inv["qbo_is_active"] = inv["Active"].map(_safe_active_bool)
    else:
        inv["qbo_is_active"] = True

    if "qbo_name_original" in inv.columns:
        names_original = _original_qbo_names(inv["qbo_name_original"])
    elif "qbo_name_raw" in inv.columns:
        names_original = _original_qbo_names(inv["qbo_name_raw"])
    else:
        names_original = _original_qbo_names(inv["Name"])
    names_display = names_original.map(_collapse_spaces)
    requested_base_names = list(base_names or [])
    derived_base_names = []
    had_pack = []
    for n in names_display.tolist():
        base, mult = strip_pack_multiplier(n)
        derived_base_names.append(_collapse_spaces(base))
        had_pack.append(mult > 1)
    inv["base_name"] = derived_base_names
    inv["base_name_norm"] = inv["base_name"].map(_normalize_name_key)
    inv["qbo_name_original"] = names_original
    inv["qbo_name_raw"] = names_original
    inv["qbo_name_display"] = names_display
    inv["qbo_has_pack"] = had_pack
    inv["qbo_qty_on_hand"] = inv.get("QtyOnHand", 0).map(_safe_float)
    inv["qbo_is_base_item"] = [not v for v in had_pack]
    inv["qbo_current_starting_qty"] = inv.get(
        "qbo_current_starting_qty", pd.Series([""] * len(inv), index=inv.index)
    ).map(_safe_float_optional)
    inv["qbo_starting_qty_source"] = inv.get(
        "qbo_starting_qty_source", pd.Series([""] * len(inv), index=inv.index)
    ).astype(str)
    inv["qbo_starting_qty_status"] = inv.get(
        "qbo_starting_qty_status", pd.Series(["not_found"] * len(inv), index=inv.index)
    ).astype(str)

    if requested_base_names:
        requested = {
            _normalize_name_key(_collapse_spaces(v))
            for v in requested_base_names
            if str(v).strip()
        }
        if requested:
            inv = inv[inv["base_name_norm"].isin(requested)].copy()

    # Group by base_name to detect ambiguity; keep ids list for base-names
    def _join_ids(series: Iterable[Any]) -> str:
        ids = []
        for raw in series:
            value = str(raw).strip()
            if value and value not in ids:
                ids.append(value)
        return ",".join(ids[:50])  # cap to avoid huge cells

    def _join_names(series: Iterable[Any]) -> str:
        names_out = []
        for raw in series:
            value = str(raw).strip()
            if value and value not in names_out:
                names_out.append(value)
        return " | ".join(names_out[:10])

    def _join_pack_names(df_group: pd.DataFrame) -> str:
        items = df_group[df_group["qbo_has_pack"] == True]  # noqa: E712
        return _join_names(items["qbo_name_display"].tolist())

    def _join_base_names(df_group: pd.DataFrame) -> str:
        items = df_group[df_group["qbo_has_pack"] == False]  # noqa: E712
        return _join_names(items["qbo_name_display"].tolist())

    def _starting_qty_from_row(row: pd.Series) -> tuple[str, str, str]:
        status = str(row.get("qbo_starting_qty_status") or "not_found").strip() or "not_found"
        qty = row.get("qbo_current_starting_qty")
        if qty is None or pd.isna(qty):
            return "", str(row.get("qbo_starting_qty_source") or "").strip(), status
        return (
            _format_optional_qty(float(qty)),
            str(row.get("qbo_starting_qty_source") or "").strip(),
            status,
        )

    def _target_starting_qty(active: pd.DataFrame, active_base: pd.DataFrame) -> tuple[str, str, str]:
        if active.empty:
            return "", "", "missing_qbo_item"
        if len(active_base) == 1:
            return _starting_qty_from_row(active_base.iloc[0])
        if len(active_base) > 1:
            return "", "", "ambiguous_multiple_base_items"
        if len(active) == 1:
            return _starting_qty_from_row(active.iloc[0])
        return "", "", "missing_qbo_base_item"

    def _pack_starting_value_plan(active_pack: pd.DataFrame) -> str:
        plan: list[str] = []
        for _, row in active_pack.sort_values("qbo_name_display").iterrows():
            name = str(row.get("qbo_name_display") or row.get("Name") or "").strip()
            if not name:
                continue
            qbo_qty = _safe_float(row.get("qbo_qty_on_hand"), 0.0)
            start_qty = row.get("qbo_current_starting_qty")
            if start_qty is None or pd.isna(start_qty):
                status = str(row.get("qbo_starting_qty_status") or "not_found").strip() or "not_found"
                plan.append(f"{name}: current QBO {_format_optional_qty(qbo_qty)} -> START unavailable ({status})")
                continue
            new_initial = float(start_qty) - qbo_qty
            plan.append(f"{name}: set New Initial Qty {_format_optional_qty(new_initial)} to make current QBO 0")
        return " | ".join(plan[:10])

    grouped_rows: list[dict[str, Any]] = []
    for base_norm, g in inv.groupby("base_name_norm"):
        active = g[g["qbo_is_active"] == True]  # noqa: E712
        active_base = active[active["qbo_has_pack"] == False]  # noqa: E712
        active_pack = active[active["qbo_has_pack"] == True]  # noqa: E712
        inactive_base = g[
            (g["qbo_is_active"] != True) & (g["qbo_has_pack"] == False)  # noqa: E712
        ]
        starting_qty, starting_source, starting_status = _target_starting_qty(active, active_base)
        grouped_rows.append(
            {
                "base_name_norm": str(base_norm),
                "base_name": str(g.iloc[0]["base_name"]),
                "qbo_qty_on_hand": float(active["qbo_qty_on_hand"].sum()) if not active.empty else 0.0,
                "qbo_base_qty_on_hand": float(active_base["qbo_qty_on_hand"].sum()) if not active_base.empty else 0.0,
                "qbo_pack_variant_qty_on_hand": float(active_pack["qbo_qty_on_hand"].sum()) if not active_pack.empty else 0.0,
                "qbo_item_row_count_for_base": int(len(active)),
                "qbo_unique_item_count_for_base": int(active["qbo_name_display"].astype(str).str.strip().nunique()) if not active.empty else 0,
                "qbo_item_count_for_base": int(len(active)),
                "qbo_has_pack_variants": bool(not active_pack.empty),
                "qbo_active_base_item_count": int(len(active_base)),
                "qbo_base_item_count": int(len(active_base)),
                "qbo_active_pack_variant_count": int(len(active_pack)),
                "qbo_item_names_for_base": _join_names(active["qbo_name_display"].tolist()),
                "qbo_base_item_names": _join_base_names(active),
                "qbo_pack_variant_names": _join_pack_names(active),
                "qbo_current_starting_qty": starting_qty,
                "qbo_starting_qty_source": starting_source,
                "qbo_starting_qty_status": starting_status,
                "qbo_pack_variant_starting_value_plan": _pack_starting_value_plan(active_pack),
                "qbo_active_base_item_ids": _join_ids(active_base["Id"].tolist()),
                "qbo_base_item_ids": _join_ids(active_base["Id"].tolist()),
                "qbo_inactive_base_item_ids": _join_ids(inactive_base["Id"].tolist()),
                "qbo_active_pack_variant_item_ids": _join_ids(active_pack["Id"].tolist()),
            }
        )

    grouped = pd.DataFrame(grouped_rows)
    if grouped.empty:
        return grouped
    grouped["qbo_base_item_names_for_base"] = grouped["qbo_base_item_names"]
    grouped["qbo_pack_variant_names_for_base"] = grouped["qbo_pack_variant_names"]
    return grouped.sort_values("base_name").reset_index(drop=True)


def load_qbo_inventory_item_rows(qbo_csv_path: str) -> pd.DataFrame:
    """
    Load per-QBO-item rows (not grouped) for Inventory + TrackQtyOnHand.

    Columns returned (best-effort):
    - Id, Name, base_name, qbo_name_original, qbo_name_raw, qbo_has_pack, qbo_qty_on_hand
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
    inv = _dedupe_qbo_rows_by_id(inv)
    if "Active" in inv.columns:
        active = inv["Active"].map(_safe_active_bool)
    else:
        active = pd.Series([True] * len(inv), index=inv.index)
    inv = inv[active == True].copy().reset_index(drop=True)  # noqa: E712
    if "qbo_name_original" in inv.columns:
        names_original = _original_qbo_names(inv["qbo_name_original"])
    elif "qbo_name_raw" in inv.columns:
        names_original = _original_qbo_names(inv["qbo_name_raw"])
    else:
        names_original = _original_qbo_names(inv["Name"])
    names_display = names_original.map(_collapse_spaces)
    base_names = []
    had_pack = []
    for n in names_display.tolist():
        base, mult = strip_pack_multiplier(n)
        base_names.append(_collapse_spaces(base))
        had_pack.append(mult > 1)

    out = pd.DataFrame(
        {
            "Id": inv.get("Id", pd.Series([""] * len(inv))),
            "Name": names_original,
            "base_name": base_names,
            "base_name_norm": [_normalize_name_key(v) for v in base_names],
            "qbo_name_original": names_original,
            "qbo_name_raw": names_original,
            "qbo_name_display": names_display,
            "qbo_has_pack": had_pack,
            "qbo_qty_on_hand": inv.get("QtyOnHand", 0).map(_safe_float),
            "Type": inv.get("Type", pd.Series([""] * len(inv))).astype(str),
            "TrackQtyOnHand": inv.get("TrackQtyOnHand", pd.Series([True] * len(inv))),
            "Active": True,
            "InvStartDate": inv.get("InvStartDate", pd.Series([""] * len(inv))).astype(str),
            "ParentRef": inv.get("ParentRef", pd.Series([""] * len(inv))).map(_qbo_parent_ref_value),
            "SubItem": inv.get("SubItem", pd.Series([""] * len(inv))).astype(str),
            "UnitPrice": inv.get("UnitPrice", pd.Series([""] * len(inv))),
            "PurchaseCost": inv.get("PurchaseCost", pd.Series([""] * len(inv))),
            "qbo_current_starting_qty": inv.get("qbo_current_starting_qty", pd.Series([""] * len(inv))).map(_safe_float_optional),
            "qbo_starting_qty_source": inv.get("qbo_starting_qty_source", pd.Series([""] * len(inv))).astype(str),
            "qbo_starting_qty_status": inv.get("qbo_starting_qty_status", pd.Series(["not_found"] * len(inv))).astype(str),
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
    if "base_name_norm" not in epos_by_base.columns:
        epos_by_base = epos_by_base.copy()
        epos_by_base["base_name_norm"] = epos_by_base["base_name"].map(_normalize_name_key)
    if "base_name_norm" not in qbo_by_base.columns:
        qbo_by_base = qbo_by_base.copy()
        qbo_by_base["base_name_norm"] = qbo_by_base["base_name"].map(_normalize_name_key)

    merged = epos_by_base.merge(
        qbo_by_base,
        on="base_name_norm",
        how="left",
        suffixes=("", "_qbo"),
    )
    if "base_name_qbo" in merged.columns:
        merged = merged.drop(columns=["base_name_qbo"])
    for col in ["epos_negative_rows_clamped"]:
        if col not in merged.columns:
            merged[col] = 0
        merged[col] = merged[col].fillna(0).astype(int)
    for col in ["epos_negative_units_clamped"]:
        if col not in merged.columns:
            merged[col] = 0.0
        merged[col] = merged[col].fillna(0.0).astype(float)
    if "epos_negative_stock_policy" not in merged.columns:
        merged["epos_negative_stock_policy"] = EPOS_NEGATIVE_STOCK_POLICY
    merged["epos_negative_stock_policy"] = merged["epos_negative_stock_policy"].fillna(EPOS_NEGATIVE_STOCK_POLICY)
    if "epos_negative_clamped_row_names" not in merged.columns:
        merged["epos_negative_clamped_row_names"] = ""
    merged["epos_negative_clamped_row_names"] = merged["epos_negative_clamped_row_names"].fillna("")
    merged["qbo_qty_on_hand"] = merged["qbo_qty_on_hand"].fillna(0.0)
    for col in ["qbo_base_qty_on_hand", "qbo_pack_variant_qty_on_hand"]:
        if col not in merged.columns:
            merged[col] = 0.0
        merged[col] = merged[col].fillna(0.0).astype(float)
    merged["qbo_item_count_for_base"] = merged["qbo_item_count_for_base"].fillna(0).astype(int)
    merged["qbo_has_pack_variants"] = (
        merged["qbo_has_pack_variants"].astype("boolean").fillna(False).astype(bool)
    )
    merged["qbo_base_item_count"] = merged.get("qbo_base_item_count", 0).fillna(0).astype(int)
    merged["qbo_base_item_ids"] = merged["qbo_base_item_ids"].fillna("")
    merged["qbo_base_item_names"] = merged.get("qbo_base_item_names", "").fillna("")
    merged["qbo_pack_variant_names"] = merged.get("qbo_pack_variant_names", "").fillna("")
    if "qbo_pack_variant_starting_value_plan" not in merged.columns:
        merged["qbo_pack_variant_starting_value_plan"] = ""
    merged["qbo_pack_variant_starting_value_plan"] = merged["qbo_pack_variant_starting_value_plan"].fillna("")
    merged["qbo_item_names_for_base"] = merged.get("qbo_item_names_for_base", "").fillna("")
    merged["qbo_base_item_names_for_base"] = merged.get("qbo_base_item_names_for_base", "").fillna("")
    merged["qbo_pack_variant_names_for_base"] = merged.get("qbo_pack_variant_names_for_base", "").fillna("")
    for col in [
        "qbo_item_row_count_for_base",
        "qbo_unique_item_count_for_base",
        "qbo_active_base_item_count",
        "qbo_active_pack_variant_count",
    ]:
        if col not in merged.columns:
            merged[col] = 0
        merged[col] = merged[col].fillna(0).astype(int)
    for col in [
        "qbo_active_base_item_ids",
        "qbo_inactive_base_item_ids",
        "qbo_active_pack_variant_item_ids",
    ]:
        if col not in merged.columns:
            merged[col] = ""
        merged[col] = merged[col].fillna("")

    merged["delta"] = merged["epos_single_units"] - merged["qbo_qty_on_hand"]
    if "qbo_current_starting_qty" not in merged.columns:
        merged["qbo_current_starting_qty"] = ""
    if "qbo_starting_qty_source" not in merged.columns:
        merged["qbo_starting_qty_source"] = ""
    if "qbo_starting_qty_status" not in merged.columns:
        merged["qbo_starting_qty_status"] = "not_found"

    def _new_initial_qty(row: pd.Series) -> str:
        if int(row.get("qbo_item_count_for_base", 0) or 0) <= 0:
            return _format_optional_qty(_safe_float(row.get("epos_single_units"), 0.0))
        if bool(row.get("qbo_has_pack_variants")) and int(row.get("qbo_base_item_count", 0) or 0) == 0:
            return _format_optional_qty(_safe_float(row.get("epos_single_units"), 0.0))
        start = _safe_float_optional(row.get("qbo_current_starting_qty"))
        if start is None:
            return ""
        if bool(row.get("qbo_has_pack_variants")) and int(row.get("qbo_base_item_count", 0) or 0) == 1:
            base_delta = _safe_float(row.get("epos_single_units"), 0.0) - _safe_float(
                row.get("qbo_base_qty_on_hand"), 0.0
            )
            return _format_optional_qty(start + base_delta)
        return _format_optional_qty(start + _safe_float(row.get("delta"), 0.0))

    merged["qbo_current_starting_qty"] = merged["qbo_current_starting_qty"].map(
        lambda value: _format_optional_qty(_safe_float_optional(value))
    )
    merged["qbo_new_initial_qty_to_enter"] = [_new_initial_qty(row) for _, row in merged.iterrows()]
    merged["qbo_starting_qty_source"] = merged["qbo_starting_qty_source"].fillna("").astype(str)
    merged["qbo_starting_qty_status"] = merged["qbo_starting_qty_status"].fillna("not_found").astype(str)

    def classify(row: pd.Series) -> str:
        if row["qbo_item_count_for_base"] <= 0:
            return "missing_in_qbo"
        if abs(float(row["delta"])) <= float(tolerance):
            return "in_sync"
        if row["qbo_item_count_for_base"] > 1:
            # Multiple QBO inventory items share the same base_name; adjusting automatically is risky.
            return "ambiguous_in_qbo"
        return "needs_adjustment"

    merged["status"] = [str(classify(row)) for _, row in merged.iterrows()]

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

    merged["catalog_issue_type"] = [str(_catalog_type(row)) for _, row in merged.iterrows()]

    missing_mask = merged["qbo_item_count_for_base"] <= 0
    only_pack_mask = merged["catalog_issue_type"] == "only_pack_variant_exists"
    merged.loc[missing_mask, "qbo_starting_qty_source"] = "new_qbo_item"
    merged.loc[missing_mask, "qbo_starting_qty_status"] = "create_item_initial_qty"
    merged.loc[only_pack_mask, "qbo_starting_qty_source"] = "new_qbo_base_item"
    merged.loc[only_pack_mask, "qbo_starting_qty_status"] = "create_base_item_initial_qty"

    def _catalog_detail(row: pd.Series) -> str:
        t = str(row.get("catalog_issue_type") or "")
        if t == "only_pack_variant_exists":
            pack = str(row.get("qbo_pack_variant_names_for_base") or "").strip()
            return f"only pack variant exists in QuickBooks: {pack}" if pack else "only pack variants exist in QuickBooks"
        if t == "base_with_pack_variants":
            pack = str(row.get("qbo_pack_variant_names_for_base") or "").strip()
            if pack:
                return f"base item and pack variants both exist; consolidate pack variants: {pack}"
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

    merged["catalog_issue_detail"] = [str(_catalog_detail(row)) for _, row in merged.iterrows()]
    merged["suggested_next_action"] = [str(_suggested_action(row)) for _, row in merged.iterrows()]

    cols = [
        "base_name",
        "epos_single_units",
        "qbo_qty_on_hand",
        "qbo_base_qty_on_hand",
        "qbo_pack_variant_qty_on_hand",
        "delta",
        "qbo_current_starting_qty",
        "qbo_new_initial_qty_to_enter",
        "qbo_starting_qty_source",
        "qbo_starting_qty_status",
        "qbo_pack_variant_starting_value_plan",
        "status",
        "catalog_issue_type",
        "catalog_issue_detail",
        "suggested_next_action",
        "epos_raw_rows",
        "epos_has_pack",
        "epos_categories",
        "epos_category_count",
        "epos_negative_rows_clamped",
        "epos_negative_units_clamped",
        "epos_negative_stock_policy",
        "epos_negative_clamped_row_names",
        "qbo_item_row_count_for_base",
        "qbo_unique_item_count_for_base",
        "qbo_item_count_for_base",
        "qbo_has_pack_variants",
        "qbo_active_base_item_count",
        "qbo_base_item_count",
        "qbo_active_pack_variant_count",
        "qbo_item_names_for_base",
        "qbo_base_item_names_for_base",
        "qbo_pack_variant_names_for_base",
        "qbo_base_item_names",
        "qbo_pack_variant_names",
        "qbo_active_base_item_ids",
        "qbo_base_item_ids",
        "qbo_inactive_base_item_ids",
        "qbo_active_pack_variant_item_ids",
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
    epos_negative_rows_clamped = 0
    epos_negative_units_clamped = 0.0
    if "epos_negative_rows_clamped" in report.columns:
        epos_negative_rows_clamped = int(
            pd.to_numeric(report["epos_negative_rows_clamped"], errors="coerce").fillna(0).sum()
        )
    if "epos_negative_units_clamped" in report.columns:
        epos_negative_units_clamped = float(
            pd.to_numeric(report["epos_negative_units_clamped"], errors="coerce").fillna(0.0).sum()
        )

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
    if epos_negative_rows_clamped:
        print(
            "EPOS negative rows clamped to zero: "
            f"{epos_negative_rows_clamped} ({epos_negative_units_clamped} units)"
        )
    print("-" * 68)
    print(f"Wrote report: {out_path}")
    print("=" * 68)

    if args.apply:
        print(
            "Error: inventory quantity apply has been removed. "
            "Use audit/preview output to perform QBO UI Adjust starting value corrections."
        )
        _emit_metadata({"mode": "apply_removed", "posted": 0, "skipped": 0})
        return 2

    if not args.dry_run:
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
                            "epos_negative_rows_clamped": epos_negative_rows_clamped,
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

    txn_date = (args.txn_date or "").strip()
    if not txn_date:
        txn_date = datetime.now().strftime("%Y-%m-%d")

    qbo_rows = load_qbo_inventory_item_rows(str(qbo_path))

    if args.allow_ambiguous:
        candidates = report[report["status"].isin(["needs_adjustment", "ambiguous_in_qbo"])].copy()
    else:
        candidates = report[report["status"] == "needs_adjustment"].copy()
    if args.product_filter:
        candidates = candidates[literal_product_filter_mask(candidates, args.product_filter)].copy()

    if candidates.empty:
        print("[INFO] No applicable rows to adjust (needs_adjustment/ambiguous with --allow-ambiguous).")
        _emit_metadata({
                "mode": "manual_starting_value_preview",
                "posted": 0,
                "planned": 0,
                "skipped": 0,
                "reason": "no_candidates",
            })
        return 0

    max_qty_delta: Optional[float] = args.max_qty_delta
    if max_qty_delta is None:
        max_qty_delta = config.inventory_max_qty_delta
    if max_qty_delta is not None and max_qty_delta <= 0:
        max_qty_delta = None
    if max_qty_delta is not None:
        print(f"[INFO] Per-item qty-delta cap active: |QtyDiff| <= {max_qty_delta}")

    planned = 0
    skipped = 0
    skipped_non_exact_pick = 0
    manual_review_examples: list[str] = []
    for _, row in candidates.iterrows():
        if planned >= int(args.max_adjustments):
            print(f"[WARN] Hit --max-adjustments={args.max_adjustments}; stopping.")
            break

        base = str(row["base_name"])
        epos_target = float(row["epos_single_units"])
        base_norm = _normalize_name_key(base)
        if "base_name_norm" in qbo_rows.columns:
            group = qbo_rows[qbo_rows["base_name_norm"] == base_norm]
        else:
            group = qbo_rows[qbo_rows["base_name"].map(_normalize_name_key) == base_norm]

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

        if reason != "exact_name_match" and not (
            args.allow_fallback_picks and reason == "fallback_largest_qty"
        ):
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
                        f"{base} - only pack variant exists in QuickBooks: {chosen_name}"
                    )
                else:
                    manual_review_examples.append(f"{base} - no exact QuickBooks item match found")
            continue

        print("-" * 68)
        print(
            "Manual starting-value correction preview "
            f"item_id={item_id} base={base!r} qbo_qty={current_qty} "
            f"epos_qty={epos_target} delta={qty_diff} date={txn_date}"
        )
        planned += 1

    print("=" * 68)
    print(f"Manual starting-value corrections planned: {planned} | skipped: {skipped}")

    _emit_metadata({
        "mode": "manual_starting_value_preview",
        "posted": 0,
        "planned": planned,
        "skipped": skipped,
        "txn_date": txn_date,
    })
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
