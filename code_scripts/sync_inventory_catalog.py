from __future__ import annotations

import argparse
import csv
import re
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

import pandas as pd

from code_scripts.company_config import get_available_companies, load_company_config
from code_scripts.qbo_upload import (
    TokenManager,
    build_desired_item_state,
    find_latest_single_csv,
    get_repo_root,
    load_category_account_mapping,
    prefetch_all_items,
    resolve_all_unique_items,
)
from code_scripts.token_manager import verify_realm_match


def _write_report(path: Path, rows: List[Dict[str, Any]], fieldnames: List[str]) -> None:
    if not rows:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def _report_stamp(target_date: Optional[str]) -> str:
    if target_date:
        return re.sub(r"[^\d-]", "_", target_date)
    return datetime.now().strftime("%Y-%m-%d")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Synchronize QBO inventory item catalog (pricing/tax/category and missing-item creation) without uploading receipts."
    )
    parser.add_argument(
        "--company",
        required=True,
        choices=get_available_companies(),
        help="Company identifier (REQUIRED). Available: %(choices)s",
    )
    parser.add_argument(
        "--csv",
        dest="csv_path",
        help="Optional input CSV path. If omitted, latest transformed CSV for the company is used.",
    )
    parser.add_argument(
        "--target-date",
        help="Optional YYYY-MM-DD used for created inventory item InvStartDate and report naming.",
    )
    return parser


def main(argv: Optional[List[str]] = None) -> int:
    args = build_parser().parse_args(argv)

    try:
        config = load_company_config(args.company)
    except Exception as exc:
        print(f"Error: Failed to load company config for '{args.company}': {exc}")
        return 1

    if not getattr(config, "inventory_enabled", False):
        print(f"Error: {config.company_key} has inventory disabled. Catalog sync is only for inventory-enabled companies.")
        return 1

    try:
        verify_realm_match(config.company_key, config.realm_id)
    except RuntimeError as exc:
        print(f"Error: Realm ID safety check failed: {exc}")
        return 1

    repo_root = get_repo_root()
    if args.csv_path:
        csv_path = args.csv_path
    else:
        csv_path = find_latest_single_csv(repo_root, config)

    print("=" * 60)
    print(f"COMPANY: {config.display_name} ({config.company_key})")
    print(f"REALM ID: {config.realm_id}")
    print("CATALOG SYNC MODE: inline_maintenance")
    print(f"CSV: {csv_path}")
    print("=" * 60)

    df = pd.read_csv(csv_path)
    desired_item_state = build_desired_item_state(df)
    unique_names = list(desired_item_state.keys())
    print(f"[INFO] Catalog sync input rows: {len(df)}")
    print(f"[INFO] Unique item names to resolve: {len(unique_names)}")

    token_mgr = TokenManager(config.company_key, config.realm_id)

    started_at = time.perf_counter()
    existing_items_by_name = prefetch_all_items(token_mgr, config.realm_id)
    mapping_cache = load_category_account_mapping(config)
    account_cache: Dict[str, Optional[str]] = {}
    category_item_cache: Dict[str, str] = {}
    item_result_by_name: Dict[str, Dict[str, Any]] = {}
    patched_items: Set[str] = set()
    items_wrong_type: List[Dict[str, Any]] = []
    items_autofixed: List[Dict[str, Any]] = []
    items_patched_pricing_tax: List[Dict[str, Any]] = []

    resolve_stats = resolve_all_unique_items(
        unique_names,
        desired_item_state,
        existing_items_by_name,
        config,
        token_mgr,
        config.realm_id,
        mapping_cache,
        account_cache,
        category_item_cache,
        args.target_date,
        item_result_by_name,
        patched_items,
        patch_existing_inventory=True,
        allow_wrong_type_autofix=True,
        items_wrong_type=items_wrong_type,
        items_autofixed=items_autofixed,
        items_patched_pricing_tax=items_patched_pricing_tax,
    )

    duration_seconds = time.perf_counter() - started_at
    request_stats = token_mgr.request_stats
    request_count = int(request_stats.get("request_count", 0))
    request_total_ms = float(request_stats.get("request_duration_ms_total", 0.0))

    print("\n=== Catalog Sync Summary ===")
    print(f"Existing items prefetched: {len(existing_items_by_name)}")
    print(f"Unique items resolved: {len(item_result_by_name)}")
    print(f"Items created: {resolve_stats['items_created']}")
    print(f"Items patched: {resolve_stats['items_patched']}")
    print(f"Existing inventory patch skipped: {resolve_stats['existing_inventory_patch_skipped']}")
    print(f"Wrong-type items observed: {len(items_wrong_type)}")
    print(f"Auto-fixed wrong-type items: {len(items_autofixed)}")
    print(f"Pricing/tax patch records: {len(items_patched_pricing_tax)}")
    print(f"Duration: {duration_seconds:.2f}s")
    print(
        f"QBO requests: total={request_count} errors={request_stats.get('request_error_count', 0)} "
        f"avg_ms={(request_total_ms / request_count):.2f}" if request_count else "QBO requests: total=0 errors=0 avg_ms=0.00"
    )

    reports_dir = Path(repo_root) / "reports"
    stamp = _report_stamp(args.target_date)
    _write_report(
        reports_dir / f"inventory_catalog_wrong_type_{config.company_key}_{stamp}.csv",
        items_wrong_type,
        ["Name", "Id", "Type", "ExpectedType"],
    )
    _write_report(
        reports_dir / f"inventory_catalog_autofixed_{config.company_key}_{stamp}.csv",
        items_autofixed,
        ["OriginalName", "OldItemId", "OldType", "OldActive", "NewName", "NewInventoryItemId", "TxnDate", "DocNumber"],
    )
    _write_report(
        reports_dir / f"inventory_catalog_patched_{config.company_key}_{stamp}.csv",
        items_patched_pricing_tax,
        [
            "ItemId",
            "Name",
            "Category",
            "UnitPrice_old",
            "UnitPrice_new",
            "PurchaseCost_old",
            "PurchaseCost_new",
            "SalesTaxIncluded_old/new",
            "PurchaseTaxIncluded_old/new",
            "Taxable_old/new",
            "TxnDate",
            "DocNumber",
        ],
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
