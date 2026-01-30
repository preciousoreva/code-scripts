#!/usr/bin/env python3
"""
Standalone diagnostic: verify that all accounts referenced in mappings/Product.Mapping.csv
exist in QBO using Name-based (leaf) resolution only. Read-only; uses same auth and request helpers as qbo_upload.py.

Example:
  python scripts/qbo_verify_mapping_accounts.py --company company_a
  python scripts/qbo_verify_mapping_accounts.py --company company_a --export-csv reports/mapping_account_verification_company_a.csv
  python scripts/qbo_verify_mapping_accounts.py --company company_a --maxresults 1000 --no-include-inactive
"""
from __future__ import annotations

import argparse
import csv
import re
import sys
from pathlib import Path
from urllib.parse import quote

# Run from repo root (parent of scripts/); add repo root to path for imports
_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

import pandas as pd

from load_env import load_env_file
from company_config import load_company_config, get_available_companies
from token_manager import verify_realm_match
from qbo_upload import BASE_URL, _make_qbo_request, TokenManager

load_env_file()

# Same canonical/synonym logic as qbo_upload.load_category_account_mapping
def _norm_col(col: str) -> str:
    return str(col).strip().lower()

SYNONYM_TO_CANONICAL = {
    "category": "category",
    "categories": "category",
    "inventory account": "inventory account",
    "revenue account": "revenue account",
    "cost of sale account": "cost of sale account",
    "cost of sale": "cost of sale account",
    "cogs": "cost of sale account",
}
REQUIRED_CANONICALS = {"category", "inventory account", "revenue account", "cost of sale account"}

SOURCE_COLUMN_NAMES = {
    "inventory account": "Inventory Account",
    "revenue account": "Revenue Account",
    "cost of sale account": "Cost of Sale Account",
}


def load_mapping_accounts_with_provenance(mapping_file: Path) -> list[tuple[str, str, str]]:
    """
    Read Product.Mapping.csv with same header normalization as qbo_upload.
    Returns list of (SourceCategory, SourceColumn, SourceAccountString) for each account reference.
    """
    if not mapping_file.exists():
        raise FileNotFoundError(f"Mapping file not found: {mapping_file}")
    df = pd.read_csv(mapping_file)

    def norm(col: str) -> str:
        return str(col).strip().lower()

    canonical_to_actual = {}
    for actual_col in df.columns:
        n = norm(actual_col)
        if not n or re.match(r"^unnamed", n):
            continue
        canonical = SYNONYM_TO_CANONICAL.get(n)
        if canonical and canonical not in canonical_to_actual:
            canonical_to_actual[canonical] = actual_col

    missing = REQUIRED_CANONICALS - set(canonical_to_actual.keys())
    if missing:
        raise ValueError(
            f"Product.Mapping.csv missing required columns (after normalization): {', '.join(sorted(missing))}. "
            f"Detected: {list(df.columns)}"
        )

    cat_col = canonical_to_actual["category"]
    inv_col = canonical_to_actual["inventory account"]
    rev_col = canonical_to_actual["revenue account"]
    cost_col = canonical_to_actual["cost of sale account"]

    rows: list[tuple[str, str, str]] = []
    for _, row in df.iterrows():
        category = str(row[cat_col]).strip()
        category = re.sub(r"\s+", " ", category)
        if not category or category.lower() in ("nan", "none", ""):
            continue
        for canonical, col_name in [("inventory account", inv_col), ("revenue account", rev_col), ("cost of sale account", cost_col)]:
            val = str(row[col_name]).strip()
            if not val or val.lower() in ("nan", "none", ""):
                continue
            source_col = SOURCE_COLUMN_NAMES.get(canonical, canonical)
            rows.append((category, source_col, val))
    return rows


ACCOUNT_SELECT = "Id, Name, Active, AccountType"


def parse_leaf(account_string: str) -> str:
    """Leaf = substring after last ':' then strip. E.g. '120000 - Inventory:120300 - Non - Food Items' -> '120300 - Non - Food Items'."""
    s = (account_string or "").strip()
    if not s:
        return ""
    return s.split(":")[-1].strip()


def query_account_by_name(
    token_mgr: TokenManager,
    realm_id: str,
    name_exact: str,
    include_inactive: bool,
) -> dict | None:
    """Query Account by exact Name. Returns full account dict or None."""
    if not name_exact:
        return None
    safe = name_exact.replace("'", "''")
    where = f"Name = '{safe}'"
    if not include_inactive:
        where += " and Active = true"
    query = f"select {ACCOUNT_SELECT} from Account where {where} maxresults 10"
    url = f"{BASE_URL}/v3/company/{realm_id}/query?query={quote(query)}&minorversion=70"
    resp = _make_qbo_request("GET", url, token_mgr)
    if resp.status_code != 200:
        return None
    data = resp.json()
    accounts = data.get("QueryResponse", {}).get("Account", [])
    if not accounts:
        return None
    return accounts[0] if isinstance(accounts[0], dict) else None


def query_accounts_by_name_like(
    token_mgr: TokenManager,
    realm_id: str,
    name_pattern: str,
    maxresults: int,
    include_inactive: bool,
) -> list[dict]:
    """Conservative LIKE query on Name. name_pattern escaped; no % or _ from input."""
    if not name_pattern or len(name_pattern) > 80:
        return []
    safe = str(name_pattern).replace("'", "''").replace("%", "").replace("_", "")[:50]
    if not safe:
        return []
    where = f"Name like '%{safe}%'"
    if not include_inactive:
        where += " and Active = true"
    query = f"select {ACCOUNT_SELECT} from Account where {where} maxresults {maxresults}"
    url = f"{BASE_URL}/v3/company/{realm_id}/query?query={quote(query)}&minorversion=70"
    resp = _make_qbo_request("GET", url, token_mgr)
    if resp.status_code != 200:
        return []
    data = resp.json()
    accounts = data.get("QueryResponse", {}).get("Account", [])
    if not isinstance(accounts, list):
        accounts = [accounts] if accounts else []
    return accounts[:maxresults]


def resolve_account(
    account_string: str,
    token_mgr: TokenManager,
    realm_id: str,
    cache: dict,
    include_inactive: bool,
) -> dict | None:
    """Resolve account string to QBO account dict using Name-based (leaf) resolution only."""
    if account_string in cache:
        return cache[account_string]
    leaf = parse_leaf(account_string)
    acc = query_account_by_name(token_mgr, realm_id, leaf, include_inactive)
    cache[account_string] = acc
    return acc


def suggest_accounts(
    account_string: str,
    token_mgr: TokenManager,
    realm_id: str,
    maxresults: int,
    include_inactive: bool,
) -> list[dict]:
    """Return up to maxresults suggestion account dicts using Name LIKE on first token of leaf."""
    leaf = parse_leaf(account_string)
    first_token = leaf.split()[0].strip() if leaf else ""
    if not first_token:
        return []
    return query_accounts_by_name_like(token_mgr, realm_id, first_token, maxresults, include_inactive)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Verify Product.Mapping.csv accounts exist in QBO using Name-based (leaf) resolution."
    )
    parser.add_argument("--company", required=True, choices=get_available_companies(), help="Company key")
    parser.add_argument("--export-csv", default=None, help="Write results to CSV (e.g. reports/mapping_account_verification_company_a.csv)")
    parser.add_argument("--maxresults", type=int, default=1000, help="Max results for suggestion queries (default 1000)")
    parser.add_argument("--include-inactive", action="store_true", default=True, help="Include inactive accounts (default True)")
    parser.add_argument("--no-include-inactive", action="store_false", dest="include_inactive", help="Exclude inactive accounts")
    args = parser.parse_args()

    config = load_company_config(args.company)
    mapping_file = config.product_mapping_file
    verify_realm_match(args.company, config.realm_id)
    token_mgr = TokenManager(config.company_key, config.realm_id)

    rows_with_provenance = load_mapping_accounts_with_provenance(mapping_file)
    # Unique account strings with first provenance
    seen: set[str] = set()
    unique_with_provenance: list[tuple[str, str, str]] = []
    for cat, col, acct in rows_with_provenance:
        if acct not in seen:
            seen.add(acct)
            unique_with_provenance.append((cat, col, acct))

    cache: dict = {}
    resolved: dict[str, dict] = {}
    for _cat, _col, acct in unique_with_provenance:
        acc = resolve_account(acct, token_mgr, config.realm_id, cache, args.include_inactive)
        if acc is not None:
            resolved[acct] = acc

    resolved_count = len(resolved)
    missing_count = len(unique_with_provenance) - resolved_count

    print(f"\n=== Mapping account verification ===")
    print(f"Total unique accounts: {len(unique_with_provenance)}")
    print(f"Resolved: {resolved_count}")
    print(f"Missing: {missing_count}")

    export_rows: list[dict] = []
    for source_category, source_column, source_account in unique_with_provenance:
        acc = resolved.get(source_account)
        if acc is not None:
            export_rows.append({
                "SourceCategory": source_category,
                "SourceColumn": source_column,
                "SourceAccountString": source_account,
                "Resolved": "true",
                "ResolvedId": acc.get("Id", ""),
                "ResolvedName": acc.get("Name", ""),
                "ResolvedActive": acc.get("Active", ""),
                "ResolvedAccountType": acc.get("AccountType", ""),
                "Suggestions": "",
            })
        else:
            suggestions = suggest_accounts(
                source_account,
                token_mgr,
                config.realm_id,
                min(5, args.maxresults),
                args.include_inactive,
            )
            suggestion_names = "; ".join(str(s.get("Name", "")) for s in suggestions)
            leaf = parse_leaf(source_account)
            print(f"\n--- Missing ---")
            print(f"  SourceCategory: {source_category}")
            print(f"  SourceColumn: {source_column}")
            print(f"  SourceAccountString: {source_account}")
            print(f"  Leaf used: {leaf!r}")
            print(f"  Suggestions: {suggestion_names or 'none'}")
            export_rows.append({
                "SourceCategory": source_category,
                "SourceColumn": source_column,
                "SourceAccountString": source_account,
                "Resolved": "false",
                "ResolvedId": "",
                "ResolvedName": "",
                "ResolvedActive": "",
                "ResolvedAccountType": "",
                "Suggestions": suggestion_names,
            })

    if args.export_csv:
        out_path = Path(args.export_csv)
        if not out_path.is_absolute():
            out_path = _REPO_ROOT / out_path
        out_path.parent.mkdir(parents=True, exist_ok=True)
        fieldnames = [
            "SourceCategory", "SourceColumn", "SourceAccountString",
            "Resolved", "ResolvedId", "ResolvedName",
            "ResolvedActive", "ResolvedAccountType",
            "Suggestions",
        ]
        with open(out_path, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=fieldnames)
            w.writeheader()
            w.writerows(export_rows)
        print(f"\nExported to {out_path}")


if __name__ == "__main__":
    main()
