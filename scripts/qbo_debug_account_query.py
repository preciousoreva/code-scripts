#!/usr/bin/env python3
"""
Diagnostic: run QBO Account queries using Name-based matching only (no AccountNumber).
Read-only; reuses auth and _make_qbo_request from qbo_upload.py.

Example:
  python scripts/qbo_debug_account_query.py --company company_a --account-name "120300 - Non - Food Items"
  python scripts/qbo_debug_account_query.py --company company_a --account-number 120000
  python scripts/qbo_debug_account_query.py --company company_a --account-number 120000 --sample-mapping "120000 - Inventory:120300 - Non - Food Items"
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from urllib.parse import quote

# Run from repo root (parent of scripts/); add repo root and scripts for imports
_REPO_ROOT = Path(__file__).resolve().parent.parent
_SCRIPTS = Path(__file__).resolve().parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))
if str(_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS))

from load_env import load_env_file
from company_config import load_company_config, get_available_companies
from token_manager import verify_realm_match
from qbo_upload import BASE_URL, _make_qbo_request, TokenManager

load_env_file()

# Name-based only (no AccountNumber)
ACCOUNT_SELECT = "Id, Name, Active, AccountType"
MAX_RESULTS = 10


def run_query(
    token_mgr: TokenManager,
    realm_id: str,
    query: str,
) -> list[dict]:
    """Execute a QBO query. On non-200 prints diagnostics; on 200 prints QueryResponse keys and returns Account list."""
    url = f"{BASE_URL}/v3/company/{realm_id}/query?query={quote(query)}&minorversion=70"
    resp = _make_qbo_request("GET", url, token_mgr)

    if resp.status_code != 200:
        print(f"HTTP status: {resp.status_code}")
        print(f"Response (first 1000 chars): {resp.text[:1000]}")
        print(f"Query: {query}")
        return []

    data = resp.json()
    qr = data.get("QueryResponse", {})
    if "totalCount" in qr:
        print(f"QueryResponse.totalCount: {qr['totalCount']}")
    entity_keys = [k for k in qr.keys() if k != "totalCount"]
    if entity_keys:
        print(f"QueryResponse entities: {entity_keys}")

    accounts = qr.get("Account", [])
    if not isinstance(accounts, list):
        accounts = [accounts] if accounts else []
    return accounts


def print_results(accounts: list[dict], verbose: bool) -> None:
    """Print count and either full JSON (verbose) or compact table (Id, Name)."""
    print(f"Count: {len(accounts)}")
    if verbose:
        print(json.dumps(accounts, indent=2))
    else:
        for i, a in enumerate(accounts[:10]):
            aid = a.get("Id", "")
            name = a.get("Name", "")
            print(f"  {i+1}. Id={aid} Name={name!r}")
        if len(accounts) > 10:
            print(f"  ... and {len(accounts) - 10} more")


def find_sample_mapping_from_file(mapping_file: Path, account_number: str) -> str | None:
    """Load Product.Mapping.csv and return first account string starting with '{account_number} - '."""
    from qbo_verify_mapping_accounts import load_mapping_accounts_with_provenance

    rows = load_mapping_accounts_with_provenance(mapping_file)
    prefix = f"{account_number} - "
    for _cat, _col, acct in rows:
        if acct.startswith(prefix):
            return acct
    return None


def parse_sample_mapping(sample_mapping: str) -> tuple[str, str]:
    """Parse mapping string: leaf = substring after last ':' then strip. E.g. '120000 - Inventory:120300 - Non - Food Items' -> leaf '120300 - Non - Food Items'."""
    s = sample_mapping.strip()
    # Leaf = after last colon (full string if no colon)
    leaf = s.split(":")[-1].strip() if s else ""
    # Name part for C = everything after first ' - ' (optional, for C we use full fqn from mapping)
    if " - " not in s:
        name_part = s
    else:
        _num, name_part = s.split(" - ", 1)
        name_part = name_part.strip()
    return name_part, leaf


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run QBO Account queries using Name-based matching only (no AccountNumber)."
    )
    parser.add_argument("--company", required=True, choices=get_available_companies(), help="Company key")
    parser.add_argument("--account-number", default=None, help="Optional: used to derive sample mapping from Product.Mapping.csv for B/C/D.")
    parser.add_argument(
        "--account-name",
        default=None,
        help="Leaf name for Query B (exact Name match). E.g. '120300 - Non - Food Items'. If omitted, derived from sample mapping.",
    )
    parser.add_argument(
        "--sample-mapping",
        default=None,
        help='Full mapping string for B/C/D (e.g. "120000 - Inventory:120300 - Non - Food Items"). If omitted, derived from Product.Mapping.csv when --account-number is set.',
    )
    parser.add_argument("--verbose", action="store_true", help="Print full JSON for each query result; otherwise count + compact table.")
    args = parser.parse_args()
    verbose = args.verbose

    config = load_company_config(args.company)
    verify_realm_match(args.company, config.realm_id)
    token_mgr = TokenManager(config.company_key, config.realm_id)
    realm_id = config.realm_id

    # Resolve sample mapping for B/C/D (needed for leaf when --account-name not provided)
    sample_mapping = args.sample_mapping
    account_number = (args.account_number or "").strip()
    if not sample_mapping and account_number and config.product_mapping_file.exists():
        sample_mapping = find_sample_mapping_from_file(config.product_mapping_file, account_number)

    # --- A: List a few accounts ---
    print("=== A) List a few accounts (minimal fields) ===")
    query_a = f"select {ACCOUNT_SELECT} from Account maxresults {MAX_RESULTS}"
    accounts_a = run_query(token_mgr, realm_id, query_a)
    print_results(accounts_a, verbose)
    print()

    # --- B: By Name exact (leaf) ---
    leaf_b = (args.account_name or "").strip() or None
    if not leaf_b and sample_mapping:
        _name_part, leaf_b = parse_sample_mapping(sample_mapping)
    if leaf_b:
        print("=== B) Query by Name exact (leaf) ===")
        print(f"leaf: {leaf_b!r}")
        safe_leaf_b = leaf_b.replace("'", "''")
        query_b = f"select {ACCOUNT_SELECT} from Account where Name = '{safe_leaf_b}' maxresults {MAX_RESULTS}"
        accounts_b = run_query(token_mgr, realm_id, query_b)
        print_results(accounts_b, verbose)
    else:
        print("=== B) Query by Name exact (leaf) ===")
        print("(Skipped: no --account-name and no sample mapping to derive leaf)")
    print()

    # --- C & D: need sample mapping ---
    if not sample_mapping:
        print("(C and D skipped: no --sample-mapping and no account string in Product.Mapping.csv for --account-number)")
    else:
        name_guess, leaf = parse_sample_mapping(sample_mapping)
        safe_name = name_guess.replace("'", "''")
        safe_leaf = leaf.replace("'", "''")[:80]

        # --- C: By Name exact (full name part after first " - ") ---
        print("=== C) Query by Name exact (full mapping name part) ===")
        print(f"name_guess: {name_guess!r}")
        query_c = f"select {ACCOUNT_SELECT} from Account where Name = '{safe_name}' maxresults {MAX_RESULTS}"
        accounts_c = run_query(token_mgr, realm_id, query_c)
        print_results(accounts_c, verbose)
        print()

        # --- D: By Name LIKE (leaf segment) ---
        print("=== D) Query by Name LIKE (leaf segment) ===")
        print(f"leaf: {leaf!r}")
        query_d = f"select {ACCOUNT_SELECT} from Account where Name like '%{safe_leaf}%' maxresults {MAX_RESULTS}"
        accounts_d = run_query(token_mgr, realm_id, query_d)
        print_results(accounts_d, verbose)


if __name__ == "__main__":
    main()
