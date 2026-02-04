#!/usr/bin/env python3
"""
Run QBO Account queries (Name-based matching). Optionally derive account name from Product.Mapping.csv.
Read-only; uses qbo_upload auth.

Usage:
  python scripts/qbo_queries/qbo_account_query.py --company company_a --account-name "120300 - Non - Food Items"
  python scripts/qbo_queries/qbo_account_query.py --company company_a --account-number 120000
  python scripts/qbo_queries/qbo_account_query.py --company company_a --account-name "120300 - Non - Food Items" --verbose
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from urllib.parse import quote

_REPO_ROOT = Path(__file__).resolve().parent.parent.parent
_QUERIES = Path(__file__).resolve().parent
for p in (_REPO_ROOT, _QUERIES):
    if str(p) not in sys.path:
        sys.path.insert(0, str(p))

from load_env import load_env_file
from company_config import load_company_config, get_available_companies
from token_manager import verify_realm_match
from qbo_upload import BASE_URL, _make_qbo_request, TokenManager
from qbo_verify_mapping_accounts import load_mapping_accounts_with_provenance

load_env_file()

ACCOUNT_SELECT = "Id, Name, Active, AccountType"
MAX_RESULTS = 10


def run_query(token_mgr: TokenManager, realm_id: str, query: str) -> list[dict]:
    url = f"{BASE_URL}/v3/company/{realm_id}/query?query={quote(query)}&minorversion=70"
    resp = _make_qbo_request("GET", url, token_mgr)
    if resp.status_code != 200:
        print(f"HTTP {resp.status_code}", resp.text[:1000])
        return []
    data = resp.json()
    qr = data.get("QueryResponse", {})
    if "totalCount" in qr:
        print(f"QueryResponse.totalCount: {qr['totalCount']}")
    accounts = qr.get("Account", [])
    if not isinstance(accounts, list):
        accounts = [accounts] if accounts else []
    return accounts


def find_sample_mapping_from_file(mapping_file: Path, account_number: str) -> str | None:
    rows = load_mapping_accounts_with_provenance(mapping_file)
    prefix = f"{account_number} - "
    for _cat, _col, acct in rows:
        if acct.startswith(prefix):
            return acct
    return None


def parse_sample_mapping(sample_mapping: str) -> tuple[str, str]:
    s = sample_mapping.strip()
    leaf = s.split(":")[-1].strip() if s else ""
    name_part = s.split(" - ", 1)[1].strip() if " - " in s else s
    return name_part, leaf


def main() -> None:
    parser = argparse.ArgumentParser(description="Run QBO Account queries (Name-based matching).")
    parser.add_argument("--company", required=True, choices=get_available_companies(), help="Company key")
    parser.add_argument("--account-number", default=None, help="Optional: derive sample from Product.Mapping.csv")
    parser.add_argument("--account-name", default=None, help="Leaf name for exact Name match (e.g. '120300 - Non - Food Items')")
    parser.add_argument("--sample-mapping", default=None, help="Full mapping string (e.g. '120000 - Inventory:120300 - Non - Food Items')")
    parser.add_argument("--verbose", action="store_true", help="Print full JSON; otherwise count + compact table")
    args = parser.parse_args()

    config = load_company_config(args.company)
    verify_realm_match(args.company, config.realm_id)
    token_mgr = TokenManager(config.company_key, config.realm_id)
    realm_id = config.realm_id

    sample_mapping = args.sample_mapping
    if not sample_mapping and args.account_number and config.product_mapping_file.exists():
        sample_mapping = find_sample_mapping_from_file(config.product_mapping_file, args.account_number.strip())

    print("=== A) List a few accounts ===")
    query_a = f"select {ACCOUNT_SELECT} from Account maxresults {MAX_RESULTS}"
    accounts_a = run_query(token_mgr, realm_id, query_a)
    print(f"Count: {len(accounts_a)}")
    if args.verbose:
        print(json.dumps(accounts_a, indent=2))
    else:
        for i, a in enumerate(accounts_a[:10]):
            print(f"  {i+1}. Id={a.get('Id')} Name={a.get('Name')!r}")
    print()

    leaf_b = (args.account_name or "").strip() or None
    if not leaf_b and sample_mapping:
        _name_part, leaf_b = parse_sample_mapping(sample_mapping)
    if leaf_b:
        print("=== B) Query by Name exact (leaf) ===")
        safe_leaf = leaf_b.replace("'", "''")
        query_b = f"select {ACCOUNT_SELECT} from Account where Name = '{safe_leaf}' maxresults {MAX_RESULTS}"
        accounts_b = run_query(token_mgr, realm_id, query_b)
        print(f"Count: {len(accounts_b)}")
        if args.verbose:
            print(json.dumps(accounts_b, indent=2))
        else:
            for i, a in enumerate(accounts_b[:10]):
                print(f"  {i+1}. Id={a.get('Id')} Name={a.get('Name')!r}")
    else:
        print("=== B) Skipped (no --account-name and no sample mapping)")
    print()

    if not sample_mapping:
        print("(C/D skipped: no --sample-mapping and no account in Product.Mapping.csv for --account-number)")
        return
    name_guess, leaf = parse_sample_mapping(sample_mapping)
    safe_name = name_guess.replace("'", "''")
    safe_leaf = leaf.replace("'", "''")[:80]
    print("=== C) Query by Name exact (full mapping name part) ===")
    query_c = f"select {ACCOUNT_SELECT} from Account where Name = '{safe_name}' maxresults {MAX_RESULTS}"
    accounts_c = run_query(token_mgr, realm_id, query_c)
    print(f"Count: {len(accounts_c)}")
    if not args.verbose and accounts_c:
        for i, a in enumerate(accounts_c[:5]):
            print(f"  {i+1}. Id={a.get('Id')} Name={a.get('Name')!r}")
    print()
    print("=== D) Query by Name LIKE (leaf segment) ===")
    query_d = f"select {ACCOUNT_SELECT} from Account where Name like '%{safe_leaf}%' maxresults {MAX_RESULTS}"
    accounts_d = run_query(token_mgr, realm_id, query_d)
    print(f"Count: {len(accounts_d)}")
    if not args.verbose and accounts_d:
        for i, a in enumerate(accounts_d[:5]):
            print(f"  {i+1}. Id={a.get('Id')} Name={a.get('Name')!r}")


if __name__ == "__main__":
    main()
