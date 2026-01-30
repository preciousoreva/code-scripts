#!/usr/bin/env python3
"""
Run an arbitrary QBO SQL-like query for a company. Uses company config for realm_id and tokens.

Usage:
  python scripts/qbo_queries/qbo_query.py --company company_a query "select Id, Name from Item maxresults 5"
  python scripts/qbo_queries/qbo_query.py --company company_b query "select Id, Name from PaymentMethod"
"""
import argparse
import json
import os
import sys
from pathlib import Path
from urllib.parse import quote

import requests

_REPO_ROOT = Path(__file__).resolve().parent.parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from load_env import load_env_file
from company_config import load_company_config, get_available_companies
from token_manager import get_access_token

load_env_file()

BASE_URL = "https://quickbooks.api.intuit.com"
MINOR_VERSION = os.environ.get("QBO_MINOR_VERSION", "70")


def qbo_query_for_company(query: str, company_key: str) -> dict:
    """Execute a QBO SQL-like query for a specific company. Returns JSON response."""
    config = load_company_config(company_key)
    realm_id = config.realm_id
    access_token = get_access_token(company_key, realm_id)
    url = f"{BASE_URL}/v3/company/{realm_id}/query?query={quote(query)}&minorversion={MINOR_VERSION}"
    resp = requests.get(url, headers={"Authorization": f"Bearer {access_token}", "Accept": "application/json"})
    resp.raise_for_status()
    return resp.json()


def main():
    parser = argparse.ArgumentParser(
        description="Query QuickBooks Online for a company (company config for realm_id and tokens)."
    )
    parser.add_argument("--company", required=True, choices=get_available_companies(), help="Company key")
    parser.add_argument("command", choices=["query"], help="Command (currently only 'query')")
    parser.add_argument("sql_query", help="SQL query string (e.g. select Id, Name from Item maxresults 5)")
    parser.add_argument("--raw-json", action="store_true", help="Print raw JSON; otherwise pretty-print")
    args = parser.parse_args()

    try:
        result = qbo_query_for_company(args.sql_query, args.company)
        if args.raw_json:
            print(json.dumps(result))
        else:
            print(json.dumps(result, indent=2))
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
