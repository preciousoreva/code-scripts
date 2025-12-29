#!/usr/bin/env python3
"""
Helper script to query QBO for a specific company using the company config system.

Usage:
    python3 query_qbo_for_company.py --company company_b query "select Id, Name from Item where Type = 'Service' MAXRESULTS 5"
    python3 query_qbo_for_company.py --company company_a query "select Id, Name from PaymentMethod"
"""

import os
import sys
import argparse
import requests
from urllib.parse import quote

from load_env import load_env_file
from company_config import load_company_config, get_available_companies
from token_manager import get_access_token

# Load .env for shared credentials
load_env_file()

BASE_URL = "https://quickbooks.api.intuit.com"
MINOR_VERSION = os.environ.get("QBO_MINOR_VERSION", "70")


def qbo_query_for_company(query: str, company_key: str) -> dict:
    """
    Execute a QBO SQL-like query for a specific company.
    
    Args:
        query: SQL query string
        company_key: Company identifier ('company_a' or 'company_b')
    
    Returns:
        JSON response from QBO API
    """
    # Load company config to get realm_id
    config = load_company_config(company_key)
    realm_id = config.realm_id
    
    # Get access token for this company
    access_token = get_access_token(company_key, realm_id)
    
    # Build URL
    url = f"{BASE_URL}/v3/company/{realm_id}/query?query={quote(query)}&minorversion={MINOR_VERSION}"
    
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
    }
    
    resp = requests.get(url, headers=headers)
    resp.raise_for_status()
    return resp.json()


def main():
    parser = argparse.ArgumentParser(
        description="Query QuickBooks Online for a specific company using company config.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Query Items for Company B
  python3 query_qbo_for_company.py --company company_b query "select Id, Name from Item where Type = 'Service' MAXRESULTS 5"
  
  # Query PaymentMethods for Company B
  python3 query_qbo_for_company.py --company company_b query "select Id, Name from PaymentMethod"
  
  # Query Accounts for Company B
  python3 query_qbo_for_company.py --company company_b query "select Id, Name, AccountType from Account where AccountType = 'Income' MAXRESULTS 10"
        """
    )
    
    parser.add_argument(
        "--company",
        required=True,
        choices=get_available_companies(),
        help="Company identifier (REQUIRED). Available: %(choices)s",
    )
    
    parser.add_argument(
        "command",
        choices=["query"],
        help="Command to execute (currently only 'query' is supported)",
    )
    
    parser.add_argument(
        "sql_query",
        help="SQL query string to execute (e.g., \"select Id, Name from Item\")",
    )
    
    args = parser.parse_args()
    
    try:
        result = qbo_query_for_company(args.sql_query, args.company)
        
        # Pretty print the result
        import json
        print(json.dumps(result, indent=2))
        
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()

