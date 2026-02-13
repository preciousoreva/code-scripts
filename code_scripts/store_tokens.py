#!/usr/bin/env python3
"""
Helper script to store QBO OAuth tokens into qbo_tokens.sqlite.

This script simplifies onboarding on a new machine by providing a CLI interface
for storing tokens without hardcoding secrets.

⚠️  WARNING: DO NOT HARDCODE TOKENS OR CREDENTIALS IN THIS FILE ⚠️

- Tokens must be passed via CLI arguments only
- Tokens are stored in qbo_tokens.sqlite (which is gitignored)
- Secret scanning runs in CI and will block PRs if secrets are detected
- Never commit access tokens, refresh tokens, or any credentials

Usage:
    # Store tokens for a company
    python store_tokens.py --company company_a --access-token "..." --refresh-token "..."

    # List stored tokens (safe fields only)
    python store_tokens.py --list
"""

import argparse
import sys
import sqlite3
from pathlib import Path
from datetime import datetime

from code_scripts.token_manager import store_tokens_from_oauth
from code_scripts.company_config import load_company_config

# SQLite database file (same as token_manager.py)
SCRIPT_DIR = Path(__file__).resolve().parent
DB_FILE = SCRIPT_DIR / "qbo_tokens.sqlite"


def redact_tokens(text: str, access_token: str = "", refresh_token: str = "") -> str:
    """Redact tokens from error messages (best-effort)."""
    if access_token:
        text = text.replace(access_token, "[REDACTED_ACCESS_TOKEN]")
    if refresh_token:
        text = text.replace(refresh_token, "[REDACTED_REFRESH_TOKEN]")
    return text


def list_stored_tokens() -> None:
    """List stored tokens (safe fields only)."""
    if not DB_FILE.exists():
        print("No database found. Tokens have not been stored yet.")
        print(f"Database location: {DB_FILE}")
        return

    try:
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.execute(
            "SELECT company_key, realm_id, environment, updated_at "
            "FROM qbo_tokens "
            "ORDER BY company_key, realm_id"
        )
        rows = cursor.fetchall()
        conn.close()

        if not rows:
            print("No tokens stored in database.")
            return

        print("\nStored tokens:")
        print("-" * 80)
        print(f"{'Company':<20} {'Realm ID':<20} {'Environment':<12} {'Updated At':<20}")
        print("-" * 80)

        for row in rows:
            company_key, realm_id, environment, updated_at = row
            # Convert Unix timestamp to human-readable
            if updated_at:
                dt = datetime.fromtimestamp(updated_at)
                updated_str = dt.strftime("%Y-%m-%d %H:%M:%S")
            else:
                updated_str = "N/A"

            env_str = environment or "production"
            print(f"{company_key:<20} {realm_id:<20} {env_str:<12} {updated_str:<20}")

        print("-" * 80)
        print(f"\nTotal: {len(rows)} token record(s)")

    except Exception as e:
        print(f"Error reading database: {e}")
        sys.exit(1)


def store_tokens(
    company_key: str,
    access_token: str,
    refresh_token: str,
    expires_in: int = 3600,
    environment: str = "production"
) -> None:
    """Store tokens for a company."""
    try:
        # Load company config to get realm_id
        config = load_company_config(company_key)

        # Store tokens
        store_tokens_from_oauth(
            company_key=company_key,
            realm_id=config.realm_id,
            access_token=access_token,
            refresh_token=refresh_token,
            expires_in=expires_in,
            environment=environment
        )

        # Print confirmation (safe fields only)
        display_name = config.display_name
        print(f"\n✅ Tokens stored successfully!")
        print(f"   Company: {company_key} ({display_name})")
        print(f"   Realm ID: {config.realm_id}")
        print(f"   Environment: {environment}")
        print(f"   Expires in: {expires_in} seconds")

    except FileNotFoundError as e:
        error_msg = redact_tokens(str(e), access_token, refresh_token)
        print(f"❌ Error: {error_msg}", file=sys.stderr)
        sys.exit(1)
    except ValueError as e:
        error_msg = redact_tokens(str(e), access_token, refresh_token)
        print(f"❌ Error: {error_msg}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        error_msg = redact_tokens(str(e), access_token, refresh_token)
        print(f"❌ Unexpected error: {error_msg}", file=sys.stderr)
        sys.exit(1)


def main():
    parser = argparse.ArgumentParser(
        description="Store QBO OAuth tokens into qbo_tokens.sqlite",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Store tokens for company_a
  python store_tokens.py --company company_a --access-token "..." --refresh-token "..."

  # Store tokens with custom expires_in and environment
  python store_tokens.py --company company_b --access-token "..." --refresh-token "..." --expires-in 3600 --env sandbox

  # List all stored tokens
  python store_tokens.py --list
        """
    )

    parser.add_argument(
        "--company",
        type=str,
        help="Company key (e.g., company_a, company_b)"
    )
    parser.add_argument(
        "--access-token",
        type=str,
        help="OAuth access token"
    )
    parser.add_argument(
        "--refresh-token",
        type=str,
        help="OAuth refresh token"
    )
    parser.add_argument(
        "--expires-in",
        type=int,
        default=3600,
        help="Token expiration time in seconds (default: 3600)"
    )
    parser.add_argument(
        "--env",
        type=str,
        default="production",
        choices=["production", "sandbox"],
        help="Environment: production or sandbox (default: production)"
    )
    parser.add_argument(
        "--list",
        action="store_true",
        help="List stored tokens (safe fields only). Ignores other arguments."
    )

    args = parser.parse_args()

    # Handle --list mode
    if args.list:
        list_stored_tokens()
        return

    # Validate required arguments for store mode
    if not args.company:
        print("❌ Error: --company is required", file=sys.stderr)
        parser.print_help()
        sys.exit(1)

    if not args.access_token:
        print("❌ Error: --access-token is required", file=sys.stderr)
        parser.print_help()
        sys.exit(1)

    if not args.refresh_token:
        print("❌ Error: --refresh-token is required", file=sys.stderr)
        parser.print_help()
        sys.exit(1)

    # Store tokens
    store_tokens(
        company_key=args.company,
        access_token=args.access_token,
        refresh_token=args.refresh_token,
        expires_in=args.expires_in,
        environment=args.env
    )


if __name__ == "__main__":
    main()

