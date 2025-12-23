import os
import sys
import json
import argparse
from typing import Any, Dict, List, Tuple, Optional
from datetime import datetime, timedelta
from pathlib import Path
from urllib.parse import quote

import pandas as pd
import requests

from qbo_auth import get_access_token
from load_env import load_env_file
from slack_notify import send_slack_success


# Load .env if present so QBO_* vars are available when running this standalone
load_env_file()

BASE_URL = "https://quickbooks.api.intuit.com"

try:
    REALM_ID = os.environ["QBO_REALM_ID"]  # companyId
except KeyError as exc:
    raise RuntimeError(
        "QBO_REALM_ID environment variable is not set. "
        "Set it in your environment or .env before running qbo_query.py."
    ) from exc

MINOR_VERSION = os.environ.get("QBO_MINOR_VERSION", "65")  # optional


def qbo_query(query: str) -> Dict[str, Any]:
    """
    Execute a QBO SQL-like query and return the JSON response.

    Intended for ad‑hoc/debug queries – NOT for high‑volume production use.
    """
    # QBO query endpoint expects GET with query as URL parameter
    url = f"{BASE_URL}/v3/company/{REALM_ID}/query?query={quote(query)}&minorversion={MINOR_VERSION}"
    access_token = get_access_token()
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
    }
    resp = requests.get(url, headers=headers)
    resp.raise_for_status()
    return resp.json()


def sales_receipt_count_for_date(date_str: str) -> Dict[str, Any]:
    """Return the COUNT(*) for SalesReceipt on a specific TxnDate."""
    query = f"SELECT COUNT(*) FROM SalesReceipt WHERE TxnDate = '{date_str}'"
    return qbo_query(query)


def sales_receipts_for_date(date_str: str, max_results: int = 1000) -> Dict[str, Any]:
    """Return basic details for SalesReceipts on a specific TxnDate."""
    query = (
        "SELECT Id, DocNumber, TxnDate, TotalAmt FROM SalesReceipt "
        f"WHERE TxnDate = '{date_str}' MAXRESULTS {max_results}"
    )
    return qbo_query(query)


def fetch_receipts_for_date_range(start_date: str, end_date: str = None) -> List[Dict[str, Any]]:
    """
    Fetch Id + SyncToken (+ some metadata) for all SalesReceipts in a date range.

    If end_date is None, only fetches receipts for start_date (single date).
    Uses pagination (STARTPOSITION / MAXRESULTS) so we don't stop at 1000.
    """
    all_receipts: List[Dict[str, Any]] = []
    start_position = 1
    page_size = 1000

    # Build WHERE clause: single date or date range
    if end_date:
        where_clause = f"TxnDate >= '{start_date}' AND TxnDate <= '{end_date}'"
    else:
        where_clause = f"TxnDate = '{start_date}'"

    while True:
        query = (
            "SELECT Id, SyncToken, DocNumber, TxnDate, TotalAmt "
            f"FROM SalesReceipt WHERE {where_clause} "
            f"STARTPOSITION {start_position} MAXRESULTS {page_size}"
        )
        data = qbo_query(query)
        qr = data.get("QueryResponse", {})
        batch = qr.get("SalesReceipt", []) or []

        if not batch:
            break

        all_receipts.extend(batch)

        # If we got less than a full page, we're done.
        if len(batch) < page_size:
            break

        start_position += page_size

    return all_receipts


def delete_sales_receipt(sales_receipt: Dict[str, Any]) -> Dict[str, Any]:
    """
    Delete a single SalesReceipt using Id + SyncToken.

    QBO uses a 'soft delete' with POST + ?operation=delete.
    """
    access_token = get_access_token()
    url = f"{BASE_URL}/v3/company/{REALM_ID}/salesreceipt?operation=delete"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Content-Type": "application/json",
    }

    payload = {
        "Id": sales_receipt["Id"],
        "SyncToken": sales_receipt["SyncToken"],
    }

    resp = requests.post(url, headers=headers, json=payload)
    try:
        body = resp.json()
    except Exception:
        body = {"raw": resp.text}

    if not (200 <= resp.status_code < 300):
        raise RuntimeError(
            f"Failed to delete SalesReceipt {sales_receipt['Id']} "
            f"(HTTP {resp.status_code}): {json.dumps(body, indent=2)[:500]}"
        )

    return body


def cmd_count(start_date: str, end_date: str = None) -> None:
    """Count SalesReceipts for a date or date range."""
    if end_date:
        where_clause = f"TxnDate >= '{start_date}' AND TxnDate <= '{end_date}'"
        date_range_str = f"{start_date} to {end_date}"
    else:
        where_clause = f"TxnDate = '{start_date}'"
        date_range_str = start_date

    query = f"SELECT COUNT(*) FROM SalesReceipt WHERE {where_clause}"
    result = qbo_query(query)
    
    count = result.get("QueryResponse", {}).get("totalCount", 0)
    print(f"SalesReceipts count for {date_range_str}: {count}")
    print(json.dumps(result, indent=2))


def cmd_list(start_date: str, end_date: str = None, max_results: int = 100) -> None:
    """List SalesReceipts for a date or date range."""
    if end_date:
        date_range_str = f"{start_date} to {end_date}"
    else:
        date_range_str = start_date

    receipts = fetch_receipts_for_date_range(start_date, end_date)
    count = len(receipts)
    
    print(f"Found {count} SalesReceipts for {date_range_str}")
    
    if count == 0:
        return
    
    # Show up to max_results
    display_count = min(count, max_results)
    for r in receipts[:display_count]:
        print(
            f"  Id={r.get('Id')}, DocNumber={r.get('DocNumber')}, "
            f"TxnDate={r.get('TxnDate')}, TotalAmt={r.get('TotalAmt')}"
        )
    if count > display_count:
        print(f"  ... and {count - display_count} more (use --max-results to see more)")


def cmd_delete(start_date: str, end_date: str = None, auto_yes: bool = False) -> None:
    """Delete SalesReceipts for a date or date range."""
    # Build date range string for display
    if end_date:
        date_range_str = f"{start_date} to {end_date}"
    else:
        date_range_str = start_date

    receipts = fetch_receipts_for_date_range(start_date, end_date)
    count = len(receipts)

    if count == 0:
        print(f"No SalesReceipts found for date range: {date_range_str}")
        return

    print(f"About to delete {count} SalesReceipts for date range: {date_range_str}")
    # Show first 10 receipts as preview
    preview_count = min(10, count)
    for r in receipts[:preview_count]:
        print(
            f"  Id={r.get('Id')}, DocNumber={r.get('DocNumber')}, "
            f"TxnDate={r.get('TxnDate')}, TotalAmt={r.get('TotalAmt')}"
        )
    if count > preview_count:
        print(f"  ... and {count - preview_count} more")

    if not auto_yes:
        confirm = input(
            f"\n⚠️  THIS IS DESTRUCTIVE. About to delete {count} SalesReceipt(s).\n"
            f"Type 'delete' to proceed with deletion: "
        ).strip()
        if confirm.lower() != "delete":
            print("Aborted. Nothing was deleted.")
            return

    print("\nDeleting...")
    deleted_count = 0
    failed_count = 0
    
    for r in receipts:
        try:
            body = delete_sales_receipt(r)
            sr = body.get("SalesReceipt") or {}
            print(
                f"  Deleted Id={sr.get('Id', r.get('Id'))}, "
                f"DocNumber={sr.get('DocNumber', r.get('DocNumber'))}"
            )
            deleted_count += 1
        except Exception as e:
            print(f"  ERROR deleting Id={r.get('Id')}, DocNumber={r.get('DocNumber')}: {e}")
            failed_count += 1

    print(f"\nDone. Deleted {deleted_count} SalesReceipts for date range: {date_range_str}")
    if failed_count > 0:
        print(f"Warning: {failed_count} deletions failed.")

    # Send Slack notification
    if deleted_count > 0:
        message = (
            f"🗑️ *SalesReceipts Deletion Completed*\n"
            f"• Date Range: {date_range_str}\n"
            f"• Deleted: {deleted_count} receipts\n"
            f"• Time: {datetime.now().isoformat(timespec='seconds')}"
        )
        if failed_count > 0:
            message += f"\n• ⚠️ Failed: {failed_count} receipts"
        send_slack_success(message)


def cmd_query(custom_query: str) -> None:
    """Execute a custom QBO query."""
    result = qbo_query(custom_query)
    print(json.dumps(result, indent=2))


def get_repo_root() -> Path:
    """Return the directory this script lives in (the repo root)."""
    return Path(__file__).resolve().parent


def get_qbo_total(start_date: str, end_date: str = None) -> Tuple[int, float]:
    """
    Get QBO total count and SUM(TotalAmt) for a date range.
    Returns (count, total_amount).
    """
    if end_date:
        where_clause = f"TxnDate >= '{start_date}' AND TxnDate <= '{end_date}'"
    else:
        where_clause = f"TxnDate = '{start_date}'"
    
    # Get count
    count_query = f"SELECT COUNT(*) FROM SalesReceipt WHERE {where_clause}"
    count_result = qbo_query(count_query)
    count = count_result.get("QueryResponse", {}).get("totalCount", 0)
    
    # Get all receipts to sum TotalAmt (QBO doesn't support SUM in SELECT directly)
    receipts = fetch_receipts_for_date_range(start_date, end_date)
    total_amount = sum(float(r.get("TotalAmt", 0) or 0) for r in receipts)
    
    return count, total_amount


def find_epos_files_for_date_range(start_date: str, end_date: str = None) -> List[Path]:
    """
    Find all processed EPOS CSV files for a date range.
    Checks:
    1. last_epos_transform.json in repo root (if normalized_date matches)
    2. Uploaded/<date>/ folders for matching dates
    3. Uploaded/<date_range>/ folders for matching ranges
    
    Returns list of paths to single_sales_receipts_*.csv files.
    """
    repo_root = get_repo_root()
    found_files: List[Path] = []
    
    # Parse date range
    start_dt = datetime.strptime(start_date, "%Y-%m-%d")
    if end_date:
        end_dt = datetime.strptime(end_date, "%Y-%m-%d")
    else:
        end_dt = start_dt
    
    # Check repo root last_epos_transform.json first
    root_metadata = repo_root / "last_epos_transform.json"
    if root_metadata.exists():
        try:
            with open(root_metadata, "r") as f:
                metadata = json.load(f)
            normalized_date = metadata.get("normalized_date")
            if normalized_date:
                meta_dt = datetime.strptime(normalized_date, "%Y-%m-%d")
                if start_dt <= meta_dt <= end_dt:
                    # Date matches, use files from metadata
                    processed_files = metadata.get("processed_files", [])
                    for filename in processed_files:
                        file_path = repo_root / filename
                        if file_path.exists():
                            found_files.append(file_path)
        except Exception:
            pass  # Skip if metadata is invalid
    
    # Check Uploaded/ folders
    uploaded_dir = repo_root / "Uploaded"
    if uploaded_dir.exists():
        for item in uploaded_dir.iterdir():
            if not item.is_dir():
                continue
            
            folder_name = item.name
            
            # Check if it's a date range folder (e.g., "2025-10-15 to 2025-10-17")
            if " to " in folder_name:
                try:
                    range_start_str, range_end_str = folder_name.split(" to ", 1)
                    range_start = datetime.strptime(range_start_str, "%Y-%m-%d")
                    range_end = datetime.strptime(range_end_str, "%Y-%m-%d")
                    
                    # Check if our date range overlaps with this folder's range
                    if not (range_end < start_dt or range_start > end_dt):
                        # Overlaps, include all processed CSVs in this folder
                        for csv_file in item.glob("single_sales_receipts_*.csv"):
                            found_files.append(csv_file)
                except ValueError:
                    continue  # Invalid date range format, skip
            else:
                # Single date folder (e.g., "2025-10-19")
                try:
                    folder_date = datetime.strptime(folder_name, "%Y-%m-%d")
                    if start_dt <= folder_date <= end_dt:
                        # Date matches, include all processed CSVs in this folder
                        for csv_file in item.glob("single_sales_receipts_*.csv"):
                            found_files.append(csv_file)
                except ValueError:
                    continue  # Invalid date format, skip
    
    return found_files


def get_epos_total(start_date: str, end_date: str = None) -> Tuple[int, float]:
    """
    Get EPOS total count and SUM(*ItemAmount) for a date range.
    Returns (count, total_amount).
    
    Counts unique SalesReceiptNos, sums all *ItemAmount values.
    """
    csv_files = find_epos_files_for_date_range(start_date, end_date)
    
    if not csv_files:
        return 0, 0.0
    
    total_amount = 0.0
    unique_receipts = set()
    
    # Parse date range for filtering
    start_dt = datetime.strptime(start_date, "%Y-%m-%d")
    if end_date:
        end_dt = datetime.strptime(end_date, "%Y-%m-%d")
    else:
        end_dt = start_dt
    
    for csv_file in csv_files:
        try:
            df = pd.read_csv(csv_file)
            
            # Filter by date range if *SalesReceiptDate column exists
            if "*SalesReceiptDate" in df.columns:
                # Convert to date only (no time component) for proper comparison
                df["*SalesReceiptDate"] = pd.to_datetime(df["*SalesReceiptDate"], errors="coerce").dt.date
                start_date_only = start_dt.date()
                end_date_only = end_dt.date()
                df = df[
                    (df["*SalesReceiptDate"] >= start_date_only) &
                    (df["*SalesReceiptDate"] <= end_date_only)
                ]
            
            # Sum *ItemAmount
            if "*ItemAmount" in df.columns:
                amounts = df["*ItemAmount"].fillna(0).astype(float)
                total_amount += amounts.sum()
            
            # Count unique SalesReceiptNos
            if "*SalesReceiptNo" in df.columns:
                unique_receipts.update(df["*SalesReceiptNo"].dropna().unique())
        
        except Exception as e:
            print(f"Warning: Failed to process {csv_file.name}: {e}", file=sys.stderr)
            continue
    
    count = len(unique_receipts)
    return count, total_amount


def format_currency(amount: float) -> str:
    """Format amount as currency with thousands separator."""
    return f"{amount:,.2f}"


def cmd_reconcile(start_date: str, end_date: str = None, tolerance: float = 0.00) -> None:
    """
    Reconcile EPOS totals vs QBO totals for a date range.
    """
    # Build date range string for display
    if end_date:
        date_range_str = f"{start_date} to {end_date}"
    else:
        date_range_str = start_date
    
    print(f"\n📊 EPOS ↔ QBO Reconciliation")
    print(f"Period: {date_range_str}")
    print("-" * 50)
    
    # Get QBO totals
    print("Fetching QBO totals...")
    try:
        qbo_count, qbo_total = get_qbo_total(start_date, end_date)
        print(f"  QBO: {qbo_count} receipts, Total: {format_currency(qbo_total)}")
    except Exception as e:
        print(f"  ❌ Error fetching QBO totals: {e}", file=sys.stderr)
        sys.exit(1)
    
    # Get EPOS totals
    print("Fetching EPOS totals...")
    try:
        epos_count, epos_total = get_epos_total(start_date, end_date)
        print(f"  EPOS: {epos_count} receipts, Total: {format_currency(epos_total)}")
    except Exception as e:
        print(f"  ❌ Error fetching EPOS totals: {e}", file=sys.stderr)
        sys.exit(1)
    
    # Compare
    print("-" * 50)
    difference = abs(qbo_total - epos_total)
    is_match = difference <= tolerance
    
    if is_match:
        status = "✅ MATCH"
        status_emoji = "✅"
    else:
        status = "❌ MISMATCH"
        status_emoji = "❌"
    
    print(f"Status: {status}")
    print(f"Difference: {format_currency(difference)}")
    
    if not is_match:
        if qbo_total > epos_total:
            print(f"QBO is {format_currency(qbo_total - epos_total)} higher than EPOS")
        else:
            print(f"EPOS is {format_currency(epos_total - qbo_total)} higher than QBO")
    
    # Send Slack notification
    message = (
        f"📊 *EPOS ↔ QBO Reconciliation*\n"
        f"• Period: {date_range_str}\n"
        f"• EPOS Total: {format_currency(epos_total)} ({epos_count} receipts)\n"
        f"• QBO Total: {format_currency(qbo_total)} ({qbo_count} receipts)\n"
        f"• Status: {status_emoji} {status}"
    )
    if not is_match:
        message += f"\n• Difference: {format_currency(difference)}"
    message += f"\n• Time: {datetime.now().isoformat(timespec='seconds')}"
    
    send_slack_success(message)
    print(f"\n✅ Slack notification sent")


def parse_date(date_str: str) -> str:
    """Validate and return a date string in YYYY-MM-DD format."""
    try:
        datetime.strptime(date_str, "%Y-%m-%d")
        return date_str
    except ValueError:
        raise ValueError(f"Invalid date format: {date_str}. Use YYYY-MM-DD")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="QuickBooks Online query and management tool",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Count receipts for a single date
  python qbo_query.py count 2025-10-19

  # Count receipts for a date range
  python qbo_query.py count 2025-10-15 2025-10-17

  # List receipts (first 100)
  python qbo_query.py list 2025-10-19

  # List receipts with custom limit
  python qbo_query.py list 2025-10-15 2025-10-17 --max-results 50

  # Delete receipts for a single date (with confirmation)
  python qbo_query.py delete 2025-10-19

  # Delete receipts for a date range (skip confirmation)
  python qbo_query.py delete 2025-10-15 2025-10-17 --yes

  # Execute a custom query
  python qbo_query.py query "SELECT * FROM Customer MAXRESULTS 10"

  # Reconcile EPOS vs QBO for a single date
  python qbo_query.py reconcile --from-date 2025-10-19

  # Reconcile EPOS vs QBO for yesterday (convenience)
  python qbo_query.py reconcile --yesterday

  # Reconcile EPOS vs QBO for a date range
  python qbo_query.py reconcile --from-date 2025-10-15 --to-date 2025-10-17

  # Reconcile with tolerance (e.g., allow ±0.01 difference)
  python qbo_query.py reconcile --from-date 2025-10-19 --tolerance 0.01
        """
    )

    subparsers = parser.add_subparsers(dest="command", help="Command to execute", required=True)

    # Count command
    count_parser = subparsers.add_parser("count", help="Count SalesReceipts for a date or date range")
    count_parser.add_argument("start_date", help="Start date (YYYY-MM-DD)")
    count_parser.add_argument("end_date", nargs="?", help="End date (YYYY-MM-DD, optional)")

    # List command
    list_parser = subparsers.add_parser("list", help="List SalesReceipts for a date or date range")
    list_parser.add_argument("start_date", help="Start date (YYYY-MM-DD)")
    list_parser.add_argument("end_date", nargs="?", help="End date (YYYY-MM-DD, optional)")
    list_parser.add_argument("--max-results", type=int, default=100, help="Maximum results to display (default: 100)")

    # Delete command
    delete_parser = subparsers.add_parser("delete", help="Delete SalesReceipts for a date or date range")
    delete_parser.add_argument("start_date", help="Start date (YYYY-MM-DD)")
    delete_parser.add_argument("end_date", nargs="?", help="End date (YYYY-MM-DD, optional)")
    delete_parser.add_argument("--yes", action="store_true", help="Skip confirmation prompt")

    # Query command
    query_parser = subparsers.add_parser("query", help="Execute a custom QBO query")
    query_parser.add_argument("query", help="QBO query string (e.g., 'SELECT * FROM Customer MAXRESULTS 10')")

    # Reconcile command
    reconcile_parser = subparsers.add_parser("reconcile", help="Reconcile EPOS totals vs QBO totals")
    reconcile_parser.add_argument("--from-date", help="Start date (YYYY-MM-DD)")
    reconcile_parser.add_argument("--to-date", help="End date (YYYY-MM-DD, optional)")
    reconcile_parser.add_argument("--yesterday", action="store_true", 
                                  help="Reconcile yesterday's date (convenience flag)")
    reconcile_parser.add_argument("--tolerance", type=float, default=0.00, 
                                  help="Tolerance for match (default: 0.00 for exact match)")

    args = parser.parse_args()

    try:
        if args.command == "count":
            start_date = parse_date(args.start_date)
            end_date = parse_date(args.end_date) if args.end_date else None
            cmd_count(start_date, end_date)

        elif args.command == "list":
            start_date = parse_date(args.start_date)
            end_date = parse_date(args.end_date) if args.end_date else None
            cmd_list(start_date, end_date, args.max_results)

        elif args.command == "delete":
            start_date = parse_date(args.start_date)
            end_date = parse_date(args.end_date) if args.end_date else None
            cmd_delete(start_date, end_date, args.yes)

        elif args.command == "query":
            cmd_query(args.query)

        elif args.command == "reconcile":
            # Handle --yesterday flag
            if args.yesterday:
                if args.from_date:
                    raise ValueError("Cannot use both --yesterday and --from-date. Use one or the other.")
                yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
                start_date = parse_date(yesterday)
                end_date = None
            elif args.from_date:
                start_date = parse_date(args.from_date)
                end_date = parse_date(args.to_date) if args.to_date else None
            else:
                raise ValueError("Must specify either --from-date or --yesterday")
            cmd_reconcile(start_date, end_date, args.tolerance)

    except ValueError as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()

#Example usage:
#COUNT:
# To count the number of SalesReceipts for a date range
#python qbo_query.py count 2025-12-17 2025-12-18

#LIST:
# To list the SalesReceipts for a date range
#python qbo_query.py list 2025-12-17 2025-12-18

# To list the SalesReceipts for a date range with a maximum of 50 results
#python qbo_query.py list 2025-12-17 --max-results 50

# To list the SalesReceipts for a date range
#python qbo_query.py list 2025-12-17

#DELETE:
# To delete the SalesReceipts for a date range
#python qbo_query.py delete 2025-12-17 2025-12-18

# To delete the SalesReceipts for a date range and skip the confirmation prompt
#python qbo_query.py delete 2025-12-17 2025-12-18 --yes

#QUERY:
# To execute a custom query
#python qbo_query.py query "SELECT * FROM Customer MAXRESULTS 10"
