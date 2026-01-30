import os
import re
import json
import csv
import logging
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, Any, List
import urllib.request
import ssl

try:
    import certifi
except ImportError:  # pragma: no cover - best effort
    certifi = None


def send_slack_success(message: str, webhook_url: str = None) -> None:
    """
    Send a Slack notification.
    
    Args:
        message: Message to send
        webhook_url: Optional webhook URL. If not provided, falls back to SLACK_WEBHOOK_URL env var.
    """
    if not webhook_url:
        webhook_url = os.getenv("SLACK_WEBHOOK_URL")

    if not webhook_url:
        logging.info("Slack webhook URL not set, skipping Slack notification.")
        return

    payload = {
        "text": message
    }

    data = json.dumps(payload).encode("utf-8")

    # Build SSL context with system certs; fall back to certifi if available.
    context = ssl.create_default_context()
    if certifi:
        try:
            context.load_verify_locations(certifi.where())
        except Exception as e:  # pragma: no cover
            logging.warning(f"Could not load certifi certs: {e}")

    req = urllib.request.Request(
        webhook_url,
        data=data,
        headers={"Content-Type": "application/json"},
    )

    try:
        with urllib.request.urlopen(req, context=context) as resp:
            logging.info(f"Slack message sent (status {resp.status})")
    except Exception as e:
        logging.error(f"Failed to send Slack message: {e}")


def notify_pipeline_update(
    pipeline_name: str,
    log_file: Path,
    summary: Dict[str, Any],
    webhook_url: str = None
) -> None:
    """
    Send a state-based watchdog/update message (sent ONCE if noteworthy).
    
    This is NOT a timer/heartbeat. Only call when there are warnings/anomalies
    that warrant mid-run notification (e.g., spill files, duplicates, partial failures).
    
    Args:
        pipeline_name: Name of the pipeline
        log_file: Path to log file
        summary: Dictionary containing update information (phase, warnings, etc.)
        webhook_url: Optional webhook URL
    """
    message = format_run_summary(pipeline_name, log_file, summary, status="update")
    send_slack_success(message, webhook_url)


def notify_pipeline_success(
    pipeline_name: str,
    log_file: Path,
    date_range: str = None,
    metadata: dict = None,
    webhook_url: str = None
) -> None:
    """
    Send a Slack notification when the pipeline completes successfully.
    
    Args:
        pipeline_name: Name of the pipeline
        log_file: Path to log file
        date_range: Optional date range string (for backward compatibility)
        metadata: Optional metadata dict with summary information
        webhook_url: Optional webhook URL
    """
    # Build summary from metadata and date_range
    summary = {}
    if metadata:
        summary.update(metadata)
    if date_range and not summary.get("target_date") and not summary.get("date_range"):
        summary["date_range"] = date_range
    
    message = format_run_summary(pipeline_name, log_file, summary, status="success")
    send_slack_success(message, webhook_url)


def _summarize_blockers_csv(repo_root: Path, company_key: str, target_date: str) -> Optional[str]:
    """
    If the inventory_start_date_blockers CSV exists for this run, read it and return
    a short summary (row count + sample items). Used for Slack when 6270 rejections occurred.
    """
    if not company_key or not target_date:
        return None
    safe_key = re.sub(r"[^\w-]", "_", company_key)
    filename = f"inventory_start_date_blockers_{safe_key}_{target_date}.csv"
    filepath = repo_root / "reports" / filename
    if not filepath.exists():
        return None
    try:
        with open(filepath, "r", newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            rows = list(reader)
        if not rows:
            return None
        # Unique (DocNumber, ItemName, InvStartDate) for a concise summary
        seen = set()
        sample_items: List[str] = []
        for r in rows:
            name = (r.get("ItemName") or "").strip()
            inv_date = (r.get("InvStartDate") or "").strip()
            key = (name, inv_date)
            if key not in seen and inv_date and inv_date != "(missing)":
                seen.add(key)
                sample_items.append(f"{name} ({inv_date})")
                if len(sample_items) >= 5:
                    break
        count = len(rows)
        if count == 0:
            return None
        summary = f"{count} blocker row(s)"
        if sample_items:
            summary += f" — e.g. {', '.join(sample_items)}"
        summary += f"\n  Report: `reports/{filename}`"
        return summary
    except Exception as e:
        logging.warning(f"Could not read blockers CSV for summary: {e}")
        return None


def format_run_summary(
    pipeline_name: str,
    log_file: Path,
    summary: Dict[str, Any],
    status: str,
    error: Optional[str] = None
) -> str:
    """
    Format a consolidated run summary message for Slack.
    
    Args:
        pipeline_name: Name of the pipeline
        log_file: Path to log file
        summary: Dictionary containing run summary data
        status: One of "success", "failure", "update"
        error: Error message (for failure status)
    
    Returns:
        Formatted Slack message string
    """
    # Status headers
    if status == "success":
        header = f"✅ *{pipeline_name} completed*"
    elif status == "failure":
        header = f"❌ *{pipeline_name} failed*"
    elif status == "update":
        header = f"⚠️ *{pipeline_name} update*"
    else:
        header = f"*{pipeline_name}*"
    
    message = f"{header}\n"
    message += f"• Time: {datetime.now().isoformat(timespec='seconds')}\n"
    message += f"• Log: `{log_file.name}`\n"
    
    # Date information
    if summary.get("target_date"):
        message += f"• Target Date: {summary['target_date']}\n"
    elif summary.get("date_range"):
        message += f"• Date Range: {summary['date_range']}\n"
    
    # Phase information (for update/failure)
    if status in ("update", "failure"):
        if summary.get("phase"):
            message += f"• Phase: {summary['phase']}\n"
        if summary.get("phase_status"):
            message += f"• Status: {summary['phase_status']}\n"
        if status == "failure" and summary.get("phase_failed"):
            message += f"• Phase Failed: {summary['phase_failed']}\n"
    
    # Failure reason
    if status == "failure" and error:
        reason = extract_error_reason(error)
        message += f"• Reason: {reason}\n"
    
    # Row statistics
    rows_kept = summary.get("rows_kept")
    rows_spilled = summary.get("rows_spilled")
    rows_total = summary.get("rows_total")
    if rows_total is not None:
        message += f"• Rows: {rows_kept} kept, {rows_spilled} spilled (total: {rows_total})\n"
    
    # Spill files
    spill_files = summary.get("spill_files", [])
    if spill_files:
        message += f"• Spill Files: {len(spill_files)} file(s)\n"
    
    # Upload statistics
    upload_stats = summary.get("upload_stats")
    if upload_stats:
        attempted = upload_stats.get("attempted", 0)
        uploaded = upload_stats.get("uploaded", 0)
        skipped = upload_stats.get("skipped", 0)
        failed = upload_stats.get("failed", 0)
        stale_ledger = upload_stats.get("stale_ledger_entries_detected", 0)
        message += f"• Upload: {uploaded} uploaded, {skipped} skipped, {failed} failed (attempted: {attempted})\n"
        if stale_ledger > 0:
            message += f"• Stale ledger entries detected: {stale_ledger} (healed by uploading)\n"
        
        # Inventory statistics
        items_created = upload_stats.get("items_created_count", 0)
        inventory_warnings = upload_stats.get("inventory_warnings_count", 0)
        inventory_rejections = upload_stats.get("inventory_rejections_count", 0)
        inventory_start_date_issues = upload_stats.get("inventory_start_date_issues_count", 0)
        target_date = summary.get("target_date", "")
        if items_created > 0 or inventory_warnings > 0 or inventory_rejections > 0:
            message += f"• Inventory: {items_created} items created"
            if inventory_warnings > 0:
                message += f", {inventory_warnings} warnings"
            if inventory_rejections > 0:
                message += f", {inventory_rejections} rejections"
            message += "\n"
        if inventory_start_date_issues > 0 and target_date:
            message += f"• Inventory StartDate: {inventory_start_date_issues} items have InvStartDate after {target_date}\n"
        # If we had rejections or failures (e.g. 6270 InvStartDate), include blockers CSV summary when present
        if (inventory_rejections > 0 or failed > 0) and target_date:
            repo_root = Path(__file__).resolve().parent
            company_key = summary.get("company_key", "")
            blockers_summary = _summarize_blockers_csv(repo_root, company_key, target_date)
            if blockers_summary:
                message += f"• InvStartDate blockers (6270): {blockers_summary}\n"
    
    # Reconciliation
    reconcile = summary.get("reconcile")
    if reconcile:
        reconcile_status = reconcile.get("status", "NOT RUN")
        if reconcile_status == "MATCH":
            message += f"• Reconciliation: MATCH\n"
        elif reconcile_status == "MISMATCH":
            message += f"• ⚠️ Reconciliation: MISMATCH\n"
        else:
            message += f"• Reconciliation: NOT RUN\n"
        
        if reconcile_status != "NOT RUN":
            epos_total = reconcile.get("epos_total", 0)
            epos_count = reconcile.get("epos_count", 0)
            qbo_total = reconcile.get("qbo_total", 0)
            qbo_count = reconcile.get("qbo_count", 0)
            difference = reconcile.get("difference", 0)
            
            message += f"  – EPOS: ₦{epos_total:,.2f} ({epos_count} receipts)\n"
            message += f"  – QBO: ₦{qbo_total:,.2f} ({qbo_count} receipts)\n"
            message += f"  – Difference: ₦{difference:,.2f}\n"
        else:
            reason_not_run = reconcile.get("reason", "upload incomplete")
            message += f"  – Reconciliation not run ({reason_not_run})\n"
    elif status == "failure":
        # If failure and no reconcile data, indicate it wasn't run
        message += f"• Reconciliation: NOT RUN\n"
        message += f"  – Reconciliation not run (upload incomplete)\n"
    
    # Trading day boundary stats (if available)
    trading_day_stats = summary.get("trading_day_stats")
    if trading_day_stats:
        cutoff = trading_day_stats.get("cutoff", "05:00")
        by_date = trading_day_stats.get("by_date", {})
        
        # For single-day or per-day summaries, show stats for the specific date
        target_date = summary.get("target_date")
        if target_date and target_date in by_date:
            day_stats = by_date[target_date]
            pre_cutoff = day_stats.get("pre_cutoff_reassigned", 0)
            if pre_cutoff > 0:
                message += f"• Trading-day adjustment: {pre_cutoff} row(s) from next calendar day (pre-cutoff) assigned to {target_date} (cutoff={cutoff} WAT)\n"
        # For range mode final summary, show aggregate or per-day stats
        elif by_date:
            # Show stats for all dates in range
            total_reassigned = sum(stats.get("pre_cutoff_reassigned", 0) for stats in by_date.values())
            if total_reassigned > 0:
                message += f"• Trading-day adjustment: {total_reassigned} total row(s) reassigned from next calendar day (cutoff={cutoff} WAT)\n"
                # Optionally show per-day breakdown (limit to 3 dates to avoid clutter)
                dates_with_reassigned = [
                    (date, stats.get("pre_cutoff_reassigned", 0))
                    for date, stats in by_date.items()
                    if stats.get("pre_cutoff_reassigned", 0) > 0
                ]
                if len(dates_with_reassigned) <= 3:
                    for date, count in dates_with_reassigned:
                        message += f"  – {date}: {count} row(s)\n"
    
    # Range Totals (only for range mode final summary)
    if status == "success" and summary.get("range_totals"):
        # Check if this is a range completion message (has from_date and to_date, or date_range contains "to")
        is_range_mode = (
            (summary.get("from_date") is not None and summary.get("to_date") is not None) or
            (summary.get("date_range") and " to " in str(summary.get("date_range")))
        )
        
        if is_range_mode:
            range_totals = summary["range_totals"]
            included_days = range_totals.get("included_days", 0)
            total_days = range_totals.get("total_days", 0)
            epos_total = range_totals.get("epos_total", 0)
            qbo_total = range_totals.get("qbo_total", 0)
            epos_count = range_totals.get("epos_count", 0)
            qbo_count = range_totals.get("qbo_count", 0)
            difference = range_totals.get("difference", 0)
            
            if included_days == total_days:
                message += f"• Range Totals (sum of per-day reconciliation):\n"
            else:
                message += f"• Range Totals (partial — {included_days}/{total_days} days included):\n"
            
            message += f"  – EPOS: ₦{epos_total:,.2f} ({epos_count} receipts)\n"
            message += f"  – QBO: ₦{qbo_total:,.2f} ({qbo_count} receipts)\n"
            message += f"  – Difference: ₦{difference:,.2f}\n"
    
    # Warnings/Notes (for update messages)
    warnings = summary.get("warnings", [])
    if warnings and len(warnings) > 0:
        message += f"• Notes:\n"
        for warning in warnings[:6]:  # Limit to 6 warnings
            message += f"  – {warning}\n"
    
    return message


def notify_pipeline_start(
    pipeline_name: str,
    log_file: Path,
    date_range: str = None,
    webhook_url: str = None,
    metadata: Dict[str, Any] = None
) -> None:
    """
    Send a Slack notification when the pipeline starts.
    
    Args:
        pipeline_name: Name of the pipeline
        log_file: Path to log file
        date_range: Optional date range string
        webhook_url: Optional webhook URL
        metadata: Optional metadata dict with target_date, company_key, etc.
    """
    summary = {}
    if metadata:
        summary.update(metadata)
    if date_range:
        summary["date_range"] = date_range
    
    message = (
        f"🚀 *{pipeline_name} started*\n"
        f"• Time: {datetime.now().isoformat(timespec='seconds')}\n"
        f"• Log: `{log_file.name}`"
    )
    
    if summary.get("target_date"):
        message += f"\n• Target Date: {summary['target_date']}"
    elif summary.get("date_range"):
        message += f"\n• Date Range: {summary['date_range']}"
    
    send_slack_success(message, webhook_url)


def extract_error_reason(error: str) -> str:
    """
    Extract a concise, user-friendly reason from an error message.
    Returns a professional summary of the error.
    Updated for multi-company + SQLite setup.
    """
    error_lower = error.lower()
    error_original = error  # Keep original for exact matches
    
    # Duplicate receipt errors
    if "duplicate" in error_lower and ("docnumber" in error_lower or "document number" in error_lower):
        return "Duplicate receipt detected (DocNumber already exists in QBO)."
    
    # Line validation errors
    if "amount must equal" in error_lower and ("unitprice" in error_lower or "qty" in error_lower):
        return "Line validation failed (Amount must equal UnitPrice × Qty)."
    
    # Department/location mapping errors
    if "department" in error_lower and ("not found" in error_lower or "invalid" in error_lower or "mapping" in error_lower):
        return "Missing/invalid Department mapping for this location."
    
    # Token-related errors (updated for SQLite)
    if "invalid_grant" in error_lower or "invalid refresh token" in error_lower:
        return "Invalid refresh token. Re-authenticate via OAuth flow and update qbo_tokens.sqlite."
    if "invalid_client" in error_lower or "qbo_client_id" in error_lower or "qbo_client_secret" in error_lower:
        return "Invalid QuickBooks credentials. Check QBO_CLIENT_ID and QBO_CLIENT_SECRET in .env file."
    if "qbo_tokens.sqlite" in error_lower and ("not found" in error_lower or "empty" in error_lower or "no tokens found" in error_lower):
        return "No tokens found in qbo_tokens.sqlite. Run OAuth flow first using --company selection."
    if "refresh token" in error_lower and ("expired" in error_lower or "invalid" in error_lower):
        return "Refresh token expired or invalid. Re-authenticate via OAuth flow for this company."
    if "company_key" in error_lower or "realm_id" in error_lower:
        if "not found" in error_lower or "missing" in error_lower:
            return "Company configuration error. Use --company selection (company_a or company_b)."
    
    # File-related errors
    if "file not found" in error_lower or "no such file" in error_lower:
        if "csv" in error_lower:
            return "Required CSV file not found. Check if EPOS download completed successfully."
        return "Required file not found. Check pipeline logs for details."
    if "single_sales_receipts" in error_lower or "gp_sales_receipts" in error_lower:
        return "Processed CSV file not found. Phase 2 (transformation) may have failed."
    
    # Network/API errors
    if "connection" in error_lower or "network" in error_lower or "timeout" in error_lower:
        return "Network connection error. Check internet connectivity and try again."
    if "401" in error or "unauthorized" in error_lower:
        return "Authentication failed. Check QuickBooks credentials and tokens."
    if "403" in error or "forbidden" in error_lower:
        return "Access forbidden. Check QuickBooks API permissions."
    if "429" in error or "rate limit" in error_lower:
        return "API rate limit exceeded. Please wait before retrying."
    
    # Phase-specific errors
    if "phase 1" in error_lower or "epos_playwright" in error_lower:
        return "EPOS download failed. Check EPOS credentials and website accessibility."
    if "phase 2" in error_lower or "transform" in error_lower:
        return "CSV transformation failed. Check input file format and data."
    if "phase 3" in error_lower or "qbo_upload" in error_lower:
        return "QuickBooks upload failed. Check API credentials and data format."
    
    # Generic fallback - extract first meaningful line
    lines = error.split('\n')
    for line in lines:
        line = line.strip()
        if line and not line.startswith('Traceback') and not line.startswith('File'):
            # Limit length
            if len(line) > 150:
                line = line[:147] + "..."
            return line
    
    return "Pipeline failed. Check logs for details."


def notify_pipeline_failure(
    pipeline_name: str,
    log_file: Path,
    error: str,
    date_range: str = None,
    webhook_url: str = None,
    metadata: dict = None
) -> None:
    """
    Send a Slack notification when the pipeline fails.
    
    Args:
        pipeline_name: Name of the pipeline
        log_file: Path to log file
        error: Error message or exception string
        date_range: Optional date range string (for backward compatibility)
        webhook_url: Optional webhook URL
        metadata: Optional metadata dict with summary information
    """
    # Build summary from metadata and date_range
    summary = {}
    if metadata:
        summary.update(metadata)
    if date_range and not summary.get("target_date") and not summary.get("date_range"):
        summary["date_range"] = date_range
    
    message = format_run_summary(pipeline_name, log_file, summary, status="failure", error=error)
    send_slack_success(message, webhook_url)