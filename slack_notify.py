import os
import json
import logging
from datetime import datetime
from pathlib import Path
import urllib.request
import ssl

try:
    import certifi
except ImportError:  # pragma: no cover - best effort
    certifi = None


def send_slack_success(message: str) -> None:
    webhook_url = os.getenv("SLACK_WEBHOOK_URL")

    if not webhook_url:
        logging.info("SLACK_WEBHOOK_URL not set, skipping Slack notification.")
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


def notify_pipeline_success(
    pipeline_name: str,
    log_file: Path,
    date_range: str = None,
    metadata: dict = None
) -> None:
    message = (
        f"✅ *{pipeline_name} completed successfully*\n"
        f"• Time: {datetime.now().isoformat(timespec='seconds')}\n"
        f"• Log: `{log_file.name}`"
    )
    if date_range:
        message += f"\n• Target Date: {date_range}"
    
    # Add summary from metadata if available
    if metadata:
        target_date = metadata.get("target_date")
        if target_date:
            message += f"\n• Target Date: {target_date}"
        
        dates_present = metadata.get("dates_present", [])
        if dates_present:
            message += f"\n• Dates Present: {', '.join(dates_present)}"
        
        rows_total = metadata.get("rows_total")
        rows_kept = metadata.get("rows_kept")
        rows_spilled = metadata.get("rows_spilled")
        if rows_total is not None:
            message += f"\n• Rows: {rows_kept} kept, {rows_spilled} spilled (total: {rows_total})"
        
        spill_files = metadata.get("spill_files", [])
        if spill_files:
            message += f"\n• Spill Files: {len(spill_files)} file(s)"
        
        upload_stats = metadata.get("upload_stats")
        if upload_stats:
            attempted = upload_stats.get("attempted", 0)
            uploaded = upload_stats.get("uploaded", 0)
            skipped = upload_stats.get("skipped", 0)
            failed = upload_stats.get("failed", 0)
            message += f"\n• Upload: {uploaded} uploaded, {skipped} skipped, {failed} failed (attempted: {attempted})"
    
    send_slack_success(message)


def notify_pipeline_start(
    pipeline_name: str,
    log_file: Path,
    date_range: str = None,
) -> None:
    message = (
        f"🚀 *{pipeline_name} started*\n"
        f"• Time: {datetime.now().isoformat(timespec='seconds')}\n"
        f"• Log: `{log_file.name}`"
    )
    if date_range:
        message += f"\n• Date Range: {date_range}"
    send_slack_success(message)


def extract_error_reason(error: str) -> str:
    """
    Extract a concise, user-friendly reason from an error message.
    Returns a professional summary of the error.
    """
    error_lower = error.lower()
    
    # Token-related errors
    if "invalid_grant" in error_lower or "invalid refresh token" in error_lower:
        return "Incorrect or invalid refresh token. Please update qbo_tokens.json with valid tokens."
    if "invalid_client" in error_lower or "qbo_client_id" in error_lower or "qbo_client_secret" in error_lower:
        return "Invalid QuickBooks credentials. Please check QBO_CLIENT_ID and QBO_CLIENT_SECRET in .env file."
    if "qbo_realm_id" in error_lower or "realm_id" in error_lower:
        return "Missing QBO_REALM_ID. Please set it in your .env file."
    if "qbo_tokens.json" in error_lower and ("not found" in error_lower or "empty" in error_lower):
        return "Missing or empty qbo_tokens.json. Please create it with valid OAuth tokens."
    if "refresh token" in error_lower and ("expired" in error_lower or "invalid" in error_lower):
        return "Refresh token expired or invalid. Please re-authenticate and update qbo_tokens.json."
    
    # File-related errors
    if "file not found" in error_lower or "no such file" in error_lower:
        if "csv" in error_lower:
            return "Required CSV file not found. Check if EPOS download completed successfully."
        return "Required file not found. Check pipeline logs for details."
    if "single_sales_receipts" in error_lower:
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
    if "phase 2" in error_lower or "epos_to_qb" in error_lower:
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
    date_range: str = None
) -> None:
    reason = extract_error_reason(error)
    message = (
        f"❌ *{pipeline_name} failed*\n"
        f"• Time: {datetime.now().isoformat(timespec='seconds')}\n"
        f"• Reason: {reason}\n"
        f"• Log: `{log_file.name}`"
    )
    if date_range:
        message += f"\n• Date Range: {date_range}"
    send_slack_success(message)