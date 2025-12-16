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


def notify_pipeline_success(pipeline_name: str, log_file: Path, date_range: str = None) -> None:
    message = (
        f"✅ *{pipeline_name} completed successfully*\n"
        f"• Time: {datetime.now().isoformat(timespec='seconds')}\n"
        f"• Log: `{log_file.name}`"
    )
    if date_range:
        message += f"\n• Date Range: {date_range}"
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


def notify_pipeline_failure(
    pipeline_name: str,
    log_file: Path,
    error: str,
    date_range: str = None
) -> None:
    message = (
        f"❌ *{pipeline_name} failed*\n"
        f"• Time: {datetime.now().isoformat(timespec='seconds')}\n"
        f"• Error: `{error}`\n"
        f"• Log: `{log_file.name}`"
    )
    if date_range:
        message += f"\n• Date Range: {date_range}"
    send_slack_success(message)