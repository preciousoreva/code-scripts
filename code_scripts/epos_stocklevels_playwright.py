import sys
import time
import argparse
import re
from contextlib import suppress
from datetime import datetime
from pathlib import Path
from playwright.sync_api import Playwright, sync_playwright

from code_scripts.artifact_paths import stock_exports_dir
from code_scripts.load_env import load_env_file
from code_scripts.company_config import load_company_config, get_available_companies
from code_scripts.paths import OPS_ROOT

load_env_file()


def get_args():
    parser = argparse.ArgumentParser(
        description="Download EPOS Stock Levels CSV for a specific company."
    )
    parser.add_argument(
        "--company",
        required=True,
        choices=get_available_companies(),
        help="Company identifier (REQUIRED). Available: %(choices)s",
    )
    parser.add_argument(
        "--output-dir",
        help="Directory to save the downloaded CSV (default: STATE_ROOT/code_scripts/exports/).",
    )
    parser.add_argument(
        "--output-filename",
        help="Explicit filename for the downloaded CSV (default: EPOS suggested filename).",
    )
    parser.add_argument(
        "--download-timeout-ms",
        type=int,
        help="Override download timeout in milliseconds (default: 90000).",
    )
    parser.add_argument(
        "--headful",
        action="store_true",
        help="Run with a visible browser (useful for debugging).",
    )
    return parser.parse_args()


_EPOS_STOCKLEVELS_URL = "https://www.eposnowhq.com/Pages/Reporting/StockReport.aspx"
_GOTO_MAX_RETRIES = 3
_GOTO_RETRY_DELAY_S = 5
_REPORT_READY_TIMEOUT_MS = 30000


def _click_first_matching_control(
    page,
    *,
    label: str,
    role_candidates: list[tuple[str, str]],
    selector_candidates: list[str],
    no_wait_after: bool = False,
) -> str:
    for role, name in role_candidates:
        loc = page.get_by_role(role, name=name)
        if loc.count() > 0:
            loc.first.click(timeout=30000, no_wait_after=no_wait_after)
            return f"{role}:{name}"
    for selector in selector_candidates:
        loc = page.locator(selector)
        if loc.count() > 0:
            loc.first.click(timeout=30000, no_wait_after=no_wait_after)
            return selector
    raise RuntimeError(f"Could not find the {label} control on the page.")


def _wait_for_stock_report_ready(page) -> None:
    with suppress(Exception):
        page.wait_for_url("**/StockReport.aspx**", timeout=_REPORT_READY_TIMEOUT_MS)
    with suppress(Exception):
        page.wait_for_load_state("networkidle", timeout=15000)
    page.wait_for_timeout(1000)


def _click_apply(page) -> str:
    return _click_first_matching_control(
        page,
        label="Stock Levels Apply",
        role_candidates=[
            ("button", "Apply"),
            ("link", "Apply"),
        ],
        selector_candidates=[
            "#MainContent_btnApply",
            "#btnApply",
            "input[type='submit'][value='Apply']",
            "input[value='Apply']",
            "button:has-text('Apply')",
            "a:has-text('Apply')",
        ],
    )


def _click_export_csv(page) -> str:
    role_candidates = [
        ("button", "Export to .csv"),
        ("button", "Export to CSV"),
        ("button", "Export CSV"),
        ("link", "Export to .csv"),
        ("link", "Export to CSV"),
        ("link", "Export CSV"),
    ]
    selector_candidates = [
        "#MainContent_btnExportCsv",
        "input[value='Export to .csv']",
        "input[value='Export to CSV']",
        "button:has-text('Export to .csv')",
        "button:has-text('Export to CSV')",
        "button:has-text('Export CSV')",
        "a:has-text('Export to .csv')",
        "a:has-text('Export to CSV')",
        "a:has-text('Export CSV')",
    ]
    return _click_first_matching_control(
        page,
        label="Stock Levels CSV export",
        role_candidates=role_candidates,
        selector_candidates=selector_candidates,
        no_wait_after=True,
    )


def _sanitize_filename_part(value: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9._-]+", "_", str(value or "").strip())
    return cleaned.strip("._") or "stock_report"


def _default_output_dir() -> Path:
    return stock_exports_dir()


def _default_output_filename(company_key: str, suggested_filename: str) -> str:
    suggested_path = Path(suggested_filename or "StockReport.csv")
    stem = _sanitize_filename_part(suggested_path.stem)
    suffix = suggested_path.suffix or ".csv"
    stamp = datetime.now().strftime("%H%M")
    return f"{_sanitize_filename_part(company_key)}_{stem}_{stamp}{suffix}"


def run(
    playwright: Playwright,
    config,
    output_dir: str | None = None,
    output_filename: str | None = None,
    download_timeout_ms: int | None = None,
    headful: bool = False,
) -> str:
    try:
        epos_username = config.epos_username
        epos_password = config.epos_password
    except RuntimeError as e:
        raise RuntimeError(
            f"Failed to get EPOS credentials for {config.display_name}: {e}\n"
            f"Please set {config._data['epos']['username_env_key']} and "
            f"{config._data['epos']['password_env_key']} in your .env file."
        )

    browser = playwright.chromium.launch(headless=not headful)
    context = browser.new_context(accept_downloads=True)
    page = context.new_page()

    for attempt in range(1, _GOTO_MAX_RETRIES + 1):
        try:
            page.goto(_EPOS_STOCKLEVELS_URL, wait_until="domcontentloaded")
            break
        except Exception as e:
            if attempt < _GOTO_MAX_RETRIES:
                print(
                    f"  [Retry {attempt}/{_GOTO_MAX_RETRIES}] page.goto failed: {e}. "
                    f"Retrying in {_GOTO_RETRY_DELAY_S}s..."
                )
                time.sleep(_GOTO_RETRY_DELAY_S)
            else:
                raise

    page.get_by_role("textbox", name="Username or email address").click()
    page.get_by_role("textbox", name="Username or email address").fill(epos_username)
    page.get_by_role("textbox", name="Password").click()
    page.get_by_role("textbox", name="Password").fill(epos_password)
    page.get_by_role("button", name="Log in").click()

    _wait_for_stock_report_ready(page)

    apply_selector = _click_apply(page)
    print(f"Applied Stock Report filters via {apply_selector}")
    _wait_for_stock_report_ready(page)

    timeout_ms = int(download_timeout_ms) if download_timeout_ms else 90000
    print(f"Waiting for Stock Levels CSV download (timeout={timeout_ms}ms)")
    with page.expect_download(timeout=timeout_ms) as download_info:
        export_selector = _click_export_csv(page)
    download = download_info.value
    print(f"Triggered CSV export via {export_selector}")

    save_dir = Path(output_dir).expanduser() if output_dir else _default_output_dir()
    save_dir.mkdir(parents=True, exist_ok=True)

    filename = output_filename or _default_output_filename(config.company_key, download.suggested_filename)

    save_path = save_dir / filename
    download.save_as(str(save_path))

    context.close()
    browser.close()
    return str(save_path)


if __name__ == "__main__":
    args = get_args()

    try:
        config = load_company_config(args.company)
    except Exception as e:
        print(f"Error: Failed to load company config for '{args.company}': {e}")
        sys.exit(1)

    with sync_playwright() as playwright:
        saved_to = run(
            playwright,
            config,
            output_dir=args.output_dir,
            output_filename=args.output_filename,
            download_timeout_ms=args.download_timeout_ms,
            headful=args.headful,
        )

    print(f"Saved: {saved_to}")
