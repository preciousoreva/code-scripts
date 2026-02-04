import re
import sys
import argparse
from datetime import datetime, timedelta
from playwright.sync_api import Playwright, sync_playwright, expect
import os

# Load .env file if it exists (makes credential management easier)
from load_env import load_env_file
from company_config import load_company_config, get_available_companies

load_env_file()


def navigate_to_month(page, target_date: str) -> None:
    """Navigate calendar to the correct month if needed."""
    target_dt = datetime.strptime(target_date, "%Y-%m-%d")
    target_month_year = target_dt.strftime("%B %Y")
    target_dt_month = datetime.strptime(target_month_year, "%B %Y")
    
    page.wait_for_timeout(500)
    calendar_title = page.locator('.ajax__calendar_title:visible, td.title:visible, th.title:visible').first
    
    if calendar_title.count() == 0:
        return
    
    try:
        current_text = calendar_title.inner_text().strip().replace(",", "").strip()
        if target_month_year in current_text:
            return
        
        current_dt = datetime.strptime(current_text, "%B %Y")
        prev_btn = page.locator('.ajax__calendar_prev:visible, a.ajax__calendar_prev:visible, a[title*="Previous" i]:visible, a[title*="Prev" i]:visible').first
        next_btn = page.locator('.ajax__calendar_next:visible, a.ajax__calendar_next:visible, a[title*="Next" i]:visible').first
        
        for _ in range(24):  # Max 2 years
            current_text = calendar_title.inner_text().strip().replace(",", "").strip()
            if target_month_year in current_text:
                break
            
            try:
                current_dt = datetime.strptime(current_text, "%B %Y")
            except ValueError:
                break
            
            btn = prev_btn if current_dt > target_dt_month else next_btn
            if btn.count() > 0:
                btn.click()
                page.wait_for_timeout(400)
                prev_btn = page.locator('.ajax__calendar_prev:visible, a.ajax__calendar_prev:visible, a[title*="Previous" i]:visible, a[title*="Prev" i]:visible').first
                next_btn = page.locator('.ajax__calendar_next:visible, a.ajax__calendar_next:visible, a[title*="Next" i]:visible').first
            else:
                break
    except Exception:
        pass


def click_date_simple(page, target_date: str) -> None:
    """Click a date in the calendar - navigate to correct month first, then find by title."""
    target_dt = datetime.strptime(target_date, "%Y-%m-%d")
    day_number = str(target_dt.day)
    target_titles = [
        target_dt.strftime("%d %B %Y"),      # "10 November 2025"
        target_dt.strftime("%d %B, %Y"),     # "10 November, 2025"
    ]
    
    navigate_to_month(page, target_date)
    page.wait_for_timeout(500)
    
    # Try to find by title attribute (most reliable)
    for title in target_titles:
        day = page.locator(f'[id*="day"][title="{title}"]:visible').first
        if day.count() > 0:
            day.click()
            page.wait_for_timeout(200)
            return
    
    # Fallback: find by day number and verify title matches
    for day_elem in page.locator(f'[id*="day"]:visible').all():
        try:
            if day_elem.inner_text().strip() == day_number:
                day_title = day_elem.get_attribute("title") or ""
                if any(title in day_title for title in target_titles):
                    day_elem.click()
                    page.wait_for_timeout(200)
                    return
        except:
            continue
    
    raise RuntimeError(f"Could not find calendar day for date {target_date}")


def get_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Download EPOS CSV for a specific company and date."
    )
    parser.add_argument(
        "--company",
        required=True,
        choices=get_available_companies(),
        help="Company identifier (REQUIRED). Available: %(choices)s",
    )
    parser.add_argument(
        "--target-date",
        help="Target business date in YYYY-MM-DD format (default: yesterday, ignored if --from-date and --to-date are provided)",
    )
    parser.add_argument(
        "--from-date",
        help="Start date for range in YYYY-MM-DD format (must be used with --to-date)",
    )
    parser.add_argument(
        "--to-date",
        help="End date for range in YYYY-MM-DD format (must be used with --from-date)",
    )
    parser.add_argument(
        "--output-dir",
        help="Directory to save the downloaded CSV (default: repo root).",
    )
    parser.add_argument(
        "--output-filename",
        help="Explicit filename for the downloaded CSV (default: EPOS suggested filename).",
    )
    args = parser.parse_args()
    
    # Validation: --from-date and --to-date must be provided together
    if (args.from_date is None) != (args.to_date is None):
        parser.error("--from-date and --to-date must be provided together")
    
    return args


def run(
    playwright: Playwright,
    config,
    from_date: str = None,
    to_date: str = None,
    target_date: str = None,
    output_dir: str = None,
    output_filename: str = None,
) -> None:
    # Get credentials from company config
    try:
        epos_username = config.epos_username
        epos_password = config.epos_password
    except RuntimeError as e:
        raise RuntimeError(
            f"Failed to get EPOS credentials for {config.display_name}: {e}\n"
            f"Please set {config._data['epos']['username_env_key']} and "
            f"{config._data['epos']['password_env_key']} in your .env file."
        )
    
    # Determine date range: prefer from_date/to_date if provided, else use target_date
    if from_date and to_date:
        date_from = from_date
        date_to = to_date
        print(f"Downloading EPOS CSV for {config.display_name} (range: {date_from} to {date_to})")
    elif target_date:
        date_from = target_date
        date_to = target_date
        print(f"Downloading EPOS CSV for {config.display_name} (date: {target_date})")
    else:
        # Default to yesterday
        date_from = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        date_to = date_from
        print(f"Downloading EPOS CSV for {config.display_name} (date: {date_from})")
    
    browser = playwright.chromium.launch(headless=True)
    context = browser.new_context()
    page = context.new_page()
    page.goto("https://www.eposnowhq.com/Pages/Reporting/SageReport.aspx")
    page.get_by_role("textbox", name="Username or email address").click()
    page.get_by_role("textbox", name="Username or email address").fill(epos_username)
    page.get_by_role("textbox", name="Password").click()
    page.get_by_role("textbox", name="Password").fill(epos_password)
    page.get_by_role("button", name="Log in").click()
    
    # Select Custom date range
    page.get_by_label("Show data from").select_option("Custom")
    
    # FROM date
    page.locator("#MainContent_timeControl_btnFromDate").click()
    page.wait_for_timeout(500)
    click_date_simple(page, date_from)
    
    # TO date
    page.locator("#MainContent_timeControl_btnToDate").click()
    page.wait_for_timeout(500)
    click_date_simple(page, date_to)
    
    # Apply date range
    page.locator("#MainContent_timeControl_btnApplyDate").click()
    page.wait_for_timeout(500)
    
    # Download CSV
    # For large date ranges, downloads can take longer - increase timeout and don't wait for navigation
    with page.expect_download(timeout=150000) as download_info:  # 2 minute timeout for large downloads
        page.get_by_role("button", name="Export to .csv").click(timeout=30000, no_wait_after=True)
    download = download_info.value

    # Determine repo root by using the current script's directory
    script_dir = os.path.dirname(os.path.abspath(__file__))
    repo_root = os.path.abspath(script_dir)
    save_dir = os.path.abspath(output_dir) if output_dir else repo_root
    os.makedirs(save_dir, exist_ok=True)

    filename = output_filename or download.suggested_filename
    save_path = os.path.join(save_dir, filename)
    download.save_as(save_path)

    # ---------------------
    context.close()
    browser.close()


if __name__ == "__main__":
    args = get_args()
    
    # Load company configuration
    try:
        config = load_company_config(args.company)
    except Exception as e:
        print(f"Error: Failed to load company config for '{args.company}': {e}")
        sys.exit(1)
    
    # Determine date parameters: if both from_date and to_date are provided, use them and ignore target_date
    from_date = args.from_date
    to_date = args.to_date
    target_date = None
    
    if from_date and to_date:
        # Range mode: ignore target_date
        target_date = None
    elif args.target_date:
        # Single day mode
        target_date = args.target_date
    else:
        # Default to yesterday
        target_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        print(f"No --target-date provided, using yesterday: {target_date}")
    
    with sync_playwright() as playwright:
        run(
            playwright,
            config,
            from_date=from_date,
            to_date=to_date,
            target_date=target_date,
            output_dir=args.output_dir,
            output_filename=args.output_filename,
        )
