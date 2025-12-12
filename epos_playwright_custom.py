import os
import argparse
from datetime import datetime
from playwright.sync_api import Playwright, sync_playwright

from load_env import load_env_file
load_env_file()


def parse_args():
    parser = argparse.ArgumentParser(
        description="Download EPOS BookKeeping CSV for a custom date range."
    )
    parser.add_argument(
        "--from-date",
        required=True,
        help="Start date in YYYY-MM-DD format (e.g. 2025-12-01)",
    )
    parser.add_argument(
        "--to-date",
        required=True,
        help="End date in YYYY-MM-DD format (e.g. 2025-12-01)",
    )
    return parser.parse_args()


def ensure_creds():
    epos_username = os.environ.get("EPOS_USERNAME")
    epos_password = os.environ.get("EPOS_PASSWORD")

    if not epos_username:
        raise RuntimeError(
            "EPOS_USERNAME environment variable is not set. "
            "Please set it before running this script:\n"
            "  export EPOS_USERNAME='your_username'"
        )
    if not epos_password:
        raise RuntimeError(
            "EPOS_PASSWORD environment variable is not set. "
            "Please set it before running this script:\n"
            "  export EPOS_PASSWORD='your_password'"
        )
    return epos_username, epos_password


def format_epos_title(date_str: str) -> str:
    """
    Convert '2025-12-01' -> '01 December 2025' to match the date picker title.
    Based on error message, titles include the year.
    """
    dt = datetime.strptime(date_str, "%Y-%m-%d")
    return dt.strftime("%d %B %Y")  # e.g. "01 December 2025"


def select_date_in_calendar(page, calendar_prefix: str, target_date: str) -> None:
    """
    Select a date in the calendar, navigating to the correct month if needed.
    Optimized version that handles calendar navigation more reliably.
    """
    dt = datetime.strptime(target_date, "%Y-%m-%d")
    target_title = dt.strftime("%d %B %Y")  # e.g. "02 December 2025"
    target_month_year = dt.strftime("%B %Y")  # e.g. "December 2025"
    target_year = dt.year
    target_month = dt.month
    
    # Wait for calendar to appear
    page.wait_for_timeout(500)
    
    # Try to find the date - if not found, calendar is showing wrong month
    day_locator = page.locator(f'[id^="{calendar_prefix}_day"][title="{target_title}"]')
    
    # If date not found, navigate to correct month
    if day_locator.count() == 0:
        max_attempts = 24  # Max 2 years of navigation
        attempts = 0
        
        while attempts < max_attempts:
            # Check if date is now available
            day_locator = page.locator(f'[id^="{calendar_prefix}_day"][title="{target_title}"]')
            if day_locator.count() > 0:
                break
            
            # Find the calendar container
            calendar_container = page.locator(f'table[id*="{calendar_prefix}"], div[id*="{calendar_prefix}"]').first
            
            # Strategy 1: Click month/year header to open month/year picker
            try:
                # Look for the month/year header (usually a clickable title)
                month_header = calendar_container.locator('td.title, th.title, .ajax__calendar_title, td:has-text("' + target_month_year.split()[0] + '")').first
                if month_header.is_visible(timeout=500):
                    month_header.click()
                    page.wait_for_timeout(400)
                    
                    # Now look for the target month/year in the picker
                    # Try multiple selectors for the month link
                    month_selectors = [
                        f'text={target_month_year}',
                        f'text={dt.strftime("%B")}',
                        f'a:has-text("{target_month_year}")',
                        f'a[title*="{target_month_year}"]'
                    ]
                    
                    for selector in month_selectors:
                        try:
                            month_link = page.locator(selector).first
                            if month_link.is_visible(timeout=300):
                                month_link.click()
                                page.wait_for_timeout(500)
                                # Recheck for the date
                                day_locator = page.locator(f'[id^="{calendar_prefix}_day"][title="{target_title}"]')
                                if day_locator.count() > 0:
                                    break
                        except:
                            continue
            except:
                pass
            
            # Strategy 2: Use next/previous month buttons (fallback)
            if day_locator.count() == 0:
                try:
                    # Find navigation buttons
                    next_btn = calendar_container.locator('a[title*="Next"], a:has-text(">"), a[title*=">"]').first
                    prev_btn = calendar_container.locator('a[title*="Previous"], a:has-text("<"), a[title*="<"]').first
                    
                    # Simple approach: try next month (we can improve this by comparing current vs target)
                    if next_btn.is_visible(timeout=300):
                        next_btn.click()
                        page.wait_for_timeout(400)
                    elif prev_btn.is_visible(timeout=300):
                        prev_btn.click()
                        page.wait_for_timeout(400)
                except:
                    pass
            
            attempts += 1
    
    # Click the date
    if day_locator.count() == 0:
        raise RuntimeError(
            f"Could not find date {target_date} ({target_title}) in calendar.\n"
            f"Calendar might not be showing {target_month_year}. "
            f"Try running with headless=False to see what month is displayed."
        )
    
    day_locator.first.click()
    page.wait_for_timeout(300)  # Wait for calendar to close


def run(playwright: Playwright, from_date: str, to_date: str) -> None:
    epos_username, epos_password = ensure_creds()

    from_title = format_epos_title(from_date)
    to_title = format_epos_title(to_date)

    browser = playwright.chromium.launch(headless=False)
    context = browser.new_context()
    page = context.new_page()

    page.goto("https://www.eposnowhq.com/Pages/Reporting/SageReport.aspx")

    # Login
    page.get_by_role("textbox", name="Username or email address").fill(epos_username)
    page.get_by_role("textbox", name="Password").fill(epos_password)
    page.get_by_role("button", name="Log in").click()

    # (Optional) wait for the report page to be ready
    page.get_by_label("Show data from").wait_for()

    # Switch to Custom date range
    page.get_by_label("Show data from").select_option("Custom")

    # FROM date
    page.locator("#MainContent_timeControl_btnFromDate").click()
    select_date_in_calendar(page, "fromDate", from_date)

    # TO date
    page.locator("#MainContent_timeControl_btnToDate").click()
    select_date_in_calendar(page, "toDate", to_date)

    # Fetch data
    page.locator("#MainContent_FetchFromServer").click()

    # Download CSV
    with page.expect_download() as download_info:
        page.get_by_role("button", name="Export to .csv").click()
    download = download_info.value

    # Save into repo root (same as your main pipeline)
    script_dir = os.path.dirname(os.path.abspath(__file__))
    repo_root = os.path.abspath(script_dir)

    filename = download.suggested_filename
    save_path = os.path.join(repo_root, filename)
    download.save_as(save_path)

    print(f"Downloaded custom-range CSV to: {save_path}")

    context.close()
    browser.close()


if __name__ == "__main__":
    args = parse_args()
    with sync_playwright() as playwright:
        run(playwright, args.from_date, args.to_date)


        # Single day (Dec 1)
# python3 epos_playwright_custom.py --from-date 2025-12-01 --to-date 2025-12-01

        # Range (Dec 1–9)
# python3 epos_playwright_custom.py --from-date 2025-12-01 --to-date 2025-12-09