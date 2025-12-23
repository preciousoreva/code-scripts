import re
from playwright.sync_api import Playwright, sync_playwright
import os
from datetime import datetime
import argparse

# Load .env file if it exists (makes credential management easier)
from load_env import load_env_file
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


def run(playwright: Playwright, from_date: str, to_date: str) -> None:
    # Get credentials from environment variables
    epos_username = os.environ.get("EPOS_USERNAME")
    epos_password = os.environ.get("EPOS_PASSWORD")
    
    if not epos_username:
        raise RuntimeError(
            "EPOS_USERNAME environment variable is not set. "
            "Please set it before running the pipeline:\n"
            "  export EPOS_USERNAME='your_username'"
        )
    if not epos_password:
        raise RuntimeError(
            "EPOS_PASSWORD environment variable is not set. "
            "Please set it before running the pipeline:\n"
            "  export EPOS_PASSWORD='your_password'"
        )
    
    browser = playwright.chromium.launch(headless=False)
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
    click_date_simple(page, from_date)
    
    # TO date
    page.locator("#MainContent_timeControl_btnToDate").click()
    page.wait_for_timeout(500)
    click_date_simple(page, to_date)
    
    # Apply date range
    page.locator("#MainContent_timeControl_btnApplyDate").click()
    page.wait_for_timeout(500)
    
    # Download CSV - same as original script
    with page.expect_download() as download_info:
        page.get_by_role("button", name="Export to .csv").click()
    download = download_info.value

    # Determine repo root by using the current script's directory
    script_dir = os.path.dirname(os.path.abspath(__file__))
    repo_root = os.path.abspath(script_dir)

    filename = download.suggested_filename
    save_path = os.path.join(repo_root, filename)
    download.save_as(save_path)

    # ---------------------
    context.close()
    browser.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Download EPOS BookKeeping CSV for a custom date range.")
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
    args = parser.parse_args()

    with sync_playwright() as playwright:
        run(playwright, args.from_date, args.to_date)

# Example usage:
# python epos_playwright_custom.py --from-date 2025-12-01 --to-date 2025-12-05
# use python3 for Mac OS