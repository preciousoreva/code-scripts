import re
from playwright.sync_api import Playwright, sync_playwright, expect
import os

# Load .env file if it exists (makes credential management easier)
from load_env import load_env_file
load_env_file()


def run(playwright: Playwright) -> None:
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
    page.get_by_label("Show data from").select_option("Yesterday")
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


with sync_playwright() as playwright:
    run(playwright)
