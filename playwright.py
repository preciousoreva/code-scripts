import re
from playwright.sync_api import Playwright, sync_playwright, expect


def run(playwright: Playwright) -> None:
    browser = playwright.chromium.launch(headless=False)
    context = browser.new_context()
    page = context.new_page()
    page.goto("https://login.eposnowhq.com/login?login_challenge=6a2223b6abad4838a88e245a7ebf43e2")
    page.get_by_role("textbox", name="Username or email address").click()
    page.get_by_role("textbox", name="Username or email address").fill("")
    page.get_by_role("textbox", name="Password").click()
    page.get_by_role("textbox", name="Password").fill("")
    page.get_by_role("button", name="Log in").click()
    page.get_by_label("Show data from").select_option("Yesterday")
    with page.expect_download() as download_info:
        page.get_by_role("button", name="Export to .csv").click()
    download = download_info.value

# Save downloaded file to a specific location. For ease, create a folder called epos_raw in C: drive

    filename = download.suggested_filename
    download.save_as(fr"C:/epos/raw/{filename}")

    # ---------------------
    context.close()
    browser.close()


with sync_playwright() as playwright:
    run(playwright)


