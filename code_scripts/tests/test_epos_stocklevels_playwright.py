import unittest
from pathlib import Path
from unittest import mock

from code_scripts import epos_stocklevels_playwright


class _FakeLocator:
    def __init__(self, key: str, *, exists: bool, clicks: list[tuple[str, bool]]) -> None:
        self._key = key
        self._exists = exists
        self._clicks = clicks

    def count(self) -> int:
        return 1 if self._exists else 0

    @property
    def first(self):
        return self

    def click(self, timeout: int = 0, no_wait_after: bool = False) -> None:
        self._clicks.append((self._key, bool(no_wait_after)))


class _FakePage:
    def __init__(
        self,
        *,
        roles: set[tuple[str, str]] | None = None,
        selectors: set[str] | None = None,
    ) -> None:
        self._roles = roles or set()
        self._selectors = selectors or set()
        self.clicks: list[tuple[str, bool]] = []

    def get_by_role(self, role: str, *, name: str):
        return _FakeLocator(
            f"role:{role}:{name}",
            exists=(role, name) in self._roles,
            clicks=self.clicks,
        )

    def locator(self, selector: str):
        return _FakeLocator(
            f"selector:{selector}",
            exists=selector in self._selectors,
            clicks=self.clicks,
        )


class EposStockLevelsPlaywrightHelpersTest(unittest.TestCase):
    def test_click_apply_falls_back_to_selector(self):
        page = _FakePage(selectors={"input[value='Apply']"})

        selected = epos_stocklevels_playwright._click_apply(page)

        self.assertEqual(selected, "input[value='Apply']")
        self.assertEqual(page.clicks, [("selector:input[value='Apply']", False)])

    def test_click_export_uses_role_and_no_wait_after(self):
        page = _FakePage(roles={("button", "Export to CSV")})

        selected = epos_stocklevels_playwright._click_export_csv(page)

        self.assertEqual(selected, "button:Export to CSV")
        self.assertEqual(page.clicks, [("role:button:Export to CSV", True)])

    def test_default_output_dir_uses_ops_exports(self):
        with mock.patch.object(epos_stocklevels_playwright, "stock_exports_dir", return_value=Path("/tmp/oiat-state/code_scripts/exports/stock_reports/2026-04-24")):
            out_dir = epos_stocklevels_playwright._default_output_dir()

        self.assertEqual(out_dir, Path("/tmp/oiat-state/code_scripts/exports/stock_reports/2026-04-24"))

    def test_default_output_filename_prefixes_company_and_date(self):
        fake_datetime = mock.Mock()
        fake_datetime.now.return_value.strftime.return_value = "1205"

        with mock.patch.object(epos_stocklevels_playwright, "datetime", fake_datetime):
            filename = epos_stocklevels_playwright._default_output_filename(
                "company sandbox",
                "Stock Report.csv",
            )

        self.assertEqual(filename, "company_sandbox_Stock_Report_1205.csv")


if __name__ == "__main__":
    unittest.main()
