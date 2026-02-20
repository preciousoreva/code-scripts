"""Unit tests for reconciliation io_discovery (EPOS and statement file discovery)."""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from code_scripts.reconciliation.io_discovery import (
    date_from_filename,
    discover_epos_file,
    discover_statement_files,
    discover_transformed_sales_file,
    is_bookkeeping,
    is_original_bookkeeping,
)


class TestDateFromFilename(unittest.TestCase):
    def test_date_from_filename(self) -> None:
        self.assertEqual(date_from_filename(Path("BookKeeping_Co_2026-02-15.csv")), "2026-02-15")
        self.assertEqual(date_from_filename(Path("ORIGINAL_BookKeeping_Co_2026-02-15_abc.csv")), "2026-02-15")
        self.assertIsNone(date_from_filename(Path("account-4000700275-jan-17.xlsx")))
        self.assertIsNone(date_from_filename(Path("no_date_here.csv")))


class TestIsOriginalBookkeeping(unittest.TestCase):
    def test_is_original_bookkeeping(self) -> None:
        self.assertTrue(is_original_bookkeeping(Path("ORIGINAL_BookKeeping_Co_2026-02-15.csv")))
        self.assertFalse(is_original_bookkeeping(Path("BookKeeping_Co_2026-02-15.csv")))
        self.assertFalse(is_original_bookkeeping(Path("ORIGINAL_BookKeeping_Co_2026-02-15.xlsx")))


class TestIsBookkeeping(unittest.TestCase):
    def test_is_bookkeeping(self) -> None:
        self.assertTrue(is_bookkeeping(Path("BookKeeping_Co_2026-02-15.csv")))
        self.assertFalse(is_bookkeeping(Path("ORIGINAL_BookKeeping_Co_2026-02-15.csv")))  # starts with ORIGINAL_
        self.assertFalse(is_bookkeeping(Path("other.csv")))


class TestDiscoverEposFile(unittest.TestCase):
    def test_date_subdir_first(self) -> None:
        """Discovery prefers epos/<date>/ over flat epos/ when both have a matching file."""
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            date_dir = root / "2026-02-15"
            date_dir.mkdir()
            flat_file = root / "BookKeeping_Co_2026-02-15.csv"
            flat_file.write_text("x")
            date_file = date_dir / "ORIGINAL_BookKeeping_Co_2026-02-15.csv"
            date_file.write_text("y")
            found = discover_epos_file(root, "2026-02-15", prefer_original=True)
            self.assertIsNotNone(found)
            self.assertEqual(found, date_file)
            self.assertEqual(found.read_text(), "y")

    def test_fallback_flat(self) -> None:
        """When no date subdir, discovery finds file in flat epos dir."""
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            f = root / "BookKeeping_Co_2026-02-15.csv"
            f.write_text("x")
            found = discover_epos_file(root, "2026-02-15", prefer_original=False)
            self.assertIsNotNone(found)
            self.assertEqual(found, f)

    def test_prefer_original(self) -> None:
        """When prefer_original=True, ORIGINAL_* is chosen over BookKeeping_*."""
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            orig = root / "ORIGINAL_BookKeeping_Co_2026-02-15.csv"
            orig.write_text("orig")
            plain = root / "BookKeeping_Co_2026-02-15.csv"
            plain.write_text("plain")
            found = discover_epos_file(root, "2026-02-15", prefer_original=True)
            self.assertIsNotNone(found)
            self.assertIn("ORIGINAL", found.name)

    def test_no_match(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            self.assertIsNone(discover_epos_file(Path(tmp), "2026-02-15", prefer_original=True))


class TestDiscoverStatementFiles(unittest.TestCase):
    def test_finds_xlsx(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "account-1.xlsx").write_text("a")
            (root / "account-2.xlsx").write_text("b")
            (root / "readme.txt").write_text("no")
            found = discover_statement_files(root)
            self.assertEqual(len(found), 2)
            names = {p.name for p in found}
            self.assertEqual(names, {"account-1.xlsx", "account-2.xlsx"})

    def test_empty_dir(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            self.assertEqual(discover_statement_files(Path(tmp)), [])


class TestDiscoverTransformedSalesFile(unittest.TestCase):
    def test_prefers_prefix_and_date_subdir(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            day_dir = root / "2026-02-18"
            day_dir.mkdir()
            a = day_dir / "single_sales_receipts_A_2026-02-18.csv"
            b = day_dir / "gp_sales_receipts_B_2026-02-18.csv"
            a.write_text("x")
            b.write_text("y")
            found = discover_transformed_sales_file(root, "2026-02-18", csv_prefix="single_sales_receipts")
            self.assertEqual(found, a)

    def test_fallback_any_sales_receipts(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            f = root / "sales_receipts_C_2026-02-18.csv"
            f.write_text("x")
            found = discover_transformed_sales_file(root, "2026-02-18")
            self.assertEqual(found, f)
