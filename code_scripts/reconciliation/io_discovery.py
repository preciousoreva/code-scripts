"""File discovery and date extraction from filenames (EPOS + statements)."""

from __future__ import annotations

import logging
import re
from pathlib import Path
from typing import List, Optional, Tuple

logger = logging.getLogger(__name__)

# BookKeeping naming: ORIGINAL_BookKeeping_*_YYYY-MM-DD_*.csv or BookKeeping_*_YYYY-MM-DD.csv
ORIGINAL_PREFIX = "ORIGINAL_"
BOOKKEEPING_PREFIX = "BookKeeping_"
BOOKKEEPING_SUFFIX = ".csv"
DATE_PATTERN = re.compile(r"(\d{4})-(\d{2})-(\d{2})")


def date_from_filename(path: Path) -> Optional[str]:
    """
    Extract YYYY-MM-DD from path stem if present.
    """
    stem = path.stem
    m = DATE_PATTERN.search(stem)
    if m:
        return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
    return None


def is_original_bookkeeping(path: Path) -> bool:
    return path.name.startswith(ORIGINAL_PREFIX) and BOOKKEEPING_PREFIX in path.name and path.suffix.lower() == BOOKKEEPING_SUFFIX


def is_bookkeeping(path: Path) -> bool:
    return path.name.startswith(BOOKKEEPING_PREFIX) and path.suffix.lower() == BOOKKEEPING_SUFFIX


def _scan_dir(d: Path, date_filter: str) -> Tuple[List[Path], List[Path]]:
    """Scan a single directory for BookKeeping CSVs matching date_filter; return (originals, fallbacks)."""
    originals: List[Path] = []
    fallbacks: List[Path] = []
    for p in d.iterdir():
        if not p.is_file():
            continue
        file_date = date_from_filename(p)
        if file_date != date_filter:
            continue
        if is_original_bookkeeping(p):
            originals.append(p)
        elif is_bookkeeping(p):
            fallbacks.append(p)
    return originals, fallbacks


def _collect_candidates(epos_dir: Path, date_filter: str) -> Tuple[List[Path], List[Path]]:
    """
    Search epos_dir: first epos_dir/<date>/, then epos_dir/ (flat).
    Return (originals, fallbacks) with date-subdir results first in each list.
    """
    originals: List[Path] = []
    fallbacks: List[Path] = []
    epos_dir = Path(epos_dir)
    if not epos_dir.is_dir():
        return originals, fallbacks

    date_subdir = epos_dir / date_filter
    if date_subdir.is_dir():
        o, f = _scan_dir(date_subdir, date_filter)
        originals.extend(o)
        fallbacks.extend(f)
    o_flat, f_flat = _scan_dir(epos_dir, date_filter)
    originals.extend(o_flat)
    fallbacks.extend(f_flat)
    return originals, fallbacks


def _pick_one_by_mtime(candidates: List[Path]) -> Optional[Path]:
    """Return the single path with latest mtime; if multiple, log warning and return latest."""
    if not candidates:
        return None
    if len(candidates) == 1:
        return candidates[0]
    by_mtime = sorted(candidates, key=lambda p: p.stat().st_mtime, reverse=True)
    logger.warning(
        "Multiple BookKeeping files found for date; using most recently modified: %s (others: %s)",
        by_mtime[0],
        [p.name for p in by_mtime[1:]],
    )
    return by_mtime[0]


def discover_epos_file(
    epos_dir: Path,
    date_filter: str,
    prefer_original: bool = True,
) -> Optional[Path]:
    """
    Find BookKeeping CSV for the given date.
    Search order: epos_dir/<date>/ first, then epos_dir/ (flat).
    Prefer ORIGINAL_BookKeeping_* if prefer_original else BookKeeping_*.
    If multiple matches exist, choose the most recently modified and log a warning.
    """
    originals, fallbacks = _collect_candidates(Path(epos_dir), date_filter)
    candidates: List[Path] = []
    if prefer_original and originals:
        candidates = originals
    elif fallbacks:
        candidates = fallbacks
    elif originals:
        candidates = originals
    return _pick_one_by_mtime(candidates)


def discover_statement_files(statements_dir: Path) -> List[Path]:
    """
    Return list of .xlsx files in statements_dir (any name pattern).
    """
    statements_dir = Path(statements_dir)
    if not statements_dir.is_dir():
        return []
    return sorted(statements_dir.glob("*.xlsx"))


def discover_transformed_sales_file(
    epos_dir: Path,
    date_filter: str,
    csv_prefix: Optional[str] = None,
) -> Optional[Path]:
    """
    Discover transformed sales CSV for a date.

    Supports names like:
    - single_sales_receipts_BookKeeping_<company>_<YYYY-MM-DD>.csv
    - gp_sales_receipts_BookKeeping_<company>_<YYYY-MM-DD>.csv
    """
    epos_dir = Path(epos_dir)
    if not epos_dir.is_dir():
        return None

    candidates: List[Path] = []
    date_subdir = epos_dir / date_filter
    search_dirs = [date_subdir, epos_dir] if date_subdir.is_dir() else [epos_dir]

    preferred_prefixes: List[str] = []
    if csv_prefix:
        preferred_prefixes.append(str(csv_prefix).strip())
    preferred_prefixes.extend(["single_sales_receipts", "gp_sales_receipts", "sales_receipts"])

    for d in search_dirs:
        for p in d.iterdir():
            if not p.is_file() or p.suffix.lower() != ".csv":
                continue
            name = p.name.lower()
            if date_filter not in name:
                continue
            if "sales_receipts" not in name:
                continue
            candidates.append(p)

    if not candidates:
        return None

    # Prefer selected prefix, then latest by modified time.
    for prefix in preferred_prefixes:
        pref = [p for p in candidates if p.name.lower().startswith(prefix.lower())]
        if pref:
            return _pick_one_by_mtime(pref)
    return _pick_one_by_mtime(candidates)
