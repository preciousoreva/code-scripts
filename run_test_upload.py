"""
Test upload: run the same pipeline as run_pipeline (single-day) but skip Phase 1 (EPOS download).

Uses a specific raw CSV you provide instead of downloading from EPOS. Runs:
  Phase 2: Transform (raw CSV -> QuickBooks-ready CSV)
  Phase 3: QBO upload (create SalesReceipts; creates inventory items when enabled)
  Phase 4: Reconciliation (EPOS vs QBO totals)
  Phase 5: Archive — only if you pass --archive (default: skip so raw file stays for repeated testing)

Use this to test inventory item creation, 6270/blockers logic, and reconciliation
without running Playwright/EPOS download. Product names from the CSV are used
as QBO line items (or new inventory items are created when inventory mode is enabled).

Usage:
    python run_test_upload.py
    python run_test_upload.py --csv BookKeeping_2026_01_29_1911.csv --company company_a --target-date 2026-01-28
    python run_test_upload.py --archive   # move raw/processed/metadata to Uploaded/<date>/
"""

import argparse
import logging
import os
import subprocess
import sys
from pathlib import Path

from load_env import load_env_file
from company_config import load_company_config, get_available_companies
from run_pipeline import reconcile_company, archive_files

# Load .env so reconciliation (QBO query) and in-process steps have env vars
load_env_file()

# Defaults match the sample CSV (BookKeeping_2026_01_29_1911.csv with date 28/01/2026)
DEFAULT_CSV = "BookKeeping_2026_01_29_1911.csv"
DEFAULT_COMPANY = "company_a"
DEFAULT_TARGET_DATE = "2026-01-28"


def run_step(cmd: list, cwd: str, label: str) -> None:
    """Run a command; log output and exit on failure."""
    logging.info(f"\n=== {label} ===")
    logging.info(f"Running: {' '.join(cmd)}")
    result = subprocess.run(cmd, cwd=cwd, capture_output=True, text=True)
    if result.stdout:
        for line in result.stdout.splitlines():
            logging.info(f"  {line}")
    if result.stderr:
        for line in result.stderr.splitlines():
            logging.warning(f"  {line}")
    if result.returncode != 0:
        logging.error(f"[ERROR] {label} failed with exit code {result.returncode}")
        if result.stdout:
            logging.error(result.stdout)
        if result.stderr:
            logging.error(result.stderr)
        sys.exit(result.returncode)
    logging.info(f"[OK] {label} completed successfully.")


def main():
    # Prefer UTF-8 on Windows to avoid UnicodeEncodeError (e.g. Naira symbol)
    os.environ.setdefault("PYTHONUTF8", "1")

    parser = argparse.ArgumentParser(
        description="Run pipeline from transform through archive using a specific raw CSV (no EPOS download)."
    )
    parser.add_argument(
        "--csv",
        default=DEFAULT_CSV,
        help=f"Path to raw EPOS CSV (relative to repo root or absolute). Default: {DEFAULT_CSV}",
    )
    parser.add_argument(
        "--company",
        default=DEFAULT_COMPANY,
        choices=get_available_companies(),
        help=f"Company key. Default: {DEFAULT_COMPANY}",
    )
    parser.add_argument(
        "--target-date",
        default=DEFAULT_TARGET_DATE,
        help=f"Target business date YYYY-MM-DD (must match CSV rows). Default: {DEFAULT_TARGET_DATE}",
    )
    parser.add_argument(
        "--archive",
        action="store_true",
        help="Move raw/processed/metadata files to Uploaded/<date>/ (default: skip so you can keep testing).",
    )
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(message)s")
    # Optional: force UTF-8 on root handler to avoid Windows cp1252 encoding errors
    try:
        import io
        if hasattr(sys.stdout, "buffer"):
            utf8_stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
            for h in logging.root.handlers[:]:
                if isinstance(h, logging.StreamHandler) and getattr(h, "stream", None) is sys.stdout:
                    h.setStream(utf8_stdout)
                    break
    except Exception:
        pass  # Leave default; NGN instead of Naira symbol avoids main crash
    repo_root = Path(__file__).resolve().parent

    # Resolve CSV path
    csv_path = Path(args.csv)
    if not csv_path.is_absolute():
        csv_path = repo_root / csv_path
    if not csv_path.exists():
        logging.error(f"[ERROR] CSV not found: {csv_path}")
        sys.exit(1)
    if not csv_path.is_file():
        logging.error(f"[ERROR] CSV path is not a file: {csv_path}")
        sys.exit(1)

    archive_mode = "on" if args.archive else "off"
    logging.info("")
    logging.info("=== Test upload (no EPOS download) ===")
    logging.info(f"  company:      {args.company}")
    logging.info(f"  target_date:  {args.target_date}")
    logging.info(f"  raw_csv_path: {csv_path}")
    logging.info(f"  archive:      {archive_mode}" + ("" if args.archive else " (use --archive to move files to Uploaded/<date>/)"))
    logging.info("")

    # Phase 2: Transform
    transform_cmd = [
        sys.executable,
        str(repo_root / "transform.py"),
        "--company",
        args.company,
        "--target-date",
        args.target_date,
        "--raw-file",
        str(csv_path),
    ]
    run_step(transform_cmd, str(repo_root), "Phase 2: Transform to single CSV (transform)")

    # Phase 3: QBO upload (same as pipeline: uses latest single_sales_receipts_*.csv)
    upload_cmd = [
        sys.executable,
        str(repo_root / "qbo_upload.py"),
        "--company",
        args.company,
        "--target-date",
        args.target_date,
    ]
    run_step(upload_cmd, str(repo_root), "Phase 3: Upload to QBO (qbo_upload)")

    # Phase 4: Reconciliation (EPOS vs QBO totals)
    logging.info("\n=== Phase 4: Reconciliation ===")
    config = load_company_config(args.company)
    reconcile_result = None
    try:
        reconcile_result = reconcile_company(args.company, args.target_date, config, repo_root)
        status = reconcile_result.get("status", "NOT RUN")
        if status == "MATCH":
            epos_total = reconcile_result.get("epos_total", 0)
            qbo_total = reconcile_result.get("qbo_total", 0)
            logging.info(f"[OK] Reconciliation: MATCH (EPOS: NGN {epos_total:,.2f}, QBO: NGN {qbo_total:,.2f})")
        elif status == "MISMATCH":
            diff = reconcile_result.get("difference", 0)
            logging.warning(f"[WARN] Reconciliation: MISMATCH (Difference: NGN {diff:,.2f})")
        else:
            reason = reconcile_result.get("reason", "unknown")
            logging.warning(f"[WARN] Reconciliation: NOT RUN ({reason})")
    except Exception as e:
        logging.error(f"[ERROR] Phase 4: Reconciliation failed: {e}")
        logging.warning("Continuing (upload was successful)")

    # Phase 5: Archive — skip by default so raw file stays in repo root for repeated testing
    if args.archive:
        logging.info("\n=== Phase 5: Archive Files ===")
        try:
            archive_files(repo_root, config)
        except Exception as e:
            logging.error(f"[ERROR] Phase 5: Archive failed: {e}")
            logging.warning("Continuing (upload was successful)")
    else:
        logging.info("\n=== Phase 5: Archive (skipped; use --archive to move files to Uploaded/<date>/) ===")

    logging.info("\nTest upload completed successfully.")


if __name__ == "__main__":
    main()
