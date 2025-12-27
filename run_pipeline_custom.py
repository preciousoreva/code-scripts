import subprocess
import sys
import json
import shutil
from pathlib import Path
from typing import Optional
import logging
from datetime import datetime
import argparse

from load_env import load_env_file
from slack_notify import (
    notify_pipeline_success,
    notify_pipeline_failure,
    notify_pipeline_start,
)
from qbo_query import cmd_reconcile

# Load .env file to make environment variables available
load_env_file()


def run_step(label: str, script_name: str, args: list = None) -> None:
    """
    Run a Python script in this repo using the current interpreter.
    Raises SystemExit if the script exits with a non-zero status.
    
    Args:
        label: Human-readable label for logging
        script_name: Name of the script file to run
        args: Optional list of command-line arguments to pass to the script
    """
    repo_root = Path(__file__).resolve().parent
    script_path = repo_root / script_name

    if not script_path.exists():
        error_msg = f"[ERROR] {label}: script not found at {script_path}"
        logging.error(error_msg)
        raise SystemExit(error_msg)

    cmd = [sys.executable, str(script_path)]
    if args:
        cmd.extend(args)

    logging.info(f"\n=== {label} ===")
    logging.info(f"Running: {' '.join(cmd)}")

    result = subprocess.run(
        cmd,
        cwd=str(repo_root),
        capture_output=True,
        text=True,
    )

    # Log stdout and stderr
    if result.stdout:
        logging.info("Script output:")
        for line in result.stdout.splitlines():
            logging.info(f"  {line}")
    if result.stderr:
        logging.warning("Script errors:")
        for line in result.stderr.splitlines():
            logging.warning(f"  {line}")

    if result.returncode != 0:
        error_msg = f"[ERROR] {label} failed with exit code {result.returncode}"
        if result.stdout:
            error_msg += f"\nOutput: {result.stdout}"
        if result.stderr:
            error_msg += f"\nErrors: {result.stderr}"
        logging.error(error_msg)
        raise SystemExit(error_msg)

    logging.info(f"[OK] {label} completed successfully.")


repo_root = Path(__file__).resolve().parent
logs_dir = repo_root / "logs"
logs_dir.mkdir(exist_ok=True)
log_file = logs_dir / f"pipeline_custom_{datetime.now().strftime('%Y%m%d-%H%M%S')}.log"

logging.basicConfig(
    level=logging.INFO,
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler(sys.stdout)
    ],
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def archive_files(repo_root: Path, date_range_str: str, from_date: str = None, to_date: str = None) -> None:
    """
    Phase 4: Archive processed files after successful upload.
    Reads last_epos_transform.json and moves files to Uploaded/<date_range>/ folder.
    
    Args:
        repo_root: Root directory of the repository
        date_range_str: Date range string (e.g., "2025-10-15 to 2025-10-17")
        from_date: Start date in YYYY-MM-DD format (optional, parsed from date_range_str if not provided)
        to_date: End date in YYYY-MM-DD format (optional, parsed from date_range_str if not provided)
    """
    # Parse from_date and to_date from date_range_str if not provided
    if not from_date or not to_date:
        if " to " in date_range_str:
            parts = date_range_str.split(" to ", 1)
            if len(parts) == 2:
                from_date = parts[0].strip()
                to_date = parts[1].strip()
    metadata_path = repo_root / "last_epos_transform.json"
    
    if not metadata_path.exists():
        logging.warning("Metadata file not found. Skipping archive step.")
        return
    
    try:
        with open(metadata_path, "r") as f:
            metadata = json.load(f)
    except Exception as e:
        logging.error(f"Failed to read metadata file: {e}")
        return
    
    # Use date_range_str for folder name instead of normalized_date
    # Create Uploaded/<date_range>/ folder (e.g., "Uploaded/2025-10-15 to 2025-10-17/")
    archive_dir = repo_root / "Uploaded" / date_range_str
    archive_dir.mkdir(parents=True, exist_ok=True)
    
    # Move raw file
    raw_file_path_str = metadata.get("raw_file_path", "")
    raw_file_path: Optional[Path] = None
    
    if raw_file_path_str:
        raw_file_path = Path(raw_file_path_str)
        if not raw_file_path.is_absolute():
            raw_file_path = repo_root / raw_file_path
    else:
        # Fallback to basename if full path not available
        raw_file_basename = metadata.get("raw_file", "")
        if raw_file_basename:
            raw_file_path = repo_root / raw_file_basename
    
    if raw_file_path:
        # Safety check: ensure we're not trying to move the repo root itself
        if raw_file_path == repo_root or raw_file_path.parent == repo_root and not raw_file_path.name:
            logging.warning(f"Invalid raw_file_path in metadata (points to repo root), skipping raw file archive")
        elif raw_file_path.exists() and raw_file_path.is_file():
            dest_raw = archive_dir / raw_file_path.name
            shutil.move(str(raw_file_path), str(dest_raw))
            logging.info(f"Moved raw file: {raw_file_path.name} -> Uploaded/{date_range_str}/")
        else:
            logging.warning(f"Raw file not found or is not a file: {raw_file_path}")
    else:
        logging.warning("No raw_file_path or raw_file in metadata, skipping raw file archive")
    
    # Move processed file(s)
    processed_files = metadata.get("processed_files", [])
    for processed_file in processed_files:
        if not processed_file or not processed_file.strip():
            logging.warning("Empty processed_file entry in metadata, skipping")
            continue
        
        processed_path = repo_root / processed_file
        
        # Safety check: ensure we're not trying to move the repo root or a directory
        if processed_path == repo_root:
            logging.warning(f"Invalid processed_file path (points to repo root): {processed_file}, skipping")
            continue
        
        if processed_path.exists() and processed_path.is_file():
            dest_processed = archive_dir / processed_file
            shutil.move(str(processed_path), str(dest_processed))
            logging.info(f"Moved processed file: {processed_file} -> Uploaded/{date_range_str}/")
        else:
            logging.warning(f"Processed file not found or is not a file: {processed_file}")
    
    # Move spill files if any - but ONLY for dates in the range being processed
    # Spill files for future dates should remain in uploads/spill/ for later processing
    spill_files = metadata.get("spill_files", [])
    if spill_files:
        # Parse date range
        from datetime import datetime
        try:
            range_start = datetime.strptime(from_date, "%Y-%m-%d")
            range_end = datetime.strptime(to_date, "%Y-%m-%d")
        except ValueError:
            range_start = None
            range_end = None
        
        for spill_file in spill_files:
            # spill_file is relative path like "uploads/spill/BookKeeping_spill_2025-12-25.csv"
            spill_filename = Path(spill_file).name
            # Extract date from filename: BookKeeping_spill_YYYY-MM-DD.csv
            try:
                spill_date_str = spill_filename.replace("BookKeeping_spill_", "").replace(".csv", "")
                spill_date = datetime.strptime(spill_date_str, "%Y-%m-%d")
                
                # Only archive if spill date is within the processed range
                if range_start and range_end and range_start <= spill_date <= range_end:
                    spill_path = repo_root / spill_file
                    if spill_path.exists() and spill_path.is_file():
                        dest_spill = archive_dir / spill_filename
                        shutil.move(str(spill_path), str(dest_spill))
                        logging.info(f"Moved spill file: {spill_filename} -> Uploaded/{date_range_str}/")
                    else:
                        logging.warning(f"Spill file not found: {spill_path}")
                else:
                    # This spill file is outside the range - keep it in uploads/spill/
                    logging.info(f"Keeping spill file {spill_filename} in uploads/spill/ (outside processed range)")
            except ValueError:
                # Couldn't parse date from filename - archive it anyway
                spill_path = repo_root / spill_file
                if spill_path.exists() and spill_path.is_file():
                    dest_spill = archive_dir / spill_filename
                    shutil.move(str(spill_path), str(dest_spill))
                    logging.info(f"Moved spill file: {spill_filename} -> Uploaded/{date_range_str}/")
    
    # Move spill files that were used/merged during processing
    used_spill_files = metadata.get("used_spill_files", [])
    if used_spill_files:
        logging.info(f"Archiving {len(used_spill_files)} used spill file(s)...")
        for spill_file in used_spill_files:
            # spill_file is relative path like "uploads/spill/BookKeeping_spill_2025-12-25.csv"
            spill_path = repo_root / spill_file
            if spill_path.exists() and spill_path.is_file():
                spill_filename = spill_path.name
                dest_spill = archive_dir / spill_filename
                shutil.move(str(spill_path), str(dest_spill))
                logging.info(f"Moved used spill file: {spill_filename} -> Uploaded/{date_range_str}/")
            else:
                logging.warning(f"Used spill file not found: {spill_path}")
    
    # Move metadata file to archive as well
    dest_metadata = archive_dir / "last_epos_transform.json"
    shutil.move(str(metadata_path), str(dest_metadata))
    logging.info(f"Moved metadata: last_epos_transform.json -> Uploaded/{date_range_str}/")
    
    logging.info(f"[OK] Phase 4: Archive completed. Files archived to Uploaded/{date_range_str}/")


def main(from_date: str, to_date: str, target_date: str = None) -> int:
    """
    Full pipeline with custom date range:

    1) epos_playwright_custom.py
       - Logs into EPOS and downloads bookkeeping CSV for the specified date range
         into the repo root directory.

    2) epos_to_qb_single.py
       - Reads the latest raw EPOS file from repo root
         and produces a single consolidated QuickBooks-ready CSV
         in repo root (single_sales_receipts_*.csv).
       - Writes metadata to last_epos_transform.json

    3) qbo_upload.py
       - Reads the latest single_sales_receipts_*.csv from repo root
         and creates Sales Receipts in the QBO sandbox via API.

    4) Archive (run_pipeline_custom.py)
       - After successful upload, reads last_epos_transform.json
       - Creates Uploaded/<date>/ folder
       - Moves raw CSV, processed CSV(s), and metadata to archive folder
    """
    pipeline_name = "EPOS -> QuickBooks Pipeline (Custom Range)"
    date_range_str = f"{from_date} to {to_date}"

    logging.info(f"Starting EPOS -> QuickBooks pipeline (Custom Range: {from_date} to {to_date})...\n")
    notify_pipeline_start(pipeline_name, log_file, date_range_str)

    try:
        # Phase 1: Download from EPOS with custom date range
        run_step(
            "Phase 1: Download EPOS CSV (epos_playwright_custom)",
            "epos_playwright_custom.py",
            ["--from-date", from_date, "--to-date", to_date]
        )

        # Phase 2: Transform to single QuickBooks-ready CSV
        # If target_date is provided, filter by it; otherwise process all dates in range
        transform_args = []
        if target_date:
            transform_args = ["--target-date", target_date]
            logging.info(f"Filtering by target_date: {target_date}")
        run_step(
            "Phase 2: Transform to single CSV (epos_to_qb_single)",
            "epos_to_qb_single.py",
            transform_args if transform_args else None
        )

        # Phase 3: Upload to QBO sandbox
        run_step("Phase 3: Upload to QBO (qbo_upload)", "qbo_upload.py")

        # Phase 4: Archive files after successful upload
        logging.info("\n=== Phase 4: Archive Files ===")
        try:
            archive_files(repo_root, date_range_str, from_date, to_date)
        except Exception as e:
            logging.error(f"[ERROR] Phase 4: Archive failed: {e}")
            # Don't fail the pipeline if archiving fails - upload already succeeded
            logging.warning("Continuing despite archive failure (upload was successful)")

        # Phase 5: Reconcile EPOS vs QBO totals
        logging.info("\n=== Phase 5: Reconciliation ===")
        logging.info(f"Reconciling for date range: {from_date} to {to_date}")
        try:
            # Run reconciliation (non-fatal - wrapped to catch SystemExit)
            try:
                cmd_reconcile(from_date, to_date, tolerance=0.00)
                logging.info("[OK] Phase 5: Reconciliation completed successfully.")
            except SystemExit:
                # cmd_reconcile calls sys.exit(1) on errors, catch it here
                logging.warning("Reconciliation encountered errors but pipeline continues")
            except Exception as e:
                logging.error(f"[ERROR] Phase 5: Reconciliation failed: {e}")
                logging.warning("Continuing despite reconciliation failure (upload was successful)")
        except Exception as e:
            logging.error(f"[ERROR] Phase 5: Reconciliation setup failed: {e}")
            logging.warning("Continuing despite reconciliation failure (upload was successful)")

        # Success notification - load metadata for summary
        metadata = None
        metadata_path = repo_root / "last_epos_transform.json"
        if metadata_path.exists():
            try:
                with open(metadata_path, "r") as f:
                    metadata = json.load(f)
            except Exception as e:
                logging.warning(f"Could not load metadata for notification: {e}")
        
        notify_pipeline_success(pipeline_name, log_file, date_range_str, metadata)
        logging.info("\nPipeline completed successfully ✅")
        return 0

    except SystemExit as e:
        logging.error("Custom-range pipeline failed", exc_info=True)
        notify_pipeline_failure(pipeline_name, log_file, str(e), date_range_str)
        return 1
    except Exception as e:
        logging.error("Custom-range pipeline failed with unexpected error", exc_info=True)
        notify_pipeline_failure(pipeline_name, log_file, str(e), date_range_str)
        return 1


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Run EPOS -> QuickBooks pipeline for a custom date range."
    )
    parser.add_argument(
        "--from-date",
        required=True,
        help="Start date in YYYY-MM-DD format (e.g. 2025-12-01)",
    )
    parser.add_argument(
        "--to-date",
        required=True,
        help="End date in YYYY-MM-DD format (e.g. 2025-12-05)",
    )
    parser.add_argument(
        "--target-date",
        help="Optional target business date in YYYY-MM-DD format. If provided, filters rows to only this date (useful for handling spillover).",
    )
    args = parser.parse_args()

    raise SystemExit(main(args.from_date, args.to_date, args.target_date))

# Example usage:
# python run_pipeline_custom.py --from-date 2025-12-01 --to-date 2025-12-05
# use python3 for Mac OS