import subprocess
import sys
import json
import shutil
from pathlib import Path
import logging
from datetime import datetime
import argparse

from load_env import load_env_file
from slack_notify import notify_pipeline_success

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
    )

    if result.returncode != 0:
        error_msg = f"[ERROR] {label} failed with exit code {result.returncode}"
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

def archive_files(repo_root: Path) -> None:
    """
    Phase 4: Archive processed files after successful upload.
    Reads last_epos_transform.json and moves files to Uploaded/<date>/ folder.
    """
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
    
    normalized_date = metadata.get("normalized_date")
    if not normalized_date:
        logging.warning("No normalized_date in metadata. Skipping archive step.")
        return
    
    # Create Uploaded/<date>/ folder
    archive_dir = repo_root / "Uploaded" / normalized_date
    archive_dir.mkdir(parents=True, exist_ok=True)
    
    # Move raw file
    raw_file_path_str = metadata.get("raw_file_path", "")
    if raw_file_path_str:
        raw_file_path = Path(raw_file_path_str)
        if not raw_file_path.is_absolute():
            raw_file_path = repo_root / raw_file_path
    else:
        # Fallback to basename if full path not available
        raw_file_path = repo_root / metadata.get("raw_file", "")
    
    if raw_file_path.exists():
        dest_raw = archive_dir / raw_file_path.name
        shutil.move(str(raw_file_path), str(dest_raw))
        logging.info(f"Moved raw file: {raw_file_path.name} → Uploaded/{normalized_date}/")
    else:
        logging.warning(f"Raw file not found: {raw_file_path}")
    
    # Move processed file(s)
    processed_files = metadata.get("processed_files", [])
    for processed_file in processed_files:
        processed_path = repo_root / processed_file
        if processed_path.exists():
            dest_processed = archive_dir / processed_file
            shutil.move(str(processed_path), str(dest_processed))
            logging.info(f"Moved processed file: {processed_file} → Uploaded/{normalized_date}/")
        else:
            logging.warning(f"Processed file not found: {processed_file}")
    
    # Move metadata file to archive as well
    dest_metadata = archive_dir / "last_epos_transform.json"
    shutil.move(str(metadata_path), str(dest_metadata))
    logging.info(f"Moved metadata: last_epos_transform.json → Uploaded/{normalized_date}/")
    
    logging.info(f"[OK] Phase 4: Archive completed. Files archived to Uploaded/{normalized_date}/")


def main(from_date: str, to_date: str) -> None:
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
    logging.info(f"Starting EPOS → QuickBooks pipeline (Custom Range: {from_date} to {to_date})...\n")

    # Phase 1: Download from EPOS with custom date range
    run_step(
        "Phase 1: Download EPOS CSV (epos_playwright_custom)",
        "epos_playwright_custom.py",
        ["--from-date", from_date, "--to-date", to_date]
    )

    # Phase 2: Transform to single QuickBooks-ready CSV
    run_step("Phase 2: Transform to single CSV (epos_to_qb_single)", "epos_to_qb_single.py")

    # Phase 3: Upload to QBO sandbox
    run_step("Phase 3: Upload to QBO (qbo_upload)", "qbo_upload.py")

    # Phase 4: Archive files after successful upload
    logging.info("\n=== Phase 4: Archive Files ===")
    try:
        archive_files(repo_root)
    except Exception as e:
        logging.error(f"[ERROR] Phase 4: Archive failed: {e}")
        # Don't fail the pipeline if archiving fails - upload already succeeded
        logging.warning("Continuing despite archive failure (upload was successful)")

    # Send Slack notification on success
    date_range_str = f"{from_date} to {to_date}"
    notify_pipeline_success("EPOS → QuickBooks Pipeline (Custom Range)", log_file, date_range_str)

    logging.info("\nPipeline completed successfully ✅")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Run EPOS → QuickBooks pipeline for a custom date range."
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
    args = parser.parse_args()

    main(args.from_date, args.to_date)

# Example usage:
# python run_pipeline_custom.py --from-date 2025-12-01 --to-date 2025-12-05
# use python3 for Mac OS