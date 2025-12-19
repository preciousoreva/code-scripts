import subprocess
import sys
import json
import shutil
from pathlib import Path
from typing import Optional
import logging
from datetime import datetime

from load_env import load_env_file
from slack_notify import (
    notify_pipeline_success,
    notify_pipeline_failure,
    notify_pipeline_start,
)

# Load .env file to make environment variables available
load_env_file()


def run_step(label: str, script_name: str) -> None:
    """
    Run a Python script in this repo using the current interpreter.
    Raises SystemExit if the script exits with a non-zero status.
    """
    repo_root = Path(__file__).resolve().parent
    script_path = repo_root / script_name

    if not script_path.exists():
        error_msg = f"[ERROR] {label}: script not found at {script_path}"
        logging.error(error_msg)
        raise SystemExit(error_msg)

    logging.info(f"\n=== {label} ===")
    logging.info(f"Running: {sys.executable} {script_path}")

    result = subprocess.run(
        [sys.executable, str(script_path)],
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
log_file = logs_dir / f"pipeline_{datetime.now().strftime('%Y%m%d-%H%M%S')}.log"

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
            logging.info(f"Moved raw file: {raw_file_path.name} → Uploaded/{normalized_date}/")
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
            logging.info(f"Moved processed file: {processed_file} → Uploaded/{normalized_date}/")
        else:
            logging.warning(f"Processed file not found or is not a file: {processed_file}")
    
    # Move metadata file to archive as well
    dest_metadata = archive_dir / "last_epos_transform.json"
    shutil.move(str(metadata_path), str(dest_metadata))
    logging.info(f"Moved metadata: last_epos_transform.json → Uploaded/{normalized_date}/")
    
    logging.info(f"[OK] Phase 4: Archive completed. Files archived to Uploaded/{normalized_date}/")


def main() -> None:
    """
    Full pipeline:

    1) epos_playwright.py
       - Logs into EPOS and downloads the latest bookkeeping CSV
         into the repo root directory.

    2) epos_to_qb_single.py
       - Reads the latest raw EPOS file from repo root
         and produces a single consolidated QuickBooks-ready CSV
         in repo root (single_sales_receipts_*.csv).
       - Writes metadata to last_epos_transform.json

    3) qbo_upload.py
       - Reads the latest single_sales_receipts_*.csv from repo root
         and creates Sales Receipts in the QBO sandbox via API.

    4) Archive (run_pipeline.py)
       - After successful upload, reads last_epos_transform.json
       - Creates Uploaded/<date>/ folder
       - Moves raw CSV, processed CSV(s), and metadata to archive folder
    """
    pipeline_name = "EPOS → QuickBooks Pipeline"
    date_range_str = None  # this pipeline uses latest available data

    logging.info("Starting EPOS → QuickBooks pipeline...\n")
    notify_pipeline_start(pipeline_name, log_file, date_range_str)

    try:
        # Phase 1: Download from EPOS
        run_step("Phase 1: Download EPOS CSV (epos_playwright)", "epos_playwright.py")

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

        # Success notification
        notify_pipeline_success(pipeline_name, log_file, date_range_str)
        logging.info("\nPipeline completed successfully ✅")

    except SystemExit as e:
        logging.error("Pipeline failed", exc_info=True)
        notify_pipeline_failure(pipeline_name, log_file, str(e), date_range_str)
        raise
    except Exception as e:
        logging.error("Pipeline failed with unexpected error", exc_info=True)
        notify_pipeline_failure(pipeline_name, log_file, str(e), date_range_str)
        raise


if __name__ == "__main__":
    main()

