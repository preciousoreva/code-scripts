import subprocess
import sys
import json
import shutil
import argparse
import os
import re
from pathlib import Path
from typing import Optional
import logging
from datetime import datetime, timedelta

from code_scripts.load_env import load_env_file
from code_scripts.slack_notify import (
    notify_pipeline_success,
    notify_pipeline_failure,
    notify_pipeline_start,
    notify_pipeline_update,
)
# qbo_query imported lazily (only when reconciliation is needed) to avoid QBO_REALM_ID requirement
from code_scripts.company_config import load_company_config, get_available_companies
from code_scripts.token_manager import verify_realm_match
from code_scripts.run_lock import hold_global_lock
import pandas as pd
from typing import List

# Load .env file to make environment variables available (shared secrets only)
load_env_file()


def company_dir_name(display_name: str) -> str:
    """
    Convert company display name to Title_Case_With_Underscores.
    Safe for filesystem paths across OSes.
    
    Examples:
        - Akponora Ventures Ltd → Akponora_Ventures_Ltd
        - Precious & Sons Nigeria → Precious_Sons_Nigeria
        - MAIN STORE (HQ) → Main_Store_Hq
    """
    # Remove special characters (anything not alphanumeric or space)
    name = re.sub(r"[^A-Za-z0-9 ]+", " ", str(display_name or "").strip())
    # Collapse multiple spaces
    name = re.sub(r"\s+", " ", name).strip()
    if not name:
        return "Company"
    # Title case each word and join with underscores
    return "_".join(word.capitalize() for word in name.split())


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


def _drop_summary_rows(df: pd.DataFrame, date_col: Optional[str]) -> tuple[pd.DataFrame, int]:
    """
    Drop EPOS summary rows (e.g., Staff == 'Total:') from a raw dataframe.
    Returns (filtered_df, dropped_count).
    """
    if "Staff" not in df.columns:
        return df, 0
    staff = df["Staff"].astype(str).str.strip().str.lower()
    summary_mask = staff.isin(["total:", "total"])
    if date_col and date_col in df.columns:
        date_raw = df[date_col]
        date_empty = date_raw.isna() | (date_raw.astype(str).str.strip() == "")
        summary_mask = summary_mask & date_empty
    dropped = int(summary_mask.sum())
    if dropped:
        df = df.loc[~summary_mask].copy()
    return df, dropped


def _compute_raw_totals(raw_path: Path) -> Optional[dict]:
    """
    Compute totals from a raw EPOS CSV, excluding summary rows.
    Returns dict with totals/rows if TOTAL Sales column exists, else None.
    """
    if not raw_path.exists():
        return None
    df = pd.read_csv(raw_path)
    date_col = "Date/Time" if "Date/Time" in df.columns else ("Date" if "Date" in df.columns else None)
    df, dropped = _drop_summary_rows(df, date_col)
    total_col = None
    for col in ("TOTAL Sales", "Total Sales", "TOTAL", "Total"):
        if col in df.columns:
            total_col = col
            break
    if not total_col:
        return None
    total = pd.to_numeric(df[total_col], errors="coerce").fillna(0).sum()
    net_total = None
    if "NET Sales" in df.columns:
        net_total = pd.to_numeric(df["NET Sales"], errors="coerce").fillna(0).sum()
    return {
        "total": float(total),
        "net_total": float(net_total) if net_total is not None else None,
        "rows": int(len(df)),
        "summary_dropped": int(dropped),
    }


def _compute_processed_totals(processed_path: Path) -> Optional[dict]:
    """
    Compute totals from a processed QBO CSV.
    Returns dict with totals/rows if *ItemAmount exists, else None.
    """
    if not processed_path.exists():
        return None
    df = pd.read_csv(processed_path)
    if "*ItemAmount" not in df.columns:
        return None
    total = pd.to_numeric(df["*ItemAmount"], errors="coerce").fillna(0).sum()
    net_total = None
    if "NET Sales" in df.columns:
        net_total = pd.to_numeric(df["NET Sales"], errors="coerce").fillna(0).sum()
    return {
        "total": float(total),
        "net_total": float(net_total) if net_total is not None else None,
        "rows": int(len(df)),
    }


def _log_raw_vs_processed_totals(raw_file_path: str, config, repo_root: Path) -> None:
    """
    Log a sanity check comparing raw line-item totals to processed totals.
    """
    try:
        raw_path = Path(raw_file_path)
        raw_stats = _compute_raw_totals(raw_path)
        if not raw_stats:
            return

        metadata_path = repo_root / config.metadata_file
        if not metadata_path.exists():
            return
        with open(metadata_path, "r") as f:
            metadata = json.load(f)

        processed_files = metadata.get("processed_files", [])
        if not processed_files:
            return
        processed_file = processed_files[0]

        display_name = getattr(config, "display_name", None) or getattr(config, "company_key", "Company")
        outputs_dir = repo_root / "outputs" / company_dir_name(display_name)
        processed_path = outputs_dir / processed_file
        if not processed_path.exists():
            processed_path = repo_root / processed_file

        processed_stats = _compute_processed_totals(processed_path)
        if not processed_stats:
            return

        diff = raw_stats["total"] - processed_stats["total"]
        logging.info(
            "Totals check: raw_total=%s (rows=%s, dropped_summary=%s) vs processed_total=%s (rows=%s) diff=%s",
            f"{raw_stats['total']:.2f}",
            raw_stats["rows"],
            raw_stats["summary_dropped"],
            f"{processed_stats['total']:.2f}",
            processed_stats["rows"],
            f"{diff:.2f}",
        )
        if raw_stats.get("net_total") is not None and processed_stats.get("net_total") is not None:
            net_diff = raw_stats["net_total"] - processed_stats["net_total"]
            logging.info(
                "Totals check (NET): raw_net=%s vs processed_net=%s diff=%s",
                f"{raw_stats['net_total']:.2f}",
                f"{processed_stats['net_total']:.2f}",
                f"{net_diff:.2f}",
            )
    except Exception as e:
        logging.warning(f"Totals check failed: {e}")


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

def archive_range_raw_files(
    repo_root: Path,
    company_dir: str,
    company_key: str,
    from_date: str,
    to_date: str,
    original_csv_path: Path
) -> None:
    """
    Archive the original range CSV and all split raw files after successful range completion.
    
    Moves:
        uploads/range_raw/<company_dir>/<from>_to_<to>/
    To:
        Uploaded/ranges/<company_dir>/<from>_to_<to>/
    
    Uses human-readable company_dir (Title_Case_With_Underscores) for folder names.
    Falls back to legacy company_key folders if company_dir folders don't exist.
    
    Renames the original CSV to ORIGINAL_<filename>.csv.
    
    This function is only called after ALL per-day loops complete successfully.
    If any per-day step fails, this function is NOT called (range remains atomic).
    
    Args:
        repo_root: Repository root path
        company_dir: Human-readable company folder name (Title_Case_With_Underscores)
        company_key: Company identifier (for legacy fallback)
        from_date: Start date in YYYY-MM-DD format
        to_date: End date in YYYY-MM-DD format
        original_csv_path: Path to the original downloaded CSV file (may be None in skip-download mode, or may not exist if already moved)
    """
    date_range = f"{from_date}_to_{to_date}"
    
    # Primary path using human-readable company_dir
    range_raw_dir = repo_root / "uploads" / "range_raw" / company_dir / date_range
    archive_dir = repo_root / "Uploaded" / "ranges" / company_dir / date_range
    
    # Legacy fallback: check if old company_key folder exists instead
    used_legacy = False
    if not range_raw_dir.exists():
        legacy_range_raw_dir = repo_root / "uploads" / "range_raw" / company_key / date_range
        if legacy_range_raw_dir.exists():
            logging.warning(
                f"Range raw directory not found at '{company_dir}', using legacy path '{company_key}'"
            )
            range_raw_dir = legacy_range_raw_dir
            # Keep archive destination with new naming (company_dir), not legacy
            used_legacy = True
        else:
            logging.warning(f"Range raw directory not found: {range_raw_dir}")
            logging.warning(f"  (Also checked legacy path: {legacy_range_raw_dir})")
            return
    
    # Create archive directory
    archive_dir.mkdir(parents=True, exist_ok=True)
    
    # Move original CSV to archive with ORIGINAL_ prefix (if it still exists in repo root)
    # Skip if original_csv_path is None (skip-download mode)
    if original_csv_path is not None:
        if original_csv_path.exists() and original_csv_path.is_file():
            original_archive_name = f"ORIGINAL_{original_csv_path.name}"
            original_archive_path = archive_dir / original_archive_name
            # Check if already exists (shouldn't happen, but safety check)
            if not original_archive_path.exists():
                shutil.move(str(original_csv_path), str(original_archive_path))
                logging.info(f"Moved original CSV: {original_csv_path.name} -> {original_archive_name}")
            else:
                logging.warning(f"Original CSV already archived: {original_archive_name}")
        else:
            logging.info(f"Original CSV not found in repo root (may have been moved already): {original_csv_path.name}")
    else:
        logging.info("Skip-download mode: no original CSV to archive")
    
    # Move all files from range_raw_dir to archive_dir
    items_moved = 0
    for item in list(range_raw_dir.iterdir()):  # Convert to list to avoid iteration issues
        dest = archive_dir / item.name
        if item.is_file():
            if not dest.exists():  # Safety check: don't overwrite
                shutil.move(str(item), str(dest))
                logging.info(f"Moved split file: {item.name}")
                items_moved += 1
            else:
                logging.warning(f"Split file already exists in archive: {item.name}")
        elif item.is_dir():
            if not dest.exists():
                shutil.move(str(item), str(dest))
                logging.info(f"Moved directory: {item.name}")
                items_moved += 1
            else:
                logging.warning(f"Directory already exists in archive: {item.name}")
    
    # Remove empty range_raw_dir
    try:
        range_raw_dir.rmdir()
        logging.info(f"Removed empty range raw directory: {range_raw_dir}")
    except OSError:
        # Directory not empty or already removed - this is OK
        pass
    
    # Remove empty parent directories if possible (cleanup)
    # Use the actual parent (could be company_dir or company_key if legacy)
    parent_folder = company_key if used_legacy else company_dir
    try:
        parent_dir = repo_root / "uploads" / "range_raw" / parent_folder
        if parent_dir.exists() and not any(parent_dir.iterdir()):
            parent_dir.rmdir()
            logging.info(f"Removed empty parent directory: {parent_dir}")
    except OSError:
        pass
    
    try:
        grandparent_dir = repo_root / "uploads" / "range_raw"
        if grandparent_dir.exists() and not any(grandparent_dir.iterdir()):
            grandparent_dir.rmdir()
            logging.info(f"Removed empty grandparent directory: {grandparent_dir}")
    except OSError:
        pass
    
    logging.info(f"[OK] Archived range raw files to Uploaded/ranges/{company_dir}/{date_range}/")
    logging.info(f"  Moved {items_moved} item(s) from range_raw directory")


def archive_files(repo_root: Path, config) -> None:
    """
    Phase 4: Archive processed files after successful upload.
    Reads metadata file and moves files to Uploaded/<date>/ folder.
    """
    metadata_path = repo_root / config.metadata_file
    
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
            logging.info(f"Moved raw file: {raw_file_path.name} -> Uploaded/{normalized_date}/")
        else:
            logging.warning(f"Raw file not found or is not a file: {raw_file_path}")
    else:
        logging.warning("No raw_file_path or raw_file in metadata, skipping raw file archive")
    
    # Move processed file(s)
    display_name = getattr(config, "display_name", None) or getattr(config, "company_key", "Company")
    company_dir = company_dir_name(display_name)
    outputs_dir = repo_root / "outputs" / company_dir
    processed_files = metadata.get("processed_files", [])
    for processed_file in processed_files:
        if not processed_file or not processed_file.strip():
            logging.warning("Empty processed_file entry in metadata, skipping")
            continue
        
        processed_path = repo_root / processed_file
        if not processed_path.exists():
            processed_path = outputs_dir / processed_file
        
        # Safety check: ensure we're not trying to move the repo root or a directory
        if processed_path == repo_root:
            logging.warning(f"Invalid processed_file path (points to repo root): {processed_file}, skipping")
            continue
        
        if processed_path.exists() and processed_path.is_file():
            dest_processed = archive_dir / processed_file
            shutil.move(str(processed_path), str(dest_processed))
            logging.info(f"Moved processed file: {processed_file} -> Uploaded/{normalized_date}/")
        else:
            logging.warning(f"Processed file not found or is not a file: {processed_file}")
    
    # NOTE: Transformed spill archiving has been removed (Step 3).
    # RAW spill files (uploads/spill_raw/) are now archived separately in run_pipeline.py
    # after each day's processing completes successfully.
    
    # Move metadata file to archive as well
    dest_metadata = archive_dir / config.metadata_file
    shutil.move(str(metadata_path), str(dest_metadata))
    logging.info(f"Moved metadata: {config.metadata_file} -> Uploaded/{normalized_date}/")
    
    logging.info(f"[OK] Phase 4: Archive completed. Files archived to Uploaded/{normalized_date}/")


def merge_raw_csvs(base_csv: Path, extra_csvs: List[Path], out_csv: Path) -> dict:
    """
    Merge multiple raw CSV files into one combined file.
    
    Args:
        base_csv: Primary raw CSV file
        extra_csvs: Additional CSV files to merge (e.g., raw spill files)
        out_csv: Output path for combined CSV
    
    Returns:
        Dict with merge statistics: {base_rows, extra_rows, total_rows}
    """
    df_base = pd.read_csv(base_csv)
    base_rows = len(df_base)
    frames = [df_base]
    extra_rows = 0
    for p in extra_csvs:
        df_extra = pd.read_csv(p)
        frames.append(df_extra)
        extra_rows += len(df_extra)
    df_all = pd.concat(frames, ignore_index=True)
    df_all.to_csv(out_csv, index=False)
    return {
        "base_rows": base_rows,
        "extra_rows": extra_rows,
        "total_rows": len(df_all)
    }


def compute_trading_date(dt_wat: datetime, start_hour: int, start_minute: int) -> datetime.date:
    """
    Compute the trading date for a given datetime in WAT.
    
    Trading day logic:
    - If dt_wat.time() < cutoff (start_hour:start_minute), trading_date = (dt_wat.date() - 1 day)
    - Otherwise, trading_date = dt_wat.date()
    
    Example:
        dt = 2025-01-31 04:59:00, cutoff = 05:00 => trading_date = 2025-01-30
        dt = 2025-01-31 05:00:00, cutoff = 05:00 => trading_date = 2025-01-31
    
    Args:
        dt_wat: Datetime in WAT timezone
        start_hour: Trading day start hour (default: 5)
        start_minute: Trading day start minute (default: 0)
    
    Returns:
        Trading date as a date object
    """
    from datetime import time
    
    cutoff = time(start_hour, start_minute)
    dt_time = dt_wat.time()
    
    if dt_time < cutoff:
        # Before cutoff: belongs to previous trading day
        trading_date = (dt_wat.date() - timedelta(days=1))
    else:
        # At or after cutoff: belongs to current trading day
        trading_date = dt_wat.date()
    
    return trading_date




def split_csv_by_date(
    csv_path: Path,
    from_date: str,
    to_date: str,
    company_dir: str,
    repo_root: Path,
    config=None,
    chunk_size: int = 100000,
    clear_existing: bool = True,
) -> tuple:
    """
    Split a downloaded EPOS CSV into per-day raw files.
    Also writes future out-of-range rows as raw spill files.

    Works for both range mode (from_date != to_date) and single-day mode (from_date == to_date).

    If trading_day_enabled is True in config:
        - Computes trading_date for each row based on datetime in WAT and trading day cutoff
        - Writes rows to uploads/range_raw/<company_dir>/<from>_to_<to>/BookKeeping_<trading_date>.csv
        - Does NOT create spill files in this mode
    If trading_day_enabled is False (default):
        - Uses calendar date (existing behavior)
        - Creates spill files for future dates

    Args:
        csv_path: Path to the downloaded CSV file
        from_date: Start date in YYYY-MM-DD format
        to_date: End date in YYYY-MM-DD format
        company_dir: Human-readable company folder name (Title_Case_With_Underscores)
        repo_root: Repository root path
        config: Optional CompanyConfig object (required if trading_day_enabled is True)
        chunk_size: Read CSV in chunks of this size to reduce memory use

    Returns:
        Tuple of three items:
        - date_to_file: Dict mapping in-range date strings (YYYY-MM-DD) to file paths
        - future_spill_to_file: Dict mapping out-of-range future dates to raw spill file paths (empty if trading_day_enabled)
        - split_stats: Dict with row counts for logging/notifications:
            - total_rows: Total rows in original CSV
            - in_range_rows: Rows written to in-range split files
            - future_rows: Rows written to future spill files (0 if trading_day_enabled)
            - past_rows: Rows for dates before from_date (ignored but logged)
            - null_rows: Rows with unparseable dates (ignored)
            - future_spill_details: Dict {date_str: row_count} for Slack notifications (empty if trading_day_enabled)
    """
    from datetime import timezone, timedelta

    # WAT timezone (UTC+1)
    WAT_TZ = timezone(timedelta(hours=1))

    # Check if trading day mode is enabled
    trading_day_enabled = False
    trading_day_start_hour = 5
    trading_day_start_minute = 0
    if config:
        trading_day_enabled = config.trading_day_enabled
        trading_day_start_hour = config.trading_day_start_hour
        trading_day_start_minute = config.trading_day_start_minute

    if trading_day_enabled:
        logging.info(f"Trading day mode enabled: cutoff={trading_day_start_hour:02d}:{trading_day_start_minute:02d} WAT")

    # Parse date range
    from_dt = datetime.strptime(from_date, "%Y-%m-%d")
    to_dt = datetime.strptime(to_date, "%Y-%m-%d")

    # Generate all dates in range
    date_list = []
    current = from_dt
    while current <= to_dt:
        date_list.append(current.strftime("%Y-%m-%d"))
        current += timedelta(days=1)
    date_set = set(date_list)

    # Parse dates from Date/Time column (fallback to Date)
    def parse_date(value):
        """Parse common date/time strings into a naive datetime."""
        if value is None or (isinstance(value, float) and pd.isna(value)):
            return None
        s = str(value).strip()
        if s == "":
            return None
        for fmt in ("%d/%m/%Y %H:%M:%S", "%Y-%m-%d %H:%M:%S", "%d/%m/%Y", "%Y-%m-%d"):
            try:
                return datetime.strptime(s, fmt)
            except Exception:
                pass
        dt = pd.to_datetime(s, errors="coerce")
        if pd.isna(dt):
            return None
        return dt.to_pydatetime()

    # Convert to WAT timezone and compute trading date or calendar date
    # Also track calendar dates for trading day boundary stats
    def get_date_in_wat(dt):
        try:
            if dt is None or pd.isna(dt):
                return None, None
        except (TypeError, ValueError):
            return None, None
        try:
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=WAT_TZ)
            elif dt.tzinfo != WAT_TZ:
                dt = dt.astimezone(WAT_TZ)

            calendar_date = dt.date()

            if trading_day_enabled:
                # Compute trading date based on cutoff
                trading_date = compute_trading_date(dt, trading_day_start_hour, trading_day_start_minute)
                return trading_date.strftime("%Y-%m-%d"), calendar_date.strftime("%Y-%m-%d")
            else:
                # Use calendar date (existing behavior)
                return calendar_date.strftime("%Y-%m-%d"), None
        except (AttributeError, ValueError, TypeError):
            return None, None

    # Create directory for split files using human-readable company folder name
    split_dir = repo_root / "uploads" / "range_raw" / company_dir / f"{from_date}_to_{to_date}"
    split_dir.mkdir(parents=True, exist_ok=True)

    # Prevent accidental append if a previous run left split files behind
    if clear_existing:
        for date_str in date_list:
            for filename in (
                f"BookKeeping_{company_dir}_{date_str}.csv",
                f"BookKeeping_{date_str}.csv",
                f"CombinedRaw_{company_dir}_{date_str}.csv",
                f"CombinedRaw_{date_str}.csv",
            ):
                existing = split_dir / filename
                if existing.exists() and existing.is_file():
                    existing.unlink()
                    logging.info(f"Removed existing split file before write: {existing.name}")

    # Output tracking
    date_to_file = {}
    future_spill_to_file = {}
    future_spill_details = {}

    in_range_rows_by_date = {}
    future_rows_by_date = {}
    past_dates_set = set()

    total_rows = 0
    in_range_count = 0
    future_count = 0
    past_count = 0
    null_count = 0
    summary_dropped = 0

    split_written = set()
    spill_written = set()

    trading_day_stats = None
    if trading_day_enabled:
        cutoff_str = f"{trading_day_start_hour:02d}:{trading_day_start_minute:02d}"
        trading_day_stats = {
            "cutoff": cutoff_str,
            "by_date": {},
        }

    sample_logged = 0
    sample_limit = 5

    date_col = None
    spill_raw_dir = None

    for chunk in pd.read_csv(csv_path, chunksize=chunk_size):
        if date_col is None:
            date_col = "Date/Time" if "Date/Time" in chunk.columns else "Date"
            if date_col not in chunk.columns:
                raise ValueError("CSV must contain either 'Date/Time' or 'Date' column")

        # Drop summary rows (e.g., Staff == 'Total:') so totals don't double-count
        chunk, dropped = _drop_summary_rows(chunk, date_col)
        summary_dropped += dropped
        if dropped:
            logging.info(f"Dropped {dropped} summary row(s) from raw CSV chunk")

        total_rows += len(chunk)
        dates_series = chunk[date_col].apply(parse_date)

        # Log trading day assignments for debugging (sample a few rows)
        if trading_day_enabled and sample_logged < sample_limit:
            for dt_val in dates_series:
                if dt_val is not None and not pd.isna(dt_val):
                    try:
                        if dt_val.tzinfo is None:
                            dt_wat = dt_val.replace(tzinfo=WAT_TZ)
                        else:
                            dt_wat = dt_val.astimezone(WAT_TZ)
                        trading_date = compute_trading_date(dt_wat, trading_day_start_hour, trading_day_start_minute)
                        logging.info(f"Trading day sample: row_datetime={dt_wat.strftime('%Y-%m-%d %H:%M:%S')} WAT => trading_date={trading_date}")
                        sample_logged += 1
                        if sample_logged >= sample_limit:
                            break
                    except Exception:
                        pass

        date_results = dates_series.apply(get_date_in_wat)
        date_strings = date_results.apply(lambda x: x[0] if isinstance(x, tuple) else x)

        # Extract calendar dates for trading day stats (only when trading_day_enabled)
        calendar_date_strings = None
        if trading_day_enabled:
            calendar_date_strings = date_results.apply(lambda x: x[1] if isinstance(x, tuple) and x[1] else None)

        null_mask = date_strings.isna()
        null_count += int(null_mask.sum())
        valid_mask = ~null_mask

        # Past dates (< from_date) - log but don't save
        past_mask = valid_mask & (date_strings < from_date)
        if past_mask.any():
            past_count += int(past_mask.sum())
            try:
                past_dates_set.update(set(date_strings[past_mask].dropna().unique()))
            except Exception:
                pass

        # In-range dates
        in_range_mask = valid_mask & (date_strings.isin(date_set))
        if in_range_mask.any():
            in_range_dates = date_strings[in_range_mask]
            for date_str, idx in in_range_dates.groupby(in_range_dates).groups.items():
                sub = chunk.loc[idx]
                split_filename = f"BookKeeping_{company_dir}_{date_str}.csv"
                split_path = split_dir / split_filename
                write_header = split_path not in split_written
                sub.to_csv(split_path, index=False, mode="a", header=write_header)
                split_written.add(split_path)
                date_to_file[date_str] = str(split_path)
                count = len(sub)
                in_range_rows_by_date[date_str] = in_range_rows_by_date.get(date_str, 0) + count
                in_range_count += count

                if trading_day_enabled and calendar_date_strings is not None:
                    cal_series = calendar_date_strings.loc[idx]
                    stats = trading_day_stats["by_date"].setdefault(
                        date_str,
                        {"total": 0, "pre_cutoff_reassigned": 0, "same_calendar_day": 0},
                    )
                    stats["total"] += count
                    next_calendar_date = (datetime.strptime(date_str, "%Y-%m-%d").date() + timedelta(days=1)).strftime("%Y-%m-%d")
                    stats["pre_cutoff_reassigned"] += int((cal_series == next_calendar_date).sum())
                    stats["same_calendar_day"] += int((cal_series == date_str).sum())

        # Future out-of-range dates (calendar day mode only)
        if not trading_day_enabled:
            future_mask = valid_mask & (date_strings > to_date)
            if future_mask.any():
                if spill_raw_dir is None:
                    spill_raw_dir = repo_root / "uploads" / "spill_raw" / company_dir
                    spill_raw_dir.mkdir(parents=True, exist_ok=True)
                future_dates = date_strings[future_mask]
                for date_str, idx in future_dates.groupby(future_dates).groups.items():
                    sub = chunk.loc[idx]
                    spill_filename = f"BookKeeping_raw_spill_{date_str}.csv"
                    spill_path = spill_raw_dir / spill_filename
                    write_header = spill_path not in spill_written
                    sub.to_csv(spill_path, index=False, mode="a", header=write_header)
                    spill_written.add(spill_path)
                    future_spill_to_file[date_str] = str(spill_path)
                    count = len(sub)
                    future_rows_by_date[date_str] = future_rows_by_date.get(date_str, 0) + count
                    future_count += count

    # Log summary (no re-reading CSVs needed)
    if summary_dropped > 0:
        logging.info(f"Dropped {summary_dropped} summary row(s) from raw CSV (Staff='Total:')")

    if null_count > 0:
        logging.warning(f"Found {null_count} row(s) with null/unparseable dates (will be ignored)")

    if past_dates_set:
        logging.info(f"Found {past_count} row(s) for past dates (< {from_date}): {', '.join(sorted(past_dates_set))} (ignored)")

    for target_date_str in date_list:
        if target_date_str in in_range_rows_by_date:
            count = in_range_rows_by_date[target_date_str]
            logging.info(
                f"Created split file for {target_date_str}: BookKeeping_{company_dir}_{target_date_str}.csv ({count} rows)"
            )
        else:
            logging.warning(f"No rows found for {target_date_str}, skipping split file")

    if not trading_day_enabled and future_rows_by_date:
        logging.info(f"\nFound {len(future_rows_by_date)} future date(s) outside range (> {to_date})")
        for future_date_str in sorted(future_rows_by_date):
            count = future_rows_by_date[future_date_str]
            logging.info(f"Created raw spill file for {future_date_str}: BookKeeping_raw_spill_{future_date_str}.csv ({count} rows)")

    logging.info(
        f"\nSplit summary: {in_range_count} rows in-range, {future_count} rows future spill, "
        f"{past_count} rows past (ignored), {null_count} rows null (ignored), "
        f"{summary_dropped} summary row(s) dropped"
    )

    # Build stats dict for caller
    split_stats = {
        "total_rows": total_rows,
        "in_range_rows": in_range_count,
        "future_rows": future_count,
        "past_rows": past_count,
        "null_rows": null_count,
        "future_spill_details": {k: v for k, v in future_rows_by_date.items()},
        "summary_rows_dropped": summary_dropped,
    }

    # Compute trading day boundary stats if trading day mode is enabled
    if trading_day_enabled and trading_day_stats is not None:
        for target_date_str, stats in trading_day_stats["by_date"].items():
            if stats.get("pre_cutoff_reassigned", 0) > 0:
                logging.info(
                    f"Trading-day adjustment for {target_date_str}: pre-cutoff reassigned={stats['pre_cutoff_reassigned']} "
                    f"(cutoff={trading_day_stats['cutoff']} WAT)"
                )
        split_stats["trading_day_stats"] = trading_day_stats
    else:
        split_stats["trading_day_stats"] = None

    return date_to_file, future_spill_to_file, split_stats

def reconcile_company(company_key: str, target_date: str, config, repo_root: Path) -> dict:
    """
    Reconcile EPOS totals vs QBO totals for a specific company and date.
    Returns a reconcile dict for inclusion in summary.
    
    Args:
        company_key: Company identifier
        target_date: Target date in YYYY-MM-DD format
        config: CompanyConfig object
        repo_root: Repository root path
    
    Returns:
        Dict with reconcile status, totals, and counts
    """
    reconcile_result = {
        "status": "NOT RUN",
        "reason": "unknown"
    }
    
    try:
        # Determine the processed CSV prefix for this company.
        # CompanyConfig implementations differ across iterations, so we support multiple attribute shapes.
        csv_prefix = None
        try:
            output_cfg = getattr(config, "output_config", None)
            if isinstance(output_cfg, dict):
                csv_prefix = output_cfg.get("csv_prefix")
        except Exception:
            csv_prefix = None

        if not csv_prefix:
            csv_prefix = getattr(config, "csv_prefix", None)

        if not csv_prefix:
            # Fallbacks (historical defaults)
            csv_prefix = "single_sales_receipts"

        # Get EPOS total from processed CSV
        # Check per-company outputs first (before archiving), then repo root, then Uploaded folder (after archiving)
        csv_pattern = f"{csv_prefix}_*.csv"
        csv_files = []
        display_name = getattr(config, "display_name", None) or getattr(config, "company_key", "Company")
        outputs_dir = repo_root / "outputs" / company_dir_name(display_name)

        if outputs_dir.exists():
            csv_files = list(outputs_dir.glob(csv_pattern))

        # Fallback to repo root (legacy location)
        if not csv_files:
            csv_files = list(repo_root.glob(csv_pattern))

        # Fallback to Uploaded folder if not found in repo root
        if not csv_files:
            uploaded_dir = repo_root / "Uploaded" / target_date
            if uploaded_dir.exists():
                csv_files = list(uploaded_dir.glob(csv_pattern))

        if not csv_files:
            reconcile_result["reason"] = "processed CSV file not found"
            return reconcile_result

        # Use most recent CSV
        latest_csv = max(csv_files, key=lambda p: p.stat().st_mtime)
        df = pd.read_csv(latest_csv)

                # Filter by target_date if *SalesReceiptDate column exists
        if "*SalesReceiptDate" in df.columns:
            # Parse *SalesReceiptDate using the company-configured format when possible.
            # Company A often uses YYYY-MM-DD; Company B may use DD/MM/YYYY.
            date_format = getattr(config, "date_format", None)

            # Attempt strict parse first if a format is provided
            if isinstance(date_format, str) and date_format.strip():
                try:
                    df["_sr_date"] = pd.to_datetime(
                        df["*SalesReceiptDate"],
                        format=date_format,
                        errors="coerce",
                    )
                except Exception:
                    df["_sr_date"] = pd.to_datetime(df["*SalesReceiptDate"], errors="coerce")
            else:
                # Heuristic: if values contain '/', prefer dayfirst=True
                sample = df["*SalesReceiptDate"].dropna().astype(str).head(20)
                dayfirst = any("/" in s for s in sample)
                df["_sr_date"] = pd.to_datetime(
                    df["*SalesReceiptDate"],
                    errors="coerce",
                    dayfirst=dayfirst,
                )

            target_dt = pd.to_datetime(target_date, errors="coerce")
            if pd.isna(target_dt):
                reconcile_result["reason"] = f"invalid target_date: {target_date}"
                return reconcile_result

            df = df[df["_sr_date"].dt.date == target_dt.date()].copy()
            df = df.drop(columns=["_sr_date"], errors="ignore")

        # Calculate EPOS totals
        if "*ItemAmount" in df.columns:
            epos_total = float(df["*ItemAmount"].sum())
        else:
            epos_total = 0.0

        epos_count = df["*SalesReceiptNo"].nunique() if "*SalesReceiptNo" in df.columns else 0

        # Get QBO total using scripts/qbo_queries/qbo_query.py
        query_script = repo_root / "scripts" / "qbo_queries" / "qbo_query.py"
        if not query_script.exists():
            reconcile_result["reason"] = "scripts/qbo_queries/qbo_query.py not found"
            return reconcile_result

        # Query QBO for receipts on target_date (get Id and TotalAmt)
        cmd = [
            sys.executable,
            str(query_script),
            "--company", company_key,
            "query",
            f"SELECT Id, TotalAmt FROM SalesReceipt WHERE TxnDate = '{target_date}'"
        ]

        result = subprocess.run(cmd, capture_output=True, text=True, cwd=str(repo_root))

        if result.returncode != 0:
            reconcile_result["reason"] = f"QBO query failed: {result.stderr[:100]}"
            return reconcile_result

        try:
            qbo_data = json.loads(result.stdout)
            receipts = qbo_data.get("QueryResponse", {}).get("SalesReceipt", [])
            if not isinstance(receipts, list):
                receipts = [receipts] if receipts else []

            qbo_total = sum(float(r.get("TotalAmt", 0) or 0) for r in receipts)
            qbo_count = len(receipts)

            # Compare
            difference = abs(qbo_total - epos_total)
            tolerance = 1.0  # Allow ₦1.00 difference for rounding

            if difference <= tolerance:
                status = "MATCH"
            else:
                status = "MISMATCH"

            reconcile_result = {
                "status": status,
                "epos_total": epos_total,
                "epos_count": epos_count,
                "qbo_total": qbo_total,
                "qbo_count": qbo_count,
                "difference": difference
            }

        except (json.JSONDecodeError, KeyError, ValueError) as e:
            reconcile_result["reason"] = f"Failed to parse QBO response: {str(e)[:100]}"
            return reconcile_result

    except Exception as e:
        logging.warning(f"Reconciliation failed: {e}")
        reconcile_result["reason"] = str(e)[:100]
        return reconcile_result

    return reconcile_result


def main(company_key: str, target_date: Optional[str] = None, from_date: Optional[str] = None, to_date: Optional[str] = None, skip_download: bool = False) -> int:
    """
    Full pipeline for a specific company:

    1) epos_playwright.py
       - Logs into EPOS and downloads the latest bookkeeping CSV
         into the repo root directory.

    2) transform.py
       - Reads the latest raw EPOS file from repo root
         and produces a single consolidated QuickBooks-ready CSV
         in repo root (company-specific prefix).
       - Writes metadata to company-specific metadata file

    3) qbo_upload.py
       - Reads the latest CSV from repo root
         and creates Sales Receipts in the QBO company via API.

    4) Archive (run_pipeline.py)
       - After successful upload, reads metadata file
       - Creates Uploaded/<date>/ folder
       - Moves raw CSV, processed CSV(s), and metadata to archive folder
    
    Args:
        company_key: Company identifier ('company_a' or 'company_b') - REQUIRED
        target_date: Target business date in YYYY-MM-DD format. If None, uses yesterday.
        from_date: Start date for range mode in YYYY-MM-DD format (must be used with to_date)
        to_date: End date for range mode in YYYY-MM-DD format (must be used with from_date)
        skip_download: If True, skip EPOS download and use existing split files in uploads/range_raw/ (range mode only)
    """
    # Load company configuration
    try:
        config = load_company_config(company_key)
    except Exception as e:
        logging.error(f"Failed to load company config for '{company_key}': {e}")
        available = get_available_companies()
        if available:
            logging.error(f"Available companies: {', '.join(available)}")
        else:
            logging.error("No company configs found. Please create config files in companies/ directory.")
        raise SystemExit(1)
    
    # Safety check: verify realm_id matches tokens
    try:
        verify_realm_match(company_key, config.realm_id)
    except RuntimeError as e:
        logging.error(f"Realm ID safety check failed: {e}")
        raise SystemExit(1)
    
    # Log company info for safety
    logging.info("=" * 60)
    logging.info(f"COMPANY: {config.display_name} ({company_key})")
    logging.info(f"REALM ID: {config.realm_id}")
    logging.info(f"DEPOSIT ACCOUNT: {config.deposit_account}")
    logging.info(f"TAX MODE: {config.tax_mode}")
    logging.info("=" * 60)
    
    # Determine mode: range mode if both from_date and to_date are provided
    is_range_mode = from_date is not None and to_date is not None
    
    if is_range_mode:
        logging.info(f"Range mode: {from_date} to {to_date}")
        date_range_str = f"{from_date} to {to_date}"
    else:
        # Single-day mode: determine target_date
        if not target_date:
            target_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
            logging.info(f"No target_date provided, using yesterday: {target_date}")
        else:
            logging.info(f"Using provided target_date: {target_date}")
        date_range_str = target_date
    
    pipeline_name = f"{config.display_name} -> QuickBooks Pipeline"

    logging.info(f"Starting {pipeline_name}...\n")
    
    # Send ONE start notification (for range or single day)
    start_metadata = {
        "target_date": target_date if not is_range_mode else None,
        "from_date": from_date if is_range_mode else None,
        "to_date": to_date if is_range_mode else None,
        "company_key": company_key,
        "company_name": config.display_name
    }
    notify_pipeline_start(pipeline_name, log_file, date_range_str, config.slack_webhook_url, start_metadata)

    # Track warnings for watchdog notification (sent ONCE if any warnings occur)
    warnings = []
    watchdog_sent = False

    try:
        if is_range_mode:
            # RANGE MODE: Atomic processing - one download, split, then process per day
            # 
            # Range mode semantics:
            # - One EPOS download for the entire range
            # - One Slack start notification for the range
            # - Per-day processing: transform, upload, reconcile, archive
            # - Per-day completion notifications (NO per-day start notifications)
            # - One final Slack completion notification for the entire range
            # - If any per-day step fails with SystemExit, the entire range stops
            # - Original range CSV and split files are archived only after ALL days complete successfully
            #
            # Spill handling:
            # - Spill files created during day N are written to uploads/spill/
            # - Spill files are automatically merged when processing day N+1 (if date matches)
            # - Spill files are archived only once they are used (via archive_files())
            logging.info("\n=== RANGE MODE: Processing date range ===")
            logging.info("Range mode is atomic: all days must complete successfully for final archival")
            
            # Compute human-readable company folder name for filesystem paths
            company_dir = company_dir_name(config.display_name)
            logging.info(f"Using company folder: {company_dir}")
            
            if skip_download:
                # SKIP DOWNLOAD MODE: Use existing split files from uploads/range_raw/
                logging.info("\n=== SKIP-DOWNLOAD MODE: Using existing split files ===")
                
                # Check for existing split files in range_raw directory
                # First try exact range folder
                split_dir = repo_root / "uploads" / "range_raw" / company_dir / f"{from_date}_to_{to_date}"
                
                # Also check legacy path (company_key instead of company_dir)
                if not split_dir.exists():
                    legacy_split_dir = repo_root / "uploads" / "range_raw" / company_key / f"{from_date}_to_{to_date}"
                    if legacy_split_dir.exists():
                        split_dir = legacy_split_dir
                        logging.info(f"Using legacy path: {split_dir}")
                
                # If exact range folder doesn't exist, search for any range folder that might contain our dates
                if not split_dir.exists():
                    logging.info(f"Exact range folder not found, searching for files in any range folder...")
                    company_range_dir = repo_root / "uploads" / "range_raw" / company_dir
                    if not company_range_dir.exists():
                        company_range_dir = repo_root / "uploads" / "range_raw" / company_key
                    
                    if company_range_dir.exists():
                        # Search all range folders for this company
                        found_dir = None
                        for range_folder in company_range_dir.iterdir():
                            if range_folder.is_dir():
                                # Check if this folder contains any of our requested date files
                                test_file = range_folder / f"BookKeeping_{company_dir}_{from_date}.csv"
                                legacy_test_file = range_folder / f"BookKeeping_{from_date}.csv"
                                if test_file.exists() or legacy_test_file.exists():
                                    found_dir = range_folder
                                    logging.info(f"Found files in range folder: {range_folder.name}")
                                    break
                        
                        if found_dir:
                            split_dir = found_dir
                        else:
                            error_msg = f"[ERROR] Skip-download mode: no split files found for date range {from_date} to {to_date}"
                            logging.error(error_msg)
                            logging.error(f"Searched in: {company_range_dir}")
                            raise SystemExit(error_msg)
                    else:
                        error_msg = f"[ERROR] Skip-download mode: range_raw directory not found for company: {company_range_dir}"
                        logging.error(error_msg)
                        raise SystemExit(error_msg)
                
                # Build date_to_file from existing split files
                date_to_file = {}
                future_spill_to_file = {}
                split_stats = {
                    "total_rows": 0,
                    "in_range_rows": 0,
                    "future_rows": 0,
                    "past_rows": 0,
                    "null_rows": 0,
                    "future_spill_details": {}
                }
                
                # Generate date list for checking
                from_dt = datetime.strptime(from_date, "%Y-%m-%d")
                to_dt = datetime.strptime(to_date, "%Y-%m-%d")
                date_list = []
                current = from_dt
                while current <= to_dt:
                    date_list.append(current.strftime("%Y-%m-%d"))
                    current += timedelta(days=1)
                
                # Look for BookKeeping_<company>_<date>.csv files (fallback to legacy name)
                for day_date in date_list:
                    split_file = split_dir / f"BookKeeping_{company_dir}_{day_date}.csv"
                    legacy_split_file = split_dir / f"BookKeeping_{day_date}.csv"
                    if split_file.exists() or legacy_split_file.exists():
                        if not split_file.exists():
                            split_file = legacy_split_file
                        date_to_file[day_date] = str(split_file)
                        # Count rows for stats
                        try:
                            df = pd.read_csv(split_file)
                            split_stats["in_range_rows"] += len(df)
                            split_stats["total_rows"] += len(df)
                            logging.info(f"Found existing split file for {day_date}: {split_file.name} ({len(df)} rows)")
                        except Exception as e:
                            logging.warning(f"Could not read {split_file.name}: {e}")
                    else:
                        logging.warning(f"No split file found for {day_date}: {split_file.name}")
                
                # Also check for CombinedRaw files (merged with spill)
                for day_date in date_list:
                    combined_file = split_dir / f"CombinedRaw_{company_dir}_{day_date}.csv"
                    legacy_combined_file = split_dir / f"CombinedRaw_{day_date}.csv"
                    if combined_file.exists():
                        # Prefer CombinedRaw over BookKeeping if both exist
                        date_to_file[day_date] = str(combined_file)
                        try:
                            df = pd.read_csv(combined_file)
                            logging.info(f"Found existing combined file for {day_date}: {combined_file.name} ({len(df)} rows)")
                        except Exception as e:
                            logging.warning(f"Could not read {combined_file.name}: {e}")
                    elif legacy_combined_file.exists():
                        date_to_file[day_date] = str(legacy_combined_file)
                        try:
                            df = pd.read_csv(legacy_combined_file)
                            logging.info(f"Found existing combined file for {day_date}: {legacy_combined_file.name} ({len(df)} rows)")
                        except Exception as e:
                            logging.warning(f"Could not read {legacy_combined_file.name}: {e}")
                
                if not date_to_file:
                    error_msg = f"[ERROR] Skip-download mode: no split files found in {split_dir}"
                    logging.error(error_msg)
                    raise SystemExit(error_msg)
                
                logging.info(f"Found {len(date_to_file)} existing split file(s) for date range")
                downloaded_csv = None  # No downloaded CSV in skip-download mode
                
                # Create minimal trading_day_stats with cutoff info (if trading day mode is enabled)
                # We can't compute per-date counts without the original raw CSV, but we can show the cutoff
                trading_day_stats = None
                if config and config.trading_day_enabled:
                    cutoff_str = f"{config.trading_day_start_hour:02d}:{config.trading_day_start_minute:02d}"
                    trading_day_stats = {
                        "cutoff": cutoff_str,
                        "by_date": {}  # Empty - no per-date stats available in skip-download mode
                    }
                    logging.info(f"Trading-day mode enabled (cutoff={cutoff_str} WAT) - stats unavailable in skip-download mode")
                
            else:
                # NORMAL MODE: Download and split
                # Save downloads into a company-specific folder to avoid collisions in parallel runs
                download_dir = repo_root / "downloads" / company_dir
                download_dir.mkdir(parents=True, exist_ok=True)
                download_tag = datetime.now().strftime("%Y%m%d-%H%M%S")
                download_tag = f"{download_tag}-{os.getpid()}"
                output_filename = f"BookKeeping_{company_dir}_{from_date}_to_{to_date}_{download_tag}.csv"
                
                # Phase 1: Download EPOS CSV once for the entire range
                run_step(
                    "Phase 1: Download EPOS CSV (epos_playwright) - Range",
                    "epos_playwright.py",
                    [
                        "--company", company_key,
                        "--from-date", from_date,
                        "--to-date", to_date,
                        "--output-dir", str(download_dir),
                        "--output-filename", output_filename,
                    ]
                )
                
                downloaded_csv = download_dir / output_filename
                if not downloaded_csv.exists():
                    # Fallback to newest CSV in download_dir if the expected filename wasn't created
                    candidates = sorted(download_dir.glob("*.csv"), key=lambda p: p.stat().st_mtime)
                    if not candidates:
                        error_msg = "[ERROR] Range mode: no raw EPOS CSV appeared in downloads folder after download"
                        logging.error(error_msg)
                        raise SystemExit(error_msg)
                    downloaded_csv = candidates[-1]
                    logging.warning(
                        "Range mode: expected download file missing; using newest in downloads folder: %s",
                        downloaded_csv.name,
                    )
                
                logging.info(f"Using raw EPOS file for splitting: {downloaded_csv.name}")
                
                # Split CSV into per-day files
                logging.info("\n=== Splitting CSV into per-day files ===")
                date_to_file, future_spill_to_file, split_stats = split_csv_by_date(
                    downloaded_csv,
                    from_date,
                    to_date,
                    company_dir,
                    repo_root,
                    config,
                    clear_existing=True,
                )
                
                if not date_to_file:
                    raise SystemExit("No data found in date range after splitting")
                
                # Store trading day stats for later use in summaries
                trading_day_stats = split_stats.get("trading_day_stats")
            
            # Add warnings for future raw spills (for Slack notification)
            if future_spill_to_file:
                logging.info(f"Future raw spill files created for: {', '.join(sorted(future_spill_to_file.keys()))}")
                for spill_date, row_count in split_stats.get("future_spill_details", {}).items():
                    warnings.append(f"Future raw spill: {spill_date} ({row_count} rows)")
                
                # Send watchdog notification for future spills (high-signal)
                if not watchdog_sent:
                    logging.info("\n=== Watchdog: Sending update notification (post-split) ===")
                    watchdog_summary = {
                        "from_date": from_date,
                        "to_date": to_date,
                        "company_key": company_key,
                        "company_name": config.display_name,
                        "phase": "Split",
                        "phase_status": "Completed",
                        "warnings": warnings.copy(),
                    }
                    if trading_day_stats:
                        watchdog_summary["trading_day_stats"] = trading_day_stats
                    notify_pipeline_update(pipeline_name, log_file, watchdog_summary, config.slack_webhook_url)
                    watchdog_sent = True
            
            # Set up spill_raw directory for merging existing raw spills
            spill_raw_dir = repo_root / "uploads" / "spill_raw" / company_dir
            
            # Generate date list for iteration
            from_dt = datetime.strptime(from_date, "%Y-%m-%d")
            to_dt = datetime.strptime(to_date, "%Y-%m-%d")
            date_list = []
            current = from_dt
            while current <= to_dt:
                date_list.append(current.strftime("%Y-%m-%d"))
                current += timedelta(days=1)
            
            # Track per-day results for final summary
            per_day_results = []
            
            # Process each day in the range (atomic: any SystemExit stops the range)
            for day_date in date_list:
                if day_date not in date_to_file:
                    logging.warning(f"No data for {day_date}, skipping")
                    continue
                
                logging.info(f"\n{'='*60}")
                logging.info(f"Processing day: {day_date}")
                logging.info(f"{'='*60}")
                
                day_raw_file = date_to_file[day_date]
                used_raw_spill_for_day = []  # Track raw spills used for this day
                
                # Check for existing raw spill file for this day and merge if present
                raw_spill_path = spill_raw_dir / f"BookKeeping_raw_spill_{day_date}.csv"
                raw_file_to_use = day_raw_file
                
                if raw_spill_path.exists():
                    logging.info(f"Found raw spill file for {day_date}: {raw_spill_path.name}")
                    
                    # Merge day_raw_file + raw_spill into a combined file
                    split_dir = Path(day_raw_file).parent
                    combined_path = split_dir / f"CombinedRaw_{company_dir}_{day_date}.csv"
                    
                    merge_stats = merge_raw_csvs(
                        Path(day_raw_file),
                        [raw_spill_path],
                        combined_path
                    )
                    logging.info(f"Merged split ({merge_stats['base_rows']} rows) + raw spill ({merge_stats['extra_rows']} rows) -> final ({merge_stats['total_rows']} rows): {combined_path.name}")
                    
                    raw_file_to_use = str(combined_path)
                    used_raw_spill_for_day.append(raw_spill_path)
                    warnings.append(f"{day_date}: merged target split ({merge_stats['base_rows']} rows) + raw spill ({merge_stats['extra_rows']} rows) -> final ({merge_stats['total_rows']} rows)")
                
                # Phase 2: Transform using raw file (combined or original)
                run_step(
                    f"Phase 2: Transform to single CSV (transform) - {day_date}",
                    "transform.py",
                    ["--company", company_key, "--target-date", day_date, "--raw-file", raw_file_to_use]
                )

                # Sanity check: compare raw line-item totals vs processed totals
                _log_raw_vs_processed_totals(raw_file_to_use, config, repo_root)

                # Check for spill files after Phase 2
                metadata_path = repo_root / config.metadata_file
                if metadata_path.exists():
                    try:
                        with open(metadata_path, "r") as f:
                            phase2_metadata = json.load(f)
                        rows_non_target = phase2_metadata.get("rows_non_target", 0)
                        if rows_non_target > 0:
                            warnings.append(f"{day_date}: {rows_non_target} non-target row(s) ignored by transform")
                    except Exception:
                        pass
                
                # Phase 3: Upload to QBO
                qbo_upload_args = ["--company", company_key]
                if config.trading_day_enabled:
                    qbo_upload_args.extend(["--target-date", day_date])
                run_step(
                    f"Phase 3: Upload to QBO (qbo_upload) - {day_date}",
                    "qbo_upload.py",
                    qbo_upload_args
                )
                
                # Check upload stats after Phase 3
                if metadata_path.exists():
                    try:
                        with open(metadata_path, "r") as f:
                            phase3_metadata = json.load(f)
                        upload_stats = phase3_metadata.get("upload_stats")
                        if upload_stats:
                            skipped = upload_stats.get("skipped", 0)
                            failed = upload_stats.get("failed", 0)
                            if skipped > 0:
                                warnings.append(f"{day_date}: {skipped} duplicate receipt(s) skipped")
                            if failed > 0:
                                warnings.append(f"{day_date}: {failed} upload(s) failed")
                    except Exception:
                        pass
                
                # Phase 4: Reconcile EPOS vs QBO totals
                # Reconciliation mismatches are warnings, not fatal (range continues)
                logging.info(f"\n=== Phase 4: Reconciliation - {day_date} ===")
                reconcile_result = None
                try:
                    reconcile_result = reconcile_company(company_key, day_date, config, repo_root)
                    if reconcile_result.get("status") == "MATCH":
                        logging.info(f"[OK] Reconciliation: MATCH (EPOS: ₦{reconcile_result.get('epos_total', 0):,.2f}, QBO: ₦{reconcile_result.get('qbo_total', 0):,.2f})")
                    elif reconcile_result.get("status") == "MISMATCH":
                        diff = reconcile_result.get("difference", 0)
                        logging.warning(f"[WARN] Reconciliation: MISMATCH (Difference: ₦{diff:,.2f})")
                        warnings.append(f"{day_date}: Reconciliation mismatch (₦{diff:,.2f})")
                    else:
                        reason = reconcile_result.get("reason", "unknown")
                        logging.warning(f"[WARN] Reconciliation: NOT RUN ({reason})")
                        warnings.append(f"{day_date}: Reconciliation not run ({reason})")
                except Exception as e:
                    logging.error(f"[ERROR] Phase 4: Reconciliation failed: {e}")
                    logging.warning("Continuing despite reconciliation failure (upload was successful)")
                    reconcile_result = {"status": "NOT RUN", "reason": str(e)[:100]}
                    warnings.append(f"{day_date}: Reconciliation failed ({str(e)[:50]})")
                
                # Phase 5: Archive files
                logging.info(f"\n=== Phase 5: Archive Files - {day_date} ===")
                try:
                    archive_files(repo_root, config)
                    
                    # Archive used raw spill files after successful archive
                    if used_raw_spill_for_day:
                        # Get the archive directory from the normalized_date
                        archive_dir = repo_root / "Uploaded" / day_date
                        archive_dir.mkdir(parents=True, exist_ok=True)
                        
                        for raw_spill_path in used_raw_spill_for_day:
                            if raw_spill_path.exists() and raw_spill_path.is_file():
                                dest_name = f"RAW_SPILL_{raw_spill_path.name}"
                                dest_path = archive_dir / dest_name
                                shutil.move(str(raw_spill_path), str(dest_path))
                                logging.info(f"Archived used raw spill: {raw_spill_path.name} -> Uploaded/{day_date}/{dest_name}")
                            else:
                                logging.warning(f"Raw spill file not found for archiving: {raw_spill_path}")
                except Exception as e:
                    logging.error(f"[ERROR] Phase 5: Archive failed: {e}")
                    logging.warning("Continuing despite archive failure (upload was successful)")
                
                # Send per-day completion notification (but NOT start notification)
                metadata = None
                if metadata_path.exists():
                    try:
                        with open(metadata_path, "r") as f:
                            metadata = json.load(f)
                    except Exception:
                        pass
                
                summary = {
                    "target_date": day_date,
                    "company_key": company_key,
                    "company_name": config.display_name,
                    "is_range_day": True,
                    "range": f"{from_date} to {to_date}"
                }
                if metadata:
                    summary.update(metadata)
                if reconcile_result:
                    summary["reconcile"] = reconcile_result
                
                # Add trading day stats for this day if available
                if trading_day_stats and day_date in trading_day_stats.get("by_date", {}):
                    summary["trading_day_stats"] = {
                        "cutoff": trading_day_stats["cutoff"],
                        "by_date": {day_date: trading_day_stats["by_date"][day_date]}
                    }
                
                # Store per-day result for final summary
                per_day_results.append({
                    "date": day_date,
                    "reconcile": reconcile_result,
                    "warnings": [w for w in warnings if w.startswith(f"{day_date}:")]
                })
                
                notify_pipeline_success(
                    f"{pipeline_name} - {day_date}",
                    log_file,
                    day_date,
                    summary,
                    config.slack_webhook_url
                )
                logging.info(f"Completed processing for {day_date} ✅")
            
            # All per-day loops completed successfully - archive range raw files
            logging.info("\n" + "="*60)
            logging.info("All days processed successfully - archiving range raw files")
            logging.info("="*60)
            
            try:
                archive_range_raw_files(repo_root, company_dir, company_key, from_date, to_date, downloaded_csv)
            except Exception as e:
                logging.error(f"[ERROR] Failed to archive range raw files: {e}")
                logging.warning("Continuing despite archive failure (all days processed successfully)")
                warnings.append(f"Range archive failed: {str(e)[:50]}")
            
            # Final success notification for the entire range
            logging.info("\n" + "="*60)
            logging.info("Range mode completed successfully ✅")
            logging.info("="*60)
            
            # Build final summary with per-day reconciliation results
            final_summary = {
                "from_date": from_date,
                "to_date": to_date,
                "company_key": company_key,
                "company_name": config.display_name,
                "days_processed": len(date_to_file),
                "per_day_results": per_day_results,
                "warnings": warnings
            }
            if trading_day_stats:
                final_summary["trading_day_stats"] = trading_day_stats
            
            # Compute Range Totals by summing per-day reconciliation results
            included_days = 0
            epos_total_sum = 0.0
            qbo_total_sum = 0.0
            epos_count_sum = 0
            qbo_count_sum = 0
            
            for day_result in per_day_results:
                reconcile = day_result.get("reconcile")
                if reconcile and reconcile.get("status") in ("MATCH", "MISMATCH"):
                    included_days += 1
                    epos_total_sum += reconcile.get("epos_total", 0) or 0
                    qbo_total_sum += reconcile.get("qbo_total", 0) or 0
                    epos_count_sum += reconcile.get("epos_count", 0) or 0
                    qbo_count_sum += reconcile.get("qbo_count", 0) or 0
            
            # Only add range_totals if we have at least one day with reconciliation
            if included_days > 0:
                difference_sum = round(epos_total_sum - qbo_total_sum, 2)
                final_summary["range_totals"] = {
                    "included_days": included_days,
                    "total_days": len(date_to_file),
                    "epos_total": round(epos_total_sum, 2),
                    "qbo_total": round(qbo_total_sum, 2),
                    "epos_count": epos_count_sum,
                    "qbo_count": qbo_count_sum,
                    "difference": difference_sum,
                }
            
            notify_pipeline_success(pipeline_name, log_file, date_range_str, final_summary, config.slack_webhook_url)
            return 0
        
        else:
            # SINGLE-DAY MODE: Now uses same deterministic raw file selection as range mode
            # This ensures consistency and prevents data loss from future rows
            logging.info("\n=== SINGLE-DAY MODE: Processing target date ===")
            
            # Compute human-readable company folder name (same as range mode)
            company_dir = company_dir_name(config.display_name)
            logging.info(f"Using company folder: {company_dir}")
            
            # Download into a company-specific folder to avoid collisions in parallel runs
            download_dir = repo_root / "downloads" / company_dir
            download_dir.mkdir(parents=True, exist_ok=True)
            download_tag = datetime.now().strftime("%Y%m%d-%H%M%S")
            download_tag = f"{download_tag}-{os.getpid()}"
            output_filename = f"BookKeeping_{company_dir}_{target_date}_{download_tag}.csv"
            
            # Phase 1: Download from EPOS with target_date and company config
            run_step(
                "Phase 1: Download EPOS CSV (epos_playwright)",
                "epos_playwright.py",
                [
                    "--company", company_key,
                    "--target-date", target_date,
                    "--output-dir", str(download_dir),
                    "--output-filename", output_filename,
                ]
            )
            
            downloaded_csv = download_dir / output_filename
            if not downloaded_csv.exists():
                # Fallback to newest CSV in download_dir if the expected filename wasn't created
                candidates = sorted(download_dir.glob("*.csv"), key=lambda p: p.stat().st_mtime)
                if not candidates:
                    error_msg = "[ERROR] Single-day mode: no raw EPOS CSV appeared in downloads folder after download"
                    logging.error(error_msg)
                    raise SystemExit(error_msg)
                downloaded_csv = candidates[-1]
                logging.warning(
                    "Single-day mode: expected download file missing; using newest in downloads folder: %s",
                    downloaded_csv.name,
                )
            
            logging.info(f"Using raw EPOS file for splitting: {downloaded_csv.name}")
            
            # Split CSV into per-day files (same mechanism as range mode)
            # For single-day, from_date == to_date
            logging.info("\n=== Splitting CSV by date ===")
            date_to_file, future_spill_to_file, split_stats = split_csv_by_date(
                downloaded_csv,
                target_date,
                target_date,
                company_dir,
                repo_root,
                config,
                clear_existing=True,
            )
            
            # Store trading day stats for later use in summaries
            trading_day_stats = split_stats.get("trading_day_stats")
            
            # Check if we have data for the target date
            if target_date not in date_to_file:
                error_msg = f"[ERROR] No rows found for target_date {target_date} after splitting; abort."
                logging.error(error_msg)
                raise SystemExit(error_msg)
            
            day_raw_file = date_to_file[target_date]
            used_raw_spill_for_day = []  # Track raw spills used for this day
            
            # Add warnings for future raw spills (for Slack notification)
            if future_spill_to_file:
                logging.info(f"Future raw spill files created for: {', '.join(sorted(future_spill_to_file.keys()))}")
                for spill_date, row_count in split_stats.get("future_spill_details", {}).items():
                    warnings.append(f"Future raw spill: {spill_date} ({row_count} rows)")
            
            # Set up spill_raw directory for merging existing raw spills
            spill_raw_dir = repo_root / "uploads" / "spill_raw" / company_dir
            
            # Check for existing raw spill file for target_date and merge if present
            raw_spill_path = spill_raw_dir / f"BookKeeping_raw_spill_{target_date}.csv"
            raw_file_to_use = day_raw_file
            
            if raw_spill_path.exists():
                logging.info(f"Found raw spill file for {target_date}: {raw_spill_path.name}")
                
                # Merge day_raw_file + raw_spill into a combined file
                split_dir = Path(day_raw_file).parent
                combined_path = split_dir / f"CombinedRaw_{target_date}.csv"
                
                merge_stats = merge_raw_csvs(
                    Path(day_raw_file),
                    [raw_spill_path],
                    combined_path
                )
                logging.info(f"Merged split ({merge_stats['base_rows']} rows) + raw spill ({merge_stats['extra_rows']} rows) -> final ({merge_stats['total_rows']} rows): {combined_path.name}")
                
                raw_file_to_use = str(combined_path)
                used_raw_spill_for_day.append(raw_spill_path)
                warnings.append(f"{target_date}: merged target split ({merge_stats['base_rows']} rows) + raw spill ({merge_stats['extra_rows']} rows) -> final ({merge_stats['total_rows']} rows)")
            
            # Send watchdog notification if we have future spills or merged spills (high-signal only)
            if (future_spill_to_file or used_raw_spill_for_day) and not watchdog_sent:
                logging.info("\n=== Watchdog: Sending update notification (post-split) ===")
                watchdog_summary = {
                    "target_date": target_date,
                    "company_key": company_key,
                    "company_name": config.display_name,
                    "phase": "Split/Merge",
                    "phase_status": "Completed",
                    "warnings": warnings.copy(),  # Copy current warnings
                }
                if trading_day_stats:
                    watchdog_summary["trading_day_stats"] = trading_day_stats
                notify_pipeline_update(pipeline_name, log_file, watchdog_summary, config.slack_webhook_url)
                watchdog_sent = True
            
            # Phase 2: Transform using raw file (combined or original)
            run_step(
                "Phase 2: Transform to single CSV (transform)",
                "transform.py",
                ["--company", company_key, "--target-date", target_date, "--raw-file", raw_file_to_use]
            )

            # Sanity check: compare raw line-item totals vs processed totals
            _log_raw_vs_processed_totals(raw_file_to_use, config, repo_root)
            
            # Check for spill files after Phase 2 (transformed spill - still logged but we don't rely on it)
            metadata_path = repo_root / config.metadata_file
            if metadata_path.exists():
                try:
                    with open(metadata_path, "r") as f:
                        phase2_metadata = json.load(f)
                    rows_non_target = phase2_metadata.get("rows_non_target", 0)
                    if rows_non_target > 0:
                        warnings.append(f"{rows_non_target} non-target row(s) ignored by transform")
                except Exception:
                    pass  # Ignore metadata read errors

            # Phase 3: Upload to QBO
            qbo_upload_args = ["--company", company_key]
            if config.trading_day_enabled:
                qbo_upload_args.extend(["--target-date", target_date])
            run_step(
                "Phase 3: Upload to QBO (qbo_upload)",
                "qbo_upload.py",
                qbo_upload_args
            )
            
            # Check upload stats after Phase 3 for partial failures
            if metadata_path.exists():
                try:
                    with open(metadata_path, "r") as f:
                        phase3_metadata = json.load(f)
                    upload_stats = phase3_metadata.get("upload_stats")
                    if upload_stats:
                        skipped = upload_stats.get("skipped", 0)
                        failed = upload_stats.get("failed", 0)
                        if skipped > 0:
                            warnings.append(f"{skipped} duplicate receipt(s) skipped")
                        if failed > 0:
                            warnings.append(f"{failed} upload(s) failed (continuing)")
                except Exception:
                    pass  # Ignore metadata read errors

            # Phase 4: Reconcile EPOS vs QBO totals (BEFORE archiving so CSV is still in repo root)
            logging.info("\n=== Phase 4: Reconciliation ===")
            reconcile_result = None
            try:
                reconcile_result = reconcile_company(company_key, target_date, config, repo_root)
                if reconcile_result.get("status") == "MATCH":
                    logging.info(f"[OK] Reconciliation: MATCH (EPOS: ₦{reconcile_result.get('epos_total', 0):,.2f}, QBO: ₦{reconcile_result.get('qbo_total', 0):,.2f})")
                elif reconcile_result.get("status") == "MISMATCH":
                    diff = reconcile_result.get("difference", 0)
                    logging.warning(f"[WARN] Reconciliation: MISMATCH (Difference: ₦{diff:,.2f})")
                else:
                    reason = reconcile_result.get("reason", "unknown")
                    logging.warning(f"[WARN] Reconciliation: NOT RUN ({reason})")
            except Exception as e:
                logging.error(f"[ERROR] Phase 4: Reconciliation failed: {e}")
                logging.warning("Continuing despite reconciliation failure (upload was successful)")
                reconcile_result = {"status": "NOT RUN", "reason": str(e)[:100]}

            # Phase 5: Archive files after successful upload and reconciliation
            logging.info("\n=== Phase 5: Archive Files ===")
            try:
                archive_files(repo_root, config)
                
                # Archive used raw spill files after successful archive
                if used_raw_spill_for_day:
                    # Get the archive directory for this date
                    archive_dir = repo_root / "Uploaded" / target_date
                    archive_dir.mkdir(parents=True, exist_ok=True)
                    
                    for raw_spill_path in used_raw_spill_for_day:
                        if raw_spill_path.exists() and raw_spill_path.is_file():
                            dest_name = f"RAW_SPILL_{raw_spill_path.name}"
                            dest_path = archive_dir / dest_name
                            shutil.move(str(raw_spill_path), str(dest_path))
                            logging.info(f"Archived used raw spill: {raw_spill_path.name} -> Uploaded/{target_date}/{dest_name}")
                        else:
                            logging.warning(f"Raw spill file not found for archiving: {raw_spill_path}")
            except Exception as e:
                logging.error(f"[ERROR] Phase 5: Archive failed: {e}")
                # Don't fail the pipeline if archiving fails - upload already succeeded
                logging.warning("Continuing despite archive failure (upload was successful)")
            
            # Single-day mode cleanup: remove scratch split directory after successful archive
            # Move any remaining files to Uploaded/<target_date>/ before deleting
            split_dir = repo_root / "uploads" / "range_raw" / company_dir / f"{target_date}_to_{target_date}"
            if split_dir.exists():
                logging.info(f"\n=== Cleaning up single-day split directory ===")
                archive_dir = repo_root / "Uploaded" / target_date
                archive_dir.mkdir(parents=True, exist_ok=True)
                
                # Move remaining .csv files with appropriate prefixes
                remaining_files = list(split_dir.glob("*.csv"))
                for csv_file in remaining_files:
                    # Determine prefix based on filename pattern
                    if csv_file.name.startswith("BookKeeping_") and not csv_file.name.startswith("BookKeeping_raw_spill_"):
                        prefix = "RAW_SPLIT_"
                    elif csv_file.name.startswith("CombinedRaw_"):
                        prefix = "RAW_COMBINED_"
                    else:
                        prefix = "RAW_INPUT_"
                    
                    dest_name = f"{prefix}{csv_file.name}"
                    dest_path = archive_dir / dest_name
                    
                    # Don't overwrite if file already exists in archive
                    if not dest_path.exists():
                        shutil.move(str(csv_file), str(dest_path))
                        logging.info(f"Moved remaining split file: {csv_file.name} -> Uploaded/{target_date}/{dest_name}")
                    else:
                        logging.info(f"Skipped (already archived): {csv_file.name}")
                        # Remove the duplicate from split dir
                        csv_file.unlink()
                
                # Remove split_dir if now empty
                try:
                    if split_dir.exists() and not any(split_dir.iterdir()):
                        split_dir.rmdir()
                        logging.info(f"Removed empty split directory: {split_dir.name}")
                except OSError as e:
                    logging.warning(f"Could not remove split directory: {e}")
                
                # Attempt to clean up parent directories if empty
                try:
                    company_range_dir = repo_root / "uploads" / "range_raw" / company_dir
                    if company_range_dir.exists() and not any(company_range_dir.iterdir()):
                        company_range_dir.rmdir()
                        logging.info(f"Removed empty company range directory: {company_range_dir.name}")
                except OSError:
                    pass  # Directory not empty or other error
                
                try:
                    range_raw_dir = repo_root / "uploads" / "range_raw"
                    if range_raw_dir.exists() and not any(range_raw_dir.iterdir()):
                        range_raw_dir.rmdir()
                        logging.info(f"Removed empty range_raw directory")
                except OSError:
                    pass  # Directory not empty or other error
                
                logging.info("[OK] Single-day split directory cleanup complete")
            
            # Archive the original downloaded EPOS CSV from repo root
            if downloaded_csv.exists() and downloaded_csv.is_file():
                archive_dir = repo_root / "Uploaded" / target_date
                archive_dir.mkdir(parents=True, exist_ok=True)
                original_dest = archive_dir / f"ORIGINAL_{downloaded_csv.name}"
                
                if original_dest.exists():
                    # Don't overwrite - suffix with timestamp
                    timestamp = datetime.now().strftime("%H%M%S")
                    original_dest = archive_dir / f"ORIGINAL_{downloaded_csv.stem}_{timestamp}{downloaded_csv.suffix}"
                    logging.warning(f"Archive destination exists, using timestamped name: {original_dest.name}")
                
                shutil.move(str(downloaded_csv), str(original_dest))
                logging.info(f"Archived original EPOS CSV: {downloaded_csv.name} -> Uploaded/{target_date}/{original_dest.name}")
            else:
                logging.warning(f"Original downloaded CSV not found for archival: {downloaded_csv}")

            # Success notification - load metadata for summary
            metadata = None
            metadata_path = repo_root / config.metadata_file
            if metadata_path.exists():
                try:
                    with open(metadata_path, "r") as f:
                        metadata = json.load(f)
                except Exception as e:
                    logging.warning(f"Could not load metadata for notification: {e}")
            
            # Build summary with company info and metadata
            summary = {
                "target_date": target_date,
                "company_key": company_key,
                "company_name": config.display_name
            }
            if metadata:
                summary.update(metadata)
            
            # Add reconciliation results to summary
            if reconcile_result:
                summary["reconcile"] = reconcile_result
            
            # Add trading day stats if available
            if trading_day_stats and target_date in trading_day_stats.get("by_date", {}):
                summary["trading_day_stats"] = {
                    "cutoff": trading_day_stats["cutoff"],
                    "by_date": {target_date: trading_day_stats["by_date"][target_date]}
                }
            
            notify_pipeline_success(pipeline_name, log_file, date_range_str, summary, config.slack_webhook_url)
            logging.info("\nPipeline completed successfully ✅")
            return 0

    except SystemExit as e:
        logging.error("Pipeline failed", exc_info=True)
        
        # In range mode, if we fail during per-day processing, do NOT archive range files
        if is_range_mode:
            logging.warning("Range mode failed - range raw files will NOT be archived")
        
        # Try to load metadata with upload stats for better error reporting
        metadata = None
        metadata_path = repo_root / config.metadata_file
        if metadata_path.exists():
            try:
                with open(metadata_path, "r") as f:
                    metadata = json.load(f)
            except Exception:
                pass
        
        # Build summary with company info and metadata
        summary = {
            "target_date": target_date if not is_range_mode else None,
            "from_date": from_date if is_range_mode else None,
            "to_date": to_date if is_range_mode else None,
            "company_key": company_key,
            "company_name": config.display_name
        }
        if metadata:
            summary.update(metadata)
        
        notify_pipeline_failure(pipeline_name, log_file, str(e), date_range_str, config.slack_webhook_url, summary)
        return 1
    except Exception as e:
        logging.error("Pipeline failed with unexpected error", exc_info=True)
        
        # In range mode, if we fail during per-day processing, do NOT archive range files
        if is_range_mode:
            logging.warning("Range mode failed - range raw files will NOT be archived")
        
        # Try to load metadata with upload stats for better error reporting
        metadata = None
        metadata_path = repo_root / config.metadata_file
        if metadata_path.exists():
            try:
                with open(metadata_path, "r") as f:
                    metadata = json.load(f)
            except Exception:
                pass
        
        # Build summary with company info and metadata
        summary = {
            "target_date": target_date if not is_range_mode else None,
            "from_date": from_date if is_range_mode else None,
            "to_date": to_date if is_range_mode else None,
            "company_key": company_key,
            "company_name": config.display_name
        }
        if metadata:
            summary.update(metadata)
        
        notify_pipeline_failure(pipeline_name, log_file, str(e), date_range_str, config.slack_webhook_url, summary)
        return 1


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Run EPOS -> QuickBooks pipeline for target business date or date range.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Single day (default)
  python3 run_pipeline.py --company company_a --target-date 2025-12-25
  python3 run_pipeline.py --company company_b  # Uses yesterday as target-date
  
  # Date range
  python3 run_pipeline.py --company company_b --from-date 2025-12-02 --to-date 2025-12-04
        """
    )
    parser.add_argument(
        "--company",
        required=True,
        choices=get_available_companies(),
        help="Company identifier (REQUIRED). Available: %(choices)s",
    )
    parser.add_argument(
        "--target-date",
        help="Target business date in YYYY-MM-DD format (default: yesterday, ignored if --from-date and --to-date are provided)",
    )
    parser.add_argument(
        "--from-date",
        help="Start date for range mode in YYYY-MM-DD format (must be used with --to-date)",
    )
    parser.add_argument(
        "--to-date",
        help="End date for range mode in YYYY-MM-DD format (must be used with --from-date)",
    )
    parser.add_argument(
        "--skip-download",
        action="store_true",
        help="Skip EPOS download and use existing split files in uploads/range_raw/ (range mode only)",
    )
    args = parser.parse_args()
    
    if not args.company:
        parser.error("--company is REQUIRED. Available companies: " + ", ".join(get_available_companies()))
    
    # Validation: --from-date and --to-date must be provided together
    if (args.from_date is None) != (args.to_date is None):
        parser.error("--from-date and --to-date must be provided together")
    
    # Validation: --skip-download only works in range mode
    if args.skip_download and (args.from_date is None or args.to_date is None):
        parser.error("--skip-download can only be used with --from-date and --to-date (range mode)")
    
    date_bits = [args.target_date or "", args.from_date or "", args.to_date or ""]
    lock_holder = f"run_pipeline:{args.company}:{':'.join(date_bits)}"
    with hold_global_lock(lock_holder) as lock:
        if not lock.acquired:
            print(f"[LOCK] Run blocked: {lock.reason}")
            raise SystemExit(2)
        raise SystemExit(main(args.company, args.target_date, args.from_date, args.to_date, args.skip_download))
