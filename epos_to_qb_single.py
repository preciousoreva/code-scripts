import os
import glob
import json
import sys
from datetime import datetime, timezone, timedelta
from typing import Optional, Tuple, List
import pandas as pd

from sales_recepit_script import TransformOptions, transform_dataframe, parse_date


def get_repo_root() -> str:
    """Return the directory where this script lives (code-scripts)."""
    return os.path.dirname(os.path.abspath(__file__))


def find_latest_raw_file(repo_root: str) -> str:
    """Find the most recently modified CSV in repo root (excluding processed files)."""
    pattern = os.path.join(repo_root, "*.csv")
    files = glob.glob(pattern)
    
    # Exclude processed files (those starting with "single_sales_receipts_")
    files = [f for f in files if not os.path.basename(f).startswith("single_sales_receipts_")]
    
    if not files:
        raise FileNotFoundError(f"No raw CSV files found in {repo_root}")
    return max(files, key=os.path.getmtime)


# WAT timezone (UTC+1)
WAT_TZ = timezone(timedelta(hours=1))


def get_target_date_from_args() -> Optional[str]:
    """Get target_date from command line args or environment variable."""
    # Check command line args
    if "--target-date" in sys.argv:
        idx = sys.argv.index("--target-date")
        if idx + 1 < len(sys.argv):
            return sys.argv[idx + 1]
    
    # Check environment variable
    target_date = os.environ.get("TARGET_DATE")
    if target_date:
        return target_date
    
    return None


def filter_rows_by_target_date(
    df: pd.DataFrame,
    target_date: str,
    raw_file: str
) -> Tuple[pd.DataFrame, pd.DataFrame, dict]:
    """
    Filter rows by target date and separate spillover rows.
    
    Returns:
        (target_rows, spillover_rows, stats_dict)
    """
    target_dt = datetime.strptime(target_date, "%Y-%m-%d")
    target_date_str = target_dt.strftime("%Y-%m-%d")
    target_prefix = f"SR-{target_dt.strftime('%Y%m%d')}-"
    
    # Parse dates from Date/Time column
    dates_series = df["Date/Time"].apply(parse_date) if "Date/Time" in df.columns else pd.Series([None] * len(df))
    
    # Convert to WAT timezone and extract date portion
    def get_date_in_wat(dt) -> Optional[str]:
        # Handle None, NaT, or other missing values - check before any operations
        try:
            if dt is None or pd.isna(dt):
                return None
        except (TypeError, ValueError):
            return None
        
        # Handle pandas Timestamp objects and datetime objects
        try:
            # If datetime is naive, assume it's already in WAT
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=WAT_TZ)
            # Convert to WAT if needed
            elif dt.tzinfo != WAT_TZ:
                dt = dt.astimezone(WAT_TZ)
            return dt.date().strftime("%Y-%m-%d")
        except (AttributeError, ValueError, TypeError):
            return None
    
    date_strings = dates_series.apply(get_date_in_wat)
    
    # Filter: keep rows where date matches target_date
    target_mask = date_strings == target_date_str
    target_rows = df[target_mask].copy().reset_index(drop=True)
    spillover_rows = df[~target_mask].copy().reset_index(drop=True)
    
    # Collect statistics - filter out None/NaN before computing min/max
    date_strings_clean = date_strings.dropna()
    dates_present_list = sorted(date_strings_clean.unique().tolist()) if len(date_strings_clean) > 0 else []
    
    # Debug: show date distribution
    date_counts = date_strings.value_counts()
    print(f"\nDate distribution in CSV:")
    for date_val, count in date_counts.items():
        marker = " <-- TARGET" if date_val == target_date_str else ""
        print(f"  {date_val}: {count} rows{marker}")
    
    stats = {
        "rows_total": len(df),
        "rows_kept": len(target_rows),
        "rows_spilled": len(spillover_rows),
        "dates_present": dates_present_list,
        "min_dt": dates_present_list[0] if dates_present_list else None,
        "max_dt": dates_present_list[-1] if dates_present_list else None,
    }
    
    print(f"\nFiltering by target date: {target_date}")
    print(f"  Total rows: {stats['rows_total']}")
    print(f"  Rows kept (target date): {stats['rows_kept']}")
    print(f"  Rows spilled (other dates): {stats['rows_spilled']}")
    print(f"  Dates present in CSV: {', '.join(stats['dates_present'])}")
    
    if len(spillover_rows) > 0:
        spillover_dates = date_strings[~target_mask].value_counts()
        print(f"\nSpillover date breakdown:")
        for date_val, count in spillover_dates.items():
            print(f"  {date_val}: {count} rows")
    
    return target_rows, spillover_rows, stats


def write_spillover_files(
    spillover_transformed: pd.DataFrame,
    repo_root: str,
    raw_file: str,
    stats: dict
) -> List[str]:
    """
    Write transformed spillover rows to separate CSV files grouped by date.
    Returns list of spill file paths (relative to repo_root).
    """
    if len(spillover_transformed) == 0:
        return []
    
    # Group by *SalesReceiptDate (which is the date string in YYYY-MM-DD format)
    if "*SalesReceiptDate" not in spillover_transformed.columns:
        print("[WARN] Cannot group spillover by date: *SalesReceiptDate column missing")
        return []
    
    # Create uploads/spill directory
    spill_dir = os.path.join(repo_root, "uploads", "spill")
    os.makedirs(spill_dir, exist_ok=True)
    
    spill_files = []
    
    # Group by date and write separate files
    for spill_date, group in spillover_transformed.groupby("*SalesReceiptDate", dropna=False):
        if pd.isna(spill_date):
            spill_date_str = "unknown"
        else:
            spill_date_str = str(spill_date)
        
        spill_filename = f"BookKeeping_spill_{spill_date_str}.csv"
        spill_path = os.path.join(spill_dir, spill_filename)
        
        group.to_csv(spill_path, index=False)
        # Store relative path from repo_root
        spill_files.append(f"uploads/spill/{spill_filename}")
        print(f"  Wrote spillover file: {spill_filename} ({len(group)} rows for date {spill_date_str})")
    
    return spill_files


def extract_date_from_dataframe(df: pd.DataFrame) -> str:
    """Extract and normalize the date from the transformed dataframe."""
    # Get the first non-null date from *SalesReceiptDate column
    if "*SalesReceiptDate" in df.columns:
        dates = df["*SalesReceiptDate"].dropna()
        if len(dates) > 0:
            # Parse the date string (format is %Y-%m-%d)
            try:
                date_obj = datetime.strptime(dates.iloc[0], "%Y-%m-%d")
                return date_obj.strftime("%Y-%m-%d")
            except ValueError:
                pass
    
    # Fallback: use current date
    return datetime.now().strftime("%Y-%m-%d")


def find_spill_files_for_date(repo_root: str, target_date: str) -> List[str]:
    """Find existing spill files for the target date."""
    spill_dir = os.path.join(repo_root, "uploads", "spill")
    if not os.path.exists(spill_dir):
        return []
    
    spill_files = []
    spill_filename_pattern = f"BookKeeping_spill_{target_date}.csv"
    spill_path = os.path.join(spill_dir, spill_filename_pattern)
    
    if os.path.exists(spill_path):
        spill_files.append(spill_path)
        print(f"Found existing spill file for {target_date}: {spill_filename_pattern}")
    
    return spill_files


def main():
    repo_root = get_repo_root()

    # 1) Get target_date from args or env
    target_date = get_target_date_from_args()
    if not target_date:
        print("Warning: No --target-date provided. Processing all rows without filtering.")
        print("  Usage: python epos_to_qb_single.py --target-date YYYY-MM-DD")
        print("  Or set TARGET_DATE environment variable")

    # 2) Pick latest raw BookKeeping CSV from repo root
    raw_file = find_latest_raw_file(repo_root)
    print(f"Using raw file: {raw_file}")

    # 3) Load raw CSV
    df = pd.read_csv(raw_file)

    # 4) Filter by target_date if provided
    spillover_rows = pd.DataFrame()
    stats = {
        "rows_total": len(df),
        "rows_kept": len(df),
        "rows_spilled": 0,
        "dates_present": [],
        "min_dt": None,
        "max_dt": None,
    }
    
    if target_date:
        target_df, spillover_rows, stats = filter_rows_by_target_date(df, target_date, raw_file)
        df = target_df  # Use filtered dataframe for transformation
        
        if len(df) == 0:
            raise ValueError(f"No rows found for target date {target_date}. Cannot proceed with empty dataset.")
    else:
        # If no target_date, analyze dates present
        dates_series = df["Date/Time"].apply(parse_date) if "Date/Time" in df.columns else pd.Series([None] * len(df))
        def get_date_in_wat(dt) -> Optional[str]:
            # Handle None, NaT, or other missing values - check before any operations
            try:
                if dt is None or pd.isna(dt):
                    return None
            except (TypeError, ValueError):
                return None
            
            # Handle pandas Timestamp objects and datetime objects
            try:
                # If datetime is naive, assume it's already in WAT
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=WAT_TZ)
                # Convert to WAT if needed
                elif dt.tzinfo != WAT_TZ:
                    dt = dt.astimezone(WAT_TZ)
                return dt.date().strftime("%Y-%m-%d")
            except (AttributeError, ValueError, TypeError):
                return None
        date_strings = dates_series.apply(get_date_in_wat)
        date_strings_clean = date_strings.dropna()
        dates_present_list = sorted(date_strings_clean.unique().tolist()) if len(date_strings_clean) > 0 else []
        stats["dates_present"] = dates_present_list
        stats["min_dt"] = dates_present_list[0] if dates_present_list else None
        stats["max_dt"] = dates_present_list[-1] if dates_present_list else None

    # 5) Transform using the existing logic (but no splitting)
    opts = TransformOptions(
        deposit_account="100900 - Undeposited Funds",
        date_format="%Y-%m-%d",
        prefix="SR",
        start_seq=1,
        override_tax_code=None,
        on_missing_date="skip",
        default_date=None,
    )

    transformed = transform_dataframe(df, opts)

    # 6) Check for existing spill files for target_date and merge them (after transformation)
    used_spill_files = []  # Track which spill files were used
    if target_date:
        existing_spill_files = find_spill_files_for_date(repo_root, target_date)
        if existing_spill_files:
            print(f"\nFound {len(existing_spill_files)} existing spill file(s) for target date {target_date}, merging...")
            for spill_file in existing_spill_files:
                try:
                    spill_df = pd.read_csv(spill_file)
                    print(f"  Merging {len(spill_df)} rows from {os.path.basename(spill_file)}")
                    # Concatenate spill data (already transformed) with transformed data
                    transformed = pd.concat([transformed, spill_df], ignore_index=True)
                    # Track this spill file as used (store relative path)
                    spill_rel_path = os.path.relpath(spill_file, repo_root)
                    used_spill_files.append(spill_rel_path)
                except Exception as e:
                    print(f"  Warning: Failed to load spill file {spill_file}: {e}")

    # 7) Process spillover rows if any
    spill_files = []
    if len(spillover_rows) > 0:
        print("\nProcessing spillover rows...")
        # Transform spillover rows first, then write them
        spillover_transformed = transform_dataframe(spillover_rows, opts)
        spill_files = write_spillover_files(spillover_transformed, repo_root, raw_file, stats)
        print(f"Created {len(spill_files)} spill file(s)")
    else:
        print("No spillover rows detected.")

    # 7) Extract normalized date for archiving (use target_date if provided, otherwise first row date)
    if target_date:
        normalized_date = target_date
    else:
        normalized_date = extract_date_from_dataframe(transformed)

    # 8) Write ONE QuickBooks-ready CSV in repo root
    base_name = os.path.splitext(os.path.basename(raw_file))[0]
    output_filename = f"single_sales_receipts_{base_name}.csv"
    output_path = os.path.join(repo_root, output_filename)

    transformed.to_csv(output_path, index=False)
    print(f"\nWrote combined QuickBooks file: {output_path}")
    print(f"Rows (including header): {len(transformed) + 1}")

    # 9) Write metadata file for archiving
    metadata = {
        "raw_file": os.path.basename(raw_file),
        "raw_file_path": raw_file,
        "processed_files": [output_filename],
        "normalized_date": normalized_date,
        "target_date": target_date,
        "dates_present": stats["dates_present"],
        "rows_total": stats["rows_total"],
        "rows_kept": stats["rows_kept"],
        "rows_spilled": stats["rows_spilled"],
        "spill_files": spill_files,  # New spill files created
        "used_spill_files": used_spill_files,  # Spill files that were merged/used
        "min_dt": stats["min_dt"],
        "max_dt": stats["max_dt"],
        "processed_at": datetime.now().isoformat(),
    }
    
    metadata_path = os.path.join(repo_root, "last_epos_transform.json")
    with open(metadata_path, "w") as f:
        json.dump(metadata, f, indent=2)
    
    print(f"Wrote metadata: {metadata_path}")


if __name__ == "__main__":
    main()