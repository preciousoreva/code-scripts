"""
Unified EPOS Transform Script

Transforms EPOS CSV files to QuickBooks-ready format using company-specific configuration.
Supports both Company A (date+tender grouping) and Company B (date+location+tender grouping).

Usage:
    python transform_epos.py --company company_a --target-date 2025-12-25
    python transform_epos.py --company company_b --target-date 2025-12-25
"""

import os
import glob
import json
import sys
import argparse
from datetime import datetime, timezone, timedelta
from typing import Optional, Tuple, List, Dict
import pandas as pd
import re


from company_config import load_company_config, get_available_companies

# ------------------------------
# Local helpers (self-contained)
# ------------------------------

REQUIRED_COLUMNS = [
    "Customer Full Name",
    "Location Name",
    "Quantity",
    "Product",
    "Category",
    "Date/Time",
    "TOTAL Sales",
]


def ensure_required_columns(df: pd.DataFrame) -> None:
    missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        raise ValueError(
            f"Missing required column(s): {', '.join(missing)}. Present: {', '.join(df.columns)}"
        )


def parse_date(value: str) -> Optional[datetime]:
    """Parse common date/time strings into a naive datetime (local to EPOS export).
    Returns None if empty/unparseable.
    """
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


def sanitize_location_for_code(location_name: str) -> str:
    """Convert location name to a short code (max 4 chars) for DocNumber."""
    if not location_name or pd.isna(location_name):
        return "UNK"

    location_name = str(location_name).strip().upper()

    # NOTE: Prefer config.location_mapping for exact control.
    # This fallback is only used when the location is not mapped in config.
    location_map = {
        "MAIN RESTAURANT": "MAIN",
        "MAIN RESTAURANT (AYANGBURE)": "MAIN",
        "MAIN RESTAURANT (DREAM PARK)": "MDPK",
        "CLUB": "CLUB",
        "CLUB (AYANGBURE)": "CLUB",
        "HOTEL": "HTL",
        "HOTEL (AYANGBURE)": "HTL",
        "LOUNGE": "LNG",
        "LOUNGE (AYANGBURE)": "LNG",
        "BASK LOUNGE": "BSK",
        "BASK LOUNGE (CHEVRON)": "BSK",
        "SHAWARMA STAND": "SHW",
        "SHAWARMA STAND (CHEVRON)": "SHWC",
        "SHAWARMA STAND (AYANGBURE)": "SHWA",
        "SHAWARMA STAND (DREAM PARK)": "SHWD",
        "TALEA MALL": "TALE",
        "TALEA MALL (MAIN)": "TALE",
        "1004 (VI)": "VI",
        "1004": "VI",
    }

    if location_name in location_map:
        return location_map[location_name]

    for key, code in location_map.items():
        if key in location_name or location_name in key:
            return code

    words = re.sub(r"[()]", " ", location_name).split()
    for word in words:
        word_upper = word.upper()
        if word_upper not in ["THE", "AND", "OR", "OF", "IN", "AT", "ON"] and len(word_upper) >= 3:
            return word_upper[:4]

    return "UNK"


def generate_gp_receipt_no(date_obj: datetime, location_code: str, seq: int, prefix: str) -> str:
    """Generate DocNumber: PREFIX-YYYYMMDD-LOC-SEQ (<= 21 chars)."""
    date_str = date_obj.strftime("%Y%m%d")
    loc_code = (location_code or "UNK")[:4]
    receipt_no = f"{prefix}-{date_str}-{loc_code}-{seq:04d}"

    if len(receipt_no) > 21:
        max_loc_len = 21 - len(f"{prefix}-{date_str}--{seq:04d}")
        loc_code = (location_code or "X")[: max_loc_len if max_loc_len > 0 else 1]
        receipt_no = f"{prefix}-{date_str}-{loc_code}-{seq:04d}"

    return receipt_no


def get_repo_root() -> str:
    """Return the directory where this script lives (code-scripts)."""
    return os.path.dirname(os.path.abspath(__file__))


def find_latest_raw_file(repo_root: str) -> str:
    """
    Find the most recently modified CSV in repo root (excluding processed files).
    
    Excludes:
    - Processed files (single_sales_receipts_*, gp_sales_receipts_*)
    - Files in uploads/range_raw/** (range mode split files)
    - Files in Uploaded/** (archived files)
    
    Only searches repo root directory (single-day mode only).
    For range mode, use --raw-file to explicitly specify the split file.
    """
    pattern = os.path.join(repo_root, "*.csv")
    files = glob.glob(pattern)
    
    # Exclude processed files
    exclude_prefixes = ["single_sales_receipts_", "gp_sales_receipts_"]
    files = [f for f in files if not any(os.path.basename(f).startswith(prefix) for prefix in exclude_prefixes)]
    
    # Exclude files in uploads/range_raw/** and Uploaded/**
    # Convert to absolute paths for comparison
    repo_root_abs = os.path.abspath(repo_root)
    uploads_range_raw = os.path.join(repo_root_abs, "uploads", "range_raw")
    uploaded_dir = os.path.join(repo_root_abs, "Uploaded")
    
    filtered_files = []
    for f in files:
        f_abs = os.path.abspath(f)
        # Skip if file is in uploads/range_raw/** or Uploaded/**
        if (uploads_range_raw in f_abs) or (uploaded_dir in f_abs):
            continue
        filtered_files.append(f)
    
    files = filtered_files
    
    if not files:
        raise FileNotFoundError(
            f"No raw CSV files found in {repo_root}. "
            f"Note: Files in uploads/range_raw/** and Uploaded/** are excluded. "
            f"For range mode, use --raw-file to specify the split file explicitly."
        )
    return max(files, key=os.path.getmtime)


# WAT timezone (UTC+1)
WAT_TZ = timezone(timedelta(hours=1))


def filter_rows_by_target_date(
    df: pd.DataFrame,
    target_date: str,
    raw_file: str
) -> Tuple[pd.DataFrame, pd.DataFrame, dict]:
    """
    Filter rows by target date and identify non-target rows.
    
    Note: Non-target rows are no longer written as spillover files.
    RAW spill handling is now managed by run_pipeline.py at the raw CSV level.
    
    Returns:
        (target_rows, non_target_rows, stats_dict)
    """
    target_dt = datetime.strptime(target_date, "%Y-%m-%d")
    target_date_str = target_dt.strftime("%Y-%m-%d")
    
    # Parse dates from Date/Time column
    dates_series = df["Date/Time"].apply(parse_date) if "Date/Time" in df.columns else pd.Series([None] * len(df))
    
    # Convert to WAT timezone and extract date portion
    def get_date_in_wat(dt) -> Optional[str]:
        try:
            if dt is None or pd.isna(dt):
                return None
        except (TypeError, ValueError):
            return None
        
        try:
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=WAT_TZ)
            elif dt.tzinfo != WAT_TZ:
                dt = dt.astimezone(WAT_TZ)
            return dt.date().strftime("%Y-%m-%d")
        except (AttributeError, ValueError, TypeError):
            return None
    
    date_strings = dates_series.apply(get_date_in_wat)
    
    # Filter: keep rows where date matches target_date
    target_mask = date_strings == target_date_str
    target_rows = df[target_mask].copy().reset_index(drop=True)
    non_target_rows = df[~target_mask].copy().reset_index(drop=True)
    
    # Collect statistics
    date_strings_clean = date_strings.dropna()
    dates_present_list = sorted(date_strings_clean.unique().tolist()) if len(date_strings_clean) > 0 else []
    
    # Show date distribution
    date_counts = date_strings.value_counts()
    print(f"\nDate distribution in CSV:")
    for date_val, count in date_counts.items():
        marker = " <-- TARGET" if date_val == target_date_str else ""
        print(f"  {date_val}: {count} rows{marker}")
    
    stats = {
        "rows_total": len(df),
        "rows_kept": len(target_rows),
        "rows_non_target": len(non_target_rows),
        "dates_present": dates_present_list,
        "min_dt": dates_present_list[0] if dates_present_list else None,
        "max_dt": dates_present_list[-1] if dates_present_list else None,
    }
    
    print(f"\nFiltering by target date: {target_date}")
    print(f"  Total rows: {stats['rows_total']}")
    print(f"  Rows kept (target date): {stats['rows_kept']}")
    print(f"  Rows ignored (non-target): {stats['rows_non_target']}")
    print(f"  Dates present in CSV: {', '.join(stats['dates_present'])}")
    
    # Warn if non-target rows exist (they should have been filtered at RAW level)
    if len(non_target_rows) > 0:
        non_target_dates = date_strings[~target_mask].value_counts()
        non_target_summary = ", ".join(f"{d}({c})" for d, c in non_target_dates.items())
        print(f"\n[WARNING] Raw file contains {len(non_target_rows)} row(s) not matching target_date (ignored).")
        print(f"          Dates present: {non_target_summary}")
        print(f"          Note: RAW spill handling should be done at pipeline level, not transform.")
    
    return target_rows, non_target_rows, stats


def transform_dataframe_unified(df: pd.DataFrame, config) -> pd.DataFrame:
    """
    Transform dataframe using company-specific configuration.
    Handles both Company A (date+tender) and Company B (date+location+tender) grouping.
    """
    ensure_required_columns(df)
    
    # Normalize and parse dates
    dates_dt = df["Date/Time"].apply(parse_date) if "Date/Time" in df.columns else pd.Series([None] * len(df))
    dates_d = df["Date"].apply(parse_date) if "Date" in df.columns else None
    dates = dates_dt if dates_d is None else dates_dt.combine_first(dates_d)
    
    # Handle missing dates
    missing_mask = dates.isna()
    if missing_mask.any():
        skipped = int(missing_mask.sum())
        if skipped:
            print(f"Skipping {skipped} row(s) with missing date values.")
        df = df.loc[~missing_mask].reset_index(drop=True)
        dates = dates.loc[~missing_mask].reset_index(drop=True)
    
    # Build output columns
    out = pd.DataFrame()
    out["_parsed_date"] = dates
    out["_date_str"] = [d.strftime(config.date_format) for d in dates]
    out["Customer"] = df.get("Customer Full Name").fillna("")
    out["*SalesReceiptDate"] = out["_date_str"]
    out["*DepositAccount"] = config.deposit_account
    out["Location"] = df.get("Location Name").fillna("")
    
    # Use Tender column for Memo
    tender_col = df.get("Tender")
    if isinstance(tender_col, pd.Series):
        out["Memo"] = tender_col.fillna("")
    else:
        out["Memo"] = ""
    
    out["Item(Product/Service)"] = df.get("Product").fillna("")
    out["ItemDescription"] = df.get("Category").fillna("")
    out["ItemQuantity"] = df.get("Quantity").fillna(0)
    out["ItemRate"] = ""
    
    # Ensure numeric amounts
    def to_number(x):
        try:
            if isinstance(x, str):
                x = x.replace(",", "")
            return float(x)
        except Exception:
            return 0.0
    
    out["*ItemAmount"] = df.get("TOTAL Sales").apply(to_number)
    
    # Tax code handling based on company config
    if config.tax_mode == "vat_inclusive_7_5":
        # Company A: infer tax code
        out["*ItemTaxCode"] = df.apply(
            lambda r: "No VAT" if 'delivery' in str(r.get("Product", "")).lower() or 'pack' in str(r.get("Product", "")).lower() else "Sales Tax",
            axis=1
        )
    else:
        # Company B: always use "Sales Tax"
        out["*ItemTaxCode"] = "Sales Tax"
    
    out["ItemTaxAmount"] = df.get("Tax", 0).apply(to_number)
    
    # Service Date
    if "Date" in df.columns:
        svc_dates = df["Date"].apply(parse_date)
        svc_dates = svc_dates.where(~svc_dates.isna(), dates)
        out["Service Date"] = [d.strftime(config.date_format) for d in svc_dates]
    else:
        out["Service Date"] = out["*SalesReceiptDate"]
    
    # Generate SalesReceiptNo based on company config
    if config.receipt_number_format == "date_location_sequence":
        # Company B: SR-YYYYMMDD-LOC-SEQ
        seq_by_date_location_tender: Dict[tuple, int] = {}
        receipt_numbers = []

        for idx, row in out.iterrows():
            date_obj = row["_parsed_date"]
            location_raw = str(row["Location"]).strip() if row["Location"] else "UNKNOWN"
            # Normalize EPOS location for mapping: uppercase, collapse spaces, strip trailing commas
            location_key = re.sub(r"\s+", " ", location_raw).strip().rstrip(",").upper()

            tender = str(row["Memo"]).strip() if row["Memo"] else "UNKNOWN"

            # Use location mapping from config (keys should be stored normalized in the same way)
            location_code = config.location_mapping.get(location_key, None)
            if not location_code:
                # Fallback to sanitize function
                location_code = sanitize_location_for_code(location_raw)

            # Group by (date, location, tender) but receipt number format is SR-YYYYMMDD-LOC-SEQ
            key = (date_obj.strftime("%Y%m%d"), location_raw, tender)
            if key not in seq_by_date_location_tender:
                seq_by_date_location_tender[key] = len(seq_by_date_location_tender) + 1
            seq = seq_by_date_location_tender[key]
            receipt_numbers.append(generate_gp_receipt_no(date_obj, location_code, seq, config.receipt_prefix))

        out["*SalesReceiptNo"] = receipt_numbers
    else:
        # Company A: SR-YYYYMMDD-SEQ (group by date+tender)
        seq_by_date_tender: Dict[tuple, int] = {}
        receipt_numbers = []
        
        for idx, row in out.iterrows():
            date_obj = row["_parsed_date"]
            tender = str(row["Memo"]).strip() if row["Memo"] else "UNKNOWN"
            
            key = (date_obj.strftime("%Y%m%d"), tender)
            if key not in seq_by_date_tender:
                seq_by_date_tender[key] = len(seq_by_date_tender) + 1
            seq = seq_by_date_tender[key]
            receipt_numbers.append(f"{config.receipt_prefix}-{date_obj.strftime('%Y%m%d')}-{seq:04d}")
        
        out["*SalesReceiptNo"] = receipt_numbers
    
    # Drop temporary columns
    out = out.drop(columns=["_parsed_date", "_date_str"])
    
    # Column order as required
    columns = [
        "*SalesReceiptNo",
        "Customer",
        "*SalesReceiptDate",
        "*DepositAccount",
        "Location",
        "Memo",
        "Item(Product/Service)",
        "ItemDescription",
        "ItemQuantity",
        "ItemRate",
        "*ItemAmount",
        "*ItemTaxCode",
        "ItemTaxAmount",
        "Service Date",
    ]
    return out[columns]


def main():
    parser = argparse.ArgumentParser(
        description="Transform EPOS CSV to QuickBooks format using company-specific configuration."
    )
    parser.add_argument(
        "--company",
        required=True,
        choices=get_available_companies(),
        help="Company identifier (REQUIRED). Available: %(choices)s",
    )
    parser.add_argument(
        "--target-date",
        help="Target business date in YYYY-MM-DD format (required for filtering)",
    )
    parser.add_argument(
        "--raw-file",
        help="Path to raw CSV file (overrides auto-detection of latest raw EPOS file)",
    )
    args = parser.parse_args()
    
    # Validate --raw-file if provided
    if args.raw_file:
        if not os.path.exists(args.raw_file):
            parser.error(f"--raw-file: file not found: {args.raw_file}")
        if not os.path.isfile(args.raw_file):
            parser.error(f"--raw-file: path is not a file: {args.raw_file}")
    
    # Load company configuration
    try:
        config = load_company_config(args.company)
    except Exception as e:
        print(f"Error: Failed to load company config for '{args.company}': {e}")
        sys.exit(1)
    
    repo_root = get_repo_root()
    
    # Get target_date
    if not args.target_date:
        print("Warning: No --target-date provided. Processing all rows without filtering.")
        print("  Usage: python transform_epos.py --company company_a --target-date YYYY-MM-DD")
        target_date = None
    else:
        target_date = args.target_date
    
    # Use provided raw_file or auto-detect latest
    if args.raw_file:
        raw_file = args.raw_file
        # Convert to absolute path if relative
        if not os.path.isabs(raw_file):
            raw_file = os.path.join(repo_root, raw_file)
        print(f"Using provided raw file: {raw_file}")
    else:
        raw_file = find_latest_raw_file(repo_root)
        print(f"Using auto-detected raw file: {raw_file}")
    
    # Load raw CSV
    df = pd.read_csv(raw_file)
    
    # Filter by target_date if provided
    stats = {
        "rows_total": len(df),
        "rows_kept": len(df),
        "rows_non_target": 0,
        "dates_present": [],
        "min_dt": None,
        "max_dt": None,
    }
    
    if target_date:
        target_df, non_target_rows, stats = filter_rows_by_target_date(df, target_date, raw_file)
        df = target_df
        
        if len(df) == 0:
            raise ValueError(f"No rows found for target date {target_date}. Cannot proceed with empty dataset.")
    else:
        # If no target_date, analyze dates present
        dates_series = df["Date/Time"].apply(parse_date) if "Date/Time" in df.columns else pd.Series([None] * len(df))
        def get_date_in_wat(dt) -> Optional[str]:
            try:
                if dt is None or pd.isna(dt):
                    return None
            except (TypeError, ValueError):
                return None
            try:
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=WAT_TZ)
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
    
    # Transform using unified transform logic
    transformed = transform_dataframe_unified(df, config)
    
    # NOTE: Transformed spill system has been deprecated.
    # RAW spill handling is now managed by run_pipeline.py at the raw CSV level:
    # - Downloads are split by WAT date before transform
    # - Future rows become RAW spill files in uploads/spill_raw/
    # - RAW spill files are merged back when their date is processed
    # This prevents double-handling and keeps transform.py simple.
    
    # Extract normalized date for archiving
    if target_date:
        normalized_date = target_date
    else:
        if "*SalesReceiptDate" in transformed.columns:
            dates = transformed["*SalesReceiptDate"].dropna()
            if len(dates) > 0:
                try:
                    date_obj = datetime.strptime(dates.iloc[0], config.date_format)
                    normalized_date = date_obj.strftime("%Y-%m-%d")
                except ValueError:
                    normalized_date = datetime.now().strftime("%Y-%m-%d")
            else:
                normalized_date = datetime.now().strftime("%Y-%m-%d")
        else:
            normalized_date = datetime.now().strftime("%Y-%m-%d")
    
    # Write ONE QuickBooks-ready CSV in repo root
    base_name = os.path.splitext(os.path.basename(raw_file))[0]
    output_filename = f"{config.csv_prefix}_{base_name}.csv"
    output_path = os.path.join(repo_root, output_filename)
    
    transformed.to_csv(output_path, index=False)
    print(f"\nWrote combined QuickBooks file: {output_path}")
    print(f"Rows (including header): {len(transformed) + 1}")
    
    # Write metadata file for archiving
    # Determine source mode from raw file name (for diagnostics)
    raw_basename = os.path.basename(raw_file)
    if raw_basename.startswith("CombinedRaw_"):
        source_mode = "raw_combined"
    elif raw_basename.startswith("BookKeeping_") and "_to_" in raw_file:
        source_mode = "raw_split"
    else:
        source_mode = "raw_direct"
    
    metadata = {
        "raw_file": raw_basename,
        "raw_file_path": raw_file,
        "processed_files": [output_filename],
        "normalized_date": normalized_date,
        "target_date": target_date,
        "rows_total": stats["rows_total"],
        "rows_kept": stats["rows_kept"],
        "rows_non_target": stats["rows_non_target"],
        "dates_present": stats["dates_present"],
        "min_dt": stats["min_dt"],
        "max_dt": stats["max_dt"],
        "processed_at": datetime.now().isoformat(),
        "company_key": config.company_key,
        "grouping": config.group_by,
        "source_mode": source_mode,
    }
    
    metadata_path = os.path.join(repo_root, config.metadata_file)
    with open(metadata_path, "w") as f:
        json.dump(metadata, f, indent=2)
    
    print(f"Wrote metadata: {metadata_path}")


if __name__ == "__main__":
    main()

