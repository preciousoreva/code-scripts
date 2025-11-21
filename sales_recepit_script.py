"""
Sales Receipt CSV transformer

Converts a source sales CSV into a QuickBooks-compatible SalesReceipt import CSV.

Input CSV columns (expected):
    Staff, Customer Full Name, Location Name, Device Name, Quantity, Product,
    Category, Barcode, Date/Time, Discount Value, Discount Reason, NET Sales,
    Tax Code, Tax, TOTAL Sales, Cost Price, Margin, Tender, Nominal, A/C Ref,
    Notes, Customer ID, ProductId

Output CSV columns:
    *SalesReceiptNo, Customer, *SalesReceiptDate, *DepositAccount, Location, Memo,
    Item(Product/Service), ItemDescription, ItemQuantity, ItemRate, *ItemAmount,
    *ItemTaxCode, ItemTaxAmount, Service Date

Notes on mapping:
    - *SalesReceiptNo: Auto-generated as {prefix}-{YYYYMMDD}-{seq}, where seq increments per date.
    - *SalesReceiptDate: Derived from input Date/Time (date portion only).
    - *DepositAccount: Defaults to "100900 - Undeposited Funds" unless overridden.
    - Memo: Populated from "Tender" column in input.
    - *ItemAmount: From "TOTAL Sales".
    - *ItemTaxCode: If not overridden, inferred as "Sales Tax" when Tax > 0 else "No VAT"; 
      if "Tax Code" exists, its non-empty values take precedence.
    - Service Date: Same date as *SalesReceiptDate.
    - Output files are grouped by date AND tender type, ensuring each file contains 
      transactions from only one date and one tender type.

Usage (examples):
    # Single file
    python sales_recepit_script.py input.csv --output updates/processed_sales_receipts.csv \
        --deposit-account "100900 - Undeposited Funds" --date-format "%d/%m/%Y" --prefix SR

    # Multiple explicit files
    python sales_recepit_script.py jan.csv feb.csv mar.csv

    # No inputs provided: auto-discover all *.csv in current working directory
    python sales_recepit_script.py

"""

from __future__ import annotations

import argparse
import re
import os
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, Optional

import pandas as pd


# Required input columns; some are optional but used when present
REQUIRED_COLUMNS = [
    "Customer Full Name",
    "Location Name",
    "Quantity",
    "Product",
    "Category",
    "Date/Time",
    "TOTAL Sales",
]

OPTIONAL_COLUMNS = [
    "Tax Code",
    "Tax",
    "Notes",
    "Tender",
]


@dataclass
class TransformOptions:
    deposit_account: str = "100900 - Undeposited Funds"
    date_format: str = "%Y-%m-%d"  # Output date format
    prefix: str = "SR"
    start_seq: int = 1
    override_tax_code: Optional[str] = None  # e.g., "Sales Tax" or "No VAT"
    on_missing_date: str = "skip"  # one of: skip, error, fill
    default_date: Optional[str] = None  # used when on_missing_date=fill


def ensure_required_columns(df: pd.DataFrame) -> None:
    missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        raise ValueError(
            f"Missing required column(s): {', '.join(missing)}. Present: {', '.join(df.columns)}"
        )


def parse_date(value: str) -> Optional[datetime]:
    """Parse common date/time strings and return a datetime or None if empty.
    Tries multiple formats; falls back to pandas.to_datetime.
    """
    # Treat None/NaN/empty-string as missing
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    s = str(value).strip()
    if s == "":
        return None
    # Try a few common formats fast
    for fmt in ("%d/%m/%Y %H:%M:%S", "%Y-%m-%d %H:%M:%S", "%d/%m/%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(s, fmt)
        except Exception:
            pass
    # Fallback to pandas
    dt = pd.to_datetime(s, errors="coerce")
    if pd.isna(dt):
        return None
    return dt.to_pydatetime()


def infer_tax_code(row: pd.Series, override: Optional[str]) -> str:
    if override:
        return override
    if "Tax Code" in row and isinstance(row["Tax Code"], str) and row["Tax Code"].strip():
        return row["Tax Code"].strip()
    # Infer from numeric Tax
    try:
        tax_val = float(row.get("Tax", 0) or 0)
    except Exception:
        tax_val = 0.0
    return "Sales Tax" if tax_val > 0 else "No VAT"


def generate_receipt_no(date_obj: datetime, seq: int, prefix: str) -> str:
    return f"{prefix}-{date_obj.strftime('%Y%m%d')}-{seq:04d}"


def transform_dataframe(df: pd.DataFrame, options: TransformOptions) -> pd.DataFrame:
    ensure_required_columns(df)

    # Normalize and parse dates using Date/Time then fallback to Date
    dates_dt = df["Date/Time"].apply(parse_date) if "Date/Time" in df.columns else pd.Series([None] * len(df))
    dates_d = df["Date"].apply(parse_date) if "Date" in df.columns else None
    dates = dates_dt if dates_d is None else dates_dt.combine_first(dates_d)

    # Handle missing dates per option
    missing_mask = dates.isna()
    if missing_mask.any():
        if options.on_missing_date == "skip":
            skipped = int(missing_mask.sum())
            if skipped:
                print(f"Skipping {skipped} row(s) with missing date values.")
            df = df.loc[~missing_mask].reset_index(drop=True)
            dates = dates.loc[~missing_mask].reset_index(drop=True)
        elif options.on_missing_date == "fill":
            if options.default_date:
                default_dt = parse_date(options.default_date)
                if default_dt is None:
                    raise ValueError(f"--default-date value '{options.default_date}' could not be parsed")
                dates = dates.fillna(default_dt)
            else:
                # forward/backward fill from available dates
                dates = dates.ffill().bfill()
                if dates.isna().any():
                    raise ValueError("Some rows still have missing dates after fill; provide --default-date")
        else:  # error
            idxs = df.index[missing_mask].tolist()
            preview = idxs[:10]
            more = "..." if len(idxs) > 10 else ""
            raise ValueError(f"Encountered empty date value in rows: {preview}{more}")

    # Build output columns - we'll assign SalesReceiptNo later after grouping by date+tender
    out = pd.DataFrame()
    out["_parsed_date"] = dates  # temporary column for grouping logic
    out["_date_str"] = [d.strftime(options.date_format) for d in dates]
    out["Customer"] = df.get("Customer Full Name").fillna("")
    out["*SalesReceiptDate"] = out["_date_str"]
    out["*DepositAccount"] = options.deposit_account
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
    out["ItemRate"] = ""  # leave blank per mapping

    # Ensure numeric amounts
    def to_number(x):
        try:
            if isinstance(x, str):
                x = x.replace(",", "")
            return float(x)
        except Exception:
            return 0.0

    out["*ItemAmount"] = df.get("TOTAL Sales").apply(to_number)
    out["*ItemTaxCode"] = df.apply(lambda r: infer_tax_code(r, options.override_tax_code), axis=1)
    out["ItemTaxAmount"] = df.get("Tax", 0).apply(to_number)
    # Prefer explicit Date column for Service Date if available; otherwise use SalesReceiptDate
    if "Date" in df.columns:
        svc_dates = df["Date"].apply(parse_date)
        svc_dates = svc_dates.where(~svc_dates.isna(), dates)  # fallback to parsed dates
        out["Service Date"] = [d.strftime(options.date_format) for d in svc_dates]
    else:
        out["Service Date"] = out["*SalesReceiptDate"]

    # Generate SalesReceiptNo per (date, tender) group so all rows in a group share the same receipt number
    # Sequence increments per unique (date, tender) combination
    seq_by_date_tender: Dict[tuple, int] = {}
    receipt_numbers = []
    for idx, row in out.iterrows():
        date_obj = row["_parsed_date"]
        tender = row["Memo"]
        key = (date_obj.strftime("%Y%m%d"), tender)
        if key not in seq_by_date_tender:
            seq_by_date_tender[key] = options.start_seq + len(seq_by_date_tender)
        seq = seq_by_date_tender[key]
        receipt_numbers.append(generate_receipt_no(date_obj, seq, options.prefix))
    
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


MAX_ROWS_PER_FILE = 1000  # hard cap per output chunk

def _chunk_dataframe(df: pd.DataFrame, size: int):
    for start in range(0, len(df), size):
        yield df.iloc[start:start+size].reset_index(drop=True)

def transform_file(input_csv: str, output_csv: str, options: TransformOptions) -> list[str]:
    """Transform input and write one or more output CSV files.
    Returns list of written file paths. Splits into chunks of MAX_ROWS_PER_FILE.
    """
    df = pd.read_csv(input_csv)
    result = transform_dataframe(df, options)
    os.makedirs(os.path.dirname(output_csv) or ".", exist_ok=True)

    # Group by date AND tender so each file contains only one date and one tender type
    # We'll use '*SalesReceiptDate' and 'Memo' (which now holds Tender)
    if "*SalesReceiptDate" not in result.columns or "Memo" not in result.columns:
        raise ValueError("Transformed output missing required columns for grouping")

    # Sort by date, tender, and receipt number for stable ordering
    result = result.sort_values(by=["*SalesReceiptDate", "Memo", "*SalesReceiptNo"]).reset_index(drop=True)

    base_dir = os.path.dirname(output_csv) or "."
    base_file = os.path.basename(output_csv)
    name, ext = os.path.splitext(base_file)

    def sanitize_for_filename(s: str) -> str:
        # Replace non-alphanumeric with '-', strip leading/trailing dashes
        s2 = re.sub(r"[^0-9A-Za-z]+", "-", str(s)).strip("-")
        return s2 or "unknown"

    outputs: list[str] = []
    # Group by both date and tender (Memo column)
    for (sales_date, tender), group in result.groupby(["*SalesReceiptDate", "Memo"], sort=False):
        safe_date = sanitize_for_filename(sales_date)
        safe_tender = sanitize_for_filename(tender) if tender else "no-tender"
        # Chunk within the (date, tender) group if it exceeds the per-file maximum
        chunks = list(_chunk_dataframe(group, MAX_ROWS_PER_FILE))
        if len(chunks) == 1:
            out_name = f"{name}_{safe_date}_{safe_tender}{ext}"
            out_path = os.path.join(base_dir, out_name)
            chunks[0].to_csv(out_path, index=False)
            outputs.append(out_path)
        else:
            for idx, chunk in enumerate(chunks, start=1):
                out_name = f"{name}_{safe_date}_{safe_tender}_part{idx}{ext}"
                out_path = os.path.join(base_dir, out_name)
                chunk.to_csv(out_path, index=False)
                outputs.append(out_path)
    return outputs


def default_output_path(input_path: str) -> str:
    base = os.path.basename(input_path)
    name, _ = os.path.splitext(base)
    return os.path.join("updates", f"processed_sales_receipts_{name}.csv")


def build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Convert a sales CSV to QuickBooks SalesReceipt CSV."
    )
    p.add_argument(
        "inputs",
        nargs="*",
        help="Input CSV file(s). If omitted, all *.csv in current directory will be processed",
    )
    p.add_argument(
        "--output",
        help="Output CSV path (default: updates/processed_sales_receipts_<name>.csv)",
    )
    p.add_argument(
        "--deposit-account",
        default="100900 - Undeposited Funds",
        help="Deposit account name to set in *DepositAccount",
    )
    p.add_argument(
        "--date-format",
        default="%Y-%m-%d",
        help="Output date format, e.g., %d/%m/%Y or %Y-%m-%d",
    )
    p.add_argument("--prefix", default="SR", help="SalesReceipt number prefix")
    p.add_argument(
        "--start-seq",
        type=int,
        default=1,
        help="Starting sequence number per date (default 1)",
    )
    p.add_argument(
        "--override-tax-code",
        default=None,
        help="Force *ItemTaxCode for all rows (e.g., 'Sales Tax' or 'No VAT')",
    )
    p.add_argument(
        "--on-missing-date",
        choices=["skip", "error", "fill"],
        default="skip",
        help="Behavior when Date/Time (and Date) are missing: skip rows, error out, or fill",
    )
    p.add_argument(
        "--default-date",
        default=None,
        help="When --on-missing-date=fill, use this date string for missing rows (e.g., '2025-01-31')",
    )
    return p


def _discover_csv_inputs() -> list[str]:
    """Return list of CSV files in current directory (non-recursive) excluding obvious processed outputs."""
    candidates = []
    for fname in os.listdir('.'):
        if not fname.lower().endswith('.csv'):
            continue
        # Exclude already processed / chunked outputs heuristically
        lower = fname.lower()
        if lower.startswith('processed_sales_receipts_') or '_part' in lower:
            continue
        candidates.append(fname)
    return sorted(candidates)


def main():
    parser = build_arg_parser()
    args = parser.parse_args()

    input_files: list[str] = args.inputs if args.inputs else _discover_csv_inputs()
    if not input_files:
        parser.error("No input CSV files provided or discovered in current directory.")

    # If multiple inputs and --output is provided, treat it as a directory (create if needed)
    output_arg = args.output
    output_is_directory = False
    if len(input_files) > 1:
        if output_arg:
            if os.path.splitext(output_arg)[1]:
                # Has an extension; require it to be a directory if exists without ext mismatch
                print("--output provided but multiple inputs detected; interpreting as directory containing outputs.")
            output_is_directory = True
            os.makedirs(output_arg, exist_ok=True)
        else:
            # default: use updates/ directory
            output_is_directory = True
            output_arg = 'updates'
            os.makedirs(output_arg, exist_ok=True)

    opts = TransformOptions(
        deposit_account=args.deposit_account,
        date_format=args.date_format,
        prefix=args.prefix,
        start_seq=args.start_seq,
        override_tax_code=args.override_tax_code,
        on_missing_date=args.on_missing_date,
        default_date=args.default_date,
    )

    all_outputs: list[str] = []
    for in_path in input_files:
        if output_is_directory:
            base_out = os.path.join(output_arg, f"processed_sales_receipts_{os.path.splitext(os.path.basename(in_path))[0]}.csv")
        else:
            base_out = output_arg or default_output_path(in_path)
        out_paths = transform_file(in_path, base_out, opts)
        all_outputs.extend(out_paths)
        print(f"Processed {in_path} -> {len(out_paths)} file(s)")

    if len(all_outputs) == 1:
        print(f"\nWrote 1 output file: {all_outputs[0]}")
    else:
        print(f"\nWrote {len(all_outputs)} total output files (max {MAX_ROWS_PER_FILE} rows each):")
        for p in all_outputs:
            print(f"  - {p}")


if __name__ == "__main__":
    main()



