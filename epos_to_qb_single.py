import os
import glob
import json
from datetime import datetime
import pandas as pd

from sales_recepit_script import TransformOptions, transform_dataframe


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


def main():
    repo_root = get_repo_root()

    # 1) Pick latest raw BookKeeping CSV from repo root
    raw_file = find_latest_raw_file(repo_root)
    print(f"Using raw file: {raw_file}")

    # 2) Load raw CSV
    df = pd.read_csv(raw_file)

    # 3) Transform using the existing logic (but no splitting)
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

    # 4) Extract normalized date for archiving
    normalized_date = extract_date_from_dataframe(transformed)

    # 5) Write ONE QuickBooks-ready CSV in repo root
    base_name = os.path.splitext(os.path.basename(raw_file))[0]
    output_filename = f"single_sales_receipts_{base_name}.csv"
    output_path = os.path.join(repo_root, output_filename)

    transformed.to_csv(output_path, index=False)
    print(f"Wrote combined QuickBooks file: {output_path}")
    print(f"Rows (including header): {len(transformed) + 1}")

    # 6) Write metadata file for archiving
    metadata = {
        "raw_file": os.path.basename(raw_file),
        "raw_file_path": raw_file,
        "processed_files": [output_filename],
        "normalized_date": normalized_date,
        "processed_at": datetime.now().isoformat(),
    }
    
    metadata_path = os.path.join(repo_root, "last_epos_transform.json")
    with open(metadata_path, "w") as f:
        json.dump(metadata, f, indent=2)
    
    print(f"Wrote metadata: {metadata_path}")


if __name__ == "__main__":
    main()