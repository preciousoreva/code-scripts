import glob
import os
from sales_recepit_script import TransformOptions, transform_file

RAW_DIR = r"C:\epos\raw"
OUTPUT_DIR = r"C:\epos\processed"

os.makedirs(OUTPUT_DIR, exist_ok=True)

def find_latest_bookkeeping() -> str:
    pattern = os.path.join(RAW_DIR, "BookKeeping_*.csv")
    candidates = glob.glob(pattern)
    if not candidates:
        raise FileNotFoundError(f"No BookKeeping_*.csv files found in {RAW_DIR}")
    # Pick newest by modification time
    latest = max(candidates, key=os.path.getmtime)
    return latest

def main():
    latest_csv = find_latest_bookkeeping()
    print(f"Using latest file: {latest_csv}")

    base_name = os.path.splitext(os.path.basename(latest_csv))[0]
    output_base = os.path.join(OUTPUT_DIR, f"processed_sales_receipts_{base_name}.csv")

    opts = TransformOptions(
        deposit_account="100900 - Undeposited Funds",
        date_format="%Y-%m-%d",
        prefix="SR",
        start_seq=1,
        override_tax_code=None,
        on_missing_date="skip",
        default_date=None,
    )

    outputs = transform_file(latest_csv, output_base, opts)
    print("\nGenerated QuickBooks files:")
    for path in outputs:
        print("  -", path)

if __name__ == "__main__":
    main()