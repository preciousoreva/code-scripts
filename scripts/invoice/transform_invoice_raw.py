"""
Transform raw company_a invoice CSV (block/section format) into template-shaped CSV.

Reads a raw file with repeated blocks (DATE, QTY, ITEMS, RATE, AMOUNT), section
headers (GOLDPLATE INVOICE ... HQ/IKORODU), and TOTAL rows. Outputs a single CSV
with columns: Customer, InvoiceDate, ServiceDate, ItemName, Description, Qty, Rate,
Amount, Location, DueDate (same as invoice_template.csv).

Usage:
  python scripts/invoice/transform_invoice_raw.py --csv invoices/company_a_raw_invoice.csv
  python scripts/invoice/transform_invoice_raw.py --csv invoices/company_a_raw_invoice.csv -o invoices/company_a_formatted.csv
"""

from __future__ import annotations

import argparse
import csv
import re
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Optional, Tuple

TEMPLATE_COLS = [
    "Customer",
    "InvoiceDate",
    "ServiceDate",
    "ItemName",
    "Description",
    "Qty",
    "Rate",
    "Amount",
    "Location",
    "DueDate",
]

CUSTOMER = "GPFH"
SKIP_ITEMS = frozenset({"TOTAL", "SUM TOTAL"})


def _parse_date(s: str) -> Optional[str]:
    if not s or not str(s).strip():
        return None
    s = str(s).strip()
    for fmt in ("%d/%m/%Y", "%d/%m/%y"):
        try:
            return datetime.strptime(s, fmt).strftime("%Y-%m-%d")
        except ValueError:
            pass
    # D/M/YYYY (e.g. 14/1/2026, 16/1/2026) - single-digit day/month
    m = re.match(r"^(\d{1,2})/(\d{1,2})/(\d{4})$", s)
    if m:
        d, mo, y = int(m.group(1)), int(m.group(2)), int(m.group(3))
        try:
            return datetime(y, mo, d).strftime("%Y-%m-%d")
        except ValueError:
            pass
    return None


def _parse_num(s: str) -> float:
    if s is None:
        return 0.0
    s = str(s).strip().replace(",", "")
    try:
        return float(s)
    except ValueError:
        return 0.0


def _parse_qty(s: str) -> float:
    if s is None or str(s).strip() == "":
        return 1.0
    return _parse_num(s) or 1.0


def _location_from_section(text: str) -> str:
    """Extract location (e.g. HQ, IKORODU) from 'GOLDPLATE INVIOCE JANUARY 2026 HQ'."""
    if not text or "GOLDPLATE" not in text.upper():
        return ""
    parts = text.strip().split()
    return parts[-1] if parts else ""


def transform_raw_to_template(csv_path: Path) -> Tuple[List[dict], str]:
    """
    Read raw invoice CSV and return (list of template-shaped row dicts, error message).
    Empty error string means success.
    """
    rows_out: List[dict] = []
    current_date: Optional[str] = None
    current_location: str = ""

    with open(csv_path, newline="", encoding="utf-8-sig") as f:
        reader = csv.reader(f)
        for row in reader:
            # Columns: 0=DATE, 1=QTY, 2=ITEMS, 3=RATE, 4=AMOUNT (rest ignored)
            while len(row) < 5:
                row.append("")
            date_s, qty_s, items_s, rate_s, amount_s = (
                (row[0] or "").strip(),
                (row[1] or "").strip(),
                (row[2] or "").strip(),
                (row[3] or "").strip(),
                (row[4] or "").strip(),
            )

            # Section header: e.g. ",,GOLDPLATE INVIOCE JANUARY 2026 HQ"
            if items_s and "GOLDPLATE" in items_s.upper():
                loc = _location_from_section(items_s)
                if loc:
                    current_location = loc
                continue

            # Header row: DATE,QTY,ITEMS,RATE,AMOUNT
            if items_s.upper() == "ITEMS" and (date_s.upper() == "DATE" or qty_s.upper() == "QTY"):
                continue

            # Skip TOTAL / SUM TOTAL
            if items_s.upper() in SKIP_ITEMS:
                continue

            # Update date when present
            if date_s:
                parsed = _parse_date(date_s)
                if parsed:
                    current_date = parsed

            # Need a date and an item name to emit a line
            if not current_date or not items_s:
                continue

            rate = _parse_num(rate_s)
            amount = _parse_num(amount_s)
            qty = _parse_qty(qty_s)
            due = (datetime.strptime(current_date, "%Y-%m-%d") + timedelta(days=30)).strftime("%Y-%m-%d")

            rows_out.append({
                "Customer": CUSTOMER,
                "InvoiceDate": current_date,
                "ServiceDate": current_date,
                "ItemName": items_s,
                "Description": "",
                "Qty": qty,
                "Rate": rate,
                "Amount": amount,
                "Location": current_location,
                "DueDate": due,
            })

    return rows_out, ""


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Transform raw invoice CSV into template-shaped CSV (Customer, InvoiceDate, ItemName, Qty, Rate, Amount, etc.)."
    )
    parser.add_argument("--csv", required=True, help="Path to raw invoice CSV")
    parser.add_argument("-o", "--output", help="Output path (default: same dir as source, stem_formatted.csv)")
    args = parser.parse_args()

    src = Path(args.csv)
    if not src.exists():
        print(f"[ERROR] File not found: {src}")
        return 1

    rows, err = transform_raw_to_template(src)
    if err:
        print(f"[ERROR] {err}")
        return 1

    if args.output:
        out_path = Path(args.output)
    else:
        out_path = src.parent / f"{src.stem}_formatted.csv"

    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=TEMPLATE_COLS)
        w.writeheader()
        w.writerows(rows)

    print(f"Formatted CSV: {out_path} ({len(rows)} rows)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
