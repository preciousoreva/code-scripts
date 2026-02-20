"""Parse BookKeeping CSV into receipt-level transactions (grouped by receipt key)."""

from __future__ import annotations

import hashlib
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import List, Tuple

import pandas as pd

from code_scripts.reconciliation.fees import apply_fee_model
from code_scripts.reconciliation.models import Receipt, TenderKind


# BookKeeping columns we use
COL_DATE_TIME = "Date/Time"
COL_DEVICE = "Device Name"
COL_STAFF = "Staff"
COL_LOCATION = "Location Name"
COL_TENDER = "Tender"
COL_TOTAL_SALES = "TOTAL Sales"

# Date format in CSV: DD/MM/YYYY HH:MM:SS
DATETIME_FMT = "%d/%m/%Y %H:%M:%S"


def _parse_datetime(s: str) -> datetime:
    """Parse BookKeeping date/time string."""
    s = (s or "").strip()
    if not s:
        raise ValueError("empty datetime")
    return datetime.strptime(s, DATETIME_FMT)


def _classify_tender(tender: str) -> TenderKind:
    """
    Classify tender: Card/Transfer => electronic, Cash => cash, else mixed (review).
    """
    t = (tender or "").strip().lower()
    if t in ("card", "transfer"):
        return TenderKind.ELECTRONIC
    if t == "cash":
        return TenderKind.CASH
    if "/" in t or "cash" in t and any(x in t for x in ("card", "transfer")):
        return TenderKind.MIXED
    if t:
        return TenderKind.ELECTRONIC  # treat unknown single tenders as electronic for matching
    return TenderKind.CASH


def _receipt_group_key(row: pd.Series) -> Tuple[str, str, str, str, str]:
    """Grouping key: (Date/Time, Device Name, Staff, Location Name, Tender)."""
    dt = str(row.get(COL_DATE_TIME, ""))
    dev = str(row.get(COL_DEVICE, ""))
    staff = str(row.get(COL_STAFF, ""))
    loc = str(row.get(COL_LOCATION, ""))
    tend = str(row.get(COL_TENDER, ""))
    return (dt, dev, staff, loc, tend)


def _stable_receipt_id(
    dt_str: str,
    device: str,
    staff: str,
    location: str,
    tender: str,
    gross: float,
) -> str:
    """Deterministic receipt_id from grouping key + gross."""
    raw = f"{dt_str}|{device}|{staff}|{location}|{tender}|{gross:.2f}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]


def load_bookkeeping_csv(path: Path) -> pd.DataFrame:
    """Load BookKeeping CSV; raises if missing or invalid."""
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"EPOS file not found: {path}")
    df = pd.read_csv(path)
    for col in (COL_DATE_TIME, COL_DEVICE, COL_STAFF, COL_LOCATION, COL_TENDER, COL_TOTAL_SALES):
        if col not in df.columns:
            raise ValueError(f"Missing column in BookKeeping CSV: {col}")
    return df


def build_receipts(df: pd.DataFrame) -> List[Receipt]:
    """
    Group rows by (Date/Time, Device Name, Staff, Location Name, Tender),
    aggregate TOTAL Sales -> gross_amount, compute fees and receipt_id.
    Marks collision when multiple groups have same key (we still emit one receipt per group).
    """
    groups: dict[Tuple[str, str, str, str, str], List[pd.Series]] = defaultdict(list)
    for _, row in df.iterrows():
        key = _receipt_group_key(row)
        groups[key].append(row)

    receipts: List[Receipt] = []
    for key, rows in groups.items():
        dt_str, device, staff, location, tender = key
        gross = sum(float(r.get(COL_TOTAL_SALES, 0) or 0) for r in rows)
        if gross <= 0:
            continue
        line_count = len(rows)
        try:
            receipt_dt = _parse_datetime(dt_str)
        except ValueError:
            continue
        tender_kind = _classify_tender(tender)
        service_fee, expected_credit, expected_emtl = apply_fee_model(gross)
        receipt_id = _stable_receipt_id(dt_str, device, staff, location, tender, gross)
        # Collision: same grouping key would produce multiple receipts (e.g. same key, different gross); single-file grouping gives one receipt per key.
        collision = False
        receipts.append(
            Receipt(
                receipt_id=receipt_id,
                receipt_datetime=receipt_dt,
                location_name=location,
                device_name=device,
                staff=staff,
                tender=tender,
                tender_kind=tender_kind,
                line_count=line_count,
                gross_amount=gross,
                service_fee=service_fee,
                expected_credit=expected_credit,
                expected_emtl=expected_emtl,
                collision=collision,
            )
        )
    return receipts


def parse_bookkeeping_to_receipts(path: Path) -> List[Receipt]:
    """
    Load BookKeeping CSV and return list of reconstructed receipts.
    """
    df = load_bookkeeping_csv(path)
    return build_receipts(df)
