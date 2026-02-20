"""Load account mapping CSV (account number -> account name/details)."""

from __future__ import annotations

import re
from pathlib import Path
from typing import Dict

import pandas as pd


def _find_column(df: pd.DataFrame, *candidates: str) -> str:
    norm = lambda s: str(s).strip().lower()
    cols = [(c, norm(c)) for c in df.columns]
    cands = [norm(c) for c in candidates]

    # Pass 1: exact match.
    for col, key in cols:
        for cand in cands:
            if key == cand:
                return col

    # Pass 2: candidate contained in column name.
    for c in df.columns:
        key = norm(c)
        for cand in candidates:
            cand_norm = norm(cand)
            if cand_norm and cand_norm in key:
                return c
    return ""


def _normalize_account_number(value: object) -> str:
    raw = str(value).strip()
    digits = "".join(re.findall(r"\d+", raw))
    return digits or raw


def load_account_mapping(path: Path) -> Dict[str, str]:
    """
    Load Akponora Account Mapping CSV.
    Returns dict: account_number -> account_name.
    Uses 'QBO / Bank Statement Account Number' (or similar) as key and
    'Monipoint Account Name' (or similar) as value.
    """
    path = Path(path)
    if not path.exists():
        return {}
    df = pd.read_csv(path)
    # Normalize column names (strip spaces)
    df.columns = [str(c).strip() for c in df.columns]
    num_col = _find_column(
        df,
        "QBO / Bank Statement Account Number",
        "Bank Statement Account Number",
        "Monipoint Online Account",
        "Account Number",
    )
    name_col = _find_column(
        df,
        "Monipoint Account Name",
        "Account Name",
    )
    if not num_col or not name_col:
        return {}
    out: Dict[str, str] = {}
    for _, row in df.iterrows():
        num = row.get(num_col)
        name = row.get(name_col)
        if pd.isna(num) or pd.isna(name):
            continue
        out[_normalize_account_number(num)] = str(name).strip()
    return out


def load_account_mapping_details(path: Path) -> Dict[str, Dict[str, str]]:
    """
    Load richer account mapping details keyed by bank statement account number.

    Returned value includes keys:
    - account_number
    - account_name
    - moniepoint_store_account
    - moniepoint_online_account
    - location_name
    - status
    """
    path = Path(path)
    if not path.exists():
        return {}
    df = pd.read_csv(path)
    df.columns = [str(c).strip() for c in df.columns]

    bank_col = _find_column(
        df,
        "QBO / Bank Statement Account Number",
        "Bank Statement Account Number",
        "Account Number",
    )
    name_col = _find_column(df, "Monipoint Account Name", "Account Name")
    store_col = _find_column(df, "Monipoint Store Account Name", "Store Account")
    online_col = _find_column(df, "Monipoint Online Account", "Online Account")
    location_col = _find_column(df, "Location Name")
    status_col = _find_column(df, "STATUS", "Status")

    if not bank_col:
        return {}

    def _safe_text(value: object) -> str:
        if value is None:
            return ""
        if pd.isna(value):
            return ""
        return str(value).strip()

    out: Dict[str, Dict[str, str]] = {}
    for _, row in df.iterrows():
        acc = _normalize_account_number(row.get(bank_col))
        if not acc:
            continue
        detail = {
            "account_number": acc,
            "account_name": _safe_text(row.get(name_col)) if name_col else "",
            "moniepoint_store_account": _normalize_account_number(row.get(store_col)) if store_col else "",
            "moniepoint_online_account": _normalize_account_number(row.get(online_col)) if online_col else "",
            "location_name": _safe_text(row.get(location_col)) if location_col else "",
            "status": _safe_text(row.get(status_col)) if status_col else "",
        }
        out[acc] = detail
    return out
