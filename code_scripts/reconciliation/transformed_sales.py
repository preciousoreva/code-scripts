"""Summarize transformed EPOS single-sales CSV by tender for daily reconciliation."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Dict

import pandas as pd


@dataclass
class TenderTotals:
    """Daily tender totals from transformed EPOS file."""

    actual_sales_total: float
    card_total: float
    transfer_total: float
    card_transfer_combo_total: float
    cash_total: float
    mixed_with_cash_total: float
    other_tender_total: float

    @property
    def electronic_total(self) -> float:
        return self.card_total + self.transfer_total + self.card_transfer_combo_total

    @property
    def potential_cash_total(self) -> float:
        """
        Cash-like total including mixed tenders with cash components.
        """
        return self.cash_total + self.mixed_with_cash_total

    def as_dict(self) -> Dict[str, float]:
        return {
            "actual_sales_total": round(self.actual_sales_total, 2),
            "card_total": round(self.card_total, 2),
            "transfer_total": round(self.transfer_total, 2),
            "card_transfer_combo_total": round(self.card_transfer_combo_total, 2),
            "cash_total": round(self.cash_total, 2),
            "mixed_with_cash_total": round(self.mixed_with_cash_total, 2),
            "other_tender_total": round(self.other_tender_total, 2),
            "electronic_total": round(self.electronic_total, 2),
            "potential_cash_total": round(self.potential_cash_total, 2),
        }


def _find_column(df: pd.DataFrame, *candidates: str) -> str:
    for c in df.columns:
        key = str(c).strip().lower()
        for cand in candidates:
            if key == cand.lower() or cand.lower() in key:
                return c
    return ""


def classify_tender_bucket(value: object) -> str:
    """
    Bucket transformed tender text for daily variance explanations.
    """
    text = str(value or "").strip().lower()
    has_card = "card" in text
    has_transfer = "transfer" in text
    has_cash = "cash" in text

    if has_cash and (has_card or has_transfer):
        return "mixed_with_cash"
    if has_card and has_transfer:
        return "card_transfer_combo"
    if has_card:
        return "card"
    if has_transfer:
        return "transfer"
    if has_cash:
        return "cash"
    return "other"


def summarize_transformed_sales(path: Path) -> TenderTotals:
    """
    Parse transformed single-sales CSV and aggregate daily totals by tender bucket.

    Expected columns include:
    - Tender-like column: `Memo` (preferred) or `Tender`
    - Amount column: `TOTAL Sales` (preferred) or `*ItemAmount`
    """
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"Transformed sales file not found: {path}")

    df = pd.read_csv(path)
    if df.empty:
        return TenderTotals(0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0)

    amount_col = _find_column(df, "TOTAL Sales", "*ItemAmount", "ItemAmount", "Gross Sales")
    tender_col = _find_column(df, "Memo", "Tender")
    if not amount_col:
        raise ValueError(
            "Unable to find amount column in transformed sales CSV "
            "(expected one of: TOTAL Sales, *ItemAmount)."
        )
    if not tender_col:
        raise ValueError(
            "Unable to find tender column in transformed sales CSV "
            "(expected one of: Memo, Tender)."
        )

    amount_series = pd.to_numeric(df[amount_col], errors="coerce").fillna(0.0)
    tender_series = df[tender_col].fillna("")

    totals = {
        "card": 0.0,
        "transfer": 0.0,
        "card_transfer_combo": 0.0,
        "cash": 0.0,
        "mixed_with_cash": 0.0,
        "other": 0.0,
    }
    for tender, amount in zip(tender_series, amount_series):
        bucket = classify_tender_bucket(tender)
        totals[bucket] = totals.get(bucket, 0.0) + float(amount or 0.0)

    return TenderTotals(
        actual_sales_total=float(amount_series.sum()),
        card_total=totals["card"],
        transfer_total=totals["transfer"],
        card_transfer_combo_total=totals["card_transfer_combo"],
        cash_total=totals["cash"],
        mixed_with_cash_total=totals["mixed_with_cash"],
        other_tender_total=totals["other"],
    )

