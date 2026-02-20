"""Dataclasses for reconciliation: Receipt, BankTxn, Match."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Optional


class TenderKind(str, Enum):
    """Classification of EPOS tender for bank matching."""

    ELECTRONIC = "electronic"  # Card, Transfer -> eligible to match bank credits
    CASH = "cash"  # Not expected as Moniepoint credit
    MIXED = "mixed"  # Cash/Transfer, Card/Cash, etc. -> review


class MatchStatus(str, Enum):
    """Match outcome for a receipt or bank credit."""

    MATCHED = "MATCHED"
    UNMATCHED_NO_BANK_CREDIT = "UNMATCHED_NO_BANK_CREDIT"
    UNMATCHED_CASH = "UNMATCHED_CASH"
    REVIEW_MIXED_TENDER = "REVIEW_MIXED_TENDER"
    REVIEW_COLLISION = "REVIEW_COLLISION"
    REVIEW_MULTIPLE_CANDIDATES = "REVIEW_MULTIPLE_CANDIDATES"
    REVIEW_TRANSFER_MATCH = "REVIEW_TRANSFER_MATCH"  # strict transfer-like match (review)
    REVIEW_RELAXED_MATCH = "REVIEW_RELAXED_MATCH"  # matched in relaxed pass (review)
    REVIEW_BUNDLED_MATCH = "REVIEW_BUNDLED_MATCH"  # matched as part of receipt bundle (review)
    UNMATCHED_BANK = "UNMATCHED_BANK"  # bank credit with no EPOS receipt


@dataclass
class Receipt:
    """Reconstructed EPOS receipt from BookKeeping line-level CSV."""

    receipt_id: str
    receipt_datetime: datetime
    location_name: str
    device_name: str
    staff: str
    tender: str  # raw value e.g. Card, Cash, Transfer, Cash/Transfer
    tender_kind: TenderKind
    line_count: int
    gross_amount: float
    service_fee: float
    expected_credit: float
    expected_emtl: float  # 0 or 50
    collision: bool = False  # True if multiple receipts shared same group key


@dataclass
class BankTxn:
    """Normalized bank statement transaction."""

    account_number: str
    posted_at: datetime
    narration: str
    reference: str
    debit: float
    credit: float
    balance: Optional[float] = None
    is_emtl: bool = False  # Electronic Money Transfer Levy debit
    linked_emtl_amount: float = 0.0  # set when this credit is linked to an EMTL debit
    linked_emtl_posted_at: Optional[datetime] = None


@dataclass
class Match:
    """A matched pair of Receipt and BankTxn (or unmatched with status)."""

    receipt: Receipt
    bank_txn: Optional[BankTxn] = None
    status: MatchStatus = MatchStatus.UNMATCHED_NO_BANK_CREDIT
    time_diff_minutes: Optional[float] = None
    amount_diff: Optional[float] = None
    account_name: Optional[str] = None  # from mapping
    candidates_count: int = 0  # number of candidate bank credits before assignment
    unmatched_reason: Optional[str] = None  # diagnostic reason when not MATCHED
    closest_amount_diff: Optional[float] = None  # closest absolute amount gap among candidate bank credits
    closest_time_diff_minutes: Optional[float] = None  # closest signed time gap among candidate bank credits
    confidence_tier: str = ""  # STRICT, RELAXED, BUNDLE
    fee_adjustment_applied: float = 0.0  # learned adjustment applied to expected credit
    expected_credit_used: Optional[float] = None  # expected credit after learned adjustment
    bundle_id: Optional[str] = None
    bundle_size: int = 0
    bundle_total_expected: Optional[float] = None
