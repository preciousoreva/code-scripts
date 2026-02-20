"""Learn and apply fee-adjustment offsets from strict matched receipts."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from statistics import median
from typing import Dict, Iterable, List, Tuple

from code_scripts.reconciliation.models import Match, MatchStatus, Receipt


def narration_channel(narration: str) -> str:
    """Classify bank narration into coarse inflow channels."""
    text = (narration or "").strip().lower()
    if "purchase for" in text:
        return "purchase"
    if any(k in text for k in ("transfer", "trf", "mfds", "nip ")):
        return "transfer"
    return "other"


@dataclass
class FeeAdjustmentModel:
    """
    Hierarchical lookup model for expected-credit offsets.

    Offsets are learned from strict matches where:
    delta = bank_credit - receipt.expected_credit
    """

    offsets: Dict[Tuple[str, ...], float]
    support: Dict[Tuple[str, ...], int]
    global_offset: float = 0.0
    enabled: bool = False

    def offset_for(self, receipt: Receipt, account_number: str, narration: str) -> float:
        """Resolve best available offset for a receipt/account/narration context."""
        if not self.enabled:
            return 0.0
        channel = narration_channel(narration)
        tender = receipt.tender_kind.value
        keys = (
            ("acct_channel_tender", account_number, channel, tender),
            ("channel_tender", channel, tender),
            ("acct_channel", account_number, channel),
            ("channel", channel),
            ("acct", account_number),
        )
        for key in keys:
            if key in self.offsets:
                return self.offsets[key]
        return self.global_offset


def _iter_training_rows(matches: Iterable[Match], max_abs_amount_diff: float) -> Iterable[tuple[Match, float]]:
    for m in matches:
        if m.status != MatchStatus.MATCHED:
            continue
        if m.bank_txn is None:
            continue
        if m.amount_diff is None:
            continue
        if abs(m.amount_diff) > max_abs_amount_diff:
            continue
        delta = m.bank_txn.credit - m.receipt.expected_credit
        yield m, delta


def build_fee_adjustment_model(
    matches: List[Match],
    min_samples: int = 5,
    max_abs_offset: float = 20.0,
    max_abs_amount_diff: float = 1.5,
) -> FeeAdjustmentModel:
    """
    Learn offset medians from strict matches.

    Offsets are clipped by max_abs_offset and only groups with min_samples are retained.
    """
    buckets: dict[Tuple[str, ...], List[float]] = defaultdict(list)
    global_deltas: List[float] = []

    for m, delta in _iter_training_rows(matches, max_abs_amount_diff=max_abs_amount_diff):
        if abs(delta) > max_abs_offset:
            continue
        b = m.bank_txn
        if b is None:
            continue
        account = b.account_number
        channel = narration_channel(b.narration)
        tender = m.receipt.tender_kind.value
        keys = (
            ("acct_channel_tender", account, channel, tender),
            ("channel_tender", channel, tender),
            ("acct_channel", account, channel),
            ("channel", channel),
            ("acct", account),
        )
        for key in keys:
            buckets[key].append(delta)
        global_deltas.append(delta)

    if len(global_deltas) < min_samples:
        return FeeAdjustmentModel(offsets={}, support={}, global_offset=0.0, enabled=False)

    offsets: Dict[Tuple[str, ...], float] = {}
    support: Dict[Tuple[str, ...], int] = {}
    for key, vals in buckets.items():
        if len(vals) < min_samples:
            continue
        offsets[key] = float(median(vals))
        support[key] = len(vals)

    global_offset = float(median(global_deltas))
    return FeeAdjustmentModel(
        offsets=offsets,
        support=support,
        global_offset=global_offset,
        enabled=True,
    )
