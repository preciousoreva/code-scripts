"""Moniepoint fee model and expected credit/EMTL calculations."""

from __future__ import annotations


def service_fee(gross_amount: float) -> float:
    """
    Service fee: min(0.005 * gross, 100).
    """
    return min(0.005 * gross_amount, 100.0)


def expected_credit(gross_amount: float) -> float:
    """
    Expected bank credit after service fee: gross - service_fee.
    """
    return gross_amount - service_fee(gross_amount)


def expected_emtl(gross_amount: float) -> float:
    """
    EMTL (Electronic Money Transfer Levy): 50 if gross >= 10000 else 0.
    Usually appears as separate debit line, not in the credit.
    """
    return 50.0 if gross_amount >= 10_000 else 0.0


def apply_fee_model(gross_amount: float) -> tuple[float, float, float]:
    """
    Returns (service_fee, expected_credit, expected_emtl) for a given gross.
    """
    sf = service_fee(gross_amount)
    ec = expected_credit(gross_amount)
    emtl = expected_emtl(gross_amount)
    return sf, ec, emtl
