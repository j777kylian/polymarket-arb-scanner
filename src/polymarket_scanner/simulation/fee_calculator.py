"""Polymarket taker fee calculator — Decimal precision, official rounding."""

from __future__ import annotations

from decimal import ROUND_HALF_UP, Decimal

from polymarket_scanner.logging_config import get_logger
from polymarket_scanner.models import FeeSchedule

logger = get_logger(__name__)

FEE_QUANT = Decimal("0.00001")  # 5 decimal places; min fee unit
ZERO = Decimal("0")


def round_fee(amount: Decimal) -> Decimal:
    """Round fee to 5 decimal places (official precision). Sub-min rounds to 0."""
    if amount <= ZERO:
        return ZERO
    rounded = amount.quantize(FEE_QUANT, rounding=ROUND_HALF_UP)
    if rounded < FEE_QUANT:
        return ZERO
    return rounded


def calculate_taker_fee(
    shares: Decimal,
    price: Decimal,
    schedule: FeeSchedule | None,
    *,
    fees_enabled: bool | None = True,
    is_taker: bool = True,
) -> Decimal:
    """
    Official formula:
        fee = C × feeRate × (p × (1 - p)) ^ exponent

    Rounded to 5 decimal places. Makers pay 0 when takerOnly=True.

    Safety: only treat as fee-free when ``fees_enabled is False``.
    If the schedule is missing while fees are enabled or unknown, apply a
    conservative rate (0.07) so fee markets are not silently treated as free.
    """
    if fees_enabled is False:
        return ZERO

    effective = schedule
    if effective is None:
        logger.warning(
            "fee schedule missing (fees_enabled=%s); using conservative rate=0.07",
            fees_enabled,
        )
        effective = FeeSchedule(
            rate=Decimal("0.07"), exponent=Decimal("1"), taker_only=True
        )

    if not is_taker and effective.taker_only:
        return ZERO
    if shares <= ZERO or price <= ZERO:
        return ZERO

    rate = effective.rate
    exponent = effective.exponent if effective.exponent is not None else Decimal("1")
    if rate <= ZERO:
        return ZERO

    # Guard unexpected schedule shapes
    if rate > Decimal("1"):
        logger.warning("Unexpected fee rate > 1: %s — using API value anyway", rate)

    p = price
    if p > Decimal("1"):
        logger.warning("Price > 1 in fee calc: %s", p)
        p = Decimal("1")

    price_component = p * (Decimal("1") - p)
    if exponent != Decimal("1"):
        price_component = price_component**exponent

    raw = shares * rate * price_component
    return round_fee(raw)


def calculate_fills_fee(
    fills: list[tuple[Decimal, Decimal]],
    schedule: FeeSchedule | None,
    *,
    fees_enabled: bool | None = True,
    is_taker: bool = True,
) -> Decimal:
    """Sum taker fees across (price, size) fill slices."""
    total = ZERO
    for price, size in fills:
        total += calculate_taker_fee(
            size, price, schedule, fees_enabled=fees_enabled, is_taker=is_taker
        )
    return total
