"""FOK / FAK fill helpers against a local order book (paper only)."""

from __future__ import annotations

from decimal import Decimal

from polymarket_scanner.models import FeeSchedule, FillSlice, OrderBookLevel
from polymarket_scanner.simulation.orderbook_walker import walk_buy_asks, walk_sell_bids

ZERO = Decimal("0")


def fill_buy(
    asks: list[OrderBookLevel],
    quantity: Decimal,
    schedule: FeeSchedule | None,
    *,
    fees_enabled: bool | None,
    tif: str,
) -> tuple[Decimal, Decimal, Decimal, list[FillSlice], str]:
    """
    Buy from asks.
    FOK: fill entire quantity or nothing.
    FAK: fill available size, remainder cancelled.
    """
    filled, cost, fees, fills, _ = walk_buy_asks(
        asks, quantity, schedule, fees_enabled=fees_enabled, is_taker=True
    )
    tif_u = tif.upper()
    if tif_u == "FOK" and filled < quantity:
        return ZERO, ZERO, ZERO, [], "rejected_fok"
    if filled <= ZERO:
        return ZERO, ZERO, ZERO, [], "no_fill"
    status = "filled" if filled >= quantity else "partial_fak"
    return filled, cost, fees, fills, status


def fill_sell(
    bids: list[OrderBookLevel],
    quantity: Decimal,
    schedule: FeeSchedule | None,
    *,
    fees_enabled: bool | None,
    tif: str,
) -> tuple[Decimal, Decimal, Decimal, list[FillSlice], str]:
    filled, proceeds, fees, fills, _ = walk_sell_bids(
        bids, quantity, schedule, fees_enabled=fees_enabled, is_taker=True
    )
    tif_u = tif.upper()
    if tif_u == "FOK" and filled < quantity:
        return ZERO, ZERO, ZERO, [], "rejected_fok"
    if filled <= ZERO:
        return ZERO, ZERO, ZERO, [], "no_fill"
    status = "filled" if filled >= quantity else "partial_fak"
    return filled, proceeds, fees, fills, status
