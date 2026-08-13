"""Multi-level order book walker for complete-set arbitrage sizing."""

from __future__ import annotations

from copy import deepcopy
from decimal import Decimal

from polymarket_scanner.models import (
    FeeSchedule,
    FillSlice,
    OrderBookLevel,
    OrderBookSnapshot,
    WalkResult,
)
from polymarket_scanner.simulation.fee_calculator import calculate_taker_fee

ZERO = Decimal("0")
ONE = Decimal("1")


def _apply_depth_factor(levels: list[OrderBookLevel], factor: Decimal) -> list[OrderBookLevel]:
    if factor >= ONE:
        return [OrderBookLevel(price=lvl.price, size=lvl.size) for lvl in levels]
    return [
        OrderBookLevel(price=lvl.price, size=(lvl.size * factor))
        for lvl in levels
        if (lvl.size * factor) > ZERO
    ]


def _apply_ask_slippage(
    levels: list[OrderBookLevel], ticks: int, tick_size: Decimal
) -> list[OrderBookLevel]:
    """Adverse slippage for buys: raise ask prices."""
    if ticks <= 0:
        return levels
    slip = tick_size * Decimal(ticks)
    out: list[OrderBookLevel] = []
    for lvl in levels:
        price = min(ONE, lvl.price + slip)
        if Decimal("0") < price <= ONE:
            out.append(OrderBookLevel(price=price, size=lvl.size))
    return out


def _apply_bid_slippage(
    levels: list[OrderBookLevel], ticks: int, tick_size: Decimal
) -> list[OrderBookLevel]:
    """Adverse slippage for sells: lower bid prices."""
    if ticks <= 0:
        return levels
    slip = tick_size * Decimal(ticks)
    out: list[OrderBookLevel] = []
    for lvl in levels:
        price = max(ZERO, lvl.price - slip)
        if price > ZERO:
            out.append(OrderBookLevel(price=price, size=lvl.size))
    return out


def walk_buy_asks(
    asks: list[OrderBookLevel],
    quantity: Decimal,
    schedule: FeeSchedule | None,
    *,
    fees_enabled: bool | None,
    is_taker: bool = True,
) -> tuple[Decimal, Decimal, Decimal, list[FillSlice], int]:
    """Walk asks to buy `quantity`. Returns filled_qty, cost, fees, fills, levels_used."""
    remaining = quantity
    cost = ZERO
    fees = ZERO
    fills: list[FillSlice] = []
    levels_used = 0
    for lvl in asks:
        if remaining <= ZERO:
            break
        take = min(remaining, lvl.size)
        if take <= ZERO:
            continue
        notional = take * lvl.price
        fee = calculate_taker_fee(
            take, lvl.price, schedule, fees_enabled=fees_enabled, is_taker=is_taker
        )
        fills.append(FillSlice(price=lvl.price, size=take, notional=notional, fee=fee))
        cost += notional
        fees += fee
        remaining -= take
        levels_used += 1
    filled = quantity - remaining
    return filled, cost, fees, fills, levels_used


def walk_sell_bids(
    bids: list[OrderBookLevel],
    quantity: Decimal,
    schedule: FeeSchedule | None,
    *,
    fees_enabled: bool | None,
    is_taker: bool = True,
) -> tuple[Decimal, Decimal, Decimal, list[FillSlice], int]:
    """Walk bids to sell `quantity`. Returns filled_qty, proceeds, fees, fills, levels_used."""
    remaining = quantity
    proceeds = ZERO
    fees = ZERO
    fills: list[FillSlice] = []
    levels_used = 0
    for lvl in bids:
        if remaining <= ZERO:
            break
        take = min(remaining, lvl.size)
        if take <= ZERO:
            continue
        notional = take * lvl.price
        fee = calculate_taker_fee(
            take, lvl.price, schedule, fees_enabled=fees_enabled, is_taker=is_taker
        )
        fills.append(FillSlice(price=lvl.price, size=take, notional=notional, fee=fee))
        proceeds += notional
        fees += fee
        remaining -= take
        levels_used += 1
    filled = quantity - remaining
    return filled, proceeds, fees, fills, levels_used


def find_optimal_forward_arb(
    yes_book: OrderBookSnapshot,
    no_book: OrderBookSnapshot,
    schedule: FeeSchedule | None,
    *,
    fees_enabled: bool | None = False,
    operational_cost: Decimal = ZERO,
    safety_buffer: Decimal = ZERO,
    depth_factor: Decimal = ONE,
    slippage_ticks: int = 0,
    is_taker: bool = True,
) -> WalkResult | None:
    """
    Pair YES and NO asks level-by-level to maximize net profit for forward complete-set arb.

    Profit(q) = q - CostYES(q) - CostNO(q) - FeeYES - FeeNO - operational_cost - safety_buffer
    Stop when marginal quantity is no longer profitable.
    """
    yes_asks = _apply_ask_slippage(
        _apply_depth_factor(yes_book.asks, depth_factor),
        slippage_ticks,
        yes_book.tick_size,
    )
    no_asks = _apply_ask_slippage(
        _apply_depth_factor(no_book.asks, depth_factor),
        slippage_ticks,
        no_book.tick_size,
    )
    if not yes_asks or not no_asks:
        return None

    # Early exit if best asks already sum >= 1 (before fees)
    if yes_asks[0].price + no_asks[0].price >= ONE:
        return None

    yes_levels = deepcopy(yes_asks)
    no_levels = deepcopy(no_asks)
    yi = 0
    ni = 0

    qty = ZERO
    yes_cost = ZERO
    no_cost = ZERO
    fee_yes = ZERO
    fee_no = ZERO
    yes_fills: list[FillSlice] = []
    no_fills: list[FillSlice] = []
    best: WalkResult | None = None
    prev_net = None

    while yi < len(yes_levels) and ni < len(no_levels):
        y = yes_levels[yi]
        n = no_levels[ni]
        if y.size <= ZERO:
            yi += 1
            continue
        if n.size <= ZERO:
            ni += 1
            continue

        # Continue while gross edge remains; fees may wipe net profit (still a raw signal).
        unit_cost = y.price + n.price
        if unit_cost >= ONE:
            break

        fee_y_unit = calculate_taker_fee(
            ONE, y.price, schedule, fees_enabled=fees_enabled, is_taker=is_taker
        )
        fee_n_unit = calculate_taker_fee(
            ONE, n.price, schedule, fees_enabled=fees_enabled, is_taker=is_taker
        )
        marginal_net_unit = ONE - unit_cost - fee_y_unit - fee_n_unit

        take = min(y.size, n.size)
        y_notional = take * y.price
        n_notional = take * n.price
        y_fee = calculate_taker_fee(
            take, y.price, schedule, fees_enabled=fees_enabled, is_taker=is_taker
        )
        n_fee = calculate_taker_fee(
            take, n.price, schedule, fees_enabled=fees_enabled, is_taker=is_taker
        )

        yes_fills.append(
            FillSlice(price=y.price, size=take, notional=y_notional, fee=y_fee)
        )
        no_fills.append(
            FillSlice(price=n.price, size=take, notional=n_notional, fee=n_fee)
        )

        qty += take
        yes_cost += y_notional
        no_cost += n_notional
        fee_yes += y_fee
        fee_no += n_fee
        y.size -= take
        n.size -= take

        total_cost = yes_cost + no_cost
        gross = qty - total_cost
        net = gross - fee_yes - fee_no - operational_cost - safety_buffer
        yes_vwap = yes_cost / qty if qty else ZERO
        no_vwap = no_cost / qty if qty else ZERO
        result = WalkResult(
            quantity=qty,
            yes_cost=yes_cost,
            no_cost=no_cost,
            yes_vwap=yes_vwap,
            no_vwap=no_vwap,
            total_cost=total_cost,
            gross_profit=gross,
            fee_yes=fee_yes,
            fee_no=fee_no,
            net_profit=net,
            net_profit_per_share=(net / qty) if qty else ZERO,
            net_profit_rate=(net / total_cost) if total_cost else ZERO,
            levels_used_yes=len(yes_fills),
            levels_used_no=len(no_fills),
            marginal_net_profit=marginal_net_unit * take,
            yes_fills=list(yes_fills),
            no_fills=list(no_fills),
            profitable=net > ZERO,
        )
        # Prefer maximum net profit; allow negative-net raw signals when gross > 0.
        if best is None or result.net_profit > best.net_profit:
            best = result
        # Stop once additional shares are not net-profitable (raw signal already kept).
        if marginal_net_unit <= ZERO:
            break
        if prev_net is not None and net < prev_net:
            break
        prev_net = net

    return best


def detect_reverse_top_of_book(
    yes_book: OrderBookSnapshot,
    no_book: OrderBookSnapshot,
) -> tuple[bool, Decimal | None, Decimal | None]:
    """Identify reverse arb at top of book: BidYES + BidNO > 1."""
    if not yes_book.bids or not no_book.bids:
        return False, None, None
    yb = yes_book.best_bid
    nb = no_book.best_bid
    if yb is None or nb is None:
        return False, None, None
    return (yb + nb > ONE), yb, nb
