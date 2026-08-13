"""Paper trading: delayed FOK/FAK, one-leg risk, YES+NO merge, capital recycle.

Never places real orders. Uses local books only.
Cash movements are booked once: buy cost, buy fees, merge proceeds, sell proceeds, sell fees.
"""

from __future__ import annotations

import asyncio
import threading
from dataclasses import dataclass
from decimal import Decimal

from sqlalchemy import select

from polymarket_scanner.config import get_config
from polymarket_scanner.database import (
    PaperAccountRow,
    PaperTradeRow,
    decimal_to_str,
    session_scope,
    utcnow,
)
from polymarket_scanner.logging_config import get_logger
from polymarket_scanner.models import MarketInfo, OpportunitySignal, OrderBookSnapshot, OutcomeSide
from polymarket_scanner.safety import assert_trading_disabled
from polymarket_scanner.simulation.fok_fak import fill_buy, fill_sell
from polymarket_scanner.simulation.orderbook_walker import walk_buy_asks

logger = get_logger(__name__)
ZERO = Decimal("0")
ONE = Decimal("1")
_ACCOUNT_LOCK = threading.Lock()
_ASYNC_LOCK: asyncio.Lock | None = None


def _async_lock() -> asyncio.Lock:
    global _ASYNC_LOCK
    if _ASYNC_LOCK is None:
        _ASYNC_LOCK = asyncio.Lock()
    return _ASYNC_LOCK


def _d(value: str | None, default: str = "0") -> Decimal:
    return Decimal(value or default)


def _avg(total: Decimal, qty: Decimal) -> Decimal:
    return (total / qty) if qty else ZERO


@dataclass(frozen=True)
class PaperSettlement:
    """Single-entry cashflow for a complete-set paper fill."""

    matched: Decimal
    remaining_inventory: Decimal
    remaining_side: str | None
    inventory_cost: Decimal
    buy_cost: Decimal
    buy_fees: Decimal
    merge_proceeds: Decimal
    sell_proceeds: Decimal
    sell_fees: Decimal
    realized_pnl: Decimal
    cash_delta: Decimal

    @property
    def cash_used(self) -> Decimal:
        return self.buy_cost + self.buy_fees + self.sell_fees


def settle_complete_set_cashflow(
    *,
    yes_qty: Decimal,
    yes_cost: Decimal,
    yes_fee: Decimal,
    no_qty: Decimal,
    no_cost: Decimal,
    no_fee: Decimal,
    close_qty: Decimal = ZERO,
    close_proceeds: Decimal = ZERO,
    close_fee: Decimal = ZERO,
    close_side: OutcomeSide | None = None,
) -> PaperSettlement:
    """Book each cash flow once. Residual inventory is not realized P&L."""
    matched = min(yes_qty, no_qty)
    yes_unhedged = max(ZERO, yes_qty - no_qty)
    no_unhedged = max(ZERO, no_qty - yes_qty)

    yes_unit_cost = _avg(yes_cost, yes_qty)
    yes_unit_fee = _avg(yes_fee, yes_qty)
    no_unit_cost = _avg(no_cost, no_qty)
    no_unit_fee = _avg(no_fee, no_qty)

    matched_buy_cost = yes_unit_cost * matched + no_unit_cost * matched
    matched_buy_fees = yes_unit_fee * matched + no_unit_fee * matched
    merge_proceeds = matched * ONE
    merge_pnl = merge_proceeds - matched_buy_cost - matched_buy_fees

    close_qty = max(ZERO, close_qty)
    if close_side == OutcomeSide.YES:
        close_qty = min(close_qty, yes_unhedged)
        closed_cost = yes_unit_cost * close_qty
        closed_fee = yes_unit_fee * close_qty
        remaining_qty = yes_unhedged - close_qty
        remaining_side: str | None = "YES" if remaining_qty > ZERO else None
        remaining_cost = (yes_unit_cost + yes_unit_fee) * remaining_qty
    elif close_side == OutcomeSide.NO:
        close_qty = min(close_qty, no_unhedged)
        closed_cost = no_unit_cost * close_qty
        closed_fee = no_unit_fee * close_qty
        remaining_qty = no_unhedged - close_qty
        remaining_side = "NO" if remaining_qty > ZERO else None
        remaining_cost = (no_unit_cost + no_unit_fee) * remaining_qty
    else:
        closed_cost = ZERO
        closed_fee = ZERO
        remaining_qty = yes_unhedged + no_unhedged
        if yes_unhedged > ZERO:
            remaining_side = "YES"
            remaining_cost = (yes_unit_cost + yes_unit_fee) * yes_unhedged
        elif no_unhedged > ZERO:
            remaining_side = "NO"
            remaining_cost = (no_unit_cost + no_unit_fee) * no_unhedged
        else:
            remaining_side = None
            remaining_cost = ZERO
        close_proceeds = ZERO
        close_fee = ZERO

    close_pnl = close_proceeds - close_fee - closed_cost - closed_fee
    realized = merge_pnl + close_pnl
    buy_cost = yes_cost + no_cost
    buy_fees = yes_fee + no_fee
    cash_delta = merge_proceeds + close_proceeds - buy_cost - buy_fees - close_fee
    return PaperSettlement(
        matched=matched,
        remaining_inventory=remaining_qty,
        remaining_side=remaining_side,
        inventory_cost=remaining_cost,
        buy_cost=buy_cost,
        buy_fees=buy_fees,
        merge_proceeds=merge_proceeds,
        sell_proceeds=close_proceeds,
        sell_fees=close_fee,
        realized_pnl=realized,
        cash_delta=cash_delta,
    )


def get_paper_account() -> tuple[Decimal, Decimal, Decimal]:
    with session_scope() as session:
        row = session.scalar(select(PaperAccountRow).limit(1))
        if row is None:
            cfg = get_config()
            row = PaperAccountRow(
                cash=str(cfg.paper.starting_capital), occupied="0", realized_pnl="0"
            )
            session.add(row)
            session.flush()
        return _d(row.cash), _d(row.occupied), _d(row.realized_pnl)


def estimate_buy_notional(
    asks: list,
    qty: Decimal,
    market: MarketInfo,
) -> tuple[Decimal, Decimal]:
    filled, cost, fees, _fills, _status = walk_buy_asks(
        asks,
        qty,
        market.fee_schedule,
        fees_enabled=market.fees_enabled,
        is_taker=True,
    )
    return filled, cost + fees


def affordable_quantity(
    market: MarketInfo,
    yes_book: OrderBookSnapshot,
    no_book: OrderBookSnapshot,
    desired: Decimal,
    cash: Decimal,
    *,
    first_leg: str,
    safety_buffer: Decimal,
) -> Decimal:
    """Largest qty whose sequential buy cost + buffer fits in cash."""
    min_size = market.minimum_order_size or Decimal("5")
    if desired <= ZERO:
        return ZERO
    budget = cash - safety_buffer
    if budget <= ZERO:
        return ZERO
    first = first_leg.upper()

    def _fit(qty: Decimal) -> tuple[Decimal, Decimal]:
        if first == "YES":
            f_qty, f_notional = estimate_buy_notional(yes_book.asks, qty, market)
            _s_qty, s_notional = estimate_buy_notional(no_book.asks, f_qty, market)
        else:
            f_qty, f_notional = estimate_buy_notional(no_book.asks, qty, market)
            _s_qty, s_notional = estimate_buy_notional(yes_book.asks, f_qty, market)
        return f_qty, f_notional + s_notional

    filled, need = _fit(desired)
    if filled > ZERO and need <= budget:
        return ZERO if filled < min_size else filled

    lo, hi = ZERO, desired
    best = ZERO
    for _ in range(40):
        mid = (lo + hi) / Decimal("2")
        if mid <= ZERO:
            break
        f_qty, need = _fit(mid)
        if need <= budget and f_qty > ZERO:
            best = f_qty
            lo = mid
        else:
            hi = mid
        if hi - lo < Decimal("0.0001"):
            break
    if 0 < best < min_size:
        return ZERO
    return best


def execute_paper_complete_set(
    market: MarketInfo,
    signal: OpportunitySignal,
    yes_book: OrderBookSnapshot,
    no_book: OrderBookSnapshot,
    *,
    episode_id: int | None = None,
    delay_ms: int | None = None,
    tif: str | None = None,
    first_leg: str | None = None,
    force_close_unhedged: bool | None = None,
    skip_min_profit: bool = False,
) -> dict[str, str] | None:
    """
    Simulate sequential legs against local books. Read-only — no CLOB orders.
    """
    assert_trading_disabled()
    cfg = get_config()
    paper_cfg = cfg.paper
    if signal.direction.value != "forward":
        return None
    if not skip_min_profit and signal.net_profit < paper_cfg.min_net_profit:
        return None

    tif = (tif or paper_cfg.time_in_force).upper()
    first = (first_leg or paper_cfg.first_leg).upper()
    delay = delay_ms if delay_ms is not None else paper_cfg.delay_ms
    force_close = (
        paper_cfg.force_close_unhedged if force_close_unhedged is None else force_close_unhedged
    )
    qty = signal.quantity
    if qty <= ZERO:
        return None

    schedule = market.fee_schedule
    fees_enabled = market.fees_enabled
    min_size = market.minimum_order_size or Decimal("5")
    safety_buffer = Decimal(str(cfg.simulation.safety_buffer))

    with _ACCOUNT_LOCK:
        cash_before, _occ, _pnl = get_paper_account()
        affordable = affordable_quantity(
            market,
            yes_book,
            no_book,
            qty,
            cash_before,
            first_leg=first,
            safety_buffer=safety_buffer,
        )
        if affordable <= ZERO or affordable < min_size:
            _record_trade(
                market.market_id,
                episode_id,
                tif,
                delay,
                "rejected_insufficient_capital",
                ZERO,
                ZERO,
                ZERO,
                ZERO,
                ZERO,
                ZERO,
                ZERO,
                "affordable qty below minimum_order_size or cash",
                cash_after=cash_before,
            )
            return {"status": "rejected_insufficient_capital", "pnl": "0"}

        qty = min(qty, affordable)

        if first == "YES":
            first_book, second_book = yes_book, no_book
            first_side, second_side = OutcomeSide.YES, OutcomeSide.NO
        else:
            first_book, second_book = no_book, yes_book
            first_side, second_side = OutcomeSide.NO, OutcomeSide.YES

        f_qty, f_cost, f_fee, _f_fills, f_status = fill_buy(
            first_book.asks, qty, schedule, fees_enabled=fees_enabled, tif=tif
        )
        if f_status == "rejected_fok" or f_qty <= ZERO:
            _record_trade(
                market.market_id,
                episode_id,
                tif,
                delay,
                "rejected_fok" if f_status == "rejected_fok" else "no_fill",
                ZERO,
                ZERO,
                ZERO,
                ZERO,
                ZERO,
                ZERO,
                ZERO,
                f"first_leg {first_side.value} {f_status}",
                cash_after=cash_before,
            )
            return {"status": f_status, "pnl": "0"}

        s_qty, s_cost, s_fee, _s_fills, s_status = fill_buy(
            second_book.asks, f_qty, schedule, fees_enabled=fees_enabled, tif=tif
        )

        yes_qty = f_qty if first_side == OutcomeSide.YES else s_qty
        no_qty = f_qty if first_side == OutcomeSide.NO else s_qty
        yes_cost = f_cost if first_side == OutcomeSide.YES else s_cost
        no_cost = f_cost if first_side == OutcomeSide.NO else s_cost
        yes_fee = f_fee if first_side == OutcomeSide.YES else s_fee
        no_fee = f_fee if first_side == OutcomeSide.NO else s_fee

        close_qty = ZERO
        close_proceeds = ZERO
        close_fee = ZERO
        close_side: OutcomeSide | None = None
        unhedged = abs(yes_qty - no_qty)
        if unhedged > ZERO and force_close:
            if yes_qty > no_qty:
                close_side = OutcomeSide.YES
                close_qty, close_proceeds, close_fee, _, _ = fill_sell(
                    yes_book.bids, unhedged, schedule, fees_enabled=fees_enabled, tif="FAK"
                )
            else:
                close_side = OutcomeSide.NO
                close_qty, close_proceeds, close_fee, _, _ = fill_sell(
                    no_book.bids, unhedged, schedule, fees_enabled=fees_enabled, tif="FAK"
                )

        settlement = settle_complete_set_cashflow(
            yes_qty=yes_qty,
            yes_cost=yes_cost,
            yes_fee=yes_fee,
            no_qty=no_qty,
            no_cost=no_cost,
            no_fee=no_fee,
            close_qty=close_qty,
            close_proceeds=close_proceeds,
            close_fee=close_fee,
            close_side=close_side,
        )

        if settlement.matched > ZERO and s_status != "rejected_fok":
            status = "merged" if settlement.remaining_inventory <= ZERO else "one_leg_merged"
        elif f_qty > ZERO and s_qty <= ZERO:
            status = "one_leg"
        else:
            status = s_status

        cash_after = _apply_account(settlement)

        _record_trade(
            market.market_id,
            episode_id,
            tif,
            delay,
            status,
            yes_qty,
            no_qty,
            settlement.matched,
            settlement.remaining_inventory,
            settlement.cash_used,
            settlement.merge_proceeds,
            settlement.realized_pnl,
            (
                f"delay_ms={delay} first={first_side.value} second={second_side.value} "
                f"first_status={f_status} second_status={s_status} merge={settlement.matched} "
                f"sell_proceeds={settlement.sell_proceeds} sell_fees={settlement.sell_fees} "
                f"inventory_cost={settlement.inventory_cost} remaining={settlement.remaining_inventory}"
            ),
            cash_after=cash_after,
            remaining_inventory=settlement.remaining_inventory,
            inventory_cost=settlement.inventory_cost,
            sell_proceeds=settlement.sell_proceeds,
            buy_fees=settlement.buy_fees,
            sell_fees=settlement.sell_fees,
        )
        logger.info(
            "Paper trade %s %s realized_pnl=%s cash_after=%s remaining=%s (simulated)",
            market.market_id,
            status,
            settlement.realized_pnl,
            cash_after,
            settlement.remaining_inventory,
        )
        return {
            "status": status,
            "pnl": format(settlement.realized_pnl, "f"),
            "realized_pnl": format(settlement.realized_pnl, "f"),
            "cash_after": format(cash_after, "f"),
            "remaining_inventory": format(settlement.remaining_inventory, "f"),
            "inventory_cost": format(settlement.inventory_cost, "f"),
        }


async def execute_paper_complete_set_async(
    market: MarketInfo,
    signal: OpportunitySignal,
    yes_book: OrderBookSnapshot,
    no_book: OrderBookSnapshot,
    *,
    episode_id: int | None = None,
    delay_ms: int | None = None,
    tif: str | None = None,
    first_leg: str | None = None,
    force_close_unhedged: bool | None = None,
    skip_min_profit: bool = False,
) -> dict[str, str] | None:
    """Serialize paper fills across concurrent asyncio tasks."""
    async with _async_lock():
        return execute_paper_complete_set(
            market,
            signal,
            yes_book,
            no_book,
            episode_id=episode_id,
            delay_ms=delay_ms,
            tif=tif,
            first_leg=first_leg,
            force_close_unhedged=force_close_unhedged,
            skip_min_profit=skip_min_profit,
        )


def _apply_account(settlement: PaperSettlement) -> Decimal:
    with session_scope() as session:
        row = session.scalar(select(PaperAccountRow).limit(1))
        if row is None:
            row = PaperAccountRow(cash=str(get_config().paper.starting_capital))
            session.add(row)
            session.flush()
        cash = _d(row.cash) + settlement.cash_delta
        occupied = _d(row.occupied) + settlement.inventory_cost
        row.cash = format(cash, "f")
        row.occupied = format(occupied, "f")
        row.realized_pnl = format(_d(row.realized_pnl) + settlement.realized_pnl, "f")
        row.updated_at = utcnow()
        return cash


def _record_trade(
    market_id: str,
    episode_id: int | None,
    tif: str,
    delay_ms: int,
    status: str,
    yes_qty: Decimal,
    no_qty: Decimal,
    matched: Decimal,
    unhedged: Decimal,
    cash_used: Decimal,
    merge_proceeds: Decimal,
    pnl: Decimal,
    details: str,
    cash_after: Decimal | None = None,
    remaining_inventory: Decimal | None = None,
    inventory_cost: Decimal | None = None,
    sell_proceeds: Decimal | None = None,
    buy_fees: Decimal | None = None,
    sell_fees: Decimal | None = None,
) -> None:
    with session_scope() as session:
        session.add(
            PaperTradeRow(
                market_id=market_id,
                episode_id=episode_id,
                tif=tif,
                delay_ms=delay_ms,
                status=status,
                yes_qty=decimal_to_str(yes_qty) or "0",
                no_qty=decimal_to_str(no_qty) or "0",
                matched_qty=decimal_to_str(matched) or "0",
                unhedged_qty=decimal_to_str(unhedged) or "0",
                cash_used=decimal_to_str(cash_used) or "0",
                merge_proceeds=decimal_to_str(merge_proceeds) or "0",
                pnl=decimal_to_str(pnl) or "0",
                cash_after=decimal_to_str(cash_after) or "0",
                remaining_inventory=decimal_to_str(remaining_inventory or unhedged) or "0",
                inventory_cost=decimal_to_str(inventory_cost or ZERO) or "0",
                sell_proceeds=decimal_to_str(sell_proceeds or ZERO) or "0",
                buy_fees=decimal_to_str(buy_fees or ZERO) or "0",
                sell_fees=decimal_to_str(sell_fees or ZERO) or "0",
                details=details,
            )
        )
