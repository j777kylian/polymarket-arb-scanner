"""Paper trading: delayed FOK/FAK, one-leg risk, YES+NO merge, capital recycle.

Never places real orders. Uses local books only.
"""

from __future__ import annotations

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

logger = get_logger(__name__)
ZERO = Decimal("0")
ONE = Decimal("1")


def _d(value: str | None, default: str = "0") -> Decimal:
    return Decimal(value or default)


def get_paper_account() -> tuple[Decimal, Decimal, Decimal]:
    with session_scope() as session:
        row = session.scalar(select(PaperAccountRow).limit(1))
        if row is None:
            cfg = get_config()
            row = PaperAccountRow(cash=str(cfg.paper.starting_capital), occupied="0", realized_pnl="0")
            session.add(row)
            session.flush()
        return _d(row.cash), _d(row.occupied), _d(row.realized_pnl)


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
) -> dict[str, str] | None:
    """
    Simulate: wait delay_ms (caller should already have delayed books),
    then FOK/FAK first leg, then second leg on possibly changed book,
    merge matched YES+NO into $1, recycle capital.
    """
    assert_trading_disabled()
    cfg = get_config().paper
    if signal.direction.value != "forward":
        return None
    if signal.net_profit < cfg.min_net_profit:
        return None

    tif = (tif or cfg.time_in_force).upper()
    first = (first_leg or cfg.first_leg).upper()
    delay = delay_ms if delay_ms is not None else cfg.delay_ms
    force_close = (
        cfg.force_close_unhedged if force_close_unhedged is None else force_close_unhedged
    )
    qty = signal.quantity
    if qty <= ZERO:
        return None

    schedule = market.fee_schedule
    fees_enabled = market.fees_enabled

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

    matched = min(yes_qty, no_qty)
    unhedged = abs(yes_qty - no_qty)
    cash_used = yes_cost + no_cost + yes_fee + no_fee
    merge_proceeds = matched * ONE
    close_pnl = ZERO

    if unhedged > ZERO and force_close:
        if yes_qty > no_qty:
            c_qty, proceeds, c_fee, _, _ = fill_sell(
                yes_book.bids, unhedged, schedule, fees_enabled=fees_enabled, tif="FAK"
            )
            extra_cost = (yes_cost / yes_qty * c_qty) if yes_qty else ZERO
            close_pnl = proceeds - extra_cost - c_fee
            unhedged = unhedged - c_qty
            cash_used += c_fee
            merge_proceeds += proceeds
        else:
            c_qty, proceeds, c_fee, _, _ = fill_sell(
                no_book.bids, unhedged, schedule, fees_enabled=fees_enabled, tif="FAK"
            )
            extra_cost = (no_cost / no_qty * c_qty) if no_qty else ZERO
            close_pnl = proceeds - extra_cost - c_fee
            unhedged = unhedged - c_qty
            cash_used += c_fee
            merge_proceeds += proceeds

    pnl = merge_proceeds - cash_used + close_pnl
    if matched > ZERO and s_status != "rejected_fok":
        status = "merged" if unhedged <= ZERO else "one_leg_merged"
    elif f_qty > ZERO and s_qty <= ZERO:
        status = "one_leg"
    else:
        status = s_status

    cash_after = _apply_account(cash_used, merge_proceeds, pnl)

    _record_trade(
        market.market_id,
        episode_id,
        tif,
        delay,
        status,
        yes_qty,
        no_qty,
        matched,
        unhedged,
        cash_used,
        merge_proceeds,
        pnl,
        (
            f"delay_ms={delay} first={first_side.value} second={second_side.value} "
            f"first_status={f_status} second_status={s_status} merge={matched}"
        ),
        cash_after=cash_after,
    )
    logger.info(
        "Paper trade %s %s pnl=%s cash_after=%s", market.market_id, status, pnl, cash_after
    )
    return {"status": status, "pnl": format(pnl, "f"), "cash_after": format(cash_after, "f")}


def _apply_account(cash_used: Decimal, merge_proceeds: Decimal, pnl: Decimal) -> Decimal:
    with session_scope() as session:
        row = session.scalar(select(PaperAccountRow).limit(1))
        if row is None:
            row = PaperAccountRow(cash=str(get_config().paper.starting_capital))
            session.add(row)
            session.flush()
        cash = _d(row.cash) - cash_used + merge_proceeds
        occupied = max(ZERO, cash_used - merge_proceeds)
        row.cash = format(cash, "f")
        row.occupied = format(occupied, "f")
        row.realized_pnl = format(_d(row.realized_pnl) + pnl, "f")
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
                details=details,
            )
        )
