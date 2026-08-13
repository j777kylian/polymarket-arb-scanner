"""Paper trading: delayed FOK/FAK, one-leg risk, YES+NO merge, capital recycle.

Never places real orders. Uses local books only.
Cash movements are booked once: buy cost, buy fees, merge proceeds, sell proceeds, sell fees.
"""

from __future__ import annotations

import asyncio
import threading
from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Any, Awaitable, Callable

from sqlalchemy import select

from polymarket_scanner.config import AppConfig, PaperConfig, get_config
from polymarket_scanner.database import (
    PaperAccountRow,
    PaperTradeRow,
    StrategyAccountRow,
    StrategyTradeRow,
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


@dataclass
class PaperTradeMeta:
    strategy_id: str = "live_default"
    strategy_version: int = 1
    signal_opportunity_id: int | None = None
    signal_time: datetime | None = None
    target_execution_time: datetime | None = None
    actual_execution_time: datetime | None = None
    first_leg_time: datetime | None = None
    second_leg_time: datetime | None = None
    close_time: datetime | None = None
    t0_book_hashes: str | None = None
    execution_book_hashes: str | None = None
    book_skew_ms: float | None = None
    expected_net_profit: Decimal | None = None
    reject_reason: str | None = None


def _book_hash_pair(yes_book: OrderBookSnapshot | None, no_book: OrderBookSnapshot | None) -> str:
    return f"{getattr(yes_book, 'hash', None) or '-'}|{getattr(no_book, 'hash', None) or '-'}"


def validate_execution_snapshot(
    *,
    episode_id: int | None,
    yes_book: OrderBookSnapshot | None,
    no_book: OrderBookSnapshot | None,
    target_time: datetime,
    now: datetime,
    generation: int | None,
    max_age_seconds: float,
    max_skew_ms: float,
    episode_open: bool | None = None,
    episode_open_fn: Callable[[int | None], bool] | None = None,
) -> str | None:
    """Return a reject_reason or None if the pair is tradable."""
    if episode_id is not None:
        open_flag = episode_open
        if open_flag is None:
            if episode_open_fn is not None:
                open_flag = episode_open_fn(episode_id)
            else:
                from polymarket_scanner.scanners.opportunity_tracker import episode_is_open

                open_flag = episode_is_open(episode_id)
        if not open_flag:
            return "episode_closed"
    if yes_book is None or no_book is None:
        return "books_not_ready"
    yes_gen = yes_book.connection_generation
    no_gen = no_book.connection_generation
    if yes_gen is not None and no_gen is not None and yes_gen != no_gen:
        return "generation_mismatch"
    if generation is not None and (yes_gen != generation or no_gen != generation):
        return "generation_mismatch"
    yes_age = (now - yes_book.fetched_at).total_seconds()
    no_age = (now - no_book.fetched_at).total_seconds()
    if yes_age > max_age_seconds or no_age > max_age_seconds:
        return "stale_books"
    skew_ms = abs((yes_book.fetched_at - no_book.fetched_at).total_seconds() * 1000.0)
    if skew_ms > max_skew_ms:
        return "books_skewed"
    snapshot_time = min(yes_book.fetched_at, no_book.fetched_at)
    if snapshot_time < target_time:
        return "snapshot_before_target"
    return None


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
    yes_book_second: OrderBookSnapshot | None = None,
    no_book_second: OrderBookSnapshot | None = None,
    yes_book_close: OrderBookSnapshot | None = None,
    no_book_close: OrderBookSnapshot | None = None,
    meta: PaperTradeMeta | None = None,
    paper_cfg: PaperConfig | None = None,
    app_cfg: AppConfig | None = None,
    account_kind: str = "live",
) -> dict[str, str] | None:
    """
    Simulate sequential legs against local books. Read-only — no CLOB orders.
    First-leg books are `yes_book`/`no_book`. Second/close books default to the same
    snapshots so existing unit tests keep a single-book path.
    """
    assert_trading_disabled()
    cfg = app_cfg or get_config()
    paper = paper_cfg or cfg.paper
    meta = meta or PaperTradeMeta(
        strategy_id=paper.strategy_id,
        strategy_version=paper.strategy_version,
        expected_net_profit=signal.net_profit,
        signal_time=signal.discovered_at,
    )
    if signal.direction.value != "forward":
        return None
    if not skip_min_profit and signal.net_profit < paper.min_net_profit:
        _reject(
            market.market_id,
            episode_id,
            "min_net_profit",
            paper,
            delay_ms,
            meta,
            account_kind=account_kind,
            details=f"net_profit={signal.net_profit} < {paper.min_net_profit}",
        )
        return {"status": "rejected", "reject_reason": "min_net_profit", "pnl": "0"}
    if paper.min_profit_per_share > ZERO and signal.net_profit_per_share < paper.min_profit_per_share:
        _reject(
            market.market_id,
            episode_id,
            "min_profit_per_share",
            paper,
            delay_ms,
            meta,
            account_kind=account_kind,
        )
        return {"status": "rejected", "reject_reason": "min_profit_per_share", "pnl": "0"}
    if paper.minimum_quantity > ZERO and signal.quantity < paper.minimum_quantity:
        _reject(
            market.market_id,
            episode_id,
            "minimum_quantity",
            paper,
            delay_ms,
            meta,
            account_kind=account_kind,
        )
        return {"status": "rejected", "reject_reason": "minimum_quantity", "pnl": "0"}

    tif = (tif or paper.time_in_force).upper()
    first = (first_leg or paper.first_leg).upper()
    delay = delay_ms if delay_ms is not None else paper.first_leg_delay_ms
    force_close = (
        paper.force_close_unhedged if force_close_unhedged is None else force_close_unhedged
    )
    qty = signal.quantity
    if qty <= ZERO:
        _reject(
            market.market_id,
            episode_id,
            "zero_quantity",
            paper,
            delay,
            meta,
            account_kind=account_kind,
        )
        return {"status": "rejected", "reject_reason": "zero_quantity", "pnl": "0"}

    yes_second = yes_book_second or yes_book
    no_second = no_book_second or no_book
    yes_close = yes_book_close or yes_book
    no_close = no_book_close or no_book

    schedule = market.fee_schedule
    fees_enabled = market.fees_enabled
    min_size = market.minimum_order_size or Decimal("5")
    safety_buffer = Decimal(str(cfg.simulation.safety_buffer))

    with _ACCOUNT_LOCK:
        cash_before, _occ, _pnl = _get_account(account_kind, meta)
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
                meta=meta,
                reject_reason="rejected_insufficient_capital",
                account_kind=account_kind,
            )
            return {"status": "rejected_insufficient_capital", "reject_reason": "rejected_insufficient_capital", "pnl": "0"}

        qty = min(qty, affordable)

        if first == "YES":
            first_book, second_book = yes_book, no_second
            first_side, second_side = OutcomeSide.YES, OutcomeSide.NO
        else:
            first_book, second_book = no_book, yes_second
            first_side, second_side = OutcomeSide.NO, OutcomeSide.YES

        f_qty, f_cost, f_fee, _f_fills, f_status = fill_buy(
            first_book.asks, qty, schedule, fees_enabled=fees_enabled, tif=tif
        )
        if f_status == "rejected_fok" or f_qty <= ZERO:
            reason = "rejected_fok" if f_status == "rejected_fok" else "no_fill"
            _record_trade(
                market.market_id,
                episode_id,
                tif,
                delay,
                reason,
                ZERO,
                ZERO,
                ZERO,
                ZERO,
                ZERO,
                ZERO,
                ZERO,
                f"first_leg {first_side.value} {f_status}",
                cash_after=cash_before,
                meta=meta,
                reject_reason=reason,
                account_kind=account_kind,
            )
            return {"status": f_status, "reject_reason": reason, "pnl": "0"}

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
                    yes_close.bids, unhedged, schedule, fees_enabled=fees_enabled, tif="FAK"
                )
            else:
                close_side = OutcomeSide.NO
                close_qty, close_proceeds, close_fee, _, _ = fill_sell(
                    no_close.bids, unhedged, schedule, fees_enabled=fees_enabled, tif="FAK"
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

        if meta.execution_book_hashes is None:
            meta.execution_book_hashes = (
                f"1:{_book_hash_pair(yes_book, no_book)};"
                f"2:{_book_hash_pair(yes_second, no_second)};"
                f"c:{_book_hash_pair(yes_close, no_close)}"
            )
        if meta.book_skew_ms is None:
            meta.book_skew_ms = abs(
                (yes_book.fetched_at - no_book.fetched_at).total_seconds() * 1000.0
            )
        if meta.actual_execution_time is None:
            meta.actual_execution_time = utcnow()

        cash_after = _apply_account(settlement, account_kind=account_kind, meta=meta)

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
            meta=meta,
            account_kind=account_kind,
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
            "first_leg_hash": yes_book.hash or "",
            "second_leg_hash": yes_second.hash or "",
            "first_leg_time": yes_book.fetched_at.isoformat(),
            "second_leg_time": yes_second.fetched_at.isoformat(),
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
    yes_book_second: OrderBookSnapshot | None = None,
    no_book_second: OrderBookSnapshot | None = None,
    yes_book_close: OrderBookSnapshot | None = None,
    no_book_close: OrderBookSnapshot | None = None,
    meta: PaperTradeMeta | None = None,
    paper_cfg: PaperConfig | None = None,
    app_cfg: AppConfig | None = None,
    account_kind: str = "live",
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
            yes_book_second=yes_book_second,
            no_book_second=no_book_second,
            yes_book_close=yes_book_close,
            no_book_close=no_book_close,
            meta=meta,
            paper_cfg=paper_cfg,
            app_cfg=app_cfg,
            account_kind=account_kind,
        )


def _get_account(account_kind: str, meta: PaperTradeMeta) -> tuple[Decimal, Decimal, Decimal]:
    if account_kind == "strategy":
        return get_strategy_account(meta.strategy_id, meta.strategy_version)
    return get_paper_account()


def get_strategy_account(strategy_id: str, version: int) -> tuple[Decimal, Decimal, Decimal]:
    with session_scope() as session:
        row = session.scalar(
            select(StrategyAccountRow).where(
                StrategyAccountRow.strategy_id == strategy_id,
                StrategyAccountRow.version == version,
            )
        )
        if row is None:
            cfg = get_config()
            row = StrategyAccountRow(
                strategy_id=strategy_id,
                version=version,
                cash=str(cfg.paper.starting_capital),
                occupied="0",
                realized_pnl="0",
                peak_equity=str(cfg.paper.starting_capital),
                max_drawdown="0",
            )
            session.add(row)
            session.flush()
        return _d(row.cash), _d(row.occupied), _d(row.realized_pnl)


def _reject(
    market_id: str,
    episode_id: int | None,
    reason: str,
    paper: PaperConfig,
    delay_ms: int | None,
    meta: PaperTradeMeta,
    *,
    account_kind: str,
    details: str | None = None,
    cash_after: Decimal | None = None,
) -> None:
    delay = delay_ms if delay_ms is not None else paper.first_leg_delay_ms
    cash = cash_after
    if cash is None:
        cash, _, _ = _get_account(account_kind, meta)
    meta.reject_reason = reason
    _record_trade(
        market_id,
        episode_id,
        paper.time_in_force,
        delay,
        "rejected",
        ZERO,
        ZERO,
        ZERO,
        ZERO,
        ZERO,
        ZERO,
        ZERO,
        details or reason,
        cash_after=cash,
        meta=meta,
        reject_reason=reason,
        account_kind=account_kind,
    )


def _apply_equity(cash: Decimal, occupied: Decimal, peak: Decimal, max_dd: Decimal) -> tuple[Decimal, Decimal, Decimal]:
    marked = occupied
    equity = cash + marked
    peak = max(peak, equity)
    drawdown = peak - equity
    max_dd = max(max_dd, drawdown)
    return peak, max_dd, marked


def _apply_account(
    settlement: PaperSettlement,
    *,
    account_kind: str = "live",
    meta: PaperTradeMeta | None = None,
) -> Decimal:
    with session_scope() as session:
        if account_kind == "strategy" and meta is not None:
            row = session.scalar(
                select(StrategyAccountRow).where(
                    StrategyAccountRow.strategy_id == meta.strategy_id,
                    StrategyAccountRow.version == meta.strategy_version,
                )
            )
            if row is None:
                start = get_config().paper.starting_capital
                row = StrategyAccountRow(
                    strategy_id=meta.strategy_id,
                    version=meta.strategy_version,
                    cash=str(start),
                    occupied="0",
                    realized_pnl="0",
                    peak_equity=str(start),
                    max_drawdown="0",
                )
                session.add(row)
                session.flush()
            cash = _d(row.cash) + settlement.cash_delta
            occupied = _d(row.occupied) + settlement.inventory_cost
            peak, max_dd, marked = _apply_equity(
                cash, occupied, _d(row.peak_equity, str(cash)), _d(row.max_drawdown)
            )
            row.cash = format(cash, "f")
            row.occupied = format(occupied, "f")
            row.realized_pnl = format(_d(row.realized_pnl) + settlement.realized_pnl, "f")
            row.peak_equity = format(peak, "f")
            row.max_drawdown = format(max_dd, "f")
            row.updated_at = utcnow()
            return cash

        paper_row = session.scalar(select(PaperAccountRow).limit(1))
        if paper_row is None:
            start = get_config().paper.starting_capital
            paper_row = PaperAccountRow(cash=str(start), peak_equity=str(start))
            session.add(paper_row)
            session.flush()
        cash = _d(paper_row.cash) + settlement.cash_delta
        occupied = _d(paper_row.occupied) + settlement.inventory_cost
        peak, max_dd, marked = _apply_equity(
            cash, occupied, _d(paper_row.peak_equity, str(cash)), _d(paper_row.max_drawdown)
        )
        paper_row.cash = format(cash, "f")
        paper_row.occupied = format(occupied, "f")
        paper_row.realized_pnl = format(_d(paper_row.realized_pnl) + settlement.realized_pnl, "f")
        paper_row.peak_equity = format(peak, "f")
        paper_row.max_drawdown = format(max_dd, "f")
        paper_row.marked_inventory = format(marked, "f")
        paper_row.updated_at = utcnow()
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
    meta: PaperTradeMeta | None = None,
    reject_reason: str | None = None,
    account_kind: str = "live",
) -> None:
    meta = meta or PaperTradeMeta()
    reason = reject_reason or meta.reject_reason
    realized = decimal_to_str(pnl) or "0"
    inv = decimal_to_str(inventory_cost or ZERO) or "0"
    remaining = decimal_to_str(remaining_inventory or unhedged) or "0"
    cash_s = decimal_to_str(cash_after) or "0"
    with session_scope() as session:
        if account_kind == "strategy":
            session.add(
                StrategyTradeRow(
                    strategy_id=meta.strategy_id,
                    strategy_version=meta.strategy_version,
                    market_id=market_id,
                    episode_id=episode_id,
                    status=status,
                    reject_reason=reason,
                    realized_pnl=realized,
                    inventory_cost=inv,
                    cash_after=cash_s,
                    remaining_inventory=remaining,
                    details=details,
                )
            )
            return
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
                pnl=realized,
                cash_after=cash_s,
                remaining_inventory=remaining,
                inventory_cost=inv,
                sell_proceeds=decimal_to_str(sell_proceeds or ZERO) or "0",
                buy_fees=decimal_to_str(buy_fees or ZERO) or "0",
                sell_fees=decimal_to_str(sell_fees or ZERO) or "0",
                details=details,
                reject_reason=reason,
                strategy_id=meta.strategy_id,
                strategy_version=meta.strategy_version,
                signal_opportunity_id=meta.signal_opportunity_id,
                signal_time=meta.signal_time,
                target_execution_time=meta.target_execution_time,
                actual_execution_time=meta.actual_execution_time,
                first_leg_time=meta.first_leg_time,
                second_leg_time=meta.second_leg_time,
                close_time=meta.close_time,
                t0_book_hashes=meta.t0_book_hashes,
                execution_book_hashes=meta.execution_book_hashes,
                book_skew_ms=meta.book_skew_ms,
                expected_net_profit=decimal_to_str(meta.expected_net_profit)
                if meta.expected_net_profit is not None
                else None,
                realized_pnl=realized,
            )
        )


def _copy_book(book: OrderBookSnapshot | None) -> OrderBookSnapshot | None:
    if book is None:
        return None
    return book.model_copy(deep=True)


async def run_delayed_paper_trade(
    *,
    cache: Any,
    market: MarketInfo,
    signal: OpportunitySignal,
    episode_id: int | None,
    cfg: AppConfig,
    paper_cfg: PaperConfig | None = None,
    account_kind: str = "live",
    strategy_id: str | None = None,
    strategy_version: int | None = None,
    sleep_fn: Callable[[float], Awaitable[None]] | None = None,
    now_fn: Callable[[], datetime] | None = None,
    episode_open_fn: Callable[[int | None], bool] | None = None,
    t0_yes: OrderBookSnapshot | None = None,
    t0_no: OrderBookSnapshot | None = None,
) -> dict[str, str] | None:
    """Wait, recapture live books, then paper-fill. Reuses LiveBookCache — no second book stack."""
    assert_trading_disabled()
    paper = paper_cfg or cfg.paper
    sleeper = sleep_fn or asyncio.sleep
    now_fn = now_fn or utcnow
    sid = strategy_id or paper.strategy_id
    sver = strategy_version if strategy_version is not None else paper.strategy_version
    first_delay = paper.first_leg_delay_ms
    signal_time = signal.discovered_at
    target = signal_time + timedelta(milliseconds=first_delay)
    meta = PaperTradeMeta(
        strategy_id=sid,
        strategy_version=sver,
        signal_time=signal_time,
        target_execution_time=target,
        t0_book_hashes=_book_hash_pair(t0_yes, t0_no),
        expected_net_profit=signal.net_profit,
    )
    generation = getattr(cache, "generation", None)

    def _capture() -> tuple[OrderBookSnapshot | None, OrderBookSnapshot | None]:
        if not market.yes_token_id or not market.no_token_id:
            return None, None
        return _copy_book(cache.get(market.yes_token_id)), _copy_book(cache.get(market.no_token_id))

    def _validate(
        yes_b: OrderBookSnapshot | None,
        no_b: OrderBookSnapshot | None,
        *,
        target_time: datetime,
        now: datetime,
        skew_limit: float,
    ) -> str | None:
        ready = True
        if hasattr(cache, "pair_ready") and market.yes_token_id and market.no_token_id:
            ready = bool(cache.pair_ready(market.yes_token_id, market.no_token_id))
        if not ready:
            return "books_not_ready"
        return validate_execution_snapshot(
            episode_id=episode_id,
            yes_book=yes_b,
            no_book=no_b,
            target_time=target_time,
            now=now,
            generation=generation,
            max_age_seconds=float(cfg.scanner.max_data_age_seconds),
            max_skew_ms=skew_limit,
            episode_open_fn=episode_open_fn,
        )

    await sleeper(first_delay / 1000.0)
    now = now_fn()
    yes1, no1 = _capture()
    reason = _validate(
        yes1, no1, target_time=target, now=now, skew_limit=float(cfg.scanner.max_book_skew_ms)
    )
    if reason or yes1 is None or no1 is None:
        _reject(
            market.market_id,
            episode_id,
            reason or "books_not_ready",
            paper,
            first_delay,
            meta,
            account_kind=account_kind,
        )
        return {"status": "rejected", "reject_reason": reason or "books_not_ready", "pnl": "0"}
    meta.first_leg_time = yes1.fetched_at
    meta.book_skew_ms = abs((yes1.fetched_at - no1.fetched_at).total_seconds() * 1000.0)

    await sleeper(paper.inter_leg_delay_ms / 1000.0)
    now = now_fn()
    second_target = target + timedelta(milliseconds=paper.inter_leg_delay_ms)
    yes2, no2 = _capture()
    reason = _validate(
        yes2, no2, target_time=second_target, now=now, skew_limit=float(cfg.scanner.max_book_skew_ms)
    )
    if reason or yes2 is None or no2 is None:
        _reject(
            market.market_id,
            episode_id,
            reason or "books_not_ready",
            paper,
            first_delay,
            meta,
            account_kind=account_kind,
            details="second_leg " + (reason or "books_not_ready"),
        )
        return {"status": "rejected", "reject_reason": reason or "books_not_ready", "pnl": "0"}
    meta.second_leg_time = yes2.fetched_at

    yes3, no3 = yes2, no2
    if paper.force_close_unhedged:
        first_side = (paper.first_leg or "YES").upper()
        if first_side == "YES":
            f_qty, _, _, _, _ = fill_buy(
                yes1.asks, signal.quantity, market.fee_schedule, fees_enabled=market.fees_enabled, tif=paper.time_in_force
            )
            s_qty, _, _, _, _ = fill_buy(
                no2.asks, f_qty, market.fee_schedule, fees_enabled=market.fees_enabled, tif=paper.time_in_force
            )
        else:
            f_qty, _, _, _, _ = fill_buy(
                no1.asks, signal.quantity, market.fee_schedule, fees_enabled=market.fees_enabled, tif=paper.time_in_force
            )
            s_qty, _, _, _, _ = fill_buy(
                yes2.asks, f_qty, market.fee_schedule, fees_enabled=market.fees_enabled, tif=paper.time_in_force
            )
        if abs(f_qty - s_qty) > ZERO:
            await sleeper(paper.force_close_delay_ms / 1000.0)
            now = now_fn()
            close_target = second_target + timedelta(milliseconds=paper.force_close_delay_ms)
            close_yes, close_no = _capture()
            reason = _validate(
                close_yes,
                close_no,
                target_time=close_target,
                now=now,
                skew_limit=float(cfg.scanner.max_book_skew_ms),
            )
            if reason or close_yes is None or close_no is None:
                yes3, no3 = yes2, no2
            else:
                yes3, no3 = close_yes, close_no
                meta.close_time = yes3.fetched_at

    meta.actual_execution_time = now_fn()
    return await execute_paper_complete_set_async(
        market,
        signal,
        yes1,
        no1,
        episode_id=episode_id,
        delay_ms=first_delay,
        tif=paper.time_in_force,
        first_leg=paper.first_leg,
        force_close_unhedged=paper.force_close_unhedged,
        skip_min_profit=False,
        yes_book_second=yes2,
        no_book_second=no2,
        yes_book_close=yes3,
        no_book_close=no3,
        meta=meta,
        paper_cfg=paper,
        app_cfg=cfg,
        account_kind=account_kind,
    )
