"""Paper inventory: conservative mark-to-market and residual handling.

Mark price is the current best bid (what we could sell for now). Equity = cash + marked
value, never occupied cost. Residuals stay open until a later valid book or settlement.
"""

from __future__ import annotations

from decimal import Decimal
from typing import Any

from sqlalchemy import select

from polymarket_scanner.database import (
    PaperAccountRow,
    PositionRow,
    StrategyAccountRow,
    StrategyPositionRow,
    session_scope,
    utcnow,
)
from polymarket_scanner.logging_config import get_logger
from polymarket_scanner.models import OrderBookSnapshot

logger = get_logger(__name__)
ZERO = Decimal("0")
ONE = Decimal("1")

OPEN_STATUSES = {"open", "residual_open", "RESIDUAL_OPEN"}


def _stamp_mark(row: Any, bid: Decimal, status: str | None = None) -> Decimal:
    qty = _d(row.quantity)
    mp, mv = conservative_mark(qty, bid)
    row.last_mark_price = format(mp, "f")
    row.marked_value = format(mv, "f")
    row.unrealized_pnl = format(mv - _d(row.cost_basis), "f")
    if status:
        row.status = status
    return mv


def _d(value: str | None, default: str = "0") -> Decimal:
    return Decimal(value or default)


def best_bid(book: OrderBookSnapshot | None) -> Decimal:
    if book is None or not book.bids:
        return ZERO
    return book.bids[0].price


def conservative_mark(qty: Decimal, bid: Decimal | None) -> tuple[Decimal, Decimal]:
    """Return (mark_price, marked_value). Missing bid → mark 0."""
    price = bid if bid is not None else ZERO
    if price < ZERO:
        price = ZERO
    return price, qty * price


def _account_row(session: Any, account_kind: str, strategy_id: str | None, version: int | None):
    if account_kind == "strategy" and strategy_id is not None and version is not None:
        row = session.scalar(
            select(StrategyAccountRow).where(
                StrategyAccountRow.strategy_id == strategy_id,
                StrategyAccountRow.version == version,
            )
        )
        return row
    return session.scalar(select(PaperAccountRow).limit(1))


def _open_positions(session: Any, *, account_kind: str, strategy_id: str | None, version: int | None):
    if account_kind == "strategy" and strategy_id is not None:
        q = select(StrategyPositionRow).where(StrategyPositionRow.status.in_(list(OPEN_STATUSES)))
        if strategy_id:
            q = q.where(StrategyPositionRow.strategy_id == strategy_id)
        if version is not None:
            q = q.where(StrategyPositionRow.strategy_version == version)
        return list(session.scalars(q).all())
    return list(
        session.scalars(select(PositionRow).where(PositionRow.status.in_(list(OPEN_STATUSES)))).all()
    )


def marked_inventory_total(
    *,
    account_kind: str = "live",
    strategy_id: str | None = None,
    strategy_version: int | None = None,
) -> Decimal:
    with session_scope() as session:
        rows = _open_positions(
            session, account_kind=account_kind, strategy_id=strategy_id, version=strategy_version
        )
        return sum((_d(r.marked_value) for r in rows), ZERO)


def refresh_account_equity(
    *,
    account_kind: str = "live",
    strategy_id: str | None = None,
    strategy_version: int | None = None,
) -> tuple[Decimal, Decimal, Decimal]:
    """equity = cash + marked_value. Updates peak/drawdown. Returns cash, marked, equity."""
    with session_scope() as session:
        rows = _open_positions(
            session, account_kind=account_kind, strategy_id=strategy_id, version=strategy_version
        )
        marked = sum((_d(r.marked_value) for r in rows), ZERO)
        occupied = sum((_d(r.cost_basis) for r in rows), ZERO)
        acct = _account_row(session, account_kind, strategy_id, strategy_version)
        if acct is None:
            return ZERO, marked, marked
        cash = _d(acct.cash)
        if cash < ZERO:
            logger.error("paper cash went negative: %s", cash)
            cash = ZERO
            acct.cash = "0"
        equity = cash + marked
        peak = max(_d(acct.peak_equity, str(cash)), equity)
        dd = peak - equity
        max_dd = max(_d(acct.max_drawdown), dd)
        acct.occupied = format(occupied, "f")
        acct.marked_inventory = format(marked, "f")
        acct.peak_equity = format(peak, "f")
        acct.max_drawdown = format(max_dd, "f")
        acct.updated_at = utcnow()
        return cash, marked, equity


def open_or_increase_position(
    *,
    market_id: str,
    token_id: str,
    outcome: str,
    quantity: Decimal,
    cost_basis: Decimal,
    mark_price: Decimal,
    episode_id: int | None = None,
    trade_id: int | None = None,
    account_kind: str = "live",
    strategy_id: str | None = None,
    strategy_version: int | None = None,
    status: str = "open",
) -> int:
    marked_price, marked_value = conservative_mark(quantity, mark_price)
    unreal = marked_value - cost_basis
    with session_scope() as session:
        if account_kind == "strategy":
            row = StrategyPositionRow(
                strategy_id=strategy_id or "",
                strategy_version=strategy_version or 1,
                market_id=market_id,
                token_id=token_id,
                outcome=outcome,
                quantity=format(quantity, "f"),
                cost_basis=format(cost_basis, "f"),
                last_mark_price=format(marked_price, "f"),
                marked_value=format(marked_value, "f"),
                unrealized_pnl=format(unreal, "f"),
                status=status,
                episode_id=episode_id,
                trade_id=trade_id,
            )
            session.add(row)
            session.flush()
            return int(row.id)
        prow = PositionRow(
            market_id=market_id,
            token_id=token_id,
            outcome=outcome,
            quantity=format(quantity, "f"),
            cost_basis=format(cost_basis, "f"),
            last_mark_price=format(marked_price, "f"),
            marked_value=format(marked_value, "f"),
            unrealized_pnl=format(unreal, "f"),
            status=status,
            episode_id=episode_id,
            trade_id=trade_id,
            strategy_id=strategy_id,
            strategy_version=strategy_version,
        )
        session.add(prow)
        session.flush()
        return int(prow.id)


def reduce_or_close_position(
    position_id: int,
    *,
    account_kind: str = "live",
    qty: Decimal,
    cost_released: Decimal,
    mark_price: Decimal | None = None,
) -> None:
    with session_scope() as session:
        row: Any
        if account_kind == "strategy":
            row = session.get(StrategyPositionRow, position_id)
        else:
            row = session.get(PositionRow, position_id)
        if row is None:
            return
        remaining_qty = _d(row.quantity) - qty
        remaining_cost = _d(row.cost_basis) - cost_released
        if remaining_qty <= ZERO:
            row.quantity = "0"
            row.cost_basis = "0"
            row.marked_value = "0"
            row.unrealized_pnl = "0"
            row.status = "closed"
            return
        row.quantity = format(remaining_qty, "f")
        row.cost_basis = format(max(ZERO, remaining_cost), "f")
        price = mark_price if mark_price is not None else _d(row.last_mark_price)
        mp, mv = conservative_mark(remaining_qty, price)
        row.last_mark_price = format(mp, "f")
        row.marked_value = format(mv, "f")
        row.unrealized_pnl = format(mv - _d(row.cost_basis), "f")


def mark_token(
    token_id: str,
    bid: Decimal,
    *,
    status: str | None = None,
) -> None:
    """Mark all open positions for a token using best bid."""
    with session_scope() as session:
        pos_rows: list[Any] = list(
            session.scalars(
                select(PositionRow).where(
                    PositionRow.token_id == token_id,
                    PositionRow.status.in_(list(OPEN_STATUSES)),
                )
            ).all()
        )
        strat_rows: list[Any] = list(
            session.scalars(
                select(StrategyPositionRow).where(
                    StrategyPositionRow.token_id == token_id,
                    StrategyPositionRow.status.in_(list(OPEN_STATUSES)),
                )
            ).all()
        )
        for row in pos_rows + strat_rows:
            _stamp_mark(row, bid, status)


def mark_market_books(
    market_id: str,
    yes_book: OrderBookSnapshot | None,
    no_book: OrderBookSnapshot | None,
    *,
    status: str | None = None,
) -> Decimal:
    """Mark YES/NO residuals from current books. Returns total marked value for the market."""
    yes_bid = best_bid(yes_book)
    no_bid = best_bid(no_book)
    total = ZERO
    with session_scope() as session:
        pos_rows: list[Any] = list(
            session.scalars(
                select(PositionRow).where(
                    PositionRow.market_id == market_id,
                    PositionRow.status.in_(list(OPEN_STATUSES)),
                )
            ).all()
        )
        strat_rows: list[Any] = list(
            session.scalars(
                select(StrategyPositionRow).where(
                    StrategyPositionRow.market_id == market_id,
                    StrategyPositionRow.status.in_(list(OPEN_STATUSES)),
                )
            ).all()
        )
        for row in pos_rows + strat_rows:
            bid = yes_bid if str(row.outcome).upper() == "YES" else no_bid
            total += _stamp_mark(row, bid, status)
    return total


def set_positions_status(market_id: str, status: str, *, trade_id: int | None = None) -> None:
    with session_scope() as session:
        q1 = select(PositionRow).where(
            PositionRow.market_id == market_id, PositionRow.status.in_(list(OPEN_STATUSES))
        )
        q2 = select(StrategyPositionRow).where(
            StrategyPositionRow.market_id == market_id,
            StrategyPositionRow.status.in_(list(OPEN_STATUSES)),
        )
        if trade_id is not None:
            q1 = q1.where(PositionRow.trade_id == trade_id)
            q2 = q2.where(StrategyPositionRow.trade_id == trade_id)
        rows: list[Any] = list(session.scalars(q1).all()) + list(session.scalars(q2).all())
        for row in rows:
            row.status = status


def positions_for_trade(
    trade_id: int, *, account_kind: str = "live"
) -> list[tuple[int, Decimal, Decimal, str]]:
    with session_scope() as session:
        if account_kind == "strategy":
            srows = session.scalars(
                select(StrategyPositionRow).where(StrategyPositionRow.trade_id == trade_id)
            ).all()
            return [
                (int(r.id), _d(r.quantity), _d(r.cost_basis), str(r.outcome))
                for r in srows
                if _d(r.quantity) > ZERO
            ]
        prows = session.scalars(select(PositionRow).where(PositionRow.trade_id == trade_id)).all()
        return [
            (int(r.id), _d(r.quantity), _d(r.cost_basis), str(r.outcome))
            for r in prows
            if _d(r.quantity) > ZERO
        ]


def settle_market_resolved(
    market_id: str,
    *,
    winning_asset_id: str | None = None,
    winning_outcome: str | None = None,
    yes_token_id: str | None = None,
    no_token_id: str | None = None,
) -> None:
    """Mark residuals to 1 (win) / 0 (lose) when the official resolution is known."""
    win_token = (winning_asset_id or "").strip() or None
    win_side = (winning_outcome or "").strip().upper() or None
    if win_side in {"YES", "Y", "1"}:
        win_side = "YES"
    elif win_side in {"NO", "N", "2"}:
        win_side = "NO"

    with session_scope() as session:
        pos_rows: list[Any] = list(
            session.scalars(
                select(PositionRow).where(
                    PositionRow.market_id == market_id,
                    PositionRow.status.in_(list(OPEN_STATUSES)),
                )
            ).all()
        )
        strat_rows: list[Any] = list(
            session.scalars(
                select(StrategyPositionRow).where(
                    StrategyPositionRow.market_id == market_id,
                    StrategyPositionRow.status.in_(list(OPEN_STATUSES)),
                )
            ).all()
        )
        for row in pos_rows + strat_rows:
            price = ZERO
            if win_token and row.token_id == win_token:
                price = ONE
            elif win_side and str(row.outcome).upper() == win_side:
                price = ONE
            elif win_token or win_side:
                price = ZERO
            else:
                continue
            _stamp_mark(row, price, "settled")

    for kind, sid, ver in _account_keys_for_market(market_id):
        refresh_account_equity(account_kind=kind, strategy_id=sid, strategy_version=ver)


def _account_keys_for_market(market_id: str) -> list[tuple[str, str | None, int | None]]:
    keys: list[tuple[str, str | None, int | None]] = [("live", None, None)]
    with session_scope() as session:
        for row in session.scalars(
            select(StrategyPositionRow).where(StrategyPositionRow.market_id == market_id)
        ).all():
            keys.append(("strategy", row.strategy_id, row.strategy_version))
    return keys


def process_residuals_with_books(
    market_id: str,
    yes_book: OrderBookSnapshot | None,
    no_book: OrderBookSnapshot | None,
) -> Decimal:
    """Re-mark residuals when a later valid book arrives. Does not invent fills."""
    marked = mark_market_books(market_id, yes_book, no_book, status="RESIDUAL_OPEN")
    refresh_account_equity()
    with session_scope() as session:
        sids = {
            (r.strategy_id, r.strategy_version)
            for r in session.scalars(
                select(StrategyPositionRow).where(
                    StrategyPositionRow.market_id == market_id,
                    StrategyPositionRow.status.in_(list(OPEN_STATUSES)),
                )
            ).all()
        }
    for sid, ver in sids:
        refresh_account_equity(account_kind="strategy", strategy_id=sid, strategy_version=ver)
    return marked


def debit_cash(
    amount: Decimal,
    *,
    account_kind: str = "live",
    strategy_id: str | None = None,
    strategy_version: int | None = None,
) -> Decimal:
    """Subtract amount from cash. Refuses to go negative; returns cash after."""
    if amount < ZERO:
        amount = ZERO
    with session_scope() as session:
        acct = _account_row(session, account_kind, strategy_id, strategy_version)
        if acct is None:
            return ZERO
        cash = _d(acct.cash) - amount
        if cash < ZERO:
            logger.error("refusing debit that would make cash negative: have=%s need=%s", acct.cash, amount)
            return _d(acct.cash)
        acct.cash = format(cash, "f")
        acct.updated_at = utcnow()
        return cash


def credit_cash(
    amount: Decimal,
    *,
    realized_pnl: Decimal = ZERO,
    account_kind: str = "live",
    strategy_id: str | None = None,
    strategy_version: int | None = None,
) -> Decimal:
    with session_scope() as session:
        acct = _account_row(session, account_kind, strategy_id, strategy_version)
        if acct is None:
            return ZERO
        cash = _d(acct.cash) + amount
        acct.cash = format(cash, "f")
        if realized_pnl != ZERO:
            acct.realized_pnl = format(_d(acct.realized_pnl) + realized_pnl, "f")
        acct.updated_at = utcnow()
        return cash
