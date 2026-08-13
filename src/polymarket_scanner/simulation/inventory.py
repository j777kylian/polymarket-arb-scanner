"""Paper inventory: conservative mark-to-market, settlement, and account snapshots.

Mark price is the current best bid (what we could sell for now). Equity = cash + marked
value, never occupied cost. Residuals stay open until a later valid book or settlement.
Paper-only — never places real orders.
"""

from __future__ import annotations

import json
from decimal import Decimal
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from polymarket_scanner.database import (
    AccountSnapshotRow,
    ApiErrorRow,
    PaperAccountRow,
    PaperTradeRow,
    PositionRow,
    StrategyAccountRow,
    StrategyPositionRow,
    StrategyTradeRow,
    session_scope,
    utcnow,
)
from polymarket_scanner.logging_config import get_logger
from polymarket_scanner.models import OrderBookSnapshot

logger = get_logger(__name__)
ZERO = Decimal("0")
ONE = Decimal("1")

OPEN_STATUSES = {"open", "residual_open", "RESIDUAL_OPEN", "CLOSE_PENDING"}


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


def _normalize_outcome(value: str | None) -> str | None:
    if not value:
        return None
    ou = value.strip().upper()
    if ou in {"YES", "Y", "1", "TRUE"}:
        return "YES"
    if ou in {"NO", "N", "0", "2", "FALSE"}:
        return "NO"
    if ou == "YES" or ou == "NO":
        return ou
    return None


def resolve_winning_side(
    *,
    winning_asset_id: str | None = None,
    winning_outcome: str | None = None,
    yes_token_id: str | None = None,
    no_token_id: str | None = None,
    payload: dict[str, Any] | None = None,
) -> tuple[str | None, str | None]:
    """Resolve winner from explicit args and payload aliases.

    Supports winningTokenId, winning_token_id, winningAssetId, winning_asset_id,
    winningOutcome, winning_outcome. Returns (token_id, YES|NO side) or (None, None).
    """
    p = payload or {}
    token_raw = (
        winning_asset_id
        or p.get("winning_asset_id")
        or p.get("winningAssetId")
        or p.get("winning_token_id")
        or p.get("winningTokenId")
    )
    outcome_raw = winning_outcome or p.get("winning_outcome") or p.get("winningOutcome")
    token = str(token_raw).strip() if token_raw else None
    side = _normalize_outcome(str(outcome_raw) if outcome_raw is not None else None)
    if token and yes_token_id and token == yes_token_id:
        side = side or "YES"
    elif token and no_token_id and token == no_token_id:
        side = side or "NO"
    if token or side:
        return token, side
    return None, None


def _account_row(
    session: Session,
    account_kind: str,
    strategy_id: str | None,
    version: int | None,
) -> PaperAccountRow | StrategyAccountRow | None:
    if account_kind == "strategy" and strategy_id is not None and version is not None:
        return session.scalar(
            select(StrategyAccountRow).where(
                StrategyAccountRow.strategy_id == strategy_id,
                StrategyAccountRow.version == version,
            )
        )
    return session.scalar(select(PaperAccountRow).limit(1))


def _open_positions_query(
    *,
    account_kind: str,
    strategy_id: str | None,
    version: int | None,
    market_id: str | None = None,
    trade_id: int | None = None,
) -> Any:
    if account_kind == "strategy":
        q: Any = select(StrategyPositionRow).where(
            StrategyPositionRow.status.in_(list(OPEN_STATUSES))
        )
        if strategy_id:
            q = q.where(StrategyPositionRow.strategy_id == strategy_id)
        if version is not None:
            q = q.where(StrategyPositionRow.strategy_version == version)
        if market_id is not None:
            q = q.where(StrategyPositionRow.market_id == market_id)
        if trade_id is not None:
            q = q.where(StrategyPositionRow.trade_id == trade_id)
        return q
    q = select(PositionRow).where(PositionRow.status.in_(list(OPEN_STATUSES)))
    if market_id is not None:
        q = q.where(PositionRow.market_id == market_id)
    if trade_id is not None:
        q = q.where(PositionRow.trade_id == trade_id)
    return q


def _open_positions(
    session: Session,
    *,
    account_kind: str,
    strategy_id: str | None,
    version: int | None,
    market_id: str | None = None,
    trade_id: int | None = None,
) -> list[Any]:
    return list(
        session.scalars(
            _open_positions_query(
                account_kind=account_kind,
                strategy_id=strategy_id,
                version=version,
                market_id=market_id,
                trade_id=trade_id,
            )
        ).all()
    )


def _stamp_mark_price_only(row: Any, bid: Decimal) -> Decimal:
    qty = _d(row.quantity)
    mp, mv = conservative_mark(qty, bid)
    row.last_mark_price = format(mp, "f")
    row.marked_value = format(mv, "f")
    row.unrealized_pnl = format(mv - _d(row.cost_basis), "f")
    return mv


def _stamp_mark(row: Any, bid: Decimal, status: str | None = None) -> Decimal:
    mv = _stamp_mark_price_only(row, bid)
    if status:
        row.status = status
    return mv


def _position_is_winner(
    row: Any,
    *,
    win_token: str | None,
    win_side: str | None,
) -> bool:
    if win_token and row.token_id == win_token:
        return True
    if win_side and str(row.outcome).upper() == win_side:
        return True
    return False


def _apply_equity_fields(acct: Any, *, cash: Decimal, marked: Decimal, occupied: Decimal) -> tuple[Decimal, Decimal]:
    equity = cash + marked
    peak = max(_d(acct.peak_equity, str(cash)), equity)
    dd = peak - equity
    max_dd = max(_d(acct.max_drawdown), dd)
    acct.occupied = format(occupied, "f")
    acct.marked_inventory = format(marked, "f")
    acct.peak_equity = format(peak, "f")
    acct.max_drawdown = format(max_dd, "f")
    acct.updated_at = utcnow()
    return equity, dd


def _account_snapshot_values(
    session: Session,
    *,
    account_kind: str,
    strategy_id: str | None,
    strategy_version: int | None,
) -> dict[str, Decimal]:
    rows = _open_positions(
        session,
        account_kind=account_kind,
        strategy_id=strategy_id,
        version=strategy_version,
    )
    marked = sum((_d(r.marked_value) for r in rows), ZERO)
    occupied = sum((_d(r.cost_basis) for r in rows), ZERO)
    unreal = sum((_d(r.unrealized_pnl) for r in rows), ZERO)
    acct = _account_row(session, account_kind, strategy_id, strategy_version)
    if acct is None:
        return {
            "cash": ZERO,
            "occupied_cost": occupied,
            "marked_inventory": marked,
            "equity": marked,
            "realized_pnl": ZERO,
            "unrealized_pnl": unreal,
            "drawdown": ZERO,
        }
    cash = _d(acct.cash)
    equity = cash + marked
    peak = _d(acct.peak_equity, str(cash))
    dd = max(ZERO, peak - equity)
    return {
        "cash": cash,
        "occupied_cost": occupied,
        "marked_inventory": marked,
        "equity": equity,
        "realized_pnl": _d(acct.realized_pnl),
        "unrealized_pnl": unreal,
        "drawdown": dd,
    }


def write_account_snapshot(
    *,
    event_type: str,
    account_kind: str = "live",
    strategy_id: str | None = None,
    strategy_version: int | None = None,
    trade_id: int | None = None,
    details: str | None = None,
    session: Session | None = None,
) -> None:
    """Record auditable account state after a paper event."""

    def _write(sess: Session) -> None:
        vals = _account_snapshot_values(
            sess,
            account_kind=account_kind,
            strategy_id=strategy_id,
            strategy_version=strategy_version,
        )
        sess.add(
            AccountSnapshotRow(
                account_kind=account_kind,
                strategy_id=strategy_id,
                strategy_version=strategy_version,
                trade_id=trade_id,
                event_type=event_type,
                cash=format(vals["cash"], "f"),
                occupied_cost=format(vals["occupied_cost"], "f"),
                marked_inventory=format(vals["marked_inventory"], "f"),
                equity=format(vals["equity"], "f"),
                realized_pnl=format(vals["realized_pnl"], "f"),
                unrealized_pnl=format(vals["unrealized_pnl"], "f"),
                drawdown=format(vals["drawdown"], "f"),
                details=details,
            )
        )

    if session is not None:
        _write(session)
        return
    with session_scope() as sess:
        _write(sess)


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
        equity, _dd = _apply_equity_fields(acct, cash=cash, marked=marked, occupied=occupied)
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
            if status:
                _stamp_mark(row, bid, status)
            else:
                _stamp_mark_price_only(row, bid)


def mark_market_books(
    market_id: str,
    yes_book: OrderBookSnapshot | None,
    no_book: OrderBookSnapshot | None,
    *,
    account_kind: str | None = None,
    strategy_id: str | None = None,
    strategy_version: int | None = None,
) -> Decimal:
    """Mark YES/NO open positions from current books. Never mutates status."""
    yes_bid = best_bid(yes_book)
    no_bid = best_bid(no_book)
    total = ZERO
    with session_scope() as session:
        if account_kind == "strategy":
            rows = _open_positions(
                session,
                account_kind="strategy",
                strategy_id=strategy_id,
                version=strategy_version,
                market_id=market_id,
            )
        elif account_kind == "live":
            rows = _open_positions(
                session,
                account_kind="live",
                strategy_id=None,
                version=None,
                market_id=market_id,
            )
        else:
            live_rows = _open_positions(
                session,
                account_kind="live",
                strategy_id=None,
                version=None,
                market_id=market_id,
            )
            strat_rows = list(
                session.scalars(
                    select(StrategyPositionRow).where(
                        StrategyPositionRow.market_id == market_id,
                        StrategyPositionRow.status.in_(list(OPEN_STATUSES)),
                    )
                ).all()
            )
            rows = live_rows + strat_rows
        for row in rows:
            bid = yes_bid if str(row.outcome).upper() == "YES" else no_bid
            total += _stamp_mark_price_only(row, bid)
    return total


def set_positions_status(
    market_id: str,
    status: str,
    *,
    trade_id: int | None = None,
    account_kind: str = "live",
    strategy_id: str | None = None,
    strategy_version: int | None = None,
) -> None:
    with session_scope() as session:
        if account_kind == "strategy":
            q = _open_positions_query(
                account_kind="strategy",
                strategy_id=strategy_id,
                version=strategy_version,
                market_id=market_id,
                trade_id=trade_id,
            )
            rows = list(session.scalars(q).all())
        else:
            q = _open_positions_query(
                account_kind="live",
                strategy_id=None,
                version=None,
                market_id=market_id,
                trade_id=trade_id,
            )
            rows = list(session.scalars(q).all())
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


def _settle_position_row(
    row: Any,
    *,
    win_token: str | None,
    win_side: str | None,
    account_kind: str,
    session: Session,
    now: Any,
) -> Decimal:
    qty = _d(row.quantity)
    if qty <= ZERO:
        return ZERO
    cost = _d(row.cost_basis)
    winner = _position_is_winner(row, win_token=win_token, win_side=win_side)
    payout = qty * ONE if winner else ZERO
    settlement_pnl = payout - cost

    acct_kind = account_kind
    sid = getattr(row, "strategy_id", None) if account_kind == "strategy" else None
    ver = getattr(row, "strategy_version", None) if account_kind == "strategy" else None
    acct = _account_row(session, acct_kind, sid, ver)
    if acct is not None:
        acct.cash = format(_d(acct.cash) + payout, "f")
        acct.realized_pnl = format(_d(acct.realized_pnl) + settlement_pnl, "f")
        acct.updated_at = now

    row.quantity = "0"
    row.cost_basis = "0"
    row.last_mark_price = "0"
    row.marked_value = "0"
    row.unrealized_pnl = "0"
    row.status = "settled"

    trade_id = int(row.trade_id) if row.trade_id is not None else None
    if trade_id is not None:
        trade: Any
        if account_kind == "strategy":
            trade = session.get(StrategyTradeRow, trade_id)
        else:
            trade = session.get(PaperTradeRow, trade_id)
        if trade is not None:
            trade.remaining_inventory = "0"
            trade.status = "settled"
            trade.settled_at = now
            if trade.realized_at is None:
                trade.realized_at = now
            prev = _d(getattr(trade, "realized_pnl", None) or getattr(trade, "pnl", None))
            new_pnl = prev + settlement_pnl
            if hasattr(trade, "realized_pnl"):
                trade.realized_pnl = format(new_pnl, "f")
            if hasattr(trade, "pnl"):
                trade.pnl = format(new_pnl, "f")
            trade.inventory_cost = "0"
    return settlement_pnl


def _finalize_account_after_settlement(
    session: Session,
    *,
    account_kind: str,
    strategy_id: str | None,
    strategy_version: int | None,
    now: Any,
) -> None:
    rows = _open_positions(
        session,
        account_kind=account_kind,
        strategy_id=strategy_id,
        version=strategy_version,
    )
    marked = sum((_d(r.marked_value) for r in rows), ZERO)
    occupied = sum((_d(r.cost_basis) for r in rows), ZERO)
    acct = _account_row(session, account_kind, strategy_id, strategy_version)
    if acct is None:
        return
    cash = _d(acct.cash)
    if cash < ZERO:
        cash = ZERO
        acct.cash = "0"
    _apply_equity_fields(acct, cash=cash, marked=marked, occupied=occupied)
    acct.updated_at = now


def settle_market_resolved(
    market_id: str,
    *,
    winning_asset_id: str | None = None,
    winning_outcome: str | None = None,
    yes_token_id: str | None = None,
    no_token_id: str | None = None,
    payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Settle open positions at 1/0 when official resolution is known."""
    win_token, win_side = resolve_winning_side(
        winning_asset_id=winning_asset_id,
        winning_outcome=winning_outcome,
        yes_token_id=yes_token_id,
        no_token_id=no_token_id,
        payload=payload,
    )
    if not win_token and not win_side:
        ctx = {
            "market_id": market_id,
            "payload_keys": list((payload or {}).keys()),
            "winning_asset_id": winning_asset_id,
            "winning_outcome": winning_outcome,
        }
        with session_scope() as session:
            session.add(
                ApiErrorRow(
                    source="inventory.settlement",
                    message="market settlement missing winner",
                    context_json=json.dumps(ctx),
                )
            )
        logger.warning("Cannot settle %s: no clear winner in payload", market_id)
        return {"settled": False, "reason": "no_winner"}

    now = utcnow()
    strategy_keys: set[tuple[str, int]] = set()
    with session_scope() as session:
        live_rows = _open_positions(
            session, account_kind="live", strategy_id=None, version=None, market_id=market_id
        )
        strat_rows = list(
            session.scalars(
                select(StrategyPositionRow).where(
                    StrategyPositionRow.market_id == market_id,
                    StrategyPositionRow.status.in_(list(OPEN_STATUSES)),
                )
            ).all()
        )
        if not live_rows and not strat_rows:
            return {"settled": True, "positions": 0}

        total_pnl = ZERO
        for row in live_rows:
            total_pnl += _settle_position_row(
                row,
                win_token=win_token,
                win_side=win_side,
                account_kind="live",
                session=session,
                now=now,
            )
        for row in strat_rows:
            strategy_keys.add((row.strategy_id, int(row.strategy_version)))
            total_pnl += _settle_position_row(
                row,
                win_token=win_token,
                win_side=win_side,
                account_kind="strategy",
                session=session,
                now=now,
            )

        _finalize_account_after_settlement(
            session, account_kind="live", strategy_id=None, strategy_version=None, now=now
        )
        for sid, ver in strategy_keys:
            _finalize_account_after_settlement(
                session,
                account_kind="strategy",
                strategy_id=sid,
                strategy_version=ver,
                now=now,
            )

        write_account_snapshot(
            event_type="market_settlement",
            account_kind="live",
            details=f"market={market_id} pnl={format(total_pnl, 'f')}",
            session=session,
        )
        for sid, ver in strategy_keys:
            write_account_snapshot(
                event_type="market_settlement",
                account_kind="strategy",
                strategy_id=sid,
                strategy_version=ver,
                details=f"market={market_id}",
                session=session,
            )

    logger.info(
        "Settled market %s winner_token=%s winner_side=%s positions=%s pnl=%s",
        market_id,
        win_token,
        win_side,
        len(live_rows) + len(strat_rows),
        total_pnl,
    )
    return {
        "settled": True,
        "positions": len(live_rows) + len(strat_rows),
        "realized_pnl": format(total_pnl, "f"),
        "win_token": win_token,
        "win_side": win_side,
    }


def _strategy_keys_for_market(market_id: str) -> set[tuple[str, int]]:
    with session_scope() as session:
        return {
            (r.strategy_id, int(r.strategy_version))
            for r in session.scalars(
                select(StrategyPositionRow).where(
                    StrategyPositionRow.market_id == market_id,
                    StrategyPositionRow.status.in_(list(OPEN_STATUSES)),
                )
            ).all()
        }


def process_residuals_with_books(
    market_id: str,
    yes_book: OrderBookSnapshot | None,
    no_book: OrderBookSnapshot | None,
) -> Decimal:
    """Re-mark residuals when a later valid book arrives. Does not change status."""
    marked = mark_market_books(market_id, yes_book, no_book)
    refresh_account_equity()
    write_account_snapshot(event_type="residual_mark", account_kind="live", details=f"market={market_id}")
    for sid, ver in _strategy_keys_for_market(market_id):
        refresh_account_equity(account_kind="strategy", strategy_id=sid, strategy_version=ver)
        write_account_snapshot(
            event_type="residual_mark",
            account_kind="strategy",
            strategy_id=sid,
            strategy_version=ver,
            details=f"market={market_id}",
        )
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
            logger.error(
                "refusing debit that would make cash negative: have=%s need=%s",
                acct.cash,
                amount,
            )
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
