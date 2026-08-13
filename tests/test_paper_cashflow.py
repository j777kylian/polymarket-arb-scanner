"""Exact Decimal paper cashflow tests — no double-counted P&L."""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal

from polymarket_scanner.database import init_db
from polymarket_scanner.models import (
    ArbDirection,
    FeeSchedule,
    MarketInfo,
    OpportunitySignal,
    OrderBookLevel,
    OrderBookSnapshot,
    OutcomeSide,
)
from polymarket_scanner.simulation.paper_trader import (
    execute_paper_complete_set,
    get_paper_account,
    settle_complete_set_cashflow,
)


def test_settle_full_merge_exact() -> None:
    s = settle_complete_set_cashflow(
        yes_qty=Decimal("10"),
        yes_cost=Decimal("4.0"),
        yes_fee=Decimal("0"),
        no_qty=Decimal("10"),
        no_cost=Decimal("5.0"),
        no_fee=Decimal("0"),
    )
    assert s.matched == Decimal("10")
    assert s.merge_proceeds == Decimal("10")
    assert s.realized_pnl == Decimal("1.0")
    assert s.remaining_inventory == Decimal("0")
    assert s.inventory_cost == Decimal("0")
    assert s.cash_delta == Decimal("1.0")
    start = Decimal("1000")
    assert start + s.cash_delta == start + s.realized_pnl - s.inventory_cost


def test_settle_partial_second_leg_full_close() -> None:
    # 10 YES @ 0.40, 5 NO @ 0.50, close 5 YES @ 0.39
    s = settle_complete_set_cashflow(
        yes_qty=Decimal("10"),
        yes_cost=Decimal("4.0"),
        yes_fee=Decimal("0"),
        no_qty=Decimal("5"),
        no_cost=Decimal("2.5"),
        no_fee=Decimal("0"),
        close_qty=Decimal("5"),
        close_proceeds=Decimal("1.95"),
        close_fee=Decimal("0"),
        close_side=OutcomeSide.YES,
    )
    assert s.matched == Decimal("5")
    assert s.merge_proceeds == Decimal("5")
    assert s.remaining_inventory == Decimal("0")
    assert s.realized_pnl == Decimal("0.45")
    assert s.cash_delta == Decimal("0.45")
    start = Decimal("1000")
    assert start + s.cash_delta == start + s.realized_pnl - s.inventory_cost


def test_settle_partial_force_close_keeps_inventory() -> None:
    s = settle_complete_set_cashflow(
        yes_qty=Decimal("10"),
        yes_cost=Decimal("4.0"),
        yes_fee=Decimal("0"),
        no_qty=Decimal("5"),
        no_cost=Decimal("2.5"),
        no_fee=Decimal("0"),
        close_qty=Decimal("3"),
        close_proceeds=Decimal("1.17"),
        close_fee=Decimal("0"),
        close_side=OutcomeSide.YES,
    )
    assert s.remaining_inventory == Decimal("2")
    assert s.inventory_cost == Decimal("0.8")
    assert s.realized_pnl == Decimal("0.47")
    start = Decimal("1000")
    cash_after = start + s.cash_delta
    assert cash_after == Decimal("999.67")
    assert cash_after == start + s.realized_pnl - s.inventory_cost


def test_settle_fees_on_buy_and_sell() -> None:
    s = settle_complete_set_cashflow(
        yes_qty=Decimal("10"),
        yes_cost=Decimal("4.0"),
        yes_fee=Decimal("0.10"),
        no_qty=Decimal("10"),
        no_cost=Decimal("5.0"),
        no_fee=Decimal("0.20"),
        close_qty=Decimal("0"),
        close_proceeds=Decimal("0"),
        close_fee=Decimal("0"),
        close_side=None,
    )
    # merge 10 - cost 9 - fees 0.30 = 0.70
    assert s.buy_fees == Decimal("0.30")
    assert s.realized_pnl == Decimal("0.70")
    assert s.cash_delta == Decimal("0.70")
    start = Decimal("1000")
    assert start + s.cash_delta == start + s.realized_pnl - s.inventory_cost


def _db(tmp_path, monkeypatch):
    db = tmp_path / "t.db"
    monkeypatch.setenv("DATABASE_URL", f"sqlite:///{db}")
    from polymarket_scanner.config import get_config

    get_config.cache_clear()
    from polymarket_scanner import database as dbmod

    dbmod._engine = None
    dbmod._SessionLocal = None
    init_db(f"sqlite:///{db}")
    return dbmod, get_config


def _sig(now, qty="10", net="1.0"):
    return OpportunitySignal(
        market_id="m1",
        condition_id="c",
        direction=ArbDirection.FORWARD,
        discovered_at=now,
        data_age_seconds=0,
        quantity=Decimal(qty),
        yes_vwap=Decimal("0.40"),
        no_vwap=Decimal("0.50"),
        gross_profit=Decimal("1.0"),
        fee_total=Decimal("0"),
        net_profit=Decimal(net),
        net_profit_per_share=Decimal("0.1"),
        net_profit_rate=Decimal("0.1"),
        levels_used_yes=1,
        levels_used_no=1,
    )


def test_paper_execute_full_merge_and_reconcile(tmp_path, monkeypatch) -> None:
    dbmod, get_config = _db(tmp_path, monkeypatch)
    now = datetime.now(timezone.utc)
    yes = OrderBookSnapshot(
        condition_id="c",
        token_id="y",
        outcome=OutcomeSide.YES,
        asks=[OrderBookLevel(price=Decimal("0.40"), size=Decimal("100"))],
        bids=[OrderBookLevel(price=Decimal("0.39"), size=Decimal("100"))],
        fetched_at=now,
        min_order_size=Decimal("1"),
    )
    no = OrderBookSnapshot(
        condition_id="c",
        token_id="n",
        outcome=OutcomeSide.NO,
        asks=[OrderBookLevel(price=Decimal("0.50"), size=Decimal("100"))],
        bids=[OrderBookLevel(price=Decimal("0.49"), size=Decimal("100"))],
        fetched_at=now,
        min_order_size=Decimal("1"),
    )
    market = MarketInfo(
        market_id="m1",
        condition_id="c",
        yes_token_id="y",
        no_token_id="n",
        fees_enabled=False,
        minimum_order_size=Decimal("1"),
    )
    result = execute_paper_complete_set(
        market, _sig(now), yes, no, tif="FOK", delay_ms=500, skip_min_profit=True
    )
    assert result is not None
    assert result["status"] == "merged"
    cash, occ, pnl = get_paper_account()
    assert pnl == Decimal("1.0")
    assert cash == Decimal("1001.0")
    assert occ == Decimal("0")
    assert Decimal(result["cash_after"]) == cash
    dbmod._engine = None
    dbmod._SessionLocal = None
    get_config.cache_clear()


def test_paper_insufficient_capital(tmp_path, monkeypatch) -> None:
    dbmod, get_config = _db(tmp_path, monkeypatch)
    from polymarket_scanner.database import PaperAccountRow, session_scope

    with session_scope() as session:
        from sqlalchemy import select

        row = session.scalar(select(PaperAccountRow).limit(1))
        assert row is not None
        row.cash = "0.01"
    now = datetime.now(timezone.utc)
    yes = OrderBookSnapshot(
        condition_id="c",
        token_id="y",
        outcome=OutcomeSide.YES,
        asks=[OrderBookLevel(price=Decimal("0.40"), size=Decimal("100"))],
        bids=[OrderBookLevel(price=Decimal("0.39"), size=Decimal("100"))],
        fetched_at=now,
    )
    no = OrderBookSnapshot(
        condition_id="c",
        token_id="n",
        outcome=OutcomeSide.NO,
        asks=[OrderBookLevel(price=Decimal("0.50"), size=Decimal("100"))],
        bids=[OrderBookLevel(price=Decimal("0.49"), size=Decimal("100"))],
        fetched_at=now,
    )
    market = MarketInfo(
        market_id="m1",
        condition_id="c",
        yes_token_id="y",
        no_token_id="n",
        fees_enabled=False,
        minimum_order_size=Decimal("5"),
    )
    result = execute_paper_complete_set(
        market, _sig(now), yes, no, tif="FAK", skip_min_profit=True
    )
    assert result is not None
    assert result["status"] == "rejected_insufficient_capital"
    dbmod._engine = None
    dbmod._SessionLocal = None
    get_config.cache_clear()


def test_paper_fees_both_sides_execute(tmp_path, monkeypatch) -> None:
    dbmod, get_config = _db(tmp_path, monkeypatch)
    now = datetime.now(timezone.utc)
    sched = FeeSchedule(rate=Decimal("0.04"), exponent=Decimal("1"), taker_only=True)
    yes = OrderBookSnapshot(
        condition_id="c",
        token_id="y",
        outcome=OutcomeSide.YES,
        asks=[OrderBookLevel(price=Decimal("0.40"), size=Decimal("100"))],
        bids=[OrderBookLevel(price=Decimal("0.39"), size=Decimal("100"))],
        fetched_at=now,
        min_order_size=Decimal("1"),
    )
    no = OrderBookSnapshot(
        condition_id="c",
        token_id="n",
        outcome=OutcomeSide.NO,
        asks=[OrderBookLevel(price=Decimal("0.50"), size=Decimal("100"))],
        bids=[OrderBookLevel(price=Decimal("0.49"), size=Decimal("100"))],
        fetched_at=now,
        min_order_size=Decimal("1"),
    )
    market = MarketInfo(
        market_id="m1",
        condition_id="c",
        yes_token_id="y",
        no_token_id="n",
        fees_enabled=True,
        fee_schedule=sched,
        minimum_order_size=Decimal("1"),
    )
    result = execute_paper_complete_set(
        market, _sig(now), yes, no, tif="FOK", skip_min_profit=True
    )
    assert result is not None
    cash, _occ, pnl = get_paper_account()
    start = Decimal("1000")
    assert Decimal(result["cash_after"]) == cash
    assert cash == start + pnl
    assert pnl < Decimal("1.0")
    dbmod._engine = None
    dbmod._SessionLocal = None
    get_config.cache_clear()
