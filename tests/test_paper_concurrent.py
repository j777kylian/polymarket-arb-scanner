"""Paper capital serialization across concurrent episodes."""

from __future__ import annotations

import asyncio
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from decimal import Decimal

from polymarket_scanner.database import init_db
from polymarket_scanner.models import (
    ArbDirection,
    MarketInfo,
    OpportunitySignal,
    OrderBookLevel,
    OrderBookSnapshot,
    OutcomeSide,
)
from polymarket_scanner.simulation.paper_trader import (
    execute_paper_complete_set,
    execute_paper_complete_set_async,
    get_paper_account,
)


def _db(tmp_path, monkeypatch):
    db = tmp_path / "c.db"
    monkeypatch.setenv("DATABASE_URL", f"sqlite:///{db}")
    from polymarket_scanner.config import get_config

    get_config.cache_clear()
    from polymarket_scanner import database as dbmod

    dbmod._engine = None
    dbmod._SessionLocal = None
    init_db(f"sqlite:///{db}")
    return dbmod, get_config


def _books(now: datetime) -> tuple[OrderBookSnapshot, OrderBookSnapshot]:
    yes = OrderBookSnapshot(
        condition_id="c",
        token_id="y",
        outcome=OutcomeSide.YES,
        asks=[OrderBookLevel(price=Decimal("0.40"), size=Decimal("1000"))],
        bids=[],
        fetched_at=now,
        min_order_size=Decimal("1"),
    )
    no = OrderBookSnapshot(
        condition_id="c",
        token_id="n",
        outcome=OutcomeSide.NO,
        asks=[],
        bids=[],
        fetched_at=now,
        min_order_size=Decimal("1"),
    )
    return yes, no


def _sig(now: datetime) -> OpportunitySignal:
    return OpportunitySignal(
        market_id="m1",
        condition_id="c",
        direction=ArbDirection.FORWARD,
        discovered_at=now,
        data_age_seconds=0,
        quantity=Decimal("10"),
        yes_vwap=Decimal("0.40"),
        no_vwap=Decimal("0.50"),
        gross_profit=Decimal("1"),
        fee_total=Decimal("0"),
        net_profit=Decimal("1"),
        net_profit_per_share=Decimal("0.1"),
        net_profit_rate=Decimal("0.1"),
        levels_used_yes=1,
        levels_used_no=1,
    )


def test_concurrent_threads_do_not_go_negative(tmp_path, monkeypatch) -> None:
    dbmod, get_config = _db(tmp_path, monkeypatch)
    from sqlalchemy import select

    from polymarket_scanner.database import PaperAccountRow, session_scope

    with session_scope() as session:
        row = session.scalar(select(PaperAccountRow).limit(1))
        assert row is not None
        row.cash = "6"

    now = datetime.now(timezone.utc)
    yes, no = _books(now)
    market = MarketInfo(
        market_id="m1",
        condition_id="c",
        yes_token_id="y",
        no_token_id="n",
        fees_enabled=False,
        minimum_order_size=Decimal("5"),
    )

    def run() -> dict | None:
        return execute_paper_complete_set(
            market,
            _sig(now),
            yes,
            no,
            tif="FAK",
            skip_min_profit=True,
            force_close_unhedged=False,
        )

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(lambda _: run(), range(2)))

    statuses = [r["status"] if r else None for r in results]
    cash, occupied, _pnl = get_paper_account()
    assert cash >= Decimal("0")
    assert "rejected_insufficient_capital" in statuses or cash == Decimal("2")
    assert occupied == Decimal("4") or cash == Decimal("2")
    dbmod._engine = None
    dbmod._SessionLocal = None
    get_config.cache_clear()


async def test_async_lock_serializes_paper(tmp_path, monkeypatch) -> None:
    dbmod, get_config = _db(tmp_path, monkeypatch)
    from sqlalchemy import select

    from polymarket_scanner.database import PaperAccountRow, session_scope

    with session_scope() as session:
        row = session.scalar(select(PaperAccountRow).limit(1))
        assert row is not None
        row.cash = "6"

    now = datetime.now(timezone.utc)
    yes, no = _books(now)
    market = MarketInfo(
        market_id="m1",
        condition_id="c",
        yes_token_id="y",
        no_token_id="n",
        fees_enabled=False,
        minimum_order_size=Decimal("5"),
    )

    async def run() -> dict | None:
        return await execute_paper_complete_set_async(
            market,
            _sig(now),
            yes,
            no,
            tif="FAK",
            skip_min_profit=True,
            force_close_unhedged=False,
        )

    results = await asyncio.gather(run(), run())
    cash, _occ, _pnl = get_paper_account()
    assert cash >= Decimal("0")
    statuses = [r["status"] if r else None for r in results]
    assert statuses.count("rejected_insufficient_capital") == 1
    assert cash == Decimal("2")
    dbmod._engine = None
    dbmod._SessionLocal = None
    get_config.cache_clear()
