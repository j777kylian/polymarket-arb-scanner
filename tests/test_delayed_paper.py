"""Delayed two-leg paper execution against live cache snapshots."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest
from sqlalchemy import select

from polymarket_scanner.config import get_config
from polymarket_scanner.database import PaperTradeRow, init_db, session_scope
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
    run_delayed_paper_trade,
)


def _db(tmp_path, monkeypatch) -> None:
    db = tmp_path / "t.db"
    monkeypatch.setenv("DATABASE_URL", f"sqlite:///{db}")
    get_config.cache_clear()
    from polymarket_scanner import database as dbmod

    dbmod._engine = None
    dbmod._SessionLocal = None
    init_db(f"sqlite:///{db}")


def _book(
    token: str, side: OutcomeSide, now: datetime, *, hash_: str, ask: str = "0.40"
) -> OrderBookSnapshot:
    return OrderBookSnapshot(
        condition_id="c",
        token_id=token,
        outcome=side,
        asks=[OrderBookLevel(price=Decimal(ask), size=Decimal("100"))],
        bids=[OrderBookLevel(price=Decimal("0.39"), size=Decimal("100"))],
        fetched_at=now,
        hash=hash_,
        connection_generation=1,
    )


def _market() -> MarketInfo:
    return MarketInfo(
        market_id="m1",
        condition_id="c",
        yes_token_id="y",
        no_token_id="n",
        fees_enabled=False,
        minimum_order_size=Decimal("1"),
    )


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
        gross_profit=Decimal("1.0"),
        fee_total=Decimal("0"),
        net_profit=Decimal("1.0"),
        net_profit_per_share=Decimal("0.1"),
        net_profit_rate=Decimal("0.1"),
        levels_used_yes=1,
        levels_used_no=1,
    )


class _Cache:
    generation = 1

    def __init__(self) -> None:
        self.books: dict[str, OrderBookSnapshot] = {}

    def get(self, token_id: str) -> OrderBookSnapshot | None:
        return self.books.get(token_id)

    def pair_ready(self, yes_token: str, no_token: str) -> bool:
        return yes_token in self.books and no_token in self.books


@pytest.mark.asyncio
async def test_episode_closed_after_delay_does_not_fill(tmp_path, monkeypatch) -> None:
    _db(tmp_path, monkeypatch)
    t0 = datetime(2026, 8, 13, 12, 0, tzinfo=timezone.utc)
    target = t0 + timedelta(milliseconds=500)
    cache = _Cache()
    cache.books["y"] = _book("y", OutcomeSide.YES, target, hash_="y1")
    cache.books["n"] = _book("n", OutcomeSide.NO, target, hash_="n1", ask="0.50")
    cfg = get_config()
    cfg.paper.signal_to_first_leg_ms = 500
    cfg.paper.inter_leg_delay_ms = 0
    cfg.paper.force_close_unhedged = False

    async def _sleep(_: float) -> None:
        return None

    result = await run_delayed_paper_trade(
        cache=cache,
        market=_market(),
        signal=_sig(t0),
        episode_id=99,
        cfg=cfg,
        paper_cfg=cfg.paper,
        sleep_fn=_sleep,
        now_fn=lambda: target,
        episode_open_fn=lambda _eid: False,
        t0_yes=cache.books["y"],
        t0_no=cache.books["n"],
    )
    assert result is not None
    assert result["status"] == "rejected"
    assert result["reject_reason"] == "episode_closed"
    with session_scope() as session:
        row = session.scalar(select(PaperTradeRow).order_by(PaperTradeRow.id.desc()))
        assert row is not None
        assert row.reject_reason == "episode_closed"
        assert Decimal(row.realized_pnl or row.pnl or "0") == 0


@pytest.mark.asyncio
async def test_stale_or_skewed_books_rejected(tmp_path, monkeypatch) -> None:
    _db(tmp_path, monkeypatch)
    t0 = datetime(2026, 8, 13, 12, 0, tzinfo=timezone.utc)
    target = t0 + timedelta(milliseconds=500)
    now = target + timedelta(seconds=120)
    cache = _Cache()
    cache.books["y"] = _book("y", OutcomeSide.YES, target, hash_="y1")
    cache.books["n"] = _book("n", OutcomeSide.NO, target, hash_="n1", ask="0.50")
    cfg = get_config()
    cfg.scanner.max_data_age_seconds = 30
    cfg.paper.inter_leg_delay_ms = 0
    cfg.paper.force_close_unhedged = False

    async def _sleep(_: float) -> None:
        return None

    result = await run_delayed_paper_trade(
        cache=cache,
        market=_market(),
        signal=_sig(t0),
        episode_id=1,
        cfg=cfg,
        paper_cfg=cfg.paper,
        sleep_fn=_sleep,
        now_fn=lambda: now,
        episode_open_fn=lambda _eid: True,
        t0_yes=cache.books["y"],
        t0_no=cache.books["n"],
    )
    assert result is not None
    assert result["reject_reason"] == "stale_books"

    cache.books["y"] = _book("y", OutcomeSide.YES, now, hash_="y2")
    cache.books["n"] = _book(
        "n", OutcomeSide.NO, now + timedelta(milliseconds=800), hash_="n2", ask="0.50"
    )
    cfg.scanner.max_data_age_seconds = 60
    cfg.scanner.max_book_skew_ms = 250
    result2 = await run_delayed_paper_trade(
        cache=cache,
        market=_market(),
        signal=_sig(now - timedelta(milliseconds=500)),
        episode_id=2,
        cfg=cfg,
        paper_cfg=cfg.paper,
        sleep_fn=_sleep,
        now_fn=lambda: now + timedelta(milliseconds=800),
        episode_open_fn=lambda _eid: True,
    )
    assert result2 is not None
    assert result2["reject_reason"] == "books_skewed"


@pytest.mark.asyncio
async def test_two_legs_use_different_time_snapshots(tmp_path, monkeypatch) -> None:
    _db(tmp_path, monkeypatch)
    t0 = datetime(2026, 8, 13, 12, 0, tzinfo=timezone.utc)
    t1 = t0 + timedelta(milliseconds=500)
    t2 = t0 + timedelta(milliseconds=650)
    cache = _Cache()
    cache.books["y"] = _book("y", OutcomeSide.YES, t1, hash_="y-t1")
    cache.books["n"] = _book("n", OutcomeSide.NO, t1, hash_="n-t1", ask="0.50")
    sleeps = {"n": 0}

    async def _sleep(_: float) -> None:
        sleeps["n"] += 1
        if sleeps["n"] == 2:
            cache.books["y"] = _book("y", OutcomeSide.YES, t2, hash_="y-t2")
            cache.books["n"] = _book("n", OutcomeSide.NO, t2, hash_="n-t2", ask="0.50")

    cfg = get_config()
    cfg.paper.signal_to_first_leg_ms = 500
    cfg.paper.inter_leg_delay_ms = 150
    cfg.paper.force_close_unhedged = False
    cfg.scanner.max_data_age_seconds = 60

    result = await run_delayed_paper_trade(
        cache=cache,
        market=_market(),
        signal=_sig(t0),
        episode_id=3,
        cfg=cfg,
        paper_cfg=cfg.paper,
        sleep_fn=_sleep,
        now_fn=lambda: t2,
        episode_open_fn=lambda _eid: True,
        t0_yes=_book("y", OutcomeSide.YES, t0, hash_="y0"),
        t0_no=_book("n", OutcomeSide.NO, t0, hash_="n0", ask="0.50"),
    )
    assert result is not None
    assert result["status"] in {"merged", "one_leg", "one_leg_merged"}
    assert result["first_leg_hash"] == "y-t1"
    assert result["second_leg_hash"] == "y-t2"
    assert result["first_leg_time"] != result["second_leg_time"]

    yes1 = _book("y", OutcomeSide.YES, t1, hash_="h1")
    no1 = _book("n", OutcomeSide.NO, t1, hash_="h1n", ask="0.50")
    yes2 = _book("y", OutcomeSide.YES, t2, hash_="h2")
    no2 = _book("n", OutcomeSide.NO, t2, hash_="h2n", ask="0.50")
    out = execute_paper_complete_set(
        _market(),
        _sig(t0),
        yes1,
        no1,
        yes_book_second=yes2,
        no_book_second=no2,
        skip_min_profit=True,
        force_close_unhedged=False,
    )
    assert out is not None
    assert out["first_leg_time"] != out["second_leg_time"]
    assert yes1.fetched_at != yes2.fetched_at
