from datetime import datetime, timezone
from decimal import Decimal

import pytest

from polymarket_scanner.config import get_config
from polymarket_scanner.database import init_db
from polymarket_scanner.discovery.book_cache import LiveBookCache
from polymarket_scanner.models import (
    ArbDirection,
    MarketInfo,
    OpportunitySignal,
    OrderBookLevel,
    OrderBookSnapshot,
    OutcomeSide,
)
from polymarket_scanner.realtime import RealtimeScanner
from polymarket_scanner.scanners.opportunity_tracker import episode_is_open, sync_episodes


def test_tick_size_change_updates_ready_book() -> None:
    cache = LiveBookCache()
    cache.begin_generation()
    now = datetime.now(timezone.utc)
    cache.upsert_snapshot(
        OrderBookSnapshot(
            condition_id="c",
            token_id="t",
            outcome=OutcomeSide.YES,
            asks=[OrderBookLevel(price=Decimal("0.40"), size=Decimal("10"))],
            bids=[OrderBookLevel(price=Decimal("0.39"), size=Decimal("5"))],
            fetched_at=now,
            tick_size=Decimal("0.01"),
        )
    )
    later = datetime.now(timezone.utc)
    updated = cache.apply_tick_size_change("t", Decimal("0.001"), fetched_at=later)
    assert updated is not None
    book = cache.get("t")
    assert book is not None
    assert book.tick_size == Decimal("0.001")
    assert cache.is_ready("t")


@pytest.mark.asyncio
async def test_realtime_tick_size_and_market_resolved(tmp_path, monkeypatch) -> None:
    db = tmp_path / "t.db"
    monkeypatch.setenv("DATABASE_URL", f"sqlite:///{db}")
    get_config.cache_clear()
    from polymarket_scanner import database as dbmod

    dbmod._engine = None
    dbmod._SessionLocal = None
    init_db(f"sqlite:///{db}")

    rt = RealtimeScanner(config=get_config(), paper=False)
    rt.cache.begin_generation()
    now = datetime.now(timezone.utc)
    rt.markets["m1"] = MarketInfo(
        market_id="m1",
        condition_id="c",
        yes_token_id="y",
        no_token_id="n",
    )
    rt.token_to_market["y"] = "m1"
    rt.token_to_market["n"] = "m1"
    rt.token_outcome["y"] = OutcomeSide.YES
    rt.token_outcome["n"] = OutcomeSide.NO
    rt.cache.upsert_snapshot(
        OrderBookSnapshot(
            condition_id="c",
            token_id="y",
            outcome=OutcomeSide.YES,
            asks=[OrderBookLevel(price=Decimal("0.40"), size=Decimal("10"))],
            bids=[OrderBookLevel(price=Decimal("0.39"), size=Decimal("5"))],
            fetched_at=now,
            tick_size=Decimal("0.01"),
        )
    )
    await rt._on_ws(
        {"event_type": "tick_size_change", "asset_id": "y", "new_tick_size": "0.001"},
        now,
    )
    book = rt.cache.get("y")
    assert book is not None
    assert book.tick_size == Decimal("0.001")

    sig = OpportunitySignal(
        market_id="m1",
        condition_id="c",
        direction=ArbDirection.FORWARD,
        discovered_at=now,
        data_age_seconds=0,
        quantity=Decimal("1"),
        yes_vwap=Decimal("0.4"),
        no_vwap=Decimal("0.5"),
        gross_profit=Decimal("1"),
        fee_total=Decimal("0"),
        net_profit=Decimal("1"),
        net_profit_per_share=Decimal("0.1"),
        net_profit_rate=Decimal("0.1"),
        levels_used_yes=1,
        levels_used_no=1,
    )
    ids, _ = sync_episodes([sig], scanned_market_ids={"m1"}, now=now)
    ep = ids[("m1", "forward")]
    assert episode_is_open(ep)
    await rt._on_ws({"event_type": "market_resolved", "market": "m1"}, now)
    assert not episode_is_open(ep)
    assert "m1" not in rt.markets
