"""Opportunity episode first-seen / disappear tests."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal

from polymarket_scanner.database import OpportunityEpisodeRow, init_db, session_scope
from polymarket_scanner.models import ArbDirection, OpportunitySignal
from polymarket_scanner.scanners.opportunity_tracker import sync_episodes
from sqlalchemy import select


def _sig(now: datetime) -> OpportunitySignal:
    return OpportunitySignal(
        market_id="m1",
        condition_id="c",
        direction=ArbDirection.FORWARD,
        discovered_at=now,
        data_age_seconds=1,
        quantity=Decimal("10"),
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


def test_episode_open_and_close(tmp_path, monkeypatch) -> None:
    db = tmp_path / "e.db"
    monkeypatch.setenv("DATABASE_URL", f"sqlite:///{db}")
    from polymarket_scanner.config import get_config

    get_config.cache_clear()
    from polymarket_scanner import database as dbmod

    dbmod._engine = None
    dbmod._SessionLocal = None
    url = f"sqlite:///{db}"
    init_db(url)

    t0 = datetime.now(timezone.utc)
    ids, stats = sync_episodes([_sig(t0)], scanned_market_ids={"m1"}, now=t0)
    assert stats["opened"] == 1
    assert ("m1", "forward") in ids

    t1 = t0 + timedelta(seconds=12)
    _ids2, stats2 = sync_episodes([], scanned_market_ids={"m1"}, now=t1)
    assert stats2["closed"] == 1
    with session_scope(url) as session:
        row = session.scalar(select(OpportunityEpisodeRow))
        assert row is not None
        assert row.is_open is False
        assert row.duration_seconds == 12
    dbmod._engine = None
    dbmod._SessionLocal = None
    get_config.cache_clear()
