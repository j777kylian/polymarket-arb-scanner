"""FOK/FAK and paper merge/capital tests."""

from __future__ import annotations

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
from polymarket_scanner.simulation.fok_fak import fill_buy
from polymarket_scanner.simulation.paper_trader import execute_paper_complete_set, get_paper_account


def _asks(*levels: tuple[str, str]) -> list[OrderBookLevel]:
    return [OrderBookLevel(price=Decimal(p), size=Decimal(s)) for p, s in levels]


def test_fok_rejects_partial() -> None:
    filled, cost, _fees, fills, status = fill_buy(
        _asks(("0.40", "10")),
        Decimal("50"),
        None,
        fees_enabled=False,
        tif="FOK",
    )
    assert status == "rejected_fok"
    assert filled == 0
    assert cost == 0
    assert fills == []


def test_fak_partial() -> None:
    filled, cost, _fees, fills, status = fill_buy(
        _asks(("0.40", "10")),
        Decimal("50"),
        None,
        fees_enabled=False,
        tif="FAK",
    )
    assert status == "partial_fak"
    assert filled == Decimal("10")
    assert cost == Decimal("4.0")
    assert len(fills) == 1


def test_paper_merge_recycles_capital(tmp_path, monkeypatch) -> None:
    db = tmp_path / "t.db"
    monkeypatch.setenv("DATABASE_URL", f"sqlite:///{db}")
    from polymarket_scanner.config import get_config

    get_config.cache_clear()
    from polymarket_scanner import database as dbmod

    dbmod._engine = None
    dbmod._SessionLocal = None
    init_db(f"sqlite:///{db}")

    now = datetime.now(timezone.utc)
    yes = OrderBookSnapshot(
        condition_id="c",
        token_id="y",
        outcome=OutcomeSide.YES,
        asks=_asks(("0.40", "100")),
        bids=_asks(("0.39", "100")),
        fetched_at=now,
    )
    no = OrderBookSnapshot(
        condition_id="c",
        token_id="n",
        outcome=OutcomeSide.NO,
        asks=_asks(("0.50", "100")),
        bids=_asks(("0.49", "100")),
        fetched_at=now,
    )
    market = MarketInfo(
        market_id="m1",
        condition_id="c",
        yes_token_id="y",
        no_token_id="n",
        fees_enabled=False,
        minimum_order_size=Decimal("1"),
    )
    sig = OpportunitySignal(
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
    result = execute_paper_complete_set(market, sig, yes, no, tif="FOK", delay_ms=500, skip_min_profit=True)
    assert result is not None
    assert result["status"] == "merged"
    cash, _occ, pnl = get_paper_account()
    # Cost 0.40+0.50=0.90 per share * 10 = 9; merge returns 10; pnl = 1
    assert pnl == Decimal("1.0")
    assert cash == Decimal("1001.0")
    dbmod._engine = None
    dbmod._SessionLocal = None
    get_config.cache_clear()
