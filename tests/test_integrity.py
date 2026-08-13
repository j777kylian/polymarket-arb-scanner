"""Order book hash dedup, discovery reconcile, pipeline rules, latency batch."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal

from sqlalchemy import func, select

from polymarket_scanner.database import (
    MarketRow,
    OpportunityRow,
    OrderBookLevelRow,
    OrderBookSnapshotRow,
    init_db,
    session_scope,
)
from polymarket_scanner.discovery.market_discovery import reconcile_unseen_markets, upsert_market
from polymarket_scanner.discovery.orderbook_collector import persist_orderbook
from polymarket_scanner.models import (
    ArbDirection,
    MarketInfo,
    OpportunitySignal,
    OrderBookLevel,
    OrderBookSnapshot,
    OutcomeSide,
    RuleCondition,
    RuleSetModel,
    SimulationQuality,
    SimulationResult,
)
from polymarket_scanner.scanners.binary_complete_set import scan_binary_market
from polymarket_scanner.scanners.pipeline import flush_latency, persist_signals, record_latency
from polymarket_scanner.scanners.rule_engine import save_rule_set


def _db(tmp_path, monkeypatch):
    db = tmp_path / "x.db"
    monkeypatch.setenv("DATABASE_URL", f"sqlite:///{db}")
    from polymarket_scanner.config import get_config

    get_config.cache_clear()
    from polymarket_scanner import database as dbmod

    dbmod._engine = None
    dbmod._SessionLocal = None
    init_db(f"sqlite:///{db}")
    return dbmod, get_config


def test_persist_same_hash_updates_last_seen(tmp_path, monkeypatch) -> None:
    dbmod, get_config = _db(tmp_path, monkeypatch)
    now = datetime.now(timezone.utc)
    book = OrderBookSnapshot(
        condition_id="c",
        token_id="y",
        outcome=OutcomeSide.YES,
        asks=[OrderBookLevel(price=Decimal("0.40"), size=Decimal("10"))],
        bids=[OrderBookLevel(price=Decimal("0.39"), size=Decimal("5"))],
        fetched_at=now,
        hash="abc",
    )
    with session_scope() as session:
        persist_orderbook(session, book)
    later = now + timedelta(seconds=2)
    book.fetched_at = later
    with session_scope() as session:
        persist_orderbook(session, book)
        n_snap = session.scalar(select(func.count()).select_from(OrderBookSnapshotRow))
        n_lvl = session.scalar(select(func.count()).select_from(OrderBookLevelRow))
        row = session.scalar(select(OrderBookSnapshotRow))
        assert n_snap == 1
        assert n_lvl == 2
        assert row is not None
        assert row.last_seen_at is not None
    dbmod._engine = None
    dbmod._SessionLocal = None
    get_config.cache_clear()


def test_reconcile_skips_when_not_full_sync(tmp_path, monkeypatch) -> None:
    dbmod, get_config = _db(tmp_path, monkeypatch)
    with session_scope() as session:
        upsert_market(
            session,
            MarketInfo(
                market_id="old",
                condition_id="c",
                yes_token_id="y",
                no_token_id="n",
                accepting_orders=True,
                enable_order_book=True,
            ),
        )
        n = reconcile_unseen_markets(session, {"new"}, full_sync=False)
        assert n == 0
        row = session.scalar(select(MarketRow).where(MarketRow.market_id == "old"))
        assert row is not None
        assert row.accepting_orders is True
        n2 = reconcile_unseen_markets(session, {"new"}, full_sync=True)
        assert n2 == 1
        assert row.accepting_orders is False
    dbmod._engine = None
    dbmod._SessionLocal = None
    get_config.cache_clear()


def test_persist_raw_and_rule_flags(tmp_path, monkeypatch) -> None:
    dbmod, get_config = _db(tmp_path, monkeypatch)
    save_rule_set(
        RuleSetModel(
            name="Balanced",
            enabled=True,
            conditions=[RuleCondition(field="net_profit", operator=">=", value=50)],
        )
    )
    now = datetime.now(timezone.utc)
    sig = OpportunitySignal(
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
        books_ready=True,
        books_skewed=False,
        stale=False,
    )
    dummy = SimulationResult(
        profile="base",
        quality=SimulationQuality.ESTIMATED,
        quantity=Decimal("10"),
        gross_profit=Decimal("1"),
        fees=Decimal("0"),
        operational_cost=Decimal("0"),
        safety_buffer=Decimal("0"),
        net_profit=Decimal("1"),
    )
    persist_signals(
        MarketInfo(market_id="m1", condition_id="c"),
        [sig],
        {"optimistic": dummy, "base": dummy, "pessimistic": dummy},
        episode_ids={("m1", "forward"): 7},
    )
    assert sig.passes_rule_set is False
    with session_scope() as session:
        row = session.scalar(select(OpportunityRow))
        assert row is not None
        assert row.passes_rule_set is False
        assert row.episode_id == 7
        assert row.rule_set_id is not None
    dbmod._engine = None
    dbmod._SessionLocal = None
    get_config.cache_clear()


def test_scan_skips_until_books_ready() -> None:
    now = datetime.now(timezone.utc)
    yes = OrderBookSnapshot(
        condition_id="c",
        token_id="y",
        outcome=OutcomeSide.YES,
        asks=[OrderBookLevel(price=Decimal("0.40"), size=Decimal("10"))],
        bids=[],
        fetched_at=now,
    )
    no = OrderBookSnapshot(
        condition_id="c",
        token_id="n",
        outcome=OutcomeSide.NO,
        asks=[OrderBookLevel(price=Decimal("0.50"), size=Decimal("10"))],
        bids=[],
        fetched_at=now,
    )
    market = MarketInfo(market_id="1", condition_id="c", yes_token_id="y", no_token_id="n")
    assert scan_binary_market(market, yes, no, books_ready=False) == []


def test_skewed_not_net_profitable(tmp_path, monkeypatch) -> None:
    dbmod, get_config = _db(tmp_path, monkeypatch)
    now = datetime.now(timezone.utc)
    sig = OpportunitySignal(
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
        books_ready=True,
        books_skewed=True,
        stale=False,
    )
    dummy = SimulationResult(
        profile="base",
        quality=SimulationQuality.ESTIMATED,
        quantity=Decimal("10"),
        gross_profit=Decimal("1"),
        fees=Decimal("0"),
        operational_cost=Decimal("0"),
        safety_buffer=Decimal("0"),
        net_profit=Decimal("1"),
    )
    persist_signals(
        MarketInfo(market_id="m1", condition_id="c"),
        [sig],
        {"base": dummy},
    )
    with session_scope() as session:
        row = session.scalar(select(OpportunityRow))
        assert row is not None
        assert row.net_profitable is False
    dbmod._engine = None
    dbmod._SessionLocal = None
    get_config.cache_clear()


def test_latency_batches_until_flush(tmp_path, monkeypatch) -> None:
    dbmod, get_config = _db(tmp_path, monkeypatch)
    from sqlalchemy import delete

    from polymarket_scanner.database import LatencySampleRow
    from polymarket_scanner.scanners import pipeline as pipeline_mod

    with pipeline_mod._latency_lock:
        pipeline_mod._latency_buf.clear()
    with session_scope() as session:
        session.execute(delete(LatencySampleRow))
    record_latency("book", 12.0, token_id="t")
    with session_scope() as session:
        n = session.scalar(select(func.count()).select_from(LatencySampleRow))
        assert n == 0
    flush_latency()
    with session_scope() as session:
        n = session.scalar(select(func.count()).select_from(LatencySampleRow))
        assert n == 1
    dbmod._engine = None
    dbmod._SessionLocal = None
    get_config.cache_clear()
