"""Live ScannerRunRow metrics: books_fetched, signals_found, api_errors."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal

from sqlalchemy import select

from polymarket_scanner.config import get_config
from polymarket_scanner.database import (
    ApiErrorRow,
    OpportunityRow,
    ScannerRunRow,
    init_db,
    session_scope,
)
from polymarket_scanner.models import MarketInfo, OrderBookLevel, OrderBookSnapshot, OutcomeSide
from polymarket_scanner.realtime import RealtimeScanner


def _tmp_db(tmp_path, monkeypatch) -> None:
    db = tmp_path / "t.db"
    monkeypatch.setenv("DATABASE_URL", f"sqlite:///{db}")
    get_config.cache_clear()
    from polymarket_scanner import database as dbmod

    dbmod._engine = None
    dbmod._SessionLocal = None
    init_db(f"sqlite:///{db}")


def _opp(*, market_id: str, discovered_at: datetime) -> OpportunityRow:
    return OpportunityRow(
        market_id=market_id,
        condition_id=f"c-{market_id}",
        direction="forward",
        discovered_at=discovered_at,
        quantity="10",
        yes_vwap="0.4",
        no_vwap="0.5",
        gross_profit="1",
        fee_total="0",
        net_profit="1",
        net_profit_per_share="0.1",
        net_profit_rate="0.1",
        net_profitable=True,
    )


def _ready_pair(rt: RealtimeScanner, market_id: str, *, at: datetime) -> None:
    yes = f"y-{market_id}"
    no = f"n-{market_id}"
    rt.markets[market_id] = MarketInfo(
        market_id=market_id,
        condition_id=f"c-{market_id}",
        yes_token_id=yes,
        no_token_id=no,
        fee_schedule=None,
    )
    rt.token_to_market[yes] = market_id
    rt.token_to_market[no] = market_id
    rt.token_outcome[yes] = OutcomeSide.YES
    rt.token_outcome[no] = OutcomeSide.NO
    for token_id, outcome in ((yes, OutcomeSide.YES), (no, OutcomeSide.NO)):
        rt.cache.upsert_snapshot(
            OrderBookSnapshot(
                condition_id=f"c-{market_id}",
                token_id=token_id,
                outcome=outcome,
                asks=[OrderBookLevel(price=Decimal("0.40"), size=Decimal("10"))],
                bids=[OrderBookLevel(price=Decimal("0.39"), size=Decimal("5"))],
                fetched_at=at,
                tick_size=Decimal("0.01"),
            )
        )


def test_live_run_metrics_exclude_preexisting_and_count_current(tmp_path, monkeypatch) -> None:
    _tmp_db(tmp_path, monkeypatch)

    started = datetime(2026, 8, 14, 12, 0, 0, tzinfo=timezone.utc)
    before = started - timedelta(minutes=30)
    during = started + timedelta(minutes=5)

    with session_scope() as session:
        session.add(_opp(market_id="old-m", discovered_at=before))
        session.add(
            ApiErrorRow(
                created_at=before,
                source="test",
                message="pre-existing error",
            )
        )
        run = ScannerRunRow(started_at=started, status="running", mode="live")
        session.add(run)
        session.flush()
        run_id = run.id
        session.add(_opp(market_id="new-m1", discovered_at=started))
        session.add(_opp(market_id="new-m2", discovered_at=during))
        session.add(
            ApiErrorRow(
                created_at=started,
                source="test",
                message="current-run error A",
            )
        )
        session.add(
            ApiErrorRow(
                created_at=during,
                source="test",
                message="current-run error B",
            )
        )

    rt = RealtimeScanner(config=get_config(), paper=False)
    rt.run_id = run_id
    rt.discovered_markets = 7
    rt.cache.begin_generation()
    _ready_pair(rt, "m1", at=during)
    _ready_pair(rt, "m2", at=during)
    # Third market indexed but not ready — must not inflate books_fetched.
    rt.markets["m3"] = MarketInfo(
        market_id="m3",
        condition_id="c-m3",
        yes_token_id="y-m3",
        no_token_id="n-m3",
    )
    rt.token_to_market["y-m3"] = "m3"
    rt.token_to_market["n-m3"] = "m3"

    rt._record_run_stats(status="stopped", finished=True)

    with session_scope() as session:
        row = session.get(ScannerRunRow, run_id)
        assert row is not None
        assert row.status == "stopped"
        assert row.finished_at is not None
        assert row.mode == "live"
        assert row.discovered_markets == 7
        assert row.subscribed_markets == 3
        assert row.subscribed_tokens == 6
        assert row.ready_market_pairs == 2
        assert row.books_fetched == 2
        assert row.fee_schedule_coverage == "0/3"
        assert row.markets_synced == 3
        assert row.signals_found == 2
        assert row.api_errors == 2

        # Sanity: DB still has the excluded pre-existing rows.
        assert session.scalar(select(OpportunityRow).where(OpportunityRow.market_id == "old-m"))
        assert (
            session.scalar(
                select(ApiErrorRow).where(ApiErrorRow.message == "pre-existing error")
            )
            is not None
        )


def test_live_run_metrics_zero_when_no_current_records(tmp_path, monkeypatch) -> None:
    _tmp_db(tmp_path, monkeypatch)

    started = datetime(2026, 8, 14, 15, 0, 0, tzinfo=timezone.utc)
    before = started - timedelta(hours=1)

    with session_scope() as session:
        session.add(_opp(market_id="old-only", discovered_at=before))
        session.add(
            ApiErrorRow(created_at=before, source="test", message="old only")
        )
        run = ScannerRunRow(started_at=started, status="running", mode="live")
        session.add(run)
        session.flush()
        run_id = run.id

    rt = RealtimeScanner(config=get_config(), paper=False)
    rt.run_id = run_id
    rt.discovered_markets = 1
    rt.cache.begin_generation()
    _ready_pair(rt, "only", at=started)

    rt._record_run_stats(status="running", finished=False)

    with session_scope() as session:
        row = session.get(ScannerRunRow, run_id)
        assert row is not None
        assert row.status == "running"
        assert row.finished_at is None
        assert row.books_fetched == 1
        assert row.ready_market_pairs == 1
        assert row.signals_found == 0
        assert row.api_errors == 0
        assert row.discovered_markets == 1
        assert row.subscribed_markets == 1
        assert row.subscribed_tokens == 2
