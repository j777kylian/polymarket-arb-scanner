"""Signed feed latency and host clock-skew observability (no network, no sleeps)."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest
from sqlalchemy import select

from polymarket_scanner.config import get_config
from polymarket_scanner.database import LatencySampleRow, init_db, session_scope
from polymarket_scanner.realtime import RealtimeScanner
from polymarket_scanner.scanners.pipeline import flush_latency, record_latency
from polymarket_scanner.scheduler import get_dashboard_stats, latency_sufficiency_label


def _db(tmp_path, monkeypatch) -> None:
    db = tmp_path / "t.db"
    monkeypatch.setenv("DATABASE_URL", f"sqlite:///{db}")
    get_config.cache_clear()
    from polymarket_scanner import database as dbmod
    from polymarket_scanner.scanners import pipeline as pipeline_mod

    dbmod._engine = None
    dbmod._SessionLocal = None
    init_db(f"sqlite:///{db}")
    with pipeline_mod._latency_lock:
        pipeline_mod._latency_buf.clear()


def _received() -> datetime:
    return datetime(2026, 8, 14, 12, 0, 0, tzinfo=timezone.utc)


@pytest.mark.asyncio
async def test_signed_feed_latency_persists_negative_when_event_ts_is_ahead(
    tmp_path, monkeypatch
) -> None:
    _db(tmp_path, monkeypatch)
    rt = RealtimeScanner(config=get_config(), paper=False)
    received = _received()
    event_ms = int(received.timestamp() * 1000) + 1250
    await rt._on_ws(
        {
            "event_type": "price_change",
            "timestamp": str(event_ms),
            "asset_id": "y",
            "price_changes": [],
        },
        received,
    )
    flush_latency()
    with session_scope() as session:
        rows = list(session.scalars(select(LatencySampleRow)))
        assert len(rows) == 1
        assert rows[0].event_type == "price_change"
        assert rows[0].latency_ms == pytest.approx(-1250.0, abs=0.5)
        assert rows[0].latency_ms < 0


@pytest.mark.asyncio
async def test_signed_feed_latency_persists_positive_when_event_ts_is_behind(
    tmp_path, monkeypatch
) -> None:
    _db(tmp_path, monkeypatch)
    rt = RealtimeScanner(config=get_config(), paper=False)
    received = _received()
    event_ms = int(received.timestamp() * 1000) - 80
    await rt._on_ws(
        {
            "event_type": "last_trade_price",
            "timestamp": str(event_ms),
            "asset_id": "y",
        },
        received,
    )
    flush_latency()
    with session_scope() as session:
        rows = list(session.scalars(select(LatencySampleRow)))
        assert len(rows) == 1
        assert rows[0].event_type == "last_trade_price"
        assert rows[0].latency_ms == pytest.approx(80.0, abs=0.5)
        assert rows[0].latency_ms > 0


def test_dashboard_clock_skew_forces_insufficient_and_keeps_signed_percentiles(
    tmp_path, monkeypatch
) -> None:
    _db(tmp_path, monkeypatch)
    record_latency("price_change", -1110.0, token_id="t")
    record_latency("price_change", -1420.0, token_id="t")
    record_latency("last_trade_price", 12.0, token_id="t")
    flush_latency()
    stats = get_dashboard_stats()
    assert stats["clock_skew_detected"] is True
    assert stats["latency_sufficient"] is False
    assert stats["latency_p50_ms"] is not None
    assert stats["latency_p95_ms"] is not None
    assert stats["latency_p50_ms"] < 0
    assert stats["latency_p95_ms"] < 0
    assert latency_sufficiency_label(stats) != "sufficient"


def test_dashboard_positive_latency_sufficient_without_clock_skew(
    tmp_path, monkeypatch
) -> None:
    _db(tmp_path, monkeypatch)
    record_latency("price_change", 12.0, token_id="t")
    record_latency("price_change", 20.0, token_id="t")
    flush_latency()
    stats = get_dashboard_stats()
    assert stats["clock_skew_detected"] is False
    assert stats["latency_sufficient"] is True
    assert stats["latency_p50_ms"] == pytest.approx(12.0)
    assert stats["latency_p95_ms"] == pytest.approx(12.0)
    assert latency_sufficiency_label(stats) == "sufficient"


def test_dashboard_latency_within_tolerance_is_not_clock_skew(tmp_path, monkeypatch) -> None:
    _db(tmp_path, monkeypatch)
    record_latency("price_change", -100.0, token_id="t")
    record_latency("price_change", 50.0, token_id="t")
    flush_latency()
    stats = get_dashboard_stats()
    assert stats["clock_skew_detected"] is False
    assert stats["latency_sufficient"] is True
    assert stats["latency_p50_ms"] == pytest.approx(-100.0)
    assert stats["latency_p95_ms"] == pytest.approx(-100.0)
