"""LiveBookCache snapshot lifecycle tests."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal

from polymarket_scanner.discovery.book_cache import LiveBookCache
from polymarket_scanner.models import OrderBookLevel, OrderBookSnapshot, OutcomeSide


def _snap(token: str, now: datetime, size: str = "10") -> OrderBookSnapshot:
    return OrderBookSnapshot(
        condition_id="c",
        token_id=token,
        outcome=OutcomeSide.YES,
        asks=[OrderBookLevel(price=Decimal("0.40"), size=Decimal(size))],
        bids=[OrderBookLevel(price=Decimal("0.39"), size=Decimal("5"))],
        fetched_at=now,
        hash="h1",
    )


def test_price_change_before_book_does_not_create_scannable_book() -> None:
    cache = LiveBookCache()
    cache.begin_generation()
    now = datetime.now(timezone.utc)
    out = cache.apply_price_change(
        "t",
        price=Decimal("0.40"),
        size=Decimal("3"),
        side="SELL",
        outcome=OutcomeSide.YES,
        fetched_at=now,
    )
    assert out is None
    assert cache.get("t") is None
    assert not cache.is_ready("t")
    assert cache.dropped_before_snapshot >= 1


def test_reconnect_marks_old_generation_not_ready() -> None:
    cache = LiveBookCache()
    now = datetime.now(timezone.utc)
    cache.begin_generation()
    cache.upsert_snapshot(_snap("t", now))
    assert cache.is_ready("t")
    gen1 = cache.generation
    cache.begin_generation()
    assert cache.generation != gen1
    assert not cache.is_ready("t")
    # price_change on old book is dropped until new snapshot
    out = cache.apply_price_change(
        "t",
        price=Decimal("0.41"),
        size=Decimal("1"),
        side="SELL",
        outcome=OutcomeSide.YES,
        fetched_at=now,
    )
    assert out is None
    assert not cache.pair_ready("y", "n")


def test_pair_ready_requires_same_generation() -> None:
    cache = LiveBookCache()
    now = datetime.now(timezone.utc)
    cache.begin_generation()
    yes = _snap("y", now)
    yes.outcome = OutcomeSide.YES
    no = OrderBookSnapshot(
        condition_id="c",
        token_id="n",
        outcome=OutcomeSide.NO,
        asks=[OrderBookLevel(price=Decimal("0.50"), size=Decimal("10"))],
        bids=[OrderBookLevel(price=Decimal("0.49"), size=Decimal("5"))],
        fetched_at=now,
    )
    cache.upsert_snapshot(yes)
    cache.upsert_snapshot(no)
    assert cache.pair_ready("y", "n")
    cache.mark_disconnected()
    assert not cache.pair_ready("y", "n")


def test_does_not_splice_old_book_after_reconnect() -> None:
    cache = LiveBookCache()
    t0 = datetime.now(timezone.utc)
    cache.begin_generation()
    cache.upsert_snapshot(_snap("t", t0, "10"))
    cache.begin_generation()
    t1 = t0 + timedelta(seconds=1)
    cache.upsert_snapshot(_snap("t", t1, "99"))
    book = cache.get("t")
    assert book is not None
    assert book.asks[0].size == Decimal("99")
    assert book.connection_generation == cache.generation
