"""Incremental order-book cache tests."""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal

from polymarket_scanner.discovery.book_cache import LiveBookCache
from polymarket_scanner.models import OrderBookLevel, OrderBookSnapshot, OutcomeSide


def test_price_change_updates_ask_and_removes_zero() -> None:
    cache = LiveBookCache()
    now = datetime.now(timezone.utc)
    cache.upsert_snapshot(
        OrderBookSnapshot(
            condition_id="c",
            token_id="t",
            outcome=OutcomeSide.YES,
            asks=[
                OrderBookLevel(price=Decimal("0.40"), size=Decimal("10")),
                OrderBookLevel(price=Decimal("0.41"), size=Decimal("20")),
            ],
            bids=[OrderBookLevel(price=Decimal("0.39"), size=Decimal("5"))],
            fetched_at=now,
        )
    )
    cache.apply_price_change(
        "t",
        price=Decimal("0.40"),
        size=Decimal("3"),
        side="SELL",
        outcome=OutcomeSide.YES,
        fetched_at=now,
    )
    book = cache.get("t")
    assert book is not None
    assert book.asks[0].price == Decimal("0.40")
    assert book.asks[0].size == Decimal("3")

    cache.apply_price_change(
        "t",
        price=Decimal("0.40"),
        size=Decimal("0"),
        side="SELL",
        outcome=OutcomeSide.YES,
        fetched_at=now,
    )
    book = cache.get("t")
    assert book is not None
    assert all(lvl.price != Decimal("0.40") for lvl in book.asks)
