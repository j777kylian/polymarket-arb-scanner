"""Order book collection with concurrency limits and hash dedup."""

from __future__ import annotations

import asyncio
import json
from datetime import datetime, timedelta

from sqlalchemy import desc, select

from polymarket_scanner.api.clob_client import ClobClient
from polymarket_scanner.config import get_config
from polymarket_scanner.database import (
    OrderBookLevelRow,
    OrderBookSnapshotRow,
    decimal_to_str,
    ensure_utc,
    record_api_error,
    session_scope,
    utcnow,
)
from polymarket_scanner.logging_config import get_logger
from polymarket_scanner.models import MarketInfo, OrderBookSnapshot, OutcomeSide

logger = get_logger(__name__)


def persist_orderbook(session, book: OrderBookSnapshot, *, skip_duplicate_hash: bool = True) -> int | None:
    if skip_duplicate_hash and book.hash:
        existing = session.scalar(
            select(OrderBookSnapshotRow)
            .where(
                OrderBookSnapshotRow.token_id == book.token_id,
                OrderBookSnapshotRow.hash == book.hash,
            )
            .order_by(desc(OrderBookSnapshotRow.fetched_at))
            .limit(1)
        )
        if existing is not None:
            existing.last_seen_at = book.fetched_at or utcnow()
            if book.connection_generation is not None:
                existing.connection_generation = book.connection_generation
            return existing.id

    row = OrderBookSnapshotRow(
        condition_id=book.condition_id,
        token_id=book.token_id,
        outcome=book.outcome.value,
        timestamp=book.timestamp,
        hash=book.hash,
        tick_size=decimal_to_str(book.tick_size),
        min_order_size=decimal_to_str(book.min_order_size),
        neg_risk=book.neg_risk,
        fetched_at=book.fetched_at,
        last_seen_at=book.fetched_at,
        connection_generation=book.connection_generation,
        raw_json=json.dumps(book.raw) if book.raw else None,
    )
    session.add(row)
    session.flush()
    for i, lvl in enumerate(book.bids):
        session.add(
            OrderBookLevelRow(
                snapshot_id=row.id,
                side="bid",
                level_index=i,
                price=decimal_to_str(lvl.price) or "0",
                size=decimal_to_str(lvl.size) or "0",
            )
        )
    for i, lvl in enumerate(book.asks):
        session.add(
            OrderBookLevelRow(
                snapshot_id=row.id,
                side="ask",
                level_index=i,
                price=decimal_to_str(lvl.price) or "0",
                size=decimal_to_str(lvl.size) or "0",
            )
        )
    return row.id


def row_to_snapshot(row: OrderBookSnapshotRow) -> OrderBookSnapshot:
    from decimal import Decimal

    from polymarket_scanner.models import OrderBookLevel

    bids = []
    asks = []
    for lvl in sorted(row.levels, key=lambda x: x.level_index):
        item = OrderBookLevel(price=Decimal(lvl.price), size=Decimal(lvl.size))
        if lvl.side == "bid":
            bids.append(item)
        else:
            asks.append(item)
    return OrderBookSnapshot(
        condition_id=row.condition_id,
        token_id=row.token_id,
        outcome=OutcomeSide(row.outcome),
        timestamp=ensure_utc(row.timestamp),
        hash=row.hash,
        bids=bids,
        asks=asks,
        tick_size=Decimal(row.tick_size or "0.01"),
        min_order_size=Decimal(row.min_order_size or "5"),
        neg_risk=row.neg_risk,
        fetched_at=ensure_utc(row.fetched_at) or utcnow(),
        raw=json.loads(row.raw_json) if row.raw_json else None,
    )


def latest_books_for_market(
    condition_id: str, yes_token: str, no_token: str
) -> tuple[OrderBookSnapshot | None, OrderBookSnapshot | None]:
    with session_scope() as session:
        def latest(token_id: str) -> OrderBookSnapshot | None:
            row = session.scalar(
                select(OrderBookSnapshotRow)
                .where(OrderBookSnapshotRow.token_id == token_id)
                .order_by(desc(OrderBookSnapshotRow.fetched_at))
                .limit(1)
            )
            if row is None:
                return None
            # ensure levels loaded
            _ = row.levels
            return row_to_snapshot(row)

        return latest(yes_token), latest(no_token)


def snapshot_in_window(
    token_id: str,
    *,
    target: datetime,
    tolerance_ms: float,
) -> OrderBookSnapshot | None:
    """Return a snapshot with target <= fetched_at <= target + tolerance."""
    end = target + timedelta(milliseconds=tolerance_ms)
    with session_scope() as session:
        row = session.scalar(
            select(OrderBookSnapshotRow)
            .where(
                OrderBookSnapshotRow.token_id == token_id,
                OrderBookSnapshotRow.fetched_at >= target,
                OrderBookSnapshotRow.fetched_at <= end,
            )
            .order_by(OrderBookSnapshotRow.fetched_at)
            .limit(1)
        )
        if row is None:
            return None
        _ = row.levels
        return row_to_snapshot(row)


async def fetch_books_for_market(
    clob: ClobClient,
    market: MarketInfo,
    semaphore: asyncio.Semaphore,
) -> tuple[OrderBookSnapshot | None, OrderBookSnapshot | None, list[str]]:
    errors: list[str] = []
    yes_book = no_book = None
    if not market.yes_token_id or not market.no_token_id:
        return None, None, ["missing token ids"]

    async def one(token_id: str, outcome: OutcomeSide) -> OrderBookSnapshot | None:
        async with semaphore:
            try:
                return await clob.get_order_book(token_id, outcome=outcome)
            except Exception as exc:
                msg = f"{outcome.value} book failed for {market.market_id}: {exc}"
                logger.warning(msg)
                errors.append(msg)
                with session_scope() as session:
                    record_api_error(
                        session,
                        source="clob",
                        message=str(exc),
                        endpoint=f"/book?token_id={token_id}",
                    )
                return None

    yes_book, no_book = await asyncio.gather(
        one(market.yes_token_id, OutcomeSide.YES),
        one(market.no_token_id, OutcomeSide.NO),
    )
    return yes_book, no_book, errors


async def collect_orderbooks(
    markets: list[MarketInfo],
    *,
    max_concurrent: int | None = None,
) -> dict[str, tuple[OrderBookSnapshot | None, OrderBookSnapshot | None]]:
    cfg = get_config()
    sem = asyncio.Semaphore(max_concurrent or cfg.api.max_concurrent_requests)
    results: dict[str, tuple[OrderBookSnapshot | None, OrderBookSnapshot | None]] = {}
    async with ClobClient() as clob:
        tasks = [fetch_books_for_market(clob, m, sem) for m in markets]
        fetched = await asyncio.gather(*tasks, return_exceptions=True)
        with session_scope() as session:
            for market, item in zip(markets, fetched):
                if not isinstance(item, tuple):
                    logger.error("Book collect failed for %s: %s", market.market_id, item)
                    results[market.market_id] = (None, None)
                    record_api_error(session, source="clob", message=str(item))
                    continue
                yes_book, no_book, _errs = item
                if yes_book:
                    persist_orderbook(session, yes_book)
                if no_book:
                    persist_orderbook(session, no_book)
                results[market.market_id] = (yes_book, no_book)
    return results
