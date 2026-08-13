"""In-memory YES/NO order books with incremental price_change patches."""

from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

from polymarket_scanner.api.clob_client import parse_orderbook
from polymarket_scanner.models import OrderBookLevel, OrderBookSnapshot, OutcomeSide

ZERO = Decimal("0")
ONE = Decimal("1")


def _levels_to_map(levels: list[OrderBookLevel]) -> dict[Decimal, Decimal]:
    return {lvl.price: lvl.size for lvl in levels}


def _map_to_levels(book_map: dict[Decimal, Decimal], *, reverse: bool) -> list[OrderBookLevel]:
    items = [(p, s) for p, s in book_map.items() if s > ZERO and Decimal("0") < p <= ONE]
    items.sort(key=lambda x: x[0], reverse=reverse)
    return [OrderBookLevel(price=p, size=s) for p, s in items]


class LiveBookCache:
    """token_id -> snapshot. Apply full book dumps or incremental price_change."""

    def __init__(self) -> None:
        self._books: dict[str, OrderBookSnapshot] = {}

    def get(self, token_id: str) -> OrderBookSnapshot | None:
        book = self._books.get(token_id)
        return deepcopy(book) if book else None

    def upsert_snapshot(self, book: OrderBookSnapshot) -> None:
        self._books[book.token_id] = book

    def apply_book_event(self, payload: dict[str, Any], outcome: OutcomeSide) -> OrderBookSnapshot:
        book = parse_orderbook(payload, outcome=outcome, expected_token_id=payload.get("asset_id"))
        self._books[book.token_id] = book
        return book

    def apply_price_change(
        self,
        token_id: str,
        *,
        price: Decimal,
        size: Decimal,
        side: str,
        outcome: OutcomeSide,
        condition_id: str | None = None,
        book_hash: str | None = None,
        fetched_at: datetime | None = None,
    ) -> OrderBookSnapshot | None:
        existing = self._books.get(token_id)
        now = fetched_at or datetime.now(timezone.utc)
        if existing is None:
            bids: list[OrderBookLevel] = []
            asks: list[OrderBookLevel] = []
            tick = Decimal("0.01")
            min_sz = Decimal("5")
            neg = False
            cond = condition_id or ""
        else:
            bids = list(existing.bids)
            asks = list(existing.asks)
            tick = existing.tick_size
            min_sz = existing.min_order_size
            neg = existing.neg_risk
            cond = condition_id or existing.condition_id

        bid_map = _levels_to_map(bids)
        ask_map = _levels_to_map(asks)
        side_u = side.upper()
        # BUY restocks bids; SELL restocks asks. size=0 removes the level.
        if side_u in {"BUY", "BID"}:
            if size <= ZERO:
                bid_map.pop(price, None)
            else:
                bid_map[price] = size
        else:
            if size <= ZERO:
                ask_map.pop(price, None)
            else:
                ask_map[price] = size

        updated = OrderBookSnapshot(
            condition_id=cond,
            token_id=token_id,
            outcome=outcome,
            timestamp=now,
            hash=book_hash,
            bids=_map_to_levels(bid_map, reverse=True),
            asks=_map_to_levels(ask_map, reverse=False),
            tick_size=tick,
            min_order_size=min_sz,
            neg_risk=neg,
            fetched_at=now,
        )
        self._books[token_id] = updated
        return updated
