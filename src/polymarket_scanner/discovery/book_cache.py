"""In-memory YES/NO order books with snapshot lifecycle and incremental patches."""

from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass, field
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


@dataclass
class BookState:
    snapshot: OrderBookSnapshot | None = None
    initialized_from_full_snapshot: bool = False
    connection_generation: int = 0
    last_full_snapshot_at: datetime | None = None
    last_update_at: datetime | None = None
    ready: bool = False
    pending_changes: list[dict[str, Any]] = field(default_factory=list)
    dropped_price_changes: int = 0


class LiveBookCache:
    """token_id -> book state. price_change is ignored until a full book snapshot."""

    def __init__(self) -> None:
        self._states: dict[str, BookState] = {}
        self.generation: int = 0
        self.dropped_before_snapshot: int = 0

    def _state(self, token_id: str) -> BookState:
        st = self._states.get(token_id)
        if st is None:
            st = BookState(connection_generation=self.generation)
            self._states[token_id] = st
        return st

    def begin_generation(self) -> int:
        """New WS connection. All prior books become not-ready."""
        self.generation += 1
        for st in self._states.values():
            st.ready = False
            st.initialized_from_full_snapshot = False
            st.pending_changes.clear()
            st.connection_generation = self.generation
        return self.generation

    def mark_disconnected(self) -> None:
        for st in self._states.values():
            st.ready = False

    def get(self, token_id: str) -> OrderBookSnapshot | None:
        st = self._states.get(token_id)
        if st is None or st.snapshot is None:
            return None
        return deepcopy(st.snapshot)

    def get_state(self, token_id: str) -> BookState | None:
        return self._states.get(token_id)

    def is_ready(self, token_id: str) -> bool:
        st = self._states.get(token_id)
        return bool(
            st
            and st.ready
            and st.initialized_from_full_snapshot
            and st.connection_generation == self.generation
            and st.snapshot is not None
        )

    def pair_ready(self, yes_token: str, no_token: str) -> bool:
        if not self.is_ready(yes_token) or not self.is_ready(no_token):
            return False
        y = self._states[yes_token]
        n = self._states[no_token]
        return y.connection_generation == n.connection_generation == self.generation

    def pair_skew_ms(self, yes_token: str, no_token: str) -> float | None:
        y = self.get(yes_token)
        n = self.get(no_token)
        if y is None or n is None:
            return None
        delta = abs((y.fetched_at - n.fetched_at).total_seconds() * 1000.0)
        return delta

    def upsert_snapshot(self, book: OrderBookSnapshot) -> None:
        st = self._state(book.token_id)
        book.connection_generation = self.generation
        st.snapshot = book
        st.initialized_from_full_snapshot = True
        st.connection_generation = self.generation
        st.last_full_snapshot_at = book.fetched_at
        st.last_update_at = book.fetched_at
        st.ready = True

    def apply_book_event(self, payload: dict[str, Any], outcome: OutcomeSide) -> OrderBookSnapshot:
        book = parse_orderbook(payload, outcome=outcome, expected_token_id=payload.get("asset_id"))
        book.connection_generation = self.generation
        st = self._state(book.token_id)
        st.snapshot = book
        st.initialized_from_full_snapshot = True
        st.connection_generation = self.generation
        st.last_full_snapshot_at = book.fetched_at
        st.last_update_at = book.fetched_at
        st.ready = True
        pending = list(st.pending_changes)
        st.pending_changes.clear()
        for change in pending:
            self.apply_price_change(**change)
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
        now = fetched_at or datetime.now(timezone.utc)
        st = self._state(token_id)
        if not st.initialized_from_full_snapshot or st.connection_generation != self.generation:
            st.dropped_price_changes += 1
            self.dropped_before_snapshot += 1
            st.pending_changes.append(
                {
                    "token_id": token_id,
                    "price": price,
                    "size": size,
                    "side": side,
                    "outcome": outcome,
                    "condition_id": condition_id,
                    "book_hash": book_hash,
                    "fetched_at": now,
                }
            )
            return None

        existing = st.snapshot
        if existing is None:
            st.dropped_price_changes += 1
            self.dropped_before_snapshot += 1
            return None

        bids = list(existing.bids)
        asks = list(existing.asks)
        bid_map = _levels_to_map(bids)
        ask_map = _levels_to_map(asks)
        side_u = side.upper()
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
            condition_id=condition_id or existing.condition_id,
            token_id=token_id,
            outcome=outcome,
            timestamp=now,
            hash=book_hash,
            bids=_map_to_levels(bid_map, reverse=True),
            asks=_map_to_levels(ask_map, reverse=False),
            tick_size=existing.tick_size,
            min_order_size=existing.min_order_size,
            neg_risk=existing.neg_risk,
            fetched_at=now,
            connection_generation=self.generation,
        )
        st.snapshot = updated
        st.last_update_at = now
        st.ready = True
        return updated

    def apply_tick_size_change(
        self, token_id: str, tick_size: Decimal, *, fetched_at: datetime | None = None
    ) -> OrderBookSnapshot | None:
        st = self._states.get(token_id)
        if st is None or st.snapshot is None or not st.initialized_from_full_snapshot:
            return None
        if st.connection_generation != self.generation:
            return None
        now = fetched_at or datetime.now(timezone.utc)
        book = st.snapshot.model_copy(update={"tick_size": tick_size, "fetched_at": now})
        st.snapshot = book
        st.last_update_at = now
        return book

    def ready_pair_count(self, markets: dict[str, Any]) -> int:
        n = 0
        for m in markets.values():
            yes = getattr(m, "yes_token_id", None)
            no = getattr(m, "no_token_id", None)
            if yes and no and self.pair_ready(yes, no):
                n += 1
        return n
