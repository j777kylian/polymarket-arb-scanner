"""Realtime scanner: public market WebSocket, dirty-market recalc, latency, optional paper."""

from __future__ import annotations

import asyncio
from datetime import datetime
from decimal import Decimal
from typing import Any

from polymarket_scanner.api.clob_client import _parse_book_timestamp
from polymarket_scanner.api.market_ws import MarketWebsocketClient, diff_tokens
from polymarket_scanner.config import get_config
from polymarket_scanner.database import session_scope, utcnow
from polymarket_scanner.discovery.book_cache import LiveBookCache
from polymarket_scanner.discovery.market_discovery import discover_and_store_markets
from polymarket_scanner.discovery.orderbook_collector import persist_orderbook
from polymarket_scanner.logging_config import get_logger
from polymarket_scanner.models import MarketInfo, OpportunitySignal, OutcomeSide
from polymarket_scanner.runtime_settings import apply_runtime_to_config
from polymarket_scanner.safety import assert_trading_disabled
from polymarket_scanner.scanners.binary_complete_set import scan_binary_market
from polymarket_scanner.scanners.opportunity_tracker import close_episodes, sync_episodes
from polymarket_scanner.scanners.pipeline import flush_latency, persist_signals, record_latency
from polymarket_scanner.simulation.execution_simulator import simulate_all_profiles
from polymarket_scanner.simulation.paper_trader import execute_paper_complete_set_async

logger = get_logger(__name__)


class RealtimeScanner:
    def __init__(self, *, paper: bool = False) -> None:
        self.cfg = apply_runtime_to_config(get_config())
        self.paper = paper or self.cfg.paper.enabled
        self.cache = LiveBookCache()
        self.markets: dict[str, MarketInfo] = {}
        self.token_to_market: dict[str, str] = {}
        self.token_outcome: dict[str, OutcomeSide] = {}
        self._dirty: set[str] = set()
        self._papered_episodes: set[int] = set()
        self._paper_tasks: set[asyncio.Task[Any]] = set()
        self.ws: MarketWebsocketClient | None = None
        self.last_recalc_at: datetime | None = None
        self._last_persist: dict[str, datetime] = {}
        self._running = False

    def _index_markets(self, markets: list[MarketInfo]) -> tuple[set[str], set[str]]:
        old_tokens = set(self.token_to_market.keys())
        old_markets = set(self.markets.keys())
        self.markets = {}
        self.token_to_market = {}
        self.token_outcome = {}
        for m in markets:
            if not m.yes_token_id or not m.no_token_id:
                continue
            self.markets[m.market_id] = m
            self.token_to_market[m.yes_token_id] = m.market_id
            self.token_to_market[m.no_token_id] = m.market_id
            self.token_outcome[m.yes_token_id] = OutcomeSide.YES
            self.token_outcome[m.no_token_id] = OutcomeSide.NO
        new_tokens = set(self.token_to_market.keys())
        added, removed = diff_tokens(old_tokens, new_tokens)
        removed_markets = old_markets - set(self.markets.keys())
        if removed_markets:
            close_episodes(market_ids=removed_markets, reason="market_removed")
        return added, removed

    async def _on_connect(self, generation: int) -> None:
        self.cache.begin_generation()
        logger.info("WS generation %s — books marked not-ready until snapshots", generation)

    async def _on_disconnect(self) -> None:
        self.cache.mark_disconnected()
        close_episodes(reason="ws_disconnected")
        logger.info("WS disconnected — open episodes paused/closed")

    def _maybe_persist_book(self, token_id: str) -> None:
        book = self.cache.get(token_id)
        if book is None:
            return
        min_ms = self.cfg.scanner.ws_persist_min_interval_ms
        last = self._last_persist.get(token_id)
        now = utcnow()
        if last is not None and (now - last).total_seconds() * 1000 < min_ms:
            return
        self._last_persist[token_id] = now
        with session_scope() as session:
            persist_orderbook(session, book)

    async def _on_ws(self, event: dict[str, Any], received: datetime) -> None:
        event_type = str(event.get("event_type") or event.get("type") or "")
        raw_payload = event.get("payload")
        payload: dict[str, Any] = raw_payload if isinstance(raw_payload, dict) else event
        ts = _parse_book_timestamp(payload.get("timestamp") or event.get("timestamp"))
        if ts is not None:
            latency_ms = max(0.0, (received - ts).total_seconds() * 1000.0)
            if latency_ms < 60_000:
                record_latency(
                    event_type or "unknown",
                    latency_ms,
                    token_id=str(payload.get("asset_id") or payload.get("tokenId") or "") or None,
                    event_ts=ts,
                    received_at=received,
                )

        if event_type in {"book", "market_book"}:
            token_id = str(payload.get("asset_id") or payload.get("tokenId") or "")
            mid = self.token_to_market.get(token_id)
            outcome = self.token_outcome.get(token_id)
            if not mid or outcome is None:
                return
            self.cache.apply_book_event(payload, outcome)
            self._maybe_persist_book(token_id)
            self._dirty.add(mid)
            return

        if event_type == "price_change":
            changes = payload.get("price_changes") or payload.get("priceChanges") or []
            cond = str(payload.get("market") or "")
            for ch in changes:
                if not isinstance(ch, dict):
                    continue
                token_id = str(ch.get("asset_id") or ch.get("tokenId") or "")
                mid = self.token_to_market.get(token_id)
                outcome = self.token_outcome.get(token_id)
                if not mid or outcome is None:
                    continue
                try:
                    price = Decimal(str(ch.get("price")))
                    size = Decimal(str(ch.get("size")))
                except Exception:
                    continue
                side = str(ch.get("side") or "SELL")
                updated = self.cache.apply_price_change(
                    token_id,
                    price=price,
                    size=size,
                    side=side,
                    outcome=outcome,
                    condition_id=cond or None,
                    book_hash=ch.get("hash"),
                    fetched_at=received,
                )
                if updated is not None:
                    self._maybe_persist_book(token_id)
                    self._dirty.add(mid)

    def _recalc_dirty(self) -> list[OpportunitySignal]:
        dirty = list(self._dirty)
        self._dirty.clear()
        all_signals: list[OpportunitySignal] = []
        scanned: set[str] = set()
        max_age = float(self.cfg.scanner.max_data_age_seconds)
        stale_markets: set[str] = set()
        for market_id in dirty:
            market = self.markets.get(market_id)
            if not market or not market.yes_token_id or not market.no_token_id:
                continue
            if not self.cache.pair_ready(market.yes_token_id, market.no_token_id):
                continue
            yes_book = self.cache.get(market.yes_token_id)
            no_book = self.cache.get(market.no_token_id)
            if yes_book is None or no_book is None:
                continue
            scanned.add(market_id)
            now = utcnow()
            yes_age = (now - yes_book.fetched_at).total_seconds()
            no_age = (now - no_book.fetched_at).total_seconds()
            if yes_age > max_age or no_age > max_age:
                stale_markets.add(market_id)
            signals = scan_binary_market(market, yes_book, no_book, books_ready=True)
            sims = simulate_all_profiles(market, yes_book, no_book)
            episode_ids, stats = sync_episodes(signals, scanned_market_ids={market_id})
            persist_signals(market, signals, sims, episode_ids=episode_ids)
            all_signals.extend(signals)
            if self.paper and stats.get("opened"):
                for sig in signals:
                    if sig.direction.value != "forward" or sig.net_profit <= 0:
                        continue
                    if sig.stale or sig.books_skewed or not sig.books_ready:
                        continue
                    if sig.passes_rule_set is False:
                        continue
                    ep = episode_ids.get((sig.market_id, sig.direction.value))
                    if ep is None or ep in self._papered_episodes:
                        continue
                    self._papered_episodes.add(ep)
                    task = asyncio.create_task(self._paper_after_delay(market, sig, ep))
                    self._paper_tasks.add(task)
                    task.add_done_callback(self._paper_tasks.discard)
        if stale_markets:
            close_episodes(market_ids=stale_markets, reason="stale_books")
        self.last_recalc_at = utcnow()
        return all_signals

    async def _paper_after_delay(
        self, market: MarketInfo, sig: OpportunitySignal, episode_id: int
    ) -> None:
        delay_s = self.cfg.paper.delay_ms / 1000.0
        await asyncio.sleep(delay_s)
        if not market.yes_token_id or not market.no_token_id:
            return
        if not self.cache.pair_ready(market.yes_token_id, market.no_token_id):
            return
        yes_book = self.cache.get(market.yes_token_id)
        no_book = self.cache.get(market.no_token_id)
        if yes_book is None or no_book is None:
            return
        await execute_paper_complete_set_async(
            market, sig, yes_book, no_book, episode_id=episode_id, delay_ms=self.cfg.paper.delay_ms
        )

    async def _shutdown_paper_tasks(self) -> None:
        tasks = list(self._paper_tasks)
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        self._paper_tasks.clear()

    def _discovery_kwargs(self) -> dict[str, Any]:
        return {
            "max_pages": self.cfg.scanner.max_pages,
            "market_limit": self.cfg.scanner.market_limit,
        }

    async def run(self) -> None:
        assert_trading_disabled()
        self._running = True
        logger.info("Realtime scanner starting (paper=%s)", self.paper)
        kwargs = self._discovery_kwargs()
        markets = await discover_and_store_markets(max_pages=kwargs["max_pages"])
        if kwargs["market_limit"]:
            markets = markets[: kwargs["market_limit"]]
        self._index_markets(markets)
        token_ids = list(self.token_to_market.keys())
        self.ws = MarketWebsocketClient(
            self._on_ws, on_connect=self._on_connect, on_disconnect=self._on_disconnect
        )
        debounce = self.cfg.scanner.ws_recalc_debounce_ms / 1000.0
        last_sync = asyncio.get_event_loop().time()

        ws_task = asyncio.create_task(self.ws.run(token_ids))
        try:
            while self._running:
                await asyncio.sleep(debounce)
                self.cfg = apply_runtime_to_config(get_config())
                if self._dirty:
                    try:
                        self._recalc_dirty()
                    except Exception:
                        logger.exception("Dirty recalc failed")
                now = asyncio.get_event_loop().time()
                if (
                    self.cfg.scanner.sync_markets
                    and now - last_sync >= self.cfg.scanner.market_sync_interval_seconds
                ):
                    try:
                        kwargs = self._discovery_kwargs()
                        markets = await discover_and_store_markets(
                            max_pages=kwargs["max_pages"]
                        )
                        if kwargs["market_limit"]:
                            markets = markets[: kwargs["market_limit"]]
                        added, removed = self._index_markets(markets)
                        if self.ws:
                            await self.ws.update_subscriptions(list(self.token_to_market.keys()))
                        logger.info(
                            "Realtime resync added_tokens=%s removed_tokens=%s",
                            len(added),
                            len(removed),
                        )
                        last_sync = now
                    except Exception:
                        logger.exception("Realtime market resync failed")
        finally:
            self._running = False
            await self._shutdown_paper_tasks()
            flush_latency()
            if self.ws:
                self.ws.stop()
            ws_task.cancel()
            try:
                await ws_task
            except (asyncio.CancelledError, Exception):
                pass
