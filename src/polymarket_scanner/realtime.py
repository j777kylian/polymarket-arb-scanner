"""Realtime scanner: public market WebSocket, dirty-market recalc, latency, optional paper."""

from __future__ import annotations

import asyncio
from datetime import datetime
from decimal import Decimal
from typing import Any

from polymarket_scanner.api.clob_client import _parse_book_timestamp
from polymarket_scanner.api.market_ws import MarketWebsocketClient
from polymarket_scanner.config import get_config
from polymarket_scanner.runtime_settings import apply_runtime_to_config
from polymarket_scanner.database import utcnow
from polymarket_scanner.discovery.book_cache import LiveBookCache
from polymarket_scanner.discovery.market_discovery import discover_and_store_markets
from polymarket_scanner.logging_config import get_logger
from polymarket_scanner.models import MarketInfo, OpportunitySignal, OutcomeSide
from polymarket_scanner.safety import assert_trading_disabled
from polymarket_scanner.scanners.opportunity_tracker import sync_episodes
from polymarket_scanner.scanners.pipeline import persist_signals, record_latency
from polymarket_scanner.scanners.binary_complete_set import scan_binary_market
from polymarket_scanner.simulation.execution_simulator import simulate_all_profiles
from polymarket_scanner.simulation.paper_trader import execute_paper_complete_set

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
        self.ws: MarketWebsocketClient | None = None
        self.last_recalc_at: datetime | None = None

    def _index_markets(self, markets: list[MarketInfo]) -> None:
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

    async def _on_ws(self, event: dict[str, Any], received: datetime) -> None:
        event_type = str(event.get("event_type") or event.get("type") or "")
        payload = event.get("payload") if isinstance(event.get("payload"), dict) else event
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
                self.cache.apply_price_change(
                    token_id,
                    price=price,
                    size=size,
                    side=side,
                    outcome=outcome,
                    condition_id=cond or None,
                    book_hash=ch.get("hash"),
                    fetched_at=received,
                )
                self._dirty.add(mid)

    def _recalc_dirty(self) -> list[OpportunitySignal]:
        dirty = list(self._dirty)
        self._dirty.clear()
        all_signals: list[OpportunitySignal] = []
        scanned: set[str] = set()
        for market_id in dirty:
            market = self.markets.get(market_id)
            if not market or not market.yes_token_id or not market.no_token_id:
                continue
            yes_book = self.cache.get(market.yes_token_id)
            no_book = self.cache.get(market.no_token_id)
            if yes_book is None or no_book is None:
                continue
            scanned.add(market_id)
            signals = scan_binary_market(market, yes_book, no_book)
            sims = simulate_all_profiles(market, yes_book, no_book)
            episode_ids, stats = sync_episodes(signals, scanned_market_ids={market_id})
            persist_signals(market, signals, sims, episode_ids=episode_ids)
            all_signals.extend(signals)
            if self.paper and stats.get("opened"):
                for sig in signals:
                    if sig.direction.value != "forward" or sig.net_profit <= 0:
                        continue
                    ep = episode_ids.get((sig.market_id, sig.direction.value))
                    if ep is None or ep in self._papered_episodes:
                        continue
                    self._papered_episodes.add(ep)
                    asyncio.create_task(self._paper_after_delay(market, sig, ep))
        self.last_recalc_at = utcnow()
        return all_signals

    async def _paper_after_delay(
        self, market: MarketInfo, sig: OpportunitySignal, episode_id: int
    ) -> None:
        delay_s = self.cfg.paper.delay_ms / 1000.0
        await asyncio.sleep(delay_s)
        if not market.yes_token_id or not market.no_token_id:
            return
        yes_book = self.cache.get(market.yes_token_id)
        no_book = self.cache.get(market.no_token_id)
        if yes_book is None or no_book is None:
            return
        execute_paper_complete_set(
            market, sig, yes_book, no_book, episode_id=episode_id, delay_ms=self.cfg.paper.delay_ms
        )

    async def run(self) -> None:
        assert_trading_disabled()
        logger.info("Realtime scanner starting (paper=%s)", self.paper)
        markets = await discover_and_store_markets()
        self._index_markets(markets)
        token_ids = list(self.token_to_market.keys())
        self.ws = MarketWebsocketClient(self._on_ws)
        debounce = self.cfg.scanner.ws_recalc_debounce_ms / 1000.0
        last_sync = asyncio.get_event_loop().time()

        ws_task = asyncio.create_task(self.ws.run(token_ids))
        try:
            while True:
                await asyncio.sleep(debounce)
                if self._dirty:
                    try:
                        self._recalc_dirty()
                    except Exception:
                        logger.exception("Dirty recalc failed")
                now = asyncio.get_event_loop().time()
                if now - last_sync >= self.cfg.scanner.market_sync_interval_seconds:
                    try:
                        markets = await discover_and_store_markets()
                        self._index_markets(markets)
                        last_sync = now
                    except Exception:
                        logger.exception("Realtime market resync failed")
        finally:
            if self.ws:
                self.ws.stop()
            ws_task.cancel()
