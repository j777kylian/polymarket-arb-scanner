"""Public CLOB market WebSocket — order books only, no auth / no trading."""

from __future__ import annotations

import asyncio
import json
from collections.abc import Awaitable, Callable
from datetime import datetime, timezone
from typing import Any

import websockets
from websockets.exceptions import ConnectionClosed

from polymarket_scanner.config import get_config
from polymarket_scanner.logging_config import get_logger
from polymarket_scanner.safety import assert_trading_disabled

logger = get_logger(__name__)

MessageHandler = Callable[[dict[str, Any], datetime], Awaitable[None]]


def _chunks(items: list[str], size: int) -> list[list[str]]:
    if size <= 0:
        return [items]
    return [items[i : i + size] for i in range(0, len(items), size)]


def parse_ws_messages(raw: str) -> list[dict[str, Any]]:
    """Normalize a WS text frame into a list of event dicts."""
    if raw in {"PONG", "pong", "PING", "ping"}:
        return []
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        logger.warning("Non-JSON WS frame: %s", raw[:120])
        return []
    if isinstance(data, list):
        return [x for x in data if isinstance(x, dict)]
    if isinstance(data, dict):
        return [data]
    return []


class MarketWebsocketClient:
    """Subscribe to wss://.../ws/market (public). Heartbeat: PING every 10s."""

    def __init__(self, on_message: MessageHandler) -> None:
        assert_trading_disabled()
        self.cfg = get_config()
        self.on_message = on_message
        self._stop = asyncio.Event()
        self.connected = False
        self.last_message_at: datetime | None = None
        self.subscribed_tokens: set[str] = set()

    def stop(self) -> None:
        self._stop.set()

    async def run(self, token_ids: list[str]) -> None:
        """Reconnect loop. token_ids may be updated via subscribed_tokens externally."""
        self.subscribed_tokens = set(token_ids)
        backoff = 1.0
        while not self._stop.is_set():
            try:
                await self._connect_once(sorted(self.subscribed_tokens))
                backoff = 1.0
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.connected = False
                logger.warning("Market WS disconnected: %s — retry in %.1fs", exc, backoff)
                try:
                    await asyncio.wait_for(self._stop.wait(), timeout=backoff)
                except TimeoutError:
                    pass
                backoff = min(backoff * 2, 30.0)

    async def _connect_once(self, token_ids: list[str]) -> None:
        url = self.cfg.api.market_ws_url
        ping_s = self.cfg.scanner.ws_ping_interval_seconds
        chunk = self.cfg.scanner.ws_subscribe_chunk
        async with websockets.connect(url, ping_interval=None, close_timeout=5) as ws:
            self.connected = True
            logger.info("Connected to market WS, subscribing %s tokens", len(token_ids))
            for group in _chunks(token_ids, chunk) or [[]]:
                if not group:
                    continue
                await ws.send(
                    json.dumps(
                        {
                            "assets_ids": group,
                            "type": "market",
                            "initial_dump": True,
                        }
                    )
                )

            async def heartbeat() -> None:
                while not self._stop.is_set():
                    await asyncio.sleep(ping_s)
                    try:
                        await ws.send("PING")
                    except Exception:
                        return

            hb = asyncio.create_task(heartbeat())
            try:
                async for raw in ws:
                    if self._stop.is_set():
                        break
                    received = datetime.now(timezone.utc)
                    self.last_message_at = received
                    if isinstance(raw, bytes):
                        raw = raw.decode("utf-8", errors="replace")
                    for event in parse_ws_messages(str(raw)):
                        await self.on_message(event, received)
            except ConnectionClosed:
                raise
            finally:
                hb.cancel()
                self.connected = False
