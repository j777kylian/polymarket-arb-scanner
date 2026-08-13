"""Public CLOB market WebSocket — order books only, no auth / no trading."""

from __future__ import annotations

import asyncio
import json
from collections.abc import Awaitable, Callable
from datetime import datetime, timezone
from typing import Any

import websockets
from websockets.exceptions import ConnectionClosed

from polymarket_scanner.config import AppConfig, get_config
from polymarket_scanner.logging_config import get_logger
from polymarket_scanner.safety import assert_trading_disabled

logger = get_logger(__name__)

MessageHandler = Callable[[dict[str, Any], datetime], Awaitable[None]]


def diff_tokens(old: set[str], new: set[str]) -> tuple[set[str], set[str]]:
    return new - old, old - new


def initial_market_message(token_ids: list[str]) -> dict[str, Any]:
    """First-connection subscribe (official market channel handshake)."""
    return {
        "assets_ids": list(token_ids),
        "type": "market",
        "initial_dump": True,
        "custom_feature_enabled": True,
    }


def subscribe_operation_message(token_ids: list[str]) -> dict[str, Any]:
    """Dynamic add after the socket is already subscribed."""
    return {
        "assets_ids": list(token_ids),
        "operation": "subscribe",
        "custom_feature_enabled": True,
    }


def unsubscribe_operation_message(token_ids: list[str]) -> dict[str, Any]:
    """Dynamic remove. Must use operation=unsubscribe, not type=unsubscribe."""
    return {
        "assets_ids": list(token_ids),
        "operation": "unsubscribe",
    }


def connection_subscribe_messages(
    token_ids: list[str], *, chunk_size: int = 0
) -> list[dict[str, Any]]:
    """Prefer one initial type=market frame. Extra chunks use operation=subscribe."""
    ids = list(token_ids)
    if not ids:
        return []
    if chunk_size <= 0 or len(ids) <= chunk_size:
        return [initial_market_message(ids)]
    groups = _chunks(ids, chunk_size)
    messages = [initial_market_message(groups[0])]
    for group in groups[1:]:
        messages.append(subscribe_operation_message(group))
    return messages


def subscribe_message(token_ids: list[str], *, initial_dump: bool = True) -> dict[str, Any]:
    """Backward-compatible helper. Dynamic adds must use subscribe_operation_message."""
    if initial_dump:
        return initial_market_message(token_ids)
    return subscribe_operation_message(token_ids)


def unsubscribe_message(token_ids: list[str]) -> dict[str, Any]:
    return unsubscribe_operation_message(token_ids)


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

    def __init__(
        self,
        on_message: MessageHandler,
        *,
        on_connect: Callable[[int], Awaitable[None] | None] | None = None,
        on_disconnect: Callable[[], Awaitable[None] | None] | None = None,
        config: AppConfig | None = None,
    ) -> None:
        assert_trading_disabled()
        self.cfg = config or get_config()
        self.on_message = on_message
        self.on_connect = on_connect
        self.on_disconnect = on_disconnect
        self._stop = asyncio.Event()
        self._rebuild = asyncio.Event()
        self.connected = False
        self.generation = 0
        self.last_message_at: datetime | None = None
        self.subscribed_tokens: set[str] = set()
        self._ws: Any = None

    def stop(self) -> None:
        self._stop.set()
        self._rebuild.set()

    def request_rebuild(self) -> None:
        """Close the current socket so the loop reconnects with initial_dump."""
        self._rebuild.set()

    async def update_subscriptions(self, token_ids: list[str] | set[str]) -> tuple[set[str], set[str]]:
        new_set = set(token_ids)
        added, removed = diff_tokens(self.subscribed_tokens, new_set)
        self.subscribed_tokens = new_set
        if not self.connected or self._ws is None:
            self.request_rebuild()
            return added, removed
        try:
            chunk = int(self.cfg.scanner.ws_subscribe_chunk or 0)
            if removed:
                groups = _chunks(sorted(removed), chunk) if chunk > 0 else [sorted(removed)]
                for group in groups:
                    await self._ws.send(json.dumps(unsubscribe_operation_message(group)))
            if added:
                groups = _chunks(sorted(added), chunk) if chunk > 0 else [sorted(added)]
                for group in groups:
                    await self._ws.send(json.dumps(subscribe_operation_message(group)))
        except Exception:
            logger.warning("WS subscribe/unsubscribe send failed; rebuilding connection")
            self.request_rebuild()
        return added, removed

    async def run(self, token_ids: list[str]) -> None:
        """Reconnect loop. token_ids may be updated via subscribed_tokens externally."""
        self.subscribed_tokens = set(token_ids)
        backoff = 1.0
        while not self._stop.is_set():
            self._rebuild.clear()
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
        chunk = int(self.cfg.scanner.ws_subscribe_chunk or 0)
        async with websockets.connect(url, ping_interval=None, close_timeout=5) as ws:
            self._ws = ws
            self.connected = True
            self.generation += 1
            logger.info(
                "Connected to market WS gen=%s, subscribing %s tokens",
                self.generation,
                len(token_ids),
            )
            if self.on_connect:
                result = self.on_connect(self.generation)
                if asyncio.iscoroutine(result):
                    await result
            for msg in connection_subscribe_messages(token_ids, chunk_size=chunk):
                await ws.send(json.dumps(msg))

            async def heartbeat() -> None:
                while not self._stop.is_set() and not self._rebuild.is_set():
                    await asyncio.sleep(ping_s)
                    try:
                        await ws.send("PING")
                    except Exception:
                        return

            hb = asyncio.create_task(heartbeat())

            async def watch_rebuild() -> None:
                await self._rebuild.wait()
                try:
                    await ws.close()
                except Exception:
                    pass

            watcher = asyncio.create_task(watch_rebuild())
            try:
                async for raw in ws:
                    if self._stop.is_set() or self._rebuild.is_set():
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
                watcher.cancel()
                self.connected = False
                self._ws = None
                if self.on_disconnect:
                    result = self.on_disconnect()
                    if asyncio.iscoroutine(result):
                        await result
