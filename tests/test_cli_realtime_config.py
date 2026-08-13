"""CLI Live Research flags must reach RealtimeScanner without being overwritten."""

from __future__ import annotations

import asyncio

import pytest

from polymarket_scanner.config import get_config
from polymarket_scanner.database import init_db
from polymarket_scanner.models import MarketInfo
from polymarket_scanner.realtime import RealtimeScanner


def _tmp_db(tmp_path, monkeypatch) -> None:
    db = tmp_path / "t.db"
    monkeypatch.setenv("DATABASE_URL", f"sqlite:///{db}")
    get_config.cache_clear()
    from polymarket_scanner import database as dbmod

    dbmod._engine = None
    dbmod._SessionLocal = None
    init_db(f"sqlite:///{db}")


def _markets(n: int) -> list[MarketInfo]:
    return [
        MarketInfo(
            market_id=f"m{i}",
            condition_id=f"c{i}",
            yes_token_id=f"y{i}",
            no_token_id=f"n{i}",
        )
        for i in range(n)
    ]


@pytest.mark.asyncio
async def test_cli_realtime_max_pages_and_market_limit(tmp_path, monkeypatch) -> None:
    _tmp_db(tmp_path, monkeypatch)
    cfg = get_config().model_copy(deep=True)
    cfg.scanner.max_pages = 1
    cfg.scanner.market_limit = 50
    cfg.scanner.sync_markets = False
    cfg.scanner.ws_recalc_debounce_ms = 1

    captured: dict[str, object] = {}

    async def fake_discover(*, max_pages=None, **_kwargs):
        captured["max_pages"] = max_pages
        return _markets(80)

    class FakeWS:
        def __init__(self, *args, **kwargs) -> None:
            captured["ws_config"] = kwargs.get("config")
            self.subscribed_tokens: set[str] = set()

        def stop(self) -> None:
            return None

        async def run(self, token_ids: list[str]) -> None:
            captured["tokens"] = list(token_ids)
            captured["n_tokens"] = len(token_ids)
            await asyncio.sleep(0.05)
            raise asyncio.CancelledError()

        async def update_subscriptions(self, token_ids: list[str]) -> tuple[set[str], set[str]]:
            return set(), set()

    def boom(_cfg=None):
        raise AssertionError("RealtimeScanner must not reload runtime config over CLI flags")

    monkeypatch.setattr(
        "polymarket_scanner.realtime.discover_and_store_markets", fake_discover
    )
    monkeypatch.setattr("polymarket_scanner.realtime.MarketWebsocketClient", FakeWS)
    monkeypatch.setattr("polymarket_scanner.runtime_settings.apply_runtime_to_config", boom)

    rt = RealtimeScanner(config=cfg, paper=True)
    assert rt.cfg.scanner.max_pages == 1
    assert rt.cfg.scanner.market_limit == 50
    task = asyncio.create_task(rt.run())
    await asyncio.sleep(0.02)
    rt._running = False
    try:
        await asyncio.wait_for(task, timeout=2)
    except (asyncio.CancelledError, TimeoutError):
        task.cancel()
        try:
            await task
        except (asyncio.CancelledError, Exception):
            pass

    assert captured["max_pages"] == 1
    assert len(rt.markets) == 50
    assert captured["n_tokens"] == 100
    assert rt.discovered_markets == 80
    ws_cfg = captured["ws_config"]
    assert ws_cfg is cfg or getattr(ws_cfg, "scanner").max_pages == 1


def test_scanner_service_overlays_cli_before_realtime(tmp_path, monkeypatch) -> None:
    _tmp_db(tmp_path, monkeypatch)
    from polymarket_scanner.scheduler import ScannerService

    svc = ScannerService()
    svc.cfg.scanner.max_pages = 1
    svc.cfg.scanner.market_limit = 50
    rt = RealtimeScanner(config=svc.cfg, paper=True)
    assert rt.cfg.scanner.max_pages == 1
    assert rt.cfg.scanner.market_limit == 50
    assert rt.cfg is svc.cfg
