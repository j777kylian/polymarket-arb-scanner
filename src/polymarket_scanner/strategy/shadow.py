"""Run shadow strategies against the same LiveBookCache snapshots as live paper."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from datetime import datetime
from typing import Any

from polymarket_scanner.config import AppConfig
from polymarket_scanner.models import MarketInfo, OpportunitySignal, OrderBookSnapshot
from polymarket_scanner.simulation.paper_trader import run_delayed_paper_trade
from polymarket_scanner.strategy.store import LoadedStrategy, ensure_strategy_account


async def run_shadow_paper(
    strategy: LoadedStrategy,
    *,
    cache: Any,
    market: MarketInfo,
    signal: OpportunitySignal,
    episode_id: int | None,
    base_cfg: AppConfig,
    t0_yes: OrderBookSnapshot | None,
    t0_no: OrderBookSnapshot | None,
    sleep_fn: Callable[[float], Awaitable[None]] | None = None,
    now_fn: Callable[[], datetime] | None = None,
    episode_open_fn: Callable[[int | None], bool] | None = None,
) -> dict[str, str] | None:
    ensure_strategy_account(
        strategy.strategy_id,
        strategy.version,
        str(strategy.params.starting_capital),
    )
    cfg = strategy.params.apply_to_app_config(
        base_cfg, strategy_id=strategy.strategy_id, strategy_version=strategy.version
    )
    return await run_delayed_paper_trade(
        cache=cache,
        market=market,
        signal=signal,
        episode_id=episode_id,
        cfg=cfg,
        paper_cfg=cfg.paper,
        account_kind="strategy",
        strategy_id=strategy.strategy_id,
        strategy_version=strategy.version,
        sleep_fn=sleep_fn,
        now_fn=now_fn,
        episode_open_fn=episode_open_fn,
        t0_yes=t0_yes,
        t0_no=t0_no,
    )
