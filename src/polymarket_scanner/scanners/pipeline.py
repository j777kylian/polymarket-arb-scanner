"""Shared persist + scan helpers used by static and realtime modes."""

from __future__ import annotations

import json
from typing import Any

from polymarket_scanner.database import (
    LatencySampleRow,
    OpportunityRow,
    decimal_to_str,
    session_scope,
)
from polymarket_scanner.models import MarketInfo, OpportunitySignal, OrderBookSnapshot
from polymarket_scanner.scanners.binary_complete_set import scan_binary_market
from polymarket_scanner.scanners.opportunity_tracker import sync_episodes
from polymarket_scanner.simulation.execution_simulator import simulate_all_profiles


def persist_signals(
    market: MarketInfo,
    signals: list[OpportunitySignal],
    sims: dict[str, Any],
    *,
    episode_ids: dict[tuple[str, str], int] | None = None,  # noqa: ARG001
) -> int:
    if not signals:
        return 0
    count = 0
    with session_scope() as session:
        for sig in signals:
            opt = sims.get("optimistic")
            base = sims.get("base")
            pes = sims.get("pessimistic")
            quality = base.quality.value if base else None
            session.add(
                OpportunityRow(
                    market_id=sig.market_id,
                    condition_id=sig.condition_id,
                    question=sig.question,
                    direction=sig.direction.value,
                    discovered_at=sig.discovered_at,
                    data_age_seconds=sig.data_age_seconds,
                    stale=sig.stale,
                    quantity=decimal_to_str(sig.quantity) or "0",
                    yes_vwap=decimal_to_str(sig.yes_vwap) or "0",
                    no_vwap=decimal_to_str(sig.no_vwap) or "0",
                    gross_profit=decimal_to_str(sig.gross_profit) or "0",
                    fee_total=decimal_to_str(sig.fee_total) or "0",
                    net_profit=decimal_to_str(sig.net_profit) or "0",
                    net_profit_per_share=decimal_to_str(sig.net_profit_per_share) or "0",
                    net_profit_rate=decimal_to_str(sig.net_profit_rate) or "0",
                    levels_used_yes=sig.levels_used_yes,
                    levels_used_no=sig.levels_used_no,
                    fees_enabled=sig.fees_enabled,
                    neg_risk=sig.neg_risk,
                    risk_tags_json=json.dumps(sig.risk_tags),
                    requires_split_inventory=sig.requires_split_inventory,
                    net_profitable=sig.net_profit > 0 and not sig.stale,
                    optimistic_net=decimal_to_str(opt.net_profit) if opt else None,
                    base_net=decimal_to_str(base.net_profit) if base else None,
                    pessimistic_net=decimal_to_str(pes.net_profit) if pes else None,
                    simulation_quality=quality,
                    payload_json=sig.model_dump_json(),
                )
            )
            count += 1
    return count


def scan_and_persist_market(
    market: MarketInfo,
    yes_book: OrderBookSnapshot,
    no_book: OrderBookSnapshot,
    *,
    yes_delayed: OrderBookSnapshot | None = None,
    no_delayed: OrderBookSnapshot | None = None,
) -> list[OpportunitySignal]:
    signals = scan_binary_market(market, yes_book, no_book)
    sims = simulate_all_profiles(
        market, yes_book, no_book, yes_delayed=yes_delayed, no_delayed=no_delayed
    )
    episode_ids, _stats = sync_episodes(signals, scanned_market_ids={market.market_id})
    persist_signals(market, signals, sims, episode_ids=episode_ids)
    return signals


def record_latency(
    event_type: str,
    latency_ms: float,
    *,
    token_id: str | None = None,
    event_ts=None,
    received_at=None,
) -> None:
    with session_scope() as session:
        session.add(
            LatencySampleRow(
                event_type=event_type,
                token_id=token_id,
                event_ts=event_ts,
                received_at=received_at,
                latency_ms=latency_ms,
            )
        )
