"""Shared persist + scan helpers used by static and realtime modes."""

from __future__ import annotations

import json
from collections import deque
from threading import Lock
from typing import Any

from polymarket_scanner.config import get_config
from polymarket_scanner.database import (
    LatencySampleRow,
    OpportunityEpisodeRow,
    OpportunityRow,
    decimal_to_str,
    session_scope,
    utcnow,
)
from polymarket_scanner.models import MarketInfo, OpportunitySignal, OrderBookSnapshot
from polymarket_scanner.scanners.binary_complete_set import scan_binary_market
from polymarket_scanner.scanners.opportunity_tracker import sync_episodes
from polymarket_scanner.scanners.rule_engine import evaluate_rule_set, get_enabled_rule_set_meta
from polymarket_scanner.simulation.execution_simulator import simulate_all_profiles

_latency_buf: deque[dict[str, Any]] = deque()
_latency_lock = Lock()
_LATENCY_FLUSH = 32


def _signal_rule_obj(sig: OpportunitySignal, sims: dict[str, Any]) -> dict[str, Any]:
    opt = sims.get("optimistic")
    base = sims.get("base")
    pes = sims.get("pessimistic")
    return {
        "net_profit": sig.net_profit,
        "net_profit_per_share": sig.net_profit_per_share,
        "gross_profit": sig.gross_profit,
        "quantity": sig.quantity,
        "data_age_seconds": sig.data_age_seconds,
        "stale": sig.stale,
        "fees_enabled": sig.fees_enabled,
        "books_skewed": sig.books_skewed,
        "books_ready": sig.books_ready,
        "base_net_profit": base.net_profit if base else None,
        "base_net": base.net_profit if base else None,
        "pessimistic_net_profit": pes.net_profit if pes else None,
        "optimistic_net_profit": opt.net_profit if opt else None,
        "risk_tags": sig.risk_tags,
    }


FEED_LATENCY_EVENTS = {"price_change", "last_trade_price", "tick_size_change"}
SNAPSHOT_LATENCY_EVENTS = {"book", "market_book", "initial_snapshot_age"}


def persist_signals(
    market: MarketInfo,
    signals: list[OpportunitySignal],
    sims: dict[str, Any],
    *,
    episode_ids: dict[tuple[str, str], int] | None = None,
) -> list[int]:
    if not signals:
        return []
    rule_set, rule_id, rule_ver = get_enabled_rule_set_meta(get_config().scanner.default_rule_set)
    ids: list[int] = []
    with session_scope() as session:
        for sig in signals:
            opt = sims.get("optimistic")
            base = sims.get("base")
            pes = sims.get("pessimistic")
            ep_id = None
            if episode_ids:
                ep_id = episode_ids.get((sig.market_id, sig.direction.value))
            passes = True
            if rule_set is not None:
                passes = evaluate_rule_set(rule_set, _signal_rule_obj(sig, sims))
            sig.passes_rule_set = passes
            net_ok = (
                sig.net_profit > 0
                and not sig.stale
                and not sig.books_skewed
                and sig.books_ready
            )
            row = OpportunityRow(
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
                net_profitable=net_ok,
                optimistic_net=decimal_to_str(opt.net_profit) if opt else None,
                base_net=decimal_to_str(base.net_profit) if base else None,
                pessimistic_net=decimal_to_str(pes.net_profit) if pes else None,
                simulation_quality=base.quality.value if base else None,
                optimistic_quality=opt.quality.value if opt else None,
                base_quality=base.quality.value if base else None,
                pessimistic_quality=pes.quality.value if pes else None,
                payload_json=sig.model_dump_json(),
                episode_id=ep_id,
                passes_rule_set=passes,
                rule_set_id=rule_id,
                rule_set_version=rule_ver,
                books_ready=sig.books_ready,
                book_skew_ms=sig.book_skew_ms,
            )
            session.add(row)
            session.flush()
            sig.opportunity_id = int(row.id)
            ids.append(int(row.id))
            if ep_id is not None:
                ep = session.get(OpportunityEpisodeRow, ep_id)
                if ep is not None:
                    ep.last_opportunity_id = int(row.id)
    return ids


def scan_and_persist_market(
    market: MarketInfo,
    yes_book: OrderBookSnapshot,
    no_book: OrderBookSnapshot,
    *,
    yes_delayed: OrderBookSnapshot | None = None,
    no_delayed: OrderBookSnapshot | None = None,
    books_ready: bool = True,
) -> list[OpportunitySignal]:
    signals = scan_binary_market(
        market, yes_book, no_book, books_ready=books_ready
    )
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
    flush: bool = False,
) -> None:
    item = {
        "event_type": event_type,
        "token_id": token_id,
        "event_ts": event_ts,
        "received_at": received_at,
        "latency_ms": latency_ms,
    }
    with _latency_lock:
        _latency_buf.append(item)
        should = flush or len(_latency_buf) >= _LATENCY_FLUSH
        batch = []
        if should:
            while _latency_buf:
                batch.append(_latency_buf.popleft())
    if batch:
        with session_scope() as session:
            for row in batch:
                if row.get("received_at") is None:
                    row["received_at"] = utcnow()
                session.add(LatencySampleRow(**row))


def flush_latency() -> None:
    with _latency_lock:
        batch = []
        while _latency_buf:
            batch.append(_latency_buf.popleft())
    if not batch:
        return
    with session_scope() as session:
        for row in batch:
            if row.get("received_at") is None:
                row["received_at"] = utcnow()
            session.add(LatencySampleRow(**row))
