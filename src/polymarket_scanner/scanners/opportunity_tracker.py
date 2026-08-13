"""Track opportunity first-seen / last-seen / disappearance."""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal

from sqlalchemy import select

from polymarket_scanner.database import (
    OpportunityEpisodeRow,
    decimal_to_str,
    ensure_utc,
    session_scope,
    utcnow,
)
from polymarket_scanner.models import OpportunitySignal


def sync_episodes(
    live_signals: list[OpportunitySignal],
    *,
    scanned_market_ids: set[str],
    now: datetime | None = None,
) -> tuple[dict[tuple[str, str], int], dict[str, int]]:
    """
    Open/update episodes for currently live signals.
    Close episodes for scanned markets that no longer have a matching signal.
    Returns (episode_id_by_market_direction, stats).
    """
    now = now or utcnow()
    live_keys = {(s.market_id, s.direction.value): s for s in live_signals}
    ids: dict[tuple[str, str], int] = {}
    opened = 0
    closed = 0
    with session_scope() as session:
        open_rows = session.scalars(
            select(OpportunityEpisodeRow).where(OpportunityEpisodeRow.is_open.is_(True))
        ).all()
        open_map = {(r.market_id, r.direction): r for r in open_rows}

        for key, sig in live_keys.items():
            row = open_map.get(key)
            net = decimal_to_str(sig.net_profit) or "0"
            qty = decimal_to_str(sig.quantity) or "0"
            if row is None:
                row = OpportunityEpisodeRow(
                    market_id=sig.market_id,
                    condition_id=sig.condition_id,
                    question=sig.question,
                    direction=sig.direction.value,
                    first_seen_at=now,
                    last_seen_at=now,
                    is_open=True,
                    peak_net_profit=net,
                    last_net_profit=net,
                    last_quantity=qty,
                )
                session.add(row)
                session.flush()
                opened += 1
            else:
                row.last_seen_at = now
                row.last_net_profit = net
                row.last_quantity = qty
                if Decimal(net) > Decimal(row.peak_net_profit or "0"):
                    row.peak_net_profit = net
            ids[key] = row.id

        for key, row in open_map.items():
            market_id, _direction = key
            if market_id not in scanned_market_ids:
                continue
            if key in live_keys:
                continue
            row.is_open = False
            row.disappeared_at = now
            row.close_reason = "signal_gone"
            first = ensure_utc(row.first_seen_at) or now
            now_u = ensure_utc(now) or now
            row.duration_seconds = max(0.0, (now_u - first).total_seconds())
            closed += 1

    return ids, {"opened": opened, "closed": closed}


def close_episodes(
    *,
    market_ids: set[str] | None = None,
    reason: str,
    now: datetime | None = None,
) -> int:
    """Close open episodes for markets (or all if market_ids is None)."""
    now = now or utcnow()
    closed = 0
    with session_scope() as session:
        q = select(OpportunityEpisodeRow).where(OpportunityEpisodeRow.is_open.is_(True))
        rows = session.scalars(q).all()
        for row in rows:
            if market_ids is not None and row.market_id not in market_ids:
                continue
            row.is_open = False
            row.disappeared_at = now
            row.close_reason = reason
            first = ensure_utc(row.first_seen_at) or now
            now_u = ensure_utc(now) or now
            row.duration_seconds = max(0.0, (now_u - first).total_seconds())
            closed += 1
    return closed


def episode_is_open(episode_id: int | None) -> bool:
    if episode_id is None:
        return False
    with session_scope() as session:
        row = session.get(OpportunityEpisodeRow, episode_id)
        return bool(row and row.is_open)
