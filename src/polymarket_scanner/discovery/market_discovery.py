"""Market discovery and persistence."""

from __future__ import annotations

import json
from datetime import datetime, timezone

from sqlalchemy import select

from polymarket_scanner.api.clob_client import ClobClient
from polymarket_scanner.api.gamma_client import GammaClient
from polymarket_scanner.database import (
    FeeScheduleRow,
    MarketRow,
    TokenRow,
    decimal_to_str,
    record_api_error,
    session_scope,
    utcnow,
)
from polymarket_scanner.logging_config import get_logger
from polymarket_scanner.models import MarketInfo

logger = get_logger(__name__)


def upsert_market(session, market: MarketInfo) -> None:
    row = session.scalar(select(MarketRow).where(MarketRow.market_id == market.market_id))
    tags_json = json.dumps(market.tags)
    raw_json = json.dumps(market.raw) if market.raw else None
    fields = dict(
        event_id=market.event_id,
        condition_id=market.condition_id,
        question=market.question,
        slug=market.slug,
        event_slug=market.event_slug,
        category=market.category,
        tags_json=tags_json,
        yes_token_id=market.yes_token_id,
        no_token_id=market.no_token_id,
        active=market.active,
        closed=market.closed,
        accepting_orders=market.accepting_orders,
        enable_order_book=market.enable_order_book,
        neg_risk=market.neg_risk,
        minimum_tick_size=decimal_to_str(market.minimum_tick_size),
        minimum_order_size=decimal_to_str(market.minimum_order_size),
        fees_enabled=market.fees_enabled,
        start_date=market.start_date,
        end_date=market.end_date,
        resolution_source=market.resolution_source,
        description=market.description,
        volume=decimal_to_str(market.volume),
        liquidity=decimal_to_str(market.liquidity),
        last_updated=market.last_updated or utcnow(),
        raw_json=raw_json,
        updated_at=utcnow(),
    )
    if row is None:
        row = MarketRow(market_id=market.market_id, **fields)
        session.add(row)
    else:
        for k, v in fields.items():
            setattr(row, k, v)

    # tokens upsert
    for outcome, token_id in (("YES", market.yes_token_id), ("NO", market.no_token_id)):
        if not token_id:
            continue
        tok = session.scalar(select(TokenRow).where(TokenRow.token_id == token_id))
        if tok is None:
            session.add(TokenRow(market_id=market.market_id, token_id=token_id, outcome=outcome))
        else:
            tok.market_id = market.market_id
            tok.outcome = outcome

    if market.fee_schedule is not None:
        fee = session.scalar(
            select(FeeScheduleRow).where(FeeScheduleRow.market_id == market.market_id)
        )
        fee_fields = dict(
            rate=decimal_to_str(market.fee_schedule.rate) or "0",
            exponent=decimal_to_str(market.fee_schedule.exponent) or "1",
            taker_only=market.fee_schedule.taker_only,
            rebate_rate=decimal_to_str(market.fee_schedule.rebate_rate) or "0",
            raw_json=json.dumps(market.fee_schedule.model_dump(mode="json")),
            updated_at=utcnow(),
        )
        if fee is None:
            session.add(FeeScheduleRow(market_id=market.market_id, **fee_fields))
        else:
            for k, v in fee_fields.items():
                setattr(fee, k, v)


async def discover_and_store_markets(
    *,
    max_pages: int | None = None,
    enrich_missing_fees: bool = True,
) -> list[MarketInfo]:
    """Fetch tradable markets from Gamma, optionally enrich fees via CLOB, upsert DB."""
    async with GammaClient() as gamma:
        markets = await gamma.fetch_tradable_markets(max_pages=max_pages)

    if enrich_missing_fees:
        need = [m for m in markets if m.fees_enabled is None or m.fee_schedule is None]
        if need:
            async with ClobClient() as clob:
                for m in need[:500]:  # soft cap enrichment volume
                    try:
                        enabled, schedule = await clob.get_fee_schedule_for_condition(
                            m.condition_id
                        )
                        if m.fees_enabled is None and enabled is not None:
                            m.fees_enabled = enabled
                        if m.fee_schedule is None and schedule is not None:
                            m.fee_schedule = schedule
                    except Exception as exc:
                        logger.warning(
                            "Fee enrichment failed for %s: %s", m.condition_id, exc
                        )
                        with session_scope() as session:
                            record_api_error(
                                session,
                                source="clob",
                                message=str(exc),
                                endpoint=f"/clob-markets/{m.condition_id}",
                            )

    with session_scope() as session:
        for m in markets:
            upsert_market(session, m)
        session.commit()
    logger.info("Upserted %s markets", len(markets))
    return markets


def load_markets_from_db(*, tradable_only: bool = True) -> list[MarketInfo]:
    from polymarket_scanner.models import FeeSchedule

    results: list[MarketInfo] = []
    with session_scope() as session:
        rows = session.scalars(select(MarketRow)).all()
        for row in rows:
            if tradable_only and not (
                row.active
                and not row.closed
                and row.accepting_orders
                and row.enable_order_book
            ):
                continue
            fee_row = row.fee_schedule
            fee = None
            if fee_row:
                fee = FeeSchedule(
                    rate=fee_row.rate,
                    exponent=fee_row.exponent,
                    taker_only=fee_row.taker_only,
                    rebate_rate=fee_row.rebate_rate,
                )
            tags = []
            if row.tags_json:
                try:
                    tags = json.loads(row.tags_json)
                except json.JSONDecodeError:
                    tags = []
            results.append(
                MarketInfo(
                    event_id=row.event_id,
                    market_id=row.market_id,
                    condition_id=row.condition_id,
                    question=row.question,
                    slug=row.slug,
                    event_slug=row.event_slug,
                    category=row.category,
                    tags=tags,
                    yes_token_id=row.yes_token_id,
                    no_token_id=row.no_token_id,
                    active=row.active,
                    closed=row.closed,
                    accepting_orders=row.accepting_orders,
                    enable_order_book=row.enable_order_book,
                    neg_risk=row.neg_risk,
                    minimum_tick_size=row.minimum_tick_size,
                    minimum_order_size=row.minimum_order_size,
                    fees_enabled=row.fees_enabled,
                    fee_schedule=fee,
                    start_date=row.start_date,
                    end_date=row.end_date,
                    resolution_source=row.resolution_source,
                    description=row.description,
                    volume=row.volume,
                    liquidity=row.liquidity,
                    last_updated=row.last_updated,
                )
            )
    return results
