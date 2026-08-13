"""Binary complete-set arbitrage scanner."""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal

from polymarket_scanner.config import get_config
from polymarket_scanner.logging_config import get_logger
from polymarket_scanner.models import (
    ArbDirection,
    FeeSchedule,
    MarketInfo,
    OpportunitySignal,
    OrderBookSnapshot,
)
from polymarket_scanner.simulation.orderbook_walker import (
    detect_reverse_top_of_book,
    find_optimal_forward_arb,
)

logger = get_logger(__name__)
ZERO = Decimal("0")


def _age_seconds(fetched_at: datetime, now: datetime | None = None) -> float:
    now = now or datetime.now(timezone.utc)
    fa = fetched_at if fetched_at.tzinfo else fetched_at.replace(tzinfo=timezone.utc)
    return max(0.0, (now - fa).total_seconds())


def build_risk_tags(
    *,
    stale: bool,
    quantity: Decimal,
    fee_total: Decimal,
    gross: Decimal,
    net: Decimal,
    neg_risk: bool,
    estimated: bool = False,
    one_leg: bool = False,
    short_lived: bool = False,
    thin_usd: Decimal | None = None,
) -> list[str]:
    tags: list[str] = []
    if stale:
        tags.append("stale data")
    if thin_usd is not None and thin_usd < Decimal("50"):
        tags.append("thin liquidity")
    if fee_total > ZERO and gross > ZERO and fee_total >= gross * Decimal("0.5"):
        tags.append("fee-sensitive")
    if one_leg:
        tags.append("one-leg risk")
    if short_lived:
        tags.append("short-lived")
    if neg_risk:
        tags.append("neg-risk complexity")
    if estimated:
        tags.append("estimated simulation")
    if quantity > ZERO and net <= ZERO < gross:
        tags.append("fee-sensitive")
    return list(dict.fromkeys(tags))


def scan_binary_market(
    market: MarketInfo,
    yes_book: OrderBookSnapshot,
    no_book: OrderBookSnapshot,
    *,
    max_data_age_seconds: float | None = None,
    operational_cost: Decimal | None = None,
    safety_buffer: Decimal | None = None,
    now: datetime | None = None,
) -> list[OpportunitySignal]:
    """Detect forward (and flag reverse) complete-set opportunities for one market."""
    cfg = get_config()
    max_age = (
        max_data_age_seconds
        if max_data_age_seconds is not None
        else float(cfg.scanner.max_data_age_seconds)
    )
    op_cost = (
        operational_cost
        if operational_cost is not None
        else Decimal(str(cfg.simulation.operational_cost))
    )
    buffer = (
        safety_buffer
        if safety_buffer is not None
        else Decimal(str(cfg.simulation.safety_buffer))
    )
    now = now or datetime.now(timezone.utc)

    age = max(_age_seconds(yes_book.fetched_at, now), _age_seconds(no_book.fetched_at, now))
    stale = age > max_age

    schedule: FeeSchedule | None = market.fee_schedule
    fees_enabled = market.fees_enabled

    signals: list[OpportunitySignal] = []

    walk = find_optimal_forward_arb(
        yes_book,
        no_book,
        schedule,
        fees_enabled=fees_enabled,
        operational_cost=op_cost,
        safety_buffer=buffer,
    )
    if walk and walk.quantity > ZERO and walk.gross_profit > ZERO:
        fee_total = walk.fee_yes + walk.fee_no
        thin = yes_book.ask_depth_usd(3) + no_book.ask_depth_usd(3)
        tags = build_risk_tags(
            stale=stale,
            quantity=walk.quantity,
            fee_total=fee_total,
            gross=walk.gross_profit,
            net=walk.net_profit,
            neg_risk=market.neg_risk,
            thin_usd=thin,
        )
        if not market.resolution_source and not market.description:
            tags.append("resolution ambiguity")
        if market.fees_enabled is not False and market.fee_schedule is None:
            tags.append("fee schedule missing")

        signals.append(
            OpportunitySignal(
                market_id=market.market_id,
                condition_id=market.condition_id,
                question=market.question,
                direction=ArbDirection.FORWARD,
                discovered_at=now,
                data_age_seconds=age,
                stale=stale,
                quantity=walk.quantity,
                yes_vwap=walk.yes_vwap,
                no_vwap=walk.no_vwap,
                gross_profit=walk.gross_profit,
                fee_total=fee_total,
                net_profit=walk.net_profit,
                net_profit_per_share=walk.net_profit_per_share,
                net_profit_rate=walk.net_profit_rate,
                levels_used_yes=walk.levels_used_yes,
                levels_used_no=walk.levels_used_no,
                fees_enabled=fees_enabled,
                neg_risk=market.neg_risk,
                risk_tags=tags,
                walk=walk,
                requires_split_inventory=False,
            )
        )

    is_rev, yb, nb = detect_reverse_top_of_book(yes_book, no_book)
    if is_rev and yb is not None and nb is not None:
        tags = build_risk_tags(
            stale=stale,
            quantity=ZERO,
            fee_total=ZERO,
            gross=yb + nb - Decimal("1"),
            net=ZERO,
            neg_risk=market.neg_risk,
        )
        tags.append("requires split/inventory")
        signals.append(
            OpportunitySignal(
                market_id=market.market_id,
                condition_id=market.condition_id,
                question=market.question,
                direction=ArbDirection.REVERSE,
                discovered_at=now,
                data_age_seconds=age,
                stale=stale,
                quantity=ZERO,
                yes_vwap=yb,
                no_vwap=nb,
                gross_profit=yb + nb - Decimal("1"),
                fee_total=ZERO,
                net_profit=ZERO,
                net_profit_per_share=ZERO,
                net_profit_rate=ZERO,
                levels_used_yes=1,
                levels_used_no=1,
                fees_enabled=fees_enabled,
                neg_risk=market.neg_risk,
                risk_tags=tags,
                walk=None,
                requires_split_inventory=True,
            )
        )

    return signals
