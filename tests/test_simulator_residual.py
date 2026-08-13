"""Exact residual inventory values for the execution simulator."""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal

from polymarket_scanner.models import (
    FeeSchedule,
    MarketInfo,
    OrderBookLevel,
    OrderBookSnapshot,
    OutcomeSide,
)
from polymarket_scanner.simulation.execution_simulator import simulate_forward
from polymarket_scanner.simulation.scenario_profiles import ScenarioProfile


def test_residual_partial_close_exact_no_fees() -> None:
    now = datetime.now(timezone.utc)
    yes = OrderBookSnapshot(
        condition_id="c",
        token_id="y",
        outcome=OutcomeSide.YES,
        bids=[OrderBookLevel(price=Decimal("0.30"), size=Decimal("5"))],
        asks=[OrderBookLevel(price=Decimal("0.40"), size=Decimal("100"))],
        tick_size=Decimal("0.01"),
        fetched_at=now,
    )
    no = OrderBookSnapshot(
        condition_id="c",
        token_id="n",
        outcome=OutcomeSide.NO,
        bids=[OrderBookLevel(price=Decimal("0.40"), size=Decimal("200"))],
        asks=[OrderBookLevel(price=Decimal("0.50"), size=Decimal("100"))],
        tick_size=Decimal("0.01"),
        fetched_at=now,
    )
    market = MarketInfo(
        market_id="1",
        condition_id="c",
        yes_token_id="y",
        no_token_id="n",
        fees_enabled=False,
    )
    profile = ScenarioProfile(
        name="pessimistic",
        delay_ms=0,
        slippage_ticks=0,
        depth_factor=Decimal("1"),
        sequential_legs=True,
        partial_second_leg_ratio=Decimal("0.4"),
        force_close_unhedged=True,
        operational_cost=Decimal("0"),
        safety_buffer=Decimal("0"),
        first_leg="YES",
    )
    result = simulate_forward(
        market, yes, no, profile, target_quantity=Decimal("100"), second_leg_fill_ratio=Decimal("0.4")
    )
    assert result.quantity == Decimal("40")
    assert result.remaining_inventory == Decimal("55")
    assert result.unrealized_inventory_cost == Decimal("22")
    assert result.gross_profit == Decimal("3.5")
    assert result.realized_pnl == Decimal("3.5")
    assert result.net_profit == Decimal("3.5")
    assert result.one_leg_risk is True


def test_residual_includes_original_buy_fee() -> None:
    now = datetime.now(timezone.utc)
    sched = FeeSchedule(rate=Decimal("0.04"), exponent=Decimal("1"), taker_only=True)
    yes = OrderBookSnapshot(
        condition_id="c",
        token_id="y",
        outcome=OutcomeSide.YES,
        bids=[OrderBookLevel(price=Decimal("0.30"), size=Decimal("5"))],
        asks=[OrderBookLevel(price=Decimal("0.40"), size=Decimal("100"))],
        tick_size=Decimal("0.01"),
        fetched_at=now,
    )
    no = OrderBookSnapshot(
        condition_id="c",
        token_id="n",
        outcome=OutcomeSide.NO,
        bids=[OrderBookLevel(price=Decimal("0.40"), size=Decimal("200"))],
        asks=[OrderBookLevel(price=Decimal("0.50"), size=Decimal("100"))],
        tick_size=Decimal("0.01"),
        fetched_at=now,
    )
    market = MarketInfo(
        market_id="1",
        condition_id="c",
        yes_token_id="y",
        no_token_id="n",
        fees_enabled=True,
        fee_schedule=sched,
    )
    profile = ScenarioProfile(
        name="pessimistic",
        delay_ms=0,
        slippage_ticks=0,
        depth_factor=Decimal("1"),
        sequential_legs=True,
        partial_second_leg_ratio=Decimal("0.4"),
        force_close_unhedged=True,
        operational_cost=Decimal("0"),
        safety_buffer=Decimal("0"),
        first_leg="YES",
    )
    result = simulate_forward(
        market, yes, no, profile, target_quantity=Decimal("100"), second_leg_fill_ratio=Decimal("0.4")
    )
    # leftover 55 YES: unit cost 0.40 + unit fee 0.04*(0.40*0.60)=0.0096 → 0.4096 * 55
    assert result.remaining_inventory == Decimal("55")
    assert result.unrealized_inventory_cost == Decimal("22.528")
    assert result.fees > Decimal("0")
    assert result.realized_pnl == result.net_profit
