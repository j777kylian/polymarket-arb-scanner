"""Execution simulator tests — partial fill / one-leg risk."""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal

from polymarket_scanner.models import (
    MarketInfo,
    OrderBookLevel,
    OrderBookSnapshot,
    OutcomeSide,
    SimulationQuality,
)
from polymarket_scanner.simulation.execution_simulator import simulate_forward
from polymarket_scanner.simulation.scenario_profiles import ScenarioProfile


def _books():
    now = datetime.now(timezone.utc)
    yes = OrderBookSnapshot(
        condition_id="c",
        token_id="y",
        outcome=OutcomeSide.YES,
        bids=[OrderBookLevel(price=Decimal("0.40"), size=Decimal("200"))],
        asks=[OrderBookLevel(price=Decimal("0.45"), size=Decimal("100"))],
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
    return yes, no


def test_partial_second_leg_one_leg_risk() -> None:
    yes, no = _books()
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
        partial_second_leg_ratio=Decimal("0.5"),
        force_close_unhedged=True,
        operational_cost=Decimal("0"),
        safety_buffer=Decimal("0"),
        first_leg="YES",
    )
    result = simulate_forward(
        market,
        yes,
        no,
        profile,
        target_quantity=Decimal("100"),
        second_leg_fill_ratio=Decimal("0.5"),
    )
    assert result.one_leg_risk is True
    assert "one-leg risk" in result.risk_tags
    # First 100 YES, second 50 NO => matched 50, unhedged closed
    assert result.quantity == Decimal("50")


def test_estimated_quality_without_delayed_snapshot() -> None:
    yes, no = _books()
    market = MarketInfo(market_id="1", condition_id="c", yes_token_id="y", no_token_id="n")
    profile = ScenarioProfile(name="base", delay_ms=500, sequential_legs=True)
    result = simulate_forward(market, yes, no, profile)
    assert result.quality == SimulationQuality.ESTIMATED
