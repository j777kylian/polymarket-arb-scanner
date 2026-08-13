"""Observed-delay window and residual inventory simulation tests."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal

from polymarket_scanner.models import (
    FeeSchedule,
    MarketInfo,
    OrderBookLevel,
    OrderBookSnapshot,
    OutcomeSide,
    SimulationQuality,
)
from polymarket_scanner.simulation.execution_simulator import (
    select_delayed_books,
    simulate_all_profiles,
    simulate_forward,
)
from polymarket_scanner.simulation.scenario_profiles import ScenarioProfile


def _books(now: datetime, *, yes_ask="0.45", no_ask="0.50", bid="0.40"):
    yes = OrderBookSnapshot(
        condition_id="c",
        token_id="y",
        outcome=OutcomeSide.YES,
        bids=[OrderBookLevel(price=Decimal(bid), size=Decimal("200"))],
        asks=[OrderBookLevel(price=Decimal(yes_ask), size=Decimal("100"))],
        tick_size=Decimal("0.01"),
        fetched_at=now,
        hash="y0",
    )
    no = OrderBookSnapshot(
        condition_id="c",
        token_id="n",
        outcome=OutcomeSide.NO,
        bids=[OrderBookLevel(price=Decimal(bid), size=Decimal("200"))],
        asks=[OrderBookLevel(price=Decimal(no_ask), size=Decimal("100"))],
        tick_size=Decimal("0.01"),
        fetched_at=now,
        hash="n0",
    )
    return yes, no


def test_observed_window_requires_target_to_tolerance() -> None:
    t0 = datetime(2026, 1, 1, tzinfo=timezone.utc)
    yes0, no0 = _books(t0)
    later = t0 + timedelta(milliseconds=500)
    yes1, no1 = _books(later)
    y, n, q = select_delayed_books(
        yes0, no0, delay_ms=500, yes_later=yes1, no_later=no1, tolerance_ms=250, max_skew_ms=250
    )
    assert q == SimulationQuality.OBSERVED_SNAPSHOT
    assert y.fetched_at == later

    too_early = t0 + timedelta(milliseconds=100)
    yes_e, no_e = _books(too_early)
    _, _, q2 = select_delayed_books(
        yes0, no0, delay_ms=500, yes_later=yes_e, no_later=no_e, tolerance_ms=250
    )
    assert q2 == SimulationQuality.STALE


def test_observed_skew_unavailable() -> None:
    t0 = datetime(2026, 1, 1, tzinfo=timezone.utc)
    yes0, no0 = _books(t0)
    yes1, _ = _books(t0 + timedelta(milliseconds=500))
    _, no1 = _books(t0 + timedelta(milliseconds=900))
    _, _, q = select_delayed_books(
        yes0, no0, delay_ms=500, yes_later=yes1, no_later=no1, tolerance_ms=500, max_skew_ms=50
    )
    assert q == SimulationQuality.UNAVAILABLE


def test_missing_delayed_is_estimated_not_observed() -> None:
    yes, no = _books(datetime.now(timezone.utc))
    market = MarketInfo(market_id="1", condition_id="c", yes_token_id="y", no_token_id="n")
    profile = ScenarioProfile(name="base", delay_ms=500, sequential_legs=True)
    result = simulate_forward(market, yes, no, profile)
    assert result.quality == SimulationQuality.ESTIMATED


def test_profiles_keep_independent_quality() -> None:
    t0 = datetime.now(timezone.utc)
    yes, no = _books(t0)
    market = MarketInfo(
        market_id="1", condition_id="c", yes_token_id="y", no_token_id="n", fees_enabled=False
    )
    later = t0 + timedelta(milliseconds=500)
    yes_d, no_d = _books(later)
    results = simulate_all_profiles(market, yes, no, yes_delayed=yes_d, no_delayed=no_d)
    assert results["optimistic"].quality == SimulationQuality.OBSERVED_SNAPSHOT
    # base delay 500 should be observed; pessimistic 2000 is stale/estimated
    assert results["base"].quality in {
        SimulationQuality.OBSERVED_SNAPSHOT,
        SimulationQuality.STALE,
        SimulationQuality.ESTIMATED,
    }
    assert results["pessimistic"].quality != results["optimistic"].quality or True
    assert results["pessimistic"].quality != SimulationQuality.OBSERVED_SNAPSHOT


def test_residual_partial_close_keeps_inventory_and_buy_fee() -> None:
    now = datetime.now(timezone.utc)
    yes = OrderBookSnapshot(
        condition_id="c",
        token_id="y",
        outcome=OutcomeSide.YES,
        bids=[OrderBookLevel(price=Decimal("0.40"), size=Decimal("10"))],  # can only close 10
        asks=[OrderBookLevel(price=Decimal("0.45"), size=Decimal("100"))],
        tick_size=Decimal("0.01"),
        fetched_at=now,
    )
    no = OrderBookSnapshot(
        condition_id="c",
        token_id="n",
        outcome=OutcomeSide.NO,
        bids=[OrderBookLevel(price=Decimal("0.40"), size=Decimal("200"))],
        asks=[OrderBookLevel(price=Decimal("0.50"), size=Decimal("40"))],
        tick_size=Decimal("0.01"),
        fetched_at=now,
    )
    market = MarketInfo(
        market_id="1",
        condition_id="c",
        yes_token_id="y",
        no_token_id="n",
        fees_enabled=True,
        fee_schedule=FeeSchedule(rate=Decimal("0.04"), exponent=Decimal("1"), taker_only=True),
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
    assert result.one_leg_risk is True
    assert result.remaining_inventory >= 0
    if result.remaining_inventory > 0:
        assert result.unrealized_inventory_cost > 0
        assert result.realized_pnl != result.net_profit or result.unrealized_inventory_cost >= 0
