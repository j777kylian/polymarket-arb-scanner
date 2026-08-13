"""Orderbook walker and binary arb tests."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal

from polymarket_scanner.models import (
    FeeSchedule,
    MarketInfo,
    OrderBookLevel,
    OrderBookSnapshot,
    OutcomeSide,
)
from polymarket_scanner.scanners.binary_complete_set import scan_binary_market
from polymarket_scanner.simulation.orderbook_walker import find_optimal_forward_arb


def _book(
    outcome: OutcomeSide,
    asks: list[tuple[str, str]],
    bids: list[tuple[str, str]] | None = None,
    *,
    age_seconds: float = 1.0,
) -> OrderBookSnapshot:
    now = datetime.now(timezone.utc)
    return OrderBookSnapshot(
        condition_id="0xcond",
        token_id=f"token-{outcome.value}",
        outcome=outcome,
        bids=[OrderBookLevel(price=Decimal(p), size=Decimal(s)) for p, s in (bids or [])],
        asks=[OrderBookLevel(price=Decimal(p), size=Decimal(s)) for p, s in asks],
        tick_size=Decimal("0.01"),
        min_order_size=Decimal("5"),
        fetched_at=now - timedelta(seconds=age_seconds),
    )


def _market(*, fees_enabled: bool = False, schedule: FeeSchedule | None = None) -> MarketInfo:
    return MarketInfo(
        market_id="m1",
        condition_id="0xcond",
        question="Test?",
        yes_token_id="token-YES",
        no_token_id="token-NO",
        fees_enabled=fees_enabled,
        fee_schedule=schedule,
    )


def test_simple_forward_arb_no_fees() -> None:
    yes = _book(OutcomeSide.YES, [("0.46", "100")])
    no = _book(OutcomeSide.NO, [("0.51", "80")])
    walk = find_optimal_forward_arb(yes, no, None, fees_enabled=False)
    assert walk is not None
    assert walk.quantity == Decimal("80")
    assert walk.yes_vwap + walk.no_vwap == Decimal("0.97")
    assert walk.gross_profit == Decimal("2.40")
    assert walk.net_profit == Decimal("2.40")  # no op cost/buffer in this call


def test_no_arb() -> None:
    yes = _book(OutcomeSide.YES, [("0.52", "100")])
    no = _book(OutcomeSide.NO, [("0.50", "100")])
    walk = find_optimal_forward_arb(yes, no, None, fees_enabled=False)
    assert walk is None
    sigs = scan_binary_market(_market(), yes, no, operational_cost=Decimal("0"), safety_buffer=Decimal("0"))
    assert not any(s.direction.value == "forward" for s in sigs)


def test_multi_level_depth() -> None:
    yes = _book(OutcomeSide.YES, [("0.46", "20"), ("0.48", "100")])
    no = _book(OutcomeSide.NO, [("0.51", "80"), ("0.53", "100")])
    walk = find_optimal_forward_arb(yes, no, None, fees_enabled=False)
    assert walk is not None
    # First 20 @ 0.46+0.51=0.97 profit 0.03 => 0.60
    # Next 60 @ 0.48+0.51=0.99 profit 0.01 => 0.60
    # Next would be 0.48+0.53=1.01 unprofitable — stop
    assert walk.quantity == Decimal("80")
    assert walk.gross_profit == Decimal("1.20")
    # Must not price all 80 at best ask 0.46
    assert walk.yes_vwap != Decimal("0.46")
    assert walk.yes_cost == Decimal("20") * Decimal("0.46") + Decimal("60") * Decimal("0.48")


def test_fees_eliminate_profit() -> None:
    schedule = FeeSchedule(
        rate=Decimal("0.07"), exponent=Decimal("1"), taker_only=True, rebate_rate=Decimal("0")
    )
    # Tiny edge: 0.495 + 0.495 = 0.99 => 0.01 gross/share
    yes = _book(OutcomeSide.YES, [("0.495", "100")])
    no = _book(OutcomeSide.NO, [("0.495", "100")])
    walk = find_optimal_forward_arb(
        yes, no, schedule, fees_enabled=True, operational_cost=Decimal("0"), safety_buffer=Decimal("0")
    )
    # Crypto fee at ~0.495 for 100 shares is large vs 1.00 gross
    sigs = scan_binary_market(
        _market(fees_enabled=True, schedule=schedule),
        yes,
        no,
        operational_cost=Decimal("0"),
        safety_buffer=Decimal("0"),
    )
    forward = [s for s in sigs if s.direction.value == "forward"]
    assert forward
    assert forward[0].gross_profit > 0
    assert forward[0].net_profit <= 0


def test_stale_data_flagged() -> None:
    yes = _book(OutcomeSide.YES, [("0.40", "50")], age_seconds=120)
    no = _book(OutcomeSide.NO, [("0.50", "50")], age_seconds=120)
    sigs = scan_binary_market(
        _market(),
        yes,
        no,
        max_data_age_seconds=15,
        operational_cost=Decimal("0"),
        safety_buffer=Decimal("0"),
    )
    assert sigs
    assert sigs[0].stale is True
    assert "stale data" in sigs[0].risk_tags


def test_decimal_precision_no_float_drift() -> None:
    yes = _book(OutcomeSide.YES, [("0.33", "3"), ("0.34", "7")])
    no = _book(OutcomeSide.NO, [("0.65", "10")])
    walk = find_optimal_forward_arb(yes, no, None, fees_enabled=False)
    assert walk is not None
    # 0.33+0.65=0.98; 0.34+0.65=0.99 — all profitable
    assert walk.quantity == Decimal("10")
    assert isinstance(walk.gross_profit, Decimal)
    assert walk.yes_cost + walk.no_cost + walk.gross_profit == walk.quantity
