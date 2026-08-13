"""Binary scanner integration-style tests."""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal

from polymarket_scanner.api.gamma_client import parse_gamma_market
from polymarket_scanner.models import MarketInfo, OrderBookLevel, OrderBookSnapshot, OutcomeSide
from polymarket_scanner.scanners.binary_complete_set import scan_binary_market


def test_reverse_arb_flagged_requires_split() -> None:
    now = datetime.now(timezone.utc)
    yes = OrderBookSnapshot(
        condition_id="c",
        token_id="y",
        outcome=OutcomeSide.YES,
        bids=[OrderBookLevel(price=Decimal("0.56"), size=Decimal("10"))],
        asks=[OrderBookLevel(price=Decimal("0.60"), size=Decimal("10"))],
        fetched_at=now,
    )
    no = OrderBookSnapshot(
        condition_id="c",
        token_id="n",
        outcome=OutcomeSide.NO,
        bids=[OrderBookLevel(price=Decimal("0.50"), size=Decimal("10"))],
        asks=[OrderBookLevel(price=Decimal("0.55"), size=Decimal("10"))],
        fetched_at=now,
    )
    market = MarketInfo(market_id="1", condition_id="c", yes_token_id="y", no_token_id="n")
    sigs = scan_binary_market(
        market, yes, no, operational_cost=Decimal("0"), safety_buffer=Decimal("0")
    )
    rev = [s for s in sigs if s.direction.value == "reverse"]
    assert rev
    assert rev[0].requires_split_inventory is True


def test_gamma_missing_optional_fields() -> None:
    raw = {
        "id": "123",
        "conditionId": "0xabc",
        "active": True,
        "closed": False,
        "acceptingOrders": True,
        "enableOrderBook": True,
        "clobTokenIds": '["111","222"]',
        "outcomes": '["Yes","No"]',
        # feesEnabled / feeSchedule / category deliberately omitted
    }
    info = parse_gamma_market(raw)
    assert info is not None
    assert info.fees_enabled is None
    assert info.fee_schedule is None
    assert info.category is None
    assert info.yes_token_id == "111"
    assert info.no_token_id == "222"
