"""CLOB /clob-markets fee enrichment and FeeSchedule parsing."""

from __future__ import annotations

from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock

import pytest

from polymarket_scanner.api.clob_client import ClobClient
from polymarket_scanner.models import FeeSchedule
from polymarket_scanner.simulation.fee_calculator import calculate_taker_fee


def test_fee_schedule_from_gamma_style() -> None:
    s = FeeSchedule.from_api(
        {"rate": 0.04, "exponent": 1, "takerOnly": True, "rebateRate": 0.25}
    )
    assert s is not None
    assert s.rate == Decimal("0.04")
    assert s.taker_only is True
    assert s.rebate_rate == Decimal("0.25")


def test_fee_schedule_from_clob_fd_compact() -> None:
    s = FeeSchedule.from_api({"r": 0.04, "e": 1, "to": True})
    assert s is not None
    assert s.rate == Decimal("0.04")
    assert s.exponent == Decimal("1")
    assert s.taker_only is True


def test_fee_schedule_unwraps_fd_wrapper() -> None:
    s = FeeSchedule.from_api({"fd": {"r": 0.05, "e": 2, "to": True}, "tbf": 1000})
    assert s is not None
    assert s.rate == Decimal("0.05")
    assert s.exponent == Decimal("2")


def test_fee_schedule_ignores_rewards_r_dict() -> None:
    # Full clob-markets root: top-level ``r`` is rewards, not fee rate
    assert FeeSchedule.from_api({"r": {"mi": 200, "ma": 3.5}, "tbf": 1000}) is None


@pytest.mark.asyncio
async def test_get_fee_schedule_uses_clob_markets_path() -> None:
    client = ClobClient(client=MagicMock())
    client.get_market = AsyncMock(
        return_value={
            "c": "0xabc",
            "fd": {"r": 0.04, "e": 1, "to": True},
            "tbf": 1000,
            "mbf": 1000,
        }
    )
    enabled, schedule = await client.get_fee_schedule_for_condition("0xabc")
    client.get_market.assert_awaited_once_with("0xabc")
    assert enabled is True
    assert schedule is not None
    assert schedule.rate == Decimal("0.04")


@pytest.mark.asyncio
async def test_get_market_path_is_clob_markets() -> None:
    http = MagicMock()
    http.get_json = AsyncMock(return_value={"fd": {"r": 0.04, "e": 1, "to": True}})
    client = ClobClient(client=http)
    await client.get_market("0xdead")
    http.get_json.assert_awaited_once_with("/clob-markets/0xdead")


def test_missing_schedule_not_treated_as_free_when_fees_unknown() -> None:
    fee = calculate_taker_fee(
        Decimal("100"), Decimal("0.5"), None, fees_enabled=None
    )
    assert fee > 0


def test_explicit_fee_free_still_zero() -> None:
    fee = calculate_taker_fee(
        Decimal("100"), Decimal("0.5"), None, fees_enabled=False
    )
    assert fee == Decimal("0")
