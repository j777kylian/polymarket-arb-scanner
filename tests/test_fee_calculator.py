"""Fee calculator tests against official documentation tables."""

from __future__ import annotations

from decimal import Decimal

import pytest

from polymarket_scanner.models import FeeSchedule
from polymarket_scanner.simulation.fee_calculator import calculate_taker_fee, round_fee

CRYPTO = FeeSchedule(rate=Decimal("0.07"), exponent=Decimal("1"), taker_only=True, rebate_rate=Decimal("0.2"))
SPORTS = FeeSchedule(rate=Decimal("0.05"), exponent=Decimal("1"), taker_only=True, rebate_rate=Decimal("0.15"))
FINANCE = FeeSchedule(rate=Decimal("0.04"), exponent=Decimal("1"), taker_only=True, rebate_rate=Decimal("0.25"))


@pytest.mark.parametrize(
    "price,expected",
    [
        # Exact 5-decimal formula values (docs table rounds some rows to cents for display)
        ("0.01", "0.06930"),
        ("0.05", "0.33250"),
        ("0.10", "0.63000"),
        ("0.20", "1.12000"),
        ("0.50", "1.75000"),
        ("0.80", "1.12000"),
        ("0.99", "0.06930"),
    ],
)
def test_crypto_fee_table_100_shares(price: str, expected: str) -> None:
    fee = calculate_taker_fee(Decimal("100"), Decimal(price), CRYPTO, fees_enabled=True)
    assert fee == Decimal(expected)


@pytest.mark.parametrize(
    "price,expected",
    [
        ("0.01", "0.04950"),
        ("0.50", "1.25000"),
        ("0.99", "0.04950"),
    ],
)
def test_sports_fee_table_100_shares(price: str, expected: str) -> None:
    fee = calculate_taker_fee(Decimal("100"), Decimal(price), SPORTS, fees_enabled=True)
    assert fee == Decimal(expected)


@pytest.mark.parametrize(
    "price,expected",
    [
        ("0.01", "0.03960"),
        ("0.50", "1.00000"),
        ("0.99", "0.03960"),
    ],
)
def test_finance_fee_table_100_shares(price: str, expected: str) -> None:
    fee = calculate_taker_fee(Decimal("100"), Decimal(price), FINANCE, fees_enabled=True)
    assert fee == Decimal(expected)


def test_fees_disabled() -> None:
    fee = calculate_taker_fee(Decimal("100"), Decimal("0.5"), CRYPTO, fees_enabled=False)
    assert fee == Decimal("0")


def test_maker_pays_zero_when_taker_only() -> None:
    fee = calculate_taker_fee(
        Decimal("100"), Decimal("0.5"), CRYPTO, fees_enabled=True, is_taker=False
    )
    assert fee == Decimal("0")


def test_round_fee_precision() -> None:
    assert round_fee(Decimal("0.000004")) == Decimal("0")
    assert round_fee(Decimal("0.000015")) == Decimal("0.00002") or round_fee(
        Decimal("0.000015")
    ) == Decimal("0.00001")
