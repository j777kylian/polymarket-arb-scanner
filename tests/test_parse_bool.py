from polymarket_scanner.api.gamma_client import parse_gamma_market
from polymarket_scanner.parse_bool import parse_bool_conservative, parse_strict_bool


def test_string_false_is_false() -> None:
    assert parse_strict_bool("false") is False
    assert parse_strict_bool("False") is False
    assert parse_strict_bool("true") is True
    assert parse_strict_bool(None) is None


def test_missing_accepting_orders_conservative() -> None:
    reasons: list[str] = []
    assert parse_bool_conservative(None, field="acceptingOrders", reasons=reasons) is False
    assert reasons


def test_gamma_string_false_not_tradable() -> None:
    raw = {
        "id": "1",
        "conditionId": "0xabc",
        "clobTokenIds": '["y","n"]',
        "outcomes": '["Yes","No"]',
        "active": True,
        "closed": False,
        "acceptingOrders": "false",
        "enableOrderBook": "true",
    }
    info = parse_gamma_market(raw)
    assert info is not None
    assert info.accepting_orders is False
    assert info.enable_order_book is True
