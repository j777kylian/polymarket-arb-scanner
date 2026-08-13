"""Rule engine tests."""

from __future__ import annotations

from polymarket_scanner.models import RuleCondition, RuleSetModel
from polymarket_scanner.scanners.rule_engine import (
    evaluate_rule_set,
    export_rule_set_json,
    filter_opportunities,
    import_rule_set_json,
)


def test_and_rules_and_operators() -> None:
    rs = RuleSetModel(
        name="t",
        conditions=[
            RuleCondition(field="net_profit", operator=">=", value=0.5),
            RuleCondition(field="category", operator="contains", value="crypto"),
            RuleCondition(field="tags", operator="in", value=["a", "b"]),
        ],
    )
    ok = {"net_profit": 1.0, "category": "Crypto Markets", "tags": ["b"]}
    bad = {"net_profit": 0.1, "category": "Crypto Markets", "tags": ["b"]}
    assert evaluate_rule_set(rs, ok) is True
    assert evaluate_rule_set(rs, bad) is False


def test_disabled_condition_ignored() -> None:
    rs = RuleSetModel(
        name="t",
        conditions=[
            RuleCondition(field="net_profit", operator=">=", value=10, enabled=False),
            RuleCondition(field="stale", operator="==", value=False),
        ],
    )
    assert evaluate_rule_set(rs, {"net_profit": 0, "stale": False}) is True


def test_json_import_export() -> None:
    rs = RuleSetModel(
        name="Balanced",
        conditions=[RuleCondition(field="quantity", operator=">", value=5)],
    )
    text = export_rule_set_json(rs)
    loaded = import_rule_set_json(text)
    assert loaded.name == "Balanced"
    assert loaded.conditions[0].operator == ">"


def test_filter_list() -> None:
    rs = RuleSetModel(
        name="t",
        conditions=[RuleCondition(field="net_profit", operator=">", value=0)],
    )
    rows = [{"net_profit": 1}, {"net_profit": -1}, {"net_profit": 0.5}]
    assert len(filter_opportunities(rows, rs)) == 2
