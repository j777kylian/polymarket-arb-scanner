"""User-configurable rule engine (AND conditions; OR groups reserved)."""

from __future__ import annotations

import json
from copy import deepcopy
from decimal import Decimal
from typing import Any

from sqlalchemy import select

from polymarket_scanner.database import RuleRow, RuleSetRow, session_scope, utcnow
from polymarket_scanner.logging_config import get_logger
from polymarket_scanner.models import OpportunitySignal, RuleCondition, RuleSetModel

logger = get_logger(__name__)

OPERATORS = {
    ">",
    ">=",
    "<",
    "<=",
    "==",
    "!=",
    "contains",
    "not contains",
    "in",
    "not in",
}


def _to_number(value: Any) -> Decimal | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        return Decimal(str(value))
    except Exception:
        return None


def _get_field(obj: Any, field: str) -> Any:
    if isinstance(obj, dict):
        return obj.get(field)
    if hasattr(obj, field):
        return getattr(obj, field)
    # nested / aliases
    aliases = {
        "base_net_profit": "base_net",
        "pessimistic_net_profit": "pessimistic_net",
        "optimistic_net_profit": "optimistic_net",
    }
    alt = aliases.get(field)
    if alt and hasattr(obj, alt):
        return getattr(obj, alt)
    if isinstance(obj, dict) and alt:
        return obj.get(alt)
    return None


def evaluate_condition(condition: RuleCondition, obj: Any) -> bool:
    if not condition.enabled:
        return True
    op = condition.operator
    if op not in OPERATORS:
        logger.warning("Unknown operator %s", op)
        return False
    left = _get_field(obj, condition.field)
    right = condition.value

    if op in {">", ">=", "<", "<="}:
        ln = _to_number(left)
        rn = _to_number(right)
        if ln is None or rn is None:
            return False
        if op == ">":
            return ln > rn
        if op == ">=":
            return ln >= rn
        if op == "<":
            return ln < rn
        return ln <= rn

    if op in {"==", "!="}:
        # bool / str / number tolerant compare
        if isinstance(right, bool) or isinstance(left, bool):
            ok = bool(left) == bool(right)
        else:
            ln = _to_number(left)
            rn = _to_number(right)
            if ln is not None and rn is not None:
                ok = ln == rn
            else:
                ok = str(left).lower() == str(right).lower()
        return ok if op == "==" else not ok

    if op == "contains":
        if left is None:
            return False
        if isinstance(left, (list, tuple, set)):
            return any(str(right).lower() == str(x).lower() for x in left)
        return str(right).lower() in str(left).lower()

    if op == "not contains":
        return not evaluate_condition(
            RuleCondition(
                field=condition.field, operator="contains", value=right, enabled=True
            ),
            obj,
        )

    if op == "in":
        values = right if isinstance(right, (list, tuple, set)) else [right]
        value_set = {str(v).lower() for v in values}
        if isinstance(left, (list, tuple, set)):
            return any(str(item).lower() in value_set for item in left)
        return str(left).lower() in value_set

    if op == "not in":
        return not evaluate_condition(
            RuleCondition(field=condition.field, operator="in", value=right, enabled=True),
            obj,
        )
    return False


def evaluate_rule_set(rule_set: RuleSetModel, obj: Any) -> bool:
    if not rule_set.enabled:
        return True
    # v1: AND across conditions; OR groups reserved in structure
    results = [evaluate_condition(c, obj) for c in rule_set.conditions]
    if rule_set.logic.upper() == "OR":
        base = any(results) if results else True
    else:
        base = all(results) if results else True
    # reserved groups default AND with parent
    for g in rule_set.groups:
        nested = RuleSetModel(
            name="_group",
            enabled=True,
            logic=g.logic,
            conditions=g.conditions,
            groups=g.groups,
        )
        base = base and evaluate_rule_set(nested, obj)
    return base


def filter_opportunities(
    opportunities: list[OpportunitySignal] | list[dict[str, Any]],
    rule_set: RuleSetModel,
) -> list[Any]:
    return [o for o in opportunities if evaluate_rule_set(rule_set, o)]


def rule_set_to_dict(rule_set: RuleSetModel) -> dict[str, Any]:
    return rule_set.model_dump(mode="json")


def rule_set_from_dict(data: dict[str, Any]) -> RuleSetModel:
    return RuleSetModel.model_validate(data)


def export_rule_set_json(rule_set: RuleSetModel) -> str:
    return json.dumps(rule_set_to_dict(rule_set), indent=2)


def import_rule_set_json(text: str) -> RuleSetModel:
    return rule_set_from_dict(json.loads(text))


def load_rule_sets_from_db() -> list[RuleSetModel]:
    out: list[RuleSetModel] = []
    with session_scope() as session:
        rows = session.scalars(select(RuleSetRow)).all()
        for row in rows:
            conditions = []
            for rule in sorted(row.rules, key=lambda r: r.sort_order):
                try:
                    value = json.loads(rule.value_json)
                except json.JSONDecodeError:
                    value = rule.value_json
                conditions.append(
                    RuleCondition(
                        field=rule.field,
                        operator=rule.operator,
                        value=value,
                        enabled=rule.enabled,
                    )
                )
            out.append(
                RuleSetModel(
                    name=row.name,
                    enabled=row.enabled,
                    description=row.description,
                    logic=row.logic,
                    conditions=conditions,
                )
            )
    return out


def get_enabled_rule_set(name: str | None = None) -> RuleSetModel | None:
    sets = load_rule_sets_from_db()
    if name:
        for rs in sets:
            if rs.name == name:
                return rs
    for rs in sets:
        if rs.enabled:
            return rs
    return sets[0] if sets else None


def save_rule_set(rule_set: RuleSetModel) -> None:
    with session_scope() as session:
        row = session.scalar(select(RuleSetRow).where(RuleSetRow.name == rule_set.name))
        if row is None:
            row = RuleSetRow(
                name=rule_set.name,
                enabled=rule_set.enabled,
                description=rule_set.description,
                logic=rule_set.logic,
            )
            session.add(row)
            session.flush()
        else:
            row.enabled = rule_set.enabled
            row.description = rule_set.description
            row.logic = rule_set.logic
            row.updated_at = utcnow()
            for old in list(row.rules):
                session.delete(old)
            session.flush()
        for i, cond in enumerate(rule_set.conditions):
            session.add(
                RuleRow(
                    rule_set_id=row.id,
                    field=cond.field,
                    operator=cond.operator,
                    value_json=json.dumps(cond.value),
                    enabled=cond.enabled,
                    sort_order=i,
                )
            )


def delete_rule_set(name: str) -> None:
    with session_scope() as session:
        row = session.scalar(select(RuleSetRow).where(RuleSetRow.name == name))
        if row:
            session.delete(row)


def duplicate_rule_set(name: str, new_name: str) -> RuleSetModel:
    sets = {rs.name: rs for rs in load_rule_sets_from_db()}
    if name not in sets:
        raise KeyError(name)
    cloned = deepcopy(sets[name])
    cloned.name = new_name
    cloned.enabled = False
    save_rule_set(cloned)
    return cloned


def explain_filter(
    opportunities: list[Any], rule_set: RuleSetModel
) -> dict[str, Any]:
    """Return counts of what each condition excludes."""
    total = len(opportunities)
    surviving = list(opportunities)
    exclusions: dict[str, int] = {}
    for cond in rule_set.conditions:
        if not cond.enabled:
            continue
        before = len(surviving)
        surviving = [o for o in surviving if evaluate_condition(cond, o)]
        exclusions[f"{cond.field} {cond.operator} {cond.value}"] = before - len(surviving)
    return {
        "input": total,
        "output": len(surviving),
        "exclusions": exclusions,
    }
