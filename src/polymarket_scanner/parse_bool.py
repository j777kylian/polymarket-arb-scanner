"""Strict boolean parsing — Python bool('false') is True and must not be used."""

from __future__ import annotations

from typing import Any


def parse_strict_bool(value: Any) -> bool | None:
    """Return True/False, or None when the value is missing/unparseable."""
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        if value == 1:
            return True
        if value == 0:
            return False
        return None
    if isinstance(value, str):
        text = value.strip().lower()
        if text in {"true", "1", "yes", "y"}:
            return True
        if text in {"false", "0", "no", "n"}:
            return False
        return None
    return None


def parse_bool_conservative(
    value: Any,
    *,
    field: str,
    reasons: list[str] | None = None,
) -> bool:
    """Missing/invalid flags default to False so the market is not treated as tradable."""
    parsed = parse_strict_bool(value)
    if parsed is None:
        if reasons is not None:
            if value is None or (isinstance(value, str) and not value.strip()):
                reasons.append(f"{field} missing; conservative False")
            else:
                reasons.append(f"{field} unparseable={value!r}; conservative False")
        return False
    return parsed
