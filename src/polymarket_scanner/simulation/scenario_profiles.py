"""Built-in simulation scenario profiles."""

from __future__ import annotations

from decimal import Decimal
from typing import Any

from pydantic import BaseModel, Field

from polymarket_scanner.config import get_config


class ScenarioProfile(BaseModel):
    name: str
    delay_ms: int = 0
    slippage_ticks: int = 0
    depth_factor: Decimal = Decimal("1.0")
    sequential_legs: bool = False
    partial_second_leg_ratio: Decimal = Decimal("1.0")
    force_close_unhedged: bool = False
    operational_cost: Decimal = Decimal("0")
    safety_buffer: Decimal = Decimal("0")
    is_taker: bool = True
    first_leg: str = "YES"  # YES | NO
    custom: dict[str, Any] = Field(default_factory=dict)


def get_builtin_profiles() -> dict[str, ScenarioProfile]:
    cfg = get_config()
    op = Decimal(str(cfg.simulation.operational_cost))
    buf = Decimal(str(cfg.simulation.safety_buffer))
    profiles: dict[str, ScenarioProfile] = {}
    for name, raw in cfg.simulation.profiles.items():
        profiles[name] = ScenarioProfile(
            name=name,
            delay_ms=raw.delay_ms,
            slippage_ticks=raw.slippage_ticks,
            depth_factor=Decimal(str(raw.depth_factor)),
            sequential_legs=raw.sequential_legs,
            partial_second_leg_ratio=Decimal(str(raw.partial_second_leg_ratio)),
            force_close_unhedged=raw.force_close_unhedged,
            operational_cost=op if name != "optimistic" else Decimal("0"),
            safety_buffer=buf if name != "optimistic" else Decimal("0"),
            is_taker=True,
            first_leg="YES",
        )
    # Ensure defaults exist even if yaml incomplete
    profiles.setdefault(
        "optimistic",
        ScenarioProfile(name="optimistic", delay_ms=0, slippage_ticks=0, depth_factor=Decimal("1")),
    )
    profiles.setdefault(
        "base",
        ScenarioProfile(
            name="base",
            delay_ms=500,
            slippage_ticks=1,
            depth_factor=Decimal("0.9"),
            sequential_legs=True,
            operational_cost=op,
            safety_buffer=buf,
        ),
    )
    profiles.setdefault(
        "pessimistic",
        ScenarioProfile(
            name="pessimistic",
            delay_ms=2000,
            slippage_ticks=2,
            depth_factor=Decimal("0.7"),
            sequential_legs=True,
            partial_second_leg_ratio=Decimal("0.5"),
            force_close_unhedged=True,
            operational_cost=op,
            safety_buffer=buf,
        ),
    )
    return profiles
