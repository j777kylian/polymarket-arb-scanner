"""Immutable strategy parameter snapshots."""

from __future__ import annotations

import json
from decimal import Decimal

from pydantic import BaseModel

from polymarket_scanner.config import AppConfig, PaperConfig


class StrategyParams(BaseModel):
    delay_ms: int = 500
    inter_leg_delay_ms: int = 100
    tif: str = "FAK"
    first_leg: str = "YES"
    min_net_profit: Decimal = Decimal("0.50")
    min_profit_per_share: Decimal = Decimal("0")
    minimum_quantity: Decimal = Decimal("0")
    max_book_skew_ms: float = 250.0
    safety_buffer: Decimal = Decimal("0.01")
    force_close: bool = True
    force_close_delay_ms: int = 200
    starting_capital: Decimal = Decimal("1000")

    def to_paper_config(self, *, strategy_id: str, strategy_version: int) -> PaperConfig:
        return PaperConfig(
            enabled=True,
            starting_capital=self.starting_capital,
            delay_ms=self.delay_ms,
            signal_to_first_leg_ms=self.delay_ms,
            inter_leg_delay_ms=self.inter_leg_delay_ms,
            force_close_delay_ms=self.force_close_delay_ms,
            time_in_force=self.tif,
            first_leg=self.first_leg,
            force_close_unhedged=self.force_close,
            min_net_profit=self.min_net_profit,
            min_profit_per_share=self.min_profit_per_share,
            minimum_quantity=self.minimum_quantity,
            strategy_id=strategy_id,
            strategy_version=strategy_version,
        )

    def apply_to_app_config(
        self, cfg: AppConfig, *, strategy_id: str, strategy_version: int
    ) -> AppConfig:
        data = cfg.model_dump()
        data.setdefault("scanner", {})["max_book_skew_ms"] = self.max_book_skew_ms
        data.setdefault("simulation", {})["safety_buffer"] = self.safety_buffer
        data["paper"] = self.to_paper_config(
            strategy_id=strategy_id,
            strategy_version=strategy_version,
        ).model_dump()
        return AppConfig.model_validate(data)


def params_from_json(raw: str | dict[str, object]) -> StrategyParams:
    payload = json.loads(raw) if isinstance(raw, str) else raw
    return StrategyParams.model_validate(payload)


def params_to_json(params: StrategyParams) -> str:
    return params.model_dump_json()
