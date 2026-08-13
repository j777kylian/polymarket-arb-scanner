"""Load immutable strategy versions. Params are never updated in place."""

from __future__ import annotations

from dataclasses import dataclass

from sqlalchemy import select

from polymarket_scanner.database import StrategyAccountRow, StrategyConfigRow, session_scope
from polymarket_scanner.strategy.params import StrategyParams, params_from_json


@dataclass(frozen=True)
class LoadedStrategy:
    strategy_id: str
    version: int
    name: str
    is_live: bool
    params: StrategyParams


def load_enabled_shadow_strategies() -> list[LoadedStrategy]:
    with session_scope() as session:
        rows = session.scalars(
            select(StrategyConfigRow).where(
                StrategyConfigRow.enabled.is_(True),
                StrategyConfigRow.is_live.is_(False),
            )
        ).all()
        return [_to_loaded(row) for row in rows]


def load_live_strategy() -> LoadedStrategy | None:
    with session_scope() as session:
        row = session.scalar(
            select(StrategyConfigRow).where(
                StrategyConfigRow.is_live.is_(True),
                StrategyConfigRow.enabled.is_(True),
            )
        )
        if row is None:
            return None
        return _to_loaded(row)


def ensure_strategy_account(strategy_id: str, version: int, starting_capital: str) -> None:
    with session_scope() as session:
        row = session.scalar(
            select(StrategyAccountRow).where(
                StrategyAccountRow.strategy_id == strategy_id,
                StrategyAccountRow.version == version,
            )
        )
        if row is None:
            session.add(
                StrategyAccountRow(
                    strategy_id=strategy_id,
                    version=version,
                    cash=starting_capital,
                    occupied="0",
                    realized_pnl="0",
                    peak_equity=starting_capital,
                    max_drawdown="0",
                )
            )


def _to_loaded(row: StrategyConfigRow) -> LoadedStrategy:
    return LoadedStrategy(
        strategy_id=row.strategy_id,
        version=row.version,
        name=row.name,
        is_live=row.is_live,
        params=params_from_json(row.params_json),
    )
