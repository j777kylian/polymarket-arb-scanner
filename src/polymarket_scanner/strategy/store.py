"""Load immutable strategy versions. Params are never updated in place."""

from __future__ import annotations

from dataclasses import dataclass

from sqlalchemy import select

from polymarket_scanner.database import (
    StrategyAccountRow,
    StrategyConfigRow,
    StrategyRunRow,
    session_scope,
    utcnow,
)
from polymarket_scanner.strategy.params import StrategyParams, params_from_json, params_to_json


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


def list_strategy_configs() -> list[StrategyConfigRow]:
    with session_scope() as session:
        rows = session.scalars(
            select(StrategyConfigRow).order_by(StrategyConfigRow.strategy_id, StrategyConfigRow.version)
        ).all()
        session.expunge_all()
        return list(rows)


def set_strategy_enabled(strategy_id: str, version: int, enabled: bool) -> None:
    with session_scope() as session:
        row = session.scalar(
            select(StrategyConfigRow).where(
                StrategyConfigRow.strategy_id == strategy_id,
                StrategyConfigRow.version == version,
            )
        )
        if row is not None:
            row.enabled = enabled


def create_strategy_version(
    strategy_id: str,
    name: str,
    params: StrategyParams,
    *,
    is_live: bool = False,
    enabled: bool = True,
) -> int:
    """Insert a new immutable version. Never UPDATE params_json."""
    with session_scope() as session:
        current = session.scalars(
            select(StrategyConfigRow)
            .where(StrategyConfigRow.strategy_id == strategy_id)
            .order_by(StrategyConfigRow.version.desc())
        ).first()
        version = (current.version + 1) if current is not None else 1
        session.add(
            StrategyConfigRow(
                strategy_id=strategy_id,
                version=version,
                name=name,
                enabled=enabled,
                is_live=is_live,
                params_json=params_to_json(params),
            )
        )
    ensure_strategy_account(strategy_id, version, str(params.starting_capital))
    return version


def start_strategy_runs() -> list[int]:
    ids: list[int] = []
    with session_scope() as session:
        rows = session.scalars(select(StrategyConfigRow).where(StrategyConfigRow.enabled.is_(True))).all()
        for row in rows:
            run = StrategyRunRow(
                strategy_id=row.strategy_id,
                strategy_version=row.version,
                status="running",
            )
            session.add(run)
            session.flush()
            ids.append(int(run.id))
    return ids


def finish_open_strategy_runs() -> None:
    with session_scope() as session:
        rows = session.scalars(select(StrategyRunRow).where(StrategyRunRow.status == "running")).all()
        now = utcnow()
        for row in rows:
            row.finished_at = now
            row.status = "stopped"
