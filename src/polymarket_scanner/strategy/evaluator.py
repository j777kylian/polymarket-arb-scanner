"""Walk-forward evaluator. Recommends a strategy; never mutates live params."""

from __future__ import annotations

import json
from datetime import datetime
from decimal import Decimal
from typing import Any

from sqlalchemy import select

from polymarket_scanner.config import get_config
from polymarket_scanner.database import (
    StrategyAccountRow,
    StrategyEvalRow,
    StrategyTradeRow,
    session_scope,
)
from polymarket_scanner.logging_config import get_logger

logger = get_logger(__name__)
ZERO = Decimal("0")


def _dec(value: str | None) -> Decimal:
    return Decimal(value or "0")


def compute_trade_metrics(trades: list[StrategyTradeRow]) -> dict[str, Any]:
    total = len(trades)
    filled = [t for t in trades if t.status not in {"rejected", "rejected_fok", "rejected_insufficient_capital", "no_fill"}]
    rejected = [t for t in trades if t.reject_reason or t.status.startswith("rejected") or t.status == "no_fill"]
    second_fail = [t for t in filled if t.status in {"one_leg", "one_leg_merged"}]
    residual = [t for t in filled if _dec(t.remaining_inventory) > ZERO]
    realized = sum((_dec(t.realized_pnl) for t in trades), ZERO)
    residual_loss = sum((_dec(t.inventory_cost) for t in residual), ZERO)
    inventory_adjusted = realized - residual_loss
    fill_rate = (len(filled) / total) if total else 0.0
    second_leg_failure_rate = (len(second_fail) / len(filled)) if filled else 0.0
    residual_rate = (len(residual) / len(filled)) if filled else 0.0
    return {
        "trade_count": total,
        "fill_count": len(filled),
        "reject_count": len(rejected),
        "realized_pnl": format(realized, "f"),
        "inventory_adjusted_pnl": format(inventory_adjusted, "f"),
        "fill_rate": fill_rate,
        "second_leg_failure_rate": second_leg_failure_rate,
        "residual_rate": residual_rate,
        "residual_loss": format(residual_loss, "f"),
    }


def _account_metrics(row: StrategyAccountRow | None, starting: Decimal, trade_metrics: dict[str, Any]) -> dict[str, Any]:
    if row is None:
        cash = starting
        occupied = ZERO
        peak = starting
        max_dd = ZERO
        realized = _dec(str(trade_metrics["realized_pnl"]))
    else:
        cash = _dec(row.cash)
        occupied = _dec(row.occupied)
        peak = _dec(row.peak_equity) or starting
        max_dd = _dec(row.max_drawdown)
        realized = _dec(row.realized_pnl)
    equity = cash + occupied
    utilization = float(occupied / starting) if starting else 0.0
    out = dict(trade_metrics)
    out.update(
        {
            "realized_pnl": format(realized, "f"),
            "inventory_adjusted_pnl": format(realized - occupied, "f"),
            "max_drawdown": format(max_dd, "f"),
            "capital_utilization": utilization,
            "cash": format(cash, "f"),
            "occupied": format(occupied, "f"),
            "equity": format(equity, "f"),
            "peak_equity": format(peak, "f"),
        }
    )
    return out


def recommend_strategy(metrics_by_key: dict[str, dict[str, Any]], *, min_trades: int) -> tuple[str | None, int | None, bool, str]:
    eligible: list[tuple[str, int, Decimal]] = []
    for key, metrics in metrics_by_key.items():
        sid, _, ver_s = key.partition("@")
        version = int(ver_s or "1")
        if int(metrics.get("trade_count") or 0) < min_trades:
            continue
        adj = _dec(str(metrics.get("inventory_adjusted_pnl") or "0"))
        eligible.append((sid, version, adj))
    if not eligible:
        return None, None, True, "insufficient_sample"
    eligible.sort(key=lambda x: x[2], reverse=True)
    sid, version, _ = eligible[0]
    return sid, version, False, "recommend_only"


def walk_forward_evaluate(
    *,
    training_start: datetime,
    training_end: datetime,
    validation_start: datetime,
    validation_end: datetime,
    min_trades: int | None = None,
) -> dict[str, Any]:
    """Train and validation windows must not overlap. Does not change live parameters."""
    if not (training_start < training_end <= validation_start < validation_end):
        raise ValueError("training and validation windows must be strictly separated")
    cfg = get_config()
    min_trades = min_trades if min_trades is not None else cfg.scanner.min_walk_forward_trades
    with session_scope() as session:
        trades = session.scalars(select(StrategyTradeRow)).all()
        accounts = {
            (r.strategy_id, r.version): r for r in session.scalars(select(StrategyAccountRow)).all()
        }
        val_by: dict[tuple[str, int], list[StrategyTradeRow]] = {}
        train_by: dict[tuple[str, int], list[StrategyTradeRow]] = {}
        for t in trades:
            key = (t.strategy_id, t.strategy_version)
            ts = t.created_at
            if ts.tzinfo is None:
                from datetime import timezone

                ts = ts.replace(tzinfo=timezone.utc)
            if training_start <= ts < training_end:
                train_by.setdefault(key, []).append(t)
            elif validation_start <= ts < validation_end:
                val_by.setdefault(key, []).append(t)

        metrics: dict[str, dict[str, Any]] = {}
        starting = cfg.paper.starting_capital
        keys = set(val_by) | set(train_by) | set(accounts)
        for sid, ver in keys:
            val_metrics = compute_trade_metrics(val_by.get((sid, ver), []))
            train_metrics = compute_trade_metrics(train_by.get((sid, ver), []))
            acct = accounts.get((sid, ver))
            combined = _account_metrics(acct, starting, val_metrics)
            combined["training"] = train_metrics
            combined["validation_trade_count"] = val_metrics["trade_count"]
            metrics[f"{sid}@{ver}"] = combined

        val_only = {
            k: {
                **v,
                "trade_count": v.get("validation_trade_count") or v.get("trade_count") or 0,
            }
            for k, v in metrics.items()
        }
        rec_id, rec_ver, insufficient, note = recommend_strategy(val_only, min_trades=min_trades)
        sample = sum(int(v.get("validation_trade_count") or 0) for v in metrics.values())
        row = StrategyEvalRow(
            training_start=training_start,
            training_end=training_end,
            validation_start=validation_start,
            validation_end=validation_end,
            sample_count=sample,
            insufficient_sample=insufficient,
            recommended_strategy_id=None if insufficient else rec_id,
            recommended_version=None if insufficient else rec_ver,
            metrics_json=json.dumps(metrics),
            note=note + " — never auto-applies live params",
        )
        session.add(row)
        session.flush()
        eval_id = row.id
    logger.info(
        "Walk-forward eval id=%s insufficient=%s recommended=%s@%s (advisory only)",
        eval_id,
        insufficient,
        rec_id,
        rec_ver,
    )
    return {
        "eval_id": eval_id,
        "insufficient_sample": insufficient,
        "recommended_strategy_id": None if insufficient else rec_id,
        "recommended_version": None if insufficient else rec_ver,
        "metrics": metrics,
        "note": note,
    }
