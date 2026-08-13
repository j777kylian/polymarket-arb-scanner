from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal

from polymarket_scanner.config import get_config
from polymarket_scanner.database import init_db
from polymarket_scanner.models import (
    ArbDirection,
    MarketInfo,
    OpportunitySignal,
    OrderBookLevel,
    OrderBookSnapshot,
    OutcomeSide,
)
from polymarket_scanner.simulation.paper_trader import (
    PaperTradeMeta,
    execute_paper_complete_set,
    get_strategy_account,
)
from polymarket_scanner.strategy.evaluator import recommend_strategy, walk_forward_evaluate
from polymarket_scanner.strategy.params import StrategyParams


def _db(tmp_path, monkeypatch) -> None:
    db = tmp_path / "t.db"
    monkeypatch.setenv("DATABASE_URL", f"sqlite:///{db}")
    get_config.cache_clear()
    from polymarket_scanner import database as dbmod

    dbmod._engine = None
    dbmod._SessionLocal = None
    init_db(f"sqlite:///{db}")


def _books(now: datetime) -> tuple[OrderBookSnapshot, OrderBookSnapshot]:
    yes = OrderBookSnapshot(
        condition_id="c",
        token_id="y",
        outcome=OutcomeSide.YES,
        asks=[OrderBookLevel(price=Decimal("0.40"), size=Decimal("100"))],
        bids=[OrderBookLevel(price=Decimal("0.39"), size=Decimal("100"))],
        fetched_at=now,
        hash="y",
    )
    no = OrderBookSnapshot(
        condition_id="c",
        token_id="n",
        outcome=OutcomeSide.NO,
        asks=[OrderBookLevel(price=Decimal("0.50"), size=Decimal("100"))],
        bids=[OrderBookLevel(price=Decimal("0.49"), size=Decimal("100"))],
        fetched_at=now,
        hash="n",
    )
    return yes, no


def _sig(now: datetime) -> OpportunitySignal:
    return OpportunitySignal(
        market_id="m1",
        condition_id="c",
        direction=ArbDirection.FORWARD,
        discovered_at=now,
        data_age_seconds=0,
        quantity=Decimal("10"),
        yes_vwap=Decimal("0.40"),
        no_vwap=Decimal("0.50"),
        gross_profit=Decimal("1.0"),
        fee_total=Decimal("0"),
        net_profit=Decimal("1.0"),
        net_profit_per_share=Decimal("0.1"),
        net_profit_rate=Decimal("0.1"),
        levels_used_yes=1,
        levels_used_no=1,
    )


def test_shadow_strategy_accounts_are_isolated(tmp_path, monkeypatch) -> None:
    _db(tmp_path, monkeypatch)
    now = datetime.now(timezone.utc)
    yes, no = _books(now)
    market = MarketInfo(
        market_id="m1",
        condition_id="c",
        yes_token_id="y",
        no_token_id="n",
        fees_enabled=False,
        minimum_order_size=Decimal("1"),
    )
    paper_a = StrategyParams().to_paper_config(strategy_id="shadow_a", strategy_version=1)
    paper_b = StrategyParams().to_paper_config(strategy_id="shadow_b", strategy_version=1)

    execute_paper_complete_set(
        market,
        _sig(now),
        yes,
        no,
        skip_min_profit=True,
        paper_cfg=paper_a,
        account_kind="strategy",
        meta=PaperTradeMeta(strategy_id="shadow_a", strategy_version=1),
        force_close_unhedged=False,
    )
    cash_a, occ_a, pnl_a = get_strategy_account("shadow_a", 1)
    cash_b, occ_b, pnl_b = get_strategy_account("shadow_b", 1)
    assert pnl_a == Decimal("1.0")
    assert cash_a == Decimal("1001.0")
    assert pnl_b == Decimal("0")
    assert cash_b == Decimal("1000")
    assert occ_b == Decimal("0")
    assert cash_a != cash_b

    execute_paper_complete_set(
        market,
        _sig(now),
        yes,
        no,
        skip_min_profit=True,
        paper_cfg=paper_b,
        account_kind="strategy",
        meta=PaperTradeMeta(strategy_id="shadow_b", strategy_version=1),
        force_close_unhedged=False,
    )
    cash_a2, _, pnl_a2 = get_strategy_account("shadow_a", 1)
    cash_b2, _, pnl_b2 = get_strategy_account("shadow_b", 1)
    assert cash_a2 == cash_a
    assert pnl_a2 == pnl_a
    assert pnl_b2 == Decimal("1.0")
    assert cash_b2 == Decimal("1001.0")


def test_walk_forward_insufficient_sample_does_not_recommend(tmp_path, monkeypatch) -> None:
    _db(tmp_path, monkeypatch)
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    mid = datetime(2026, 2, 1, tzinfo=timezone.utc)
    end = datetime(2026, 3, 1, tzinfo=timezone.utc)
    result = walk_forward_evaluate(
        training_start=start,
        training_end=mid,
        validation_start=mid,
        validation_end=end,
        min_trades=30,
    )
    assert result["insufficient_sample"] is True
    assert result["recommended_strategy_id"] is None

    rec_id, rec_ver, insufficient, _note = recommend_strategy(
        {"shadow_fast@1": {"trade_count": 5, "inventory_adjusted_pnl": "10"}},
        min_trades=30,
    )
    assert insufficient is True
    assert rec_id is None
    assert rec_ver is None
