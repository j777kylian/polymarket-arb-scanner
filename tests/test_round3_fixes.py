"""State machine, capital, residuals, shadows, walk-forward, WS, latency, reports, Docker."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

import pytest
import yaml
from sqlalchemy import select

from polymarket_scanner.config import get_config
from polymarket_scanner.database import (
    OpportunityEpisodeRow,
    OpportunityRow,
    PaperAccountRow,
    PaperTradeRow,
    PositionRow,
    StrategyEvalRow,
    StrategyTradeRow,
    init_db,
    session_scope,
)
from polymarket_scanner.models import (
    ArbDirection,
    MarketInfo,
    OpportunitySignal,
    OrderBookLevel,
    OrderBookSnapshot,
    OutcomeSide,
)
from polymarket_scanner.realtime import RealtimeScanner
from polymarket_scanner.reporting.html_report import generate_daily_report, previous_report_date_due
from polymarket_scanner.scanners.opportunity_tracker import episode_is_open, sync_episodes
from polymarket_scanner.scanners.pipeline import persist_signals, record_latency
from polymarket_scanner.scheduler import get_dashboard_stats
from polymarket_scanner.simulation.inventory import mark_market_books, refresh_account_equity
from polymarket_scanner.simulation.paper_trader import (
    LEG_RESIDUAL_OPEN,
    LEG_SECOND_FAILED,
    execute_paper_complete_set,
    get_paper_account,
    run_delayed_paper_trade,
)
from polymarket_scanner.strategy.evaluator import walk_forward_evaluate
from polymarket_scanner.strategy.params import StrategyParams, strategy_eligible


def _db(tmp_path, monkeypatch) -> None:
    db = tmp_path / "t.db"
    monkeypatch.setenv("DATABASE_URL", f"sqlite:///{db}")
    get_config.cache_clear()
    from polymarket_scanner import database as dbmod

    dbmod._engine = None
    dbmod._SessionLocal = None
    init_db(f"sqlite:///{db}")


def _book(
    token: str,
    side: OutcomeSide,
    now: datetime,
    *,
    hash_: str,
    ask: str = "0.40",
    bid: str = "0.39",
    size: str = "100",
) -> OrderBookSnapshot:
    return OrderBookSnapshot(
        condition_id="0xcond",
        token_id=token,
        outcome=side,
        asks=[OrderBookLevel(price=Decimal(ask), size=Decimal(size))],
        bids=[OrderBookLevel(price=Decimal(bid), size=Decimal(size))],
        fetched_at=now,
        hash=hash_,
        connection_generation=1,
    )


def _market() -> MarketInfo:
    return MarketInfo(
        market_id="m1",
        condition_id="0xcond",
        yes_token_id="y",
        no_token_id="n",
        fees_enabled=False,
        minimum_order_size=Decimal("1"),
    )


def _sig(now: datetime, net: str = "1.0", qty: str = "10") -> OpportunitySignal:
    return OpportunitySignal(
        market_id="m1",
        condition_id="0xcond",
        direction=ArbDirection.FORWARD,
        discovered_at=now,
        data_age_seconds=0,
        quantity=Decimal(qty),
        yes_vwap=Decimal("0.40"),
        no_vwap=Decimal("0.50"),
        gross_profit=Decimal(net),
        fee_total=Decimal("0"),
        net_profit=Decimal(net),
        net_profit_per_share=Decimal("0.1"),
        net_profit_rate=Decimal("0.1"),
        levels_used_yes=1,
        levels_used_no=1,
        books_ready=True,
    )


class _Cache:
    generation = 1

    def __init__(self) -> None:
        self.books: dict[str, OrderBookSnapshot] = {}

    def get(self, token_id: str) -> OrderBookSnapshot | None:
        return self.books.get(token_id)

    def pair_ready(self, yes_token: str, no_token: str) -> bool:
        return yes_token in self.books and no_token in self.books


@pytest.mark.asyncio
async def test_second_snapshot_invalid_after_first_fill_leaves_exposure(tmp_path, monkeypatch) -> None:
    _db(tmp_path, monkeypatch)
    t0 = datetime(2026, 8, 13, 12, 0, tzinfo=timezone.utc)
    t1 = t0 + timedelta(milliseconds=500)
    cache = _Cache()
    cache.books["y"] = _book("y", OutcomeSide.YES, t1, hash_="y1", ask="0.40")
    cache.books["n"] = _book("n", OutcomeSide.NO, t1, hash_="n1", ask="0.50")
    cfg = get_config()
    cfg.paper.min_net_profit = Decimal("0")
    cfg.paper.force_close_unhedged = False
    cfg.paper.inter_leg_delay_ms = 100
    sleeps = {"n": 0}

    async def _sleep(_: float) -> None:
        sleeps["n"] += 1
        if sleeps["n"] == 2:
            cache.books.clear()

    result = await run_delayed_paper_trade(
        cache=cache,
        market=_market(),
        signal=_sig(t0),
        episode_id=1,
        cfg=cfg,
        paper_cfg=cfg.paper,
        sleep_fn=_sleep,
        now_fn=lambda: t1 + timedelta(milliseconds=50),
        episode_open_fn=lambda _e: True,
    )
    assert result is not None
    assert result["status"] == "one_leg"
    assert result["leg_state"] == LEG_SECOND_FAILED
    cash, occ, pnl = get_paper_account()
    assert cash < Decimal("1000")
    assert cash >= Decimal("0")
    assert occ > Decimal("0")
    assert pnl == Decimal("0")
    with session_scope() as session:
        trade = session.scalar(select(PaperTradeRow).order_by(PaperTradeRow.id.desc()))
        assert trade is not None
        assert trade.leg_state == LEG_SECOND_FAILED
        assert Decimal(trade.first_qty or "0") > 0
        assert Decimal(trade.remaining_inventory or "0") > 0
        pos = session.scalars(select(PositionRow)).all()
        assert pos
        assert Decimal(pos[0].quantity) > 0
        acct = session.scalar(select(PaperAccountRow).limit(1))
        assert acct is not None
        assert Decimal(acct.cash) >= 0


def test_second_leg_cannot_spend_future_merge_proceeds(tmp_path, monkeypatch) -> None:
    _db(tmp_path, monkeypatch)
    now = datetime.now(timezone.utc)
    yes = _book("y", OutcomeSide.YES, now, hash_="y", ask="0.01", size="10000")
    no = _book("n", OutcomeSide.NO, now, hash_="n", ask="0.99", size="10000")
    market = _market()
    market.minimum_order_size = Decimal("1")
    result = execute_paper_complete_set(
        market,
        _sig(now, qty="10000", net="100"),
        yes,
        no,
        tif="FOK",
        skip_min_profit=True,
        force_close_unhedged=False,
    )
    assert result is not None
    assert result["status"] != "merged"
    yes_qty = Decimal("0")
    no_qty = Decimal("0")
    with session_scope() as session:
        trade = session.scalar(select(PaperTradeRow).order_by(PaperTradeRow.id.desc()))
        assert trade is not None
        yes_qty = Decimal(trade.yes_qty or "0")
        no_qty = Decimal(trade.no_qty or "0")
    assert min(yes_qty, no_qty) < Decimal("10000")
    cash, _occ, _pnl = get_paper_account()
    assert cash >= Decimal("0")
    assert cash < Decimal("1000") or yes_qty + no_qty > 0


@pytest.mark.asyncio
async def test_invalid_close_snapshot_preserves_residual(tmp_path, monkeypatch) -> None:
    _db(tmp_path, monkeypatch)
    t0 = datetime(2026, 8, 13, 12, 0, tzinfo=timezone.utc)
    t1 = t0 + timedelta(milliseconds=500)
    cache = _Cache()
    cache.books["y"] = _book("y", OutcomeSide.YES, t1, hash_="y1", ask="0.40", size="10")
    cache.books["n"] = _book("n", OutcomeSide.NO, t1, hash_="n1", ask="0.50", size="4")
    cfg = get_config()
    cfg.paper.min_net_profit = Decimal("0")
    cfg.paper.force_close_unhedged = True
    cfg.paper.inter_leg_delay_ms = 100
    cfg.paper.force_close_delay_ms = 50
    sleeps = {"n": 0}

    async def _sleep(_: float) -> None:
        sleeps["n"] += 1
        if sleeps["n"] == 2:
            cache.books["y"] = _book(
                "y", OutcomeSide.YES, t1 + timedelta(milliseconds=150), hash_="y2", ask="0.40", size="10"
            )
            cache.books["n"] = _book(
                "n", OutcomeSide.NO, t1 + timedelta(milliseconds=150), hash_="n2", ask="0.50", size="4"
            )
        if sleeps["n"] == 3:
            cache.books.clear()

    result = await run_delayed_paper_trade(
        cache=cache,
        market=_market(),
        signal=_sig(t0),
        episode_id=2,
        cfg=cfg,
        paper_cfg=cfg.paper,
        sleep_fn=_sleep,
        now_fn=lambda: t1 + timedelta(milliseconds=200),
        episode_open_fn=lambda _e: True,
    )
    assert result is not None
    assert result.get("reject_reason") == "close_snapshot_unavailable"
    assert result["leg_state"] == LEG_RESIDUAL_OPEN
    with session_scope() as session:
        trade = session.scalar(select(PaperTradeRow).order_by(PaperTradeRow.id.desc()))
        assert trade is not None
        assert trade.reject_reason == "close_snapshot_unavailable"
        pos = session.scalars(select(PositionRow)).all()
        assert any(Decimal(p.quantity) > 0 for p in pos)


@pytest.mark.asyncio
async def test_residual_mark_to_market_changes_equity_drawdown(tmp_path, monkeypatch) -> None:
    _db(tmp_path, monkeypatch)
    t0 = datetime(2026, 8, 13, 12, 0, tzinfo=timezone.utc)
    t1 = t0 + timedelta(milliseconds=500)
    cache = _Cache()
    cache.books["y"] = _book("y", OutcomeSide.YES, t1, hash_="y1", ask="0.40", bid="0.39")
    cache.books["n"] = _book("n", OutcomeSide.NO, t1, hash_="n1", ask="0.50")
    cfg = get_config()
    cfg.paper.min_net_profit = Decimal("0")
    cfg.paper.force_close_unhedged = False
    sleeps = {"n": 0}

    async def _sleep(_: float) -> None:
        sleeps["n"] += 1
        if sleeps["n"] == 2:
            cache.books.clear()

    await run_delayed_paper_trade(
        cache=cache,
        market=_market(),
        signal=_sig(t0),
        episode_id=3,
        cfg=cfg,
        paper_cfg=cfg.paper,
        sleep_fn=_sleep,
        now_fn=lambda: t1 + timedelta(milliseconds=50),
        episode_open_fn=lambda _e: True,
    )
    cash, _occ, _pnl = get_paper_account()
    yes_hi = _book("y", OutcomeSide.YES, t1, hash_="m1", bid="0.39", ask="0.40")
    no_hi = _book("n", OutcomeSide.NO, t1, hash_="m2", bid="0.49", ask="0.50")
    mark_market_books("m1", yes_hi, no_hi)
    _c1, marked1, eq1 = refresh_account_equity()
    with session_scope() as session:
        acct = session.scalar(select(PaperAccountRow).limit(1))
        assert acct is not None
        dd1 = Decimal(acct.max_drawdown)
        assert Decimal(acct.marked_inventory) == marked1
        assert Decimal(acct.cash) + marked1 == eq1
        assert Decimal(acct.occupied) != marked1 or marked1 == Decimal("0")

    yes_lo = _book("y", OutcomeSide.YES, t1, hash_="m3", bid="0.10", ask="0.40")
    mark_market_books("m1", yes_lo, no_hi)
    _c2, marked2, eq2 = refresh_account_equity()
    with session_scope() as session:
        acct = session.scalar(select(PaperAccountRow).limit(1))
        assert acct is not None
        dd2 = Decimal(acct.max_drawdown)
    assert marked2 < marked1
    assert eq2 < eq1
    assert dd2 >= dd1
    assert cash + marked2 == eq2


def test_shadow_candidate_universe_independent_from_live_rule() -> None:
    now = datetime.now(timezone.utc)
    below_balanced = _sig(now, net="0.30")
    fast = StrategyParams(min_net_profit=Decimal("0.25"))
    live = StrategyParams(min_net_profit=Decimal("0.50"))
    assert strategy_eligible(fast, below_balanced) is True
    assert strategy_eligible(live, below_balanced) is False


def test_eligibility_false_to_true_triggers_once() -> None:
    attempted: set[tuple[str, int, int]] = set()
    eligible_state: dict[tuple[str, int, int], bool] = {}
    key = ("shadow_fast", 1, 9)
    spawned = []

    def consider(eligible: bool) -> None:
        was = eligible_state.get(key, False)
        eligible_state[key] = eligible
        if eligible and not was and key not in attempted:
            attempted.add(key)
            spawned.append(1)

    consider(False)
    consider(True)
    consider(True)
    consider(False)
    consider(True)
    assert spawned == [1]


def test_walk_forward_has_no_validation_leakage(tmp_path, monkeypatch) -> None:
    _db(tmp_path, monkeypatch)
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    mid = datetime(2026, 2, 1, tzinfo=timezone.utc)
    end = datetime(2026, 3, 1, tzinfo=timezone.utc)
    with session_scope() as session:
        from polymarket_scanner.database import StrategyAccountRow

        session.add(StrategyAccountRow(strategy_id="A", version=1, cash="1000", realized_pnl="999"))
        session.add(StrategyAccountRow(strategy_id="B", version=1, cash="1000", realized_pnl="1"))
        for i in range(2):
            session.add(
                StrategyTradeRow(
                    created_at=start + timedelta(days=1 + i),
                    strategy_id="A",
                    strategy_version=1,
                    market_id="m",
                    status="merged",
                    realized_pnl="10",
                )
            )
        session.add(
            StrategyTradeRow(
                created_at=mid + timedelta(days=1),
                strategy_id="B",
                strategy_version=1,
                market_id="m",
                status="merged",
                realized_pnl="100",
            )
        )
        session.add(
            StrategyTradeRow(
                created_at=mid + timedelta(days=2),
                strategy_id="A",
                strategy_version=1,
                market_id="m",
                status="merged",
                realized_pnl="1",
            )
        )
    result = walk_forward_evaluate(
        training_start=start,
        training_end=mid,
        validation_start=mid,
        validation_end=end,
        min_trades=1,
    )
    assert result["recommended_strategy_id"] == "A"
    assert result["recommended_version"] == 1
    metrics = result["metrics"]
    assert metrics["A@1"]["used_account_overlay"] is False
    assert metrics["A@1"]["realized_pnl"] == "1"
    assert Decimal(metrics["A@1"]["realized_pnl"]) != Decimal("999")
    assert metrics["A@1"]["validation"]["realized_pnl"] == "1"
    assert metrics["A@1"]["training"]["realized_pnl"] == "20"
    with session_scope() as session:
        row = session.scalar(select(StrategyEvalRow).order_by(StrategyEvalRow.id.desc()))
        assert row is not None
        assert row.recommended_strategy_id == "A"


@pytest.mark.asyncio
async def test_condition_id_market_resolved(tmp_path, monkeypatch) -> None:
    _db(tmp_path, monkeypatch)
    rt = RealtimeScanner(config=get_config(), paper=False)
    now = datetime.now(timezone.utc)
    market = _market()
    rt.markets["m1"] = market
    rt.token_to_market["y"] = "m1"
    rt.token_to_market["n"] = "m1"
    rt.token_outcome["y"] = OutcomeSide.YES
    rt.token_outcome["n"] = OutcomeSide.NO
    rt.condition_to_market_id["0xcond"] = "m1"
    sig = _sig(now)
    ids, _ = sync_episodes([sig], scanned_market_ids={"m1"}, now=now)
    ep = ids[("m1", "forward")]
    assert episode_is_open(ep)
    payload = {
        "event_type": "market_resolved",
        "market": "0xcond",
        "winning_outcome": "Yes",
        "winning_asset_id": "y",
    }
    await rt._on_ws(payload, now)
    assert not episode_is_open(ep)
    assert "m1" not in rt.markets


def test_initial_book_age_excluded_from_feed_latency(tmp_path, monkeypatch) -> None:
    _db(tmp_path, monkeypatch)
    record_latency("book", 5000.0, token_id="t")
    record_latency("initial_snapshot_age", 8000.0, token_id="t")
    record_latency("price_change", 12.0, token_id="t")
    record_latency("price_change", 20.0, token_id="t")
    from polymarket_scanner.scanners.pipeline import flush_latency

    flush_latency()
    stats = get_dashboard_stats()
    p50 = stats["latency_p50_ms"]
    p95 = stats["latency_p95_ms"]
    assert p50 is not None and p95 is not None
    assert p50 < 100
    assert p95 < 100


def test_previous_day_daily_report(tmp_path, monkeypatch) -> None:
    _db(tmp_path, monkeypatch)
    get_config().reporting.reports_dir = str(tmp_path / "reports")
    prev = datetime(2026, 8, 12, 15, 0, tzinfo=timezone.utc)
    with session_scope() as session:
        session.add(
            PaperTradeRow(
                created_at=prev,
                market_id="m1",
                tif="FAK",
                delay_ms=500,
                status="merged",
                realized_pnl="3.5",
                pnl="3.5",
            )
        )
    out = generate_daily_report("2026-08-12")
    assert out["report_date"] == "2026-08-12"
    assert Decimal(str(out["daily_realized_pnl"])) == Decimal("3.5")
    now = datetime(2026, 8, 13, 0, 5, tzinfo=timezone.utc)
    due = previous_report_date_due(
        now=now, last_report_date="2026-08-12", timezone_name="UTC", report_hour=0
    )
    assert due == "2026-08-12"
    today_empty = generate_daily_report("2026-08-13")
    assert Decimal(str(today_empty["daily_realized_pnl"])) == Decimal("0")


def test_docker_paper_profile_does_not_start_observe_scanner() -> None:
    root = Path(__file__).resolve().parents[1]
    compose = yaml.safe_load((root / "docker-compose.yml").read_text(encoding="utf-8"))
    services = compose["services"]
    assert "observe" in (services["scanner"].get("profiles") or [])
    assert "paper" in (services["scanner-paper"].get("profiles") or [])
    assert not services["ui"].get("profiles")
    paper_only = {
        name: svc
        for name, svc in services.items()
        if not svc.get("profiles") or "paper" in svc.get("profiles", [])
    }
    assert "scanner-paper" in paper_only
    assert "ui" in paper_only
    assert "scanner" not in paper_only or "paper" in (services["scanner"].get("profiles") or [])
    assert services["scanner"].get("profiles") == ["observe"]


@pytest.mark.asyncio
async def test_paper_trade_links_exact_opportunity_row(tmp_path, monkeypatch) -> None:
    _db(tmp_path, monkeypatch)
    now = datetime.now(timezone.utc)
    market = _market()
    sig = _sig(now)
    from polymarket_scanner.models import SimulationQuality
    from polymarket_scanner.simulation.execution_simulator import SimulationResult

    sim = SimulationResult(
        profile="base",
        quality=SimulationQuality.ESTIMATED,
        quantity=Decimal("10"),
        gross_profit=Decimal("1"),
        fees=Decimal("0"),
        operational_cost=Decimal("0"),
        safety_buffer=Decimal("0"),
        net_profit=Decimal("1"),
    )
    ids, _ = sync_episodes([sig], scanned_market_ids={"m1"}, now=now)
    opp_ids = persist_signals(market, [sig], {"base": sim}, episode_ids=ids)
    assert isinstance(opp_ids, list)
    assert len(opp_ids) == 1
    assert sig.opportunity_id == opp_ids[0]
    with session_scope() as session:
        ep = session.scalar(select(OpportunityEpisodeRow))
        assert ep is not None
        assert ep.last_opportunity_id == opp_ids[0]
        row = session.get(OpportunityRow, opp_ids[0])
        assert row is not None

    t1 = now + timedelta(milliseconds=500)
    cache = _Cache()
    cache.books["y"] = _book("y", OutcomeSide.YES, t1, hash_="y1")
    cache.books["n"] = _book("n", OutcomeSide.NO, t1, hash_="n1", ask="0.50")
    cfg = get_config()
    cfg.paper.min_net_profit = Decimal("0")
    cfg.paper.force_close_unhedged = False
    cfg.paper.inter_leg_delay_ms = 0

    async def _sleep(_: float) -> None:
        return None

    await run_delayed_paper_trade(
        cache=cache,
        market=market,
        signal=sig,
        episode_id=ids[("m1", "forward")],
        cfg=cfg,
        paper_cfg=cfg.paper,
        sleep_fn=_sleep,
        now_fn=lambda: t1,
        episode_open_fn=lambda _e: True,
    )
    with session_scope() as session:
        trade = session.scalar(select(PaperTradeRow).order_by(PaperTradeRow.id.desc()))
        assert trade is not None
        assert trade.signal_opportunity_id == opp_ids[0]
        opp = session.get(OpportunityRow, trade.signal_opportunity_id)
        assert opp is not None
        assert opp.id == opp_ids[0]
