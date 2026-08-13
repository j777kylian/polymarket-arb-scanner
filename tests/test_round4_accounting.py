"""Round-4 paper accounting: settlement, isolation, delayed legs, reports."""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest
from sqlalchemy import select

from polymarket_scanner.config import get_config
from polymarket_scanner.database import (
    ApiErrorRow,
    DailyReportRow,
    OpportunityEpisodeRow,
    OpportunityRow,
    PaperTradeRow,
    PositionRow,
    StrategyPositionRow,
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
from polymarket_scanner.reporting.html_report import generate_daily_report
from polymarket_scanner.simulation.inventory import (
    mark_market_books,
    open_or_increase_position,
    refresh_account_equity,
    resolve_winning_side,
    set_positions_status,
    settle_market_resolved,
)
from polymarket_scanner.simulation.paper_trader import (
    LEG_MERGED,
    get_paper_account,
    run_delayed_paper_trade,
)


def _db(tmp_path, monkeypatch) -> None:
    db = tmp_path / "t.db"
    monkeypatch.setenv("DATABASE_URL", f"sqlite:///{db}")
    get_config.cache_clear()
    from polymarket_scanner import database as dbmod

    dbmod._engine = None
    dbmod._SessionLocal = None
    init_db(f"sqlite:///{db}")
    cfg = get_config()
    cfg.reporting.reports_dir = str(tmp_path / "reports")


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


class Cache:
    generation = 1

    def __init__(self) -> None:
        self.books: dict[str, OrderBookSnapshot] = {}

    def get(self, token_id: str) -> OrderBookSnapshot | None:
        return self.books.get(token_id)

    def pair_ready(self, yes_token: str, no_token: str) -> bool:
        return yes_token in self.books and no_token in self.books


def test_winner_residual_settlement_credits_cash_and_realized(tmp_path, monkeypatch) -> None:
    _db(tmp_path, monkeypatch)
    qty = Decimal("10")
    cost = Decimal("4")
    open_or_increase_position(
        market_id="m1",
        token_id="y",
        outcome="YES",
        quantity=qty,
        cost_basis=cost,
        mark_price=Decimal("0.39"),
        status="RESIDUAL_OPEN",
    )
    cash_before, _occ, _pnl = get_paper_account()
    result = settle_market_resolved(
        "m1",
        winning_outcome="YES",
        yes_token_id="y",
        no_token_id="n",
    )
    assert result["settled"] is True
    cash_after, occ, pnl = get_paper_account()
    assert cash_after == cash_before + qty
    assert pnl == qty - cost
    assert occ == Decimal("0")
    with session_scope() as session:
        pos = session.scalars(select(PositionRow)).all()
        assert len(pos) == 1
        assert pos[0].status == "settled"
        assert Decimal(pos[0].quantity) == 0


def test_loser_settlement_zero_payout_releases_cost(tmp_path, monkeypatch) -> None:
    _db(tmp_path, monkeypatch)
    qty = Decimal("10")
    cost = Decimal("4")
    open_or_increase_position(
        market_id="m1",
        token_id="y",
        outcome="YES",
        quantity=qty,
        cost_basis=cost,
        mark_price=Decimal("0.39"),
        status="RESIDUAL_OPEN",
    )
    refresh_account_equity()
    cash_before, occ_before, pnl_before = get_paper_account()
    result = settle_market_resolved("m1", winning_outcome="NO", yes_token_id="y", no_token_id="n")
    assert result["settled"] is True
    cash_after, occ_after, pnl_after = get_paper_account()
    assert cash_after == cash_before
    assert occ_after == Decimal("0")
    assert occ_before > Decimal("0")
    assert pnl_after == pnl_before - cost


def test_settled_marked_value_does_not_vanish_from_equity(tmp_path, monkeypatch) -> None:
    _db(tmp_path, monkeypatch)
    qty = Decimal("10")
    cost = Decimal("4")
    bid = Decimal("0.39")
    open_or_increase_position(
        market_id="m1",
        token_id="y",
        outcome="YES",
        quantity=qty,
        cost_basis=cost,
        mark_price=bid,
        status="RESIDUAL_OPEN",
    )
    cash, marked, equity_before = refresh_account_equity()
    assert equity_before == cash + marked
    settle_market_resolved("m1", winning_outcome="YES", yes_token_id="y", no_token_id="n")
    cash2, marked2, equity_after = refresh_account_equity()
    assert marked2 == Decimal("0")
    assert equity_after == cash2
    assert equity_after >= equity_before


def test_same_trade_id_shadow_does_not_cross_mutate_live(tmp_path, monkeypatch) -> None:
    _db(tmp_path, monkeypatch)
    trade_id = 42
    with session_scope() as session:
        session.add(
            PaperTradeRow(
                id=trade_id,
                market_id="m1",
                tif="FAK",
                delay_ms=500,
                status="one_leg",
            )
        )
        session.add(
            StrategyTradeRow(
                id=trade_id,
                strategy_id="shadow_fast",
                strategy_version=1,
                market_id="m1",
                status="one_leg",
            )
        )
        session.add(
            PositionRow(
                market_id="m1",
                token_id="y",
                outcome="YES",
                quantity="5",
                cost_basis="2",
                status="open",
                trade_id=trade_id,
            )
        )
        session.add(
            StrategyPositionRow(
                strategy_id="shadow_fast",
                strategy_version=1,
                market_id="m1",
                token_id="y",
                outcome="YES",
                quantity="5",
                cost_basis="2",
                status="open",
                trade_id=trade_id,
            )
        )
    set_positions_status("m1", "RESIDUAL_OPEN", trade_id=trade_id, account_kind="live")
    with session_scope() as session:
        live = session.scalar(select(PositionRow))
        strat = session.scalar(select(StrategyPositionRow))
        assert live is not None and live.status == "RESIDUAL_OPEN"
        assert strat is not None and strat.status == "open"
    set_positions_status(
        "m1",
        "CLOSE_PENDING",
        trade_id=trade_id,
        account_kind="strategy",
        strategy_id="shadow_fast",
        strategy_version=1,
    )
    with session_scope() as session:
        live = session.scalar(select(PositionRow))
        strat = session.scalar(select(StrategyPositionRow))
        assert live is not None and live.status == "RESIDUAL_OPEN"
        assert strat is not None and strat.status == "CLOSE_PENDING"


def test_one_shadow_does_not_mutate_another(tmp_path, monkeypatch) -> None:
    _db(tmp_path, monkeypatch)
    with session_scope() as session:
        for sid in ("shadow_a", "shadow_b"):
            session.add(
                StrategyPositionRow(
                    strategy_id=sid,
                    strategy_version=1,
                    market_id="m1",
                    token_id="y",
                    outcome="YES",
                    quantity="3",
                    cost_basis="1",
                    status="open",
                )
            )
    set_positions_status(
        "m1",
        "RESIDUAL_OPEN",
        account_kind="strategy",
        strategy_id="shadow_a",
        strategy_version=1,
    )
    with session_scope() as session:
        rows = {
            r.strategy_id: r.status
            for r in session.scalars(select(StrategyPositionRow)).all()
        }
    assert rows["shadow_a"] == "RESIDUAL_OPEN"
    assert rows["shadow_b"] == "open"


@pytest.mark.asyncio
async def test_episode_closed_after_first_second_leg_still_hedges(tmp_path, monkeypatch) -> None:
    _db(tmp_path, monkeypatch)
    t0 = datetime(2026, 8, 13, 12, 0, tzinfo=timezone.utc)
    t1 = t0 + timedelta(milliseconds=500)
    t2 = t1 + timedelta(milliseconds=100)
    cache = Cache()
    cache.books["y"] = _book("y", OutcomeSide.YES, t1, hash_="y1", ask="0.40")
    cache.books["n"] = _book("n", OutcomeSide.NO, t1, hash_="n1", ask="0.50")
    cfg = get_config()
    cfg.paper.min_net_profit = Decimal("0")
    cfg.paper.force_close_unhedged = False
    cfg.paper.inter_leg_delay_ms = 100
    calls = {"n": 0}
    sleeps = {"n": 0}

    def episode_open_fn(_eid: int | None) -> bool:
        calls["n"] += 1
        return calls["n"] == 1

    async def _sleep(_: float) -> None:
        sleeps["n"] += 1
        if sleeps["n"] == 2:
            cache.books["y"] = _book("y", OutcomeSide.YES, t2, hash_="y2", ask="0.40")
            cache.books["n"] = _book("n", OutcomeSide.NO, t2, hash_="n2", ask="0.50")

    result = await run_delayed_paper_trade(
        cache=cache,
        market=_market(),
        signal=_sig(t0),
        episode_id=7,
        cfg=cfg,
        paper_cfg=cfg.paper,
        sleep_fn=_sleep,
        now_fn=lambda: t2,
        episode_open_fn=episode_open_fn,
    )
    assert result is not None
    assert result["status"] == "merged"
    assert result.get("reject_reason") != "episode_closed"
    assert result.get("leg_state") == LEG_MERGED
    assert calls["n"] >= 1


@pytest.mark.asyncio
async def test_second_leg_fail_enters_emergency_close(tmp_path, monkeypatch) -> None:
    _db(tmp_path, monkeypatch)
    t0 = datetime(2026, 8, 13, 12, 0, tzinfo=timezone.utc)
    t1 = t0 + timedelta(milliseconds=500)
    t_close = t1 + timedelta(milliseconds=250)
    cache = Cache()
    cache.books["y"] = _book("y", OutcomeSide.YES, t1, hash_="y1", ask="0.40", bid="0.38")
    cache.books["n"] = _book("n", OutcomeSide.NO, t1, hash_="n1", ask="0.50")
    cfg = get_config()
    cfg.paper.min_net_profit = Decimal("0")
    cfg.paper.force_close_unhedged = True
    cfg.paper.inter_leg_delay_ms = 100
    cfg.paper.force_close_delay_ms = 50
    sleeps = {"n": 0}

    async def _sleep(_: float) -> None:
        sleeps["n"] += 1
        if sleeps["n"] == 2:
            cache.books.clear()
        if sleeps["n"] == 3:
            cache.books["y"] = _book(
                "y", OutcomeSide.YES, t_close, hash_="y2", ask="0.40", bid="0.38"
            )
            cache.books["n"] = _book(
                "n", OutcomeSide.NO, t_close, hash_="n2", ask="0.50", bid="0.49"
            )

    result = await run_delayed_paper_trade(
        cache=cache,
        market=_market(),
        signal=_sig(t0),
        episode_id=8,
        cfg=cfg,
        paper_cfg=cfg.paper,
        sleep_fn=_sleep,
        now_fn=lambda: t_close,
        episode_open_fn=lambda _e: True,
    )
    assert result is not None
    assert sleeps["n"] >= 3
    assert result["status"] in {"merged", "closed", "one_leg_merged", "one_leg"}
    assert result.get("reject_reason") != "books_not_ready" or result["status"] != "one_leg"


def test_winning_token_id_payload_settles(tmp_path, monkeypatch) -> None:
    _db(tmp_path, monkeypatch)
    open_or_increase_position(
        market_id="m1",
        token_id="y",
        outcome="YES",
        quantity=Decimal("5"),
        cost_basis=Decimal("2"),
        mark_price=Decimal("0.39"),
        status="RESIDUAL_OPEN",
    )
    token, side = resolve_winning_side(
        payload={"winningTokenId": "y"},
        yes_token_id="y",
        no_token_id="n",
    )
    assert token == "y"
    assert side == "YES"
    result = settle_market_resolved(
        "m1",
        yes_token_id="y",
        no_token_id="n",
        payload={"winningTokenId": "y"},
    )
    assert result["settled"] is True
    cash, _occ, pnl = get_paper_account()
    assert cash == Decimal("1005")
    assert pnl == Decimal("3")


def test_no_winner_does_not_settle(tmp_path, monkeypatch) -> None:
    _db(tmp_path, monkeypatch)
    open_or_increase_position(
        market_id="m1",
        token_id="y",
        outcome="YES",
        quantity=Decimal("5"),
        cost_basis=Decimal("2"),
        mark_price=Decimal("0.39"),
        status="RESIDUAL_OPEN",
    )
    result = settle_market_resolved("m1", yes_token_id="y", no_token_id="n", payload={})
    assert result["settled"] is False
    with session_scope() as session:
        pos = session.scalar(select(PositionRow))
        assert pos is not None
        assert Decimal(pos.quantity) == Decimal("5")
        assert pos.status == "RESIDUAL_OPEN"
        err = session.scalar(select(ApiErrorRow))
        assert err is not None
        assert "missing winner" in err.message


def test_html_unique_episodes_no_dupes_and_daily_by_realized_at(tmp_path, monkeypatch) -> None:
    _db(tmp_path, monkeypatch)
    day_a = datetime(2026, 8, 12, 10, 0, tzinfo=timezone.utc)
    day_b = datetime(2026, 8, 13, 10, 0, tzinfo=timezone.utc)
    with session_scope() as session:
        ep = OpportunityEpisodeRow(
            market_id="m1",
            condition_id="c",
            direction="forward",
            first_seen_at=day_a,
            last_seen_at=day_a,
            is_open=False,
        )
        session.add(ep)
        session.flush()
        for i in range(3):
            session.add(
                OpportunityRow(
                    market_id="m1",
                    condition_id="c",
                    direction="forward",
                    discovered_at=day_a + timedelta(minutes=i),
                    quantity="10",
                    yes_vwap="0.4",
                    no_vwap="0.5",
                    gross_profit="1",
                    fee_total="0",
                    net_profit=str(1 + i),
                    net_profit_per_share="0.1",
                    net_profit_rate="0.1",
                    episode_id=ep.id,
                    net_profitable=True,
                )
            )
        session.flush()
        last_opp = session.scalar(select(OpportunityRow).order_by(OpportunityRow.id.desc()))
        ep.last_opportunity_id = last_opp.id if last_opp else None
        session.add(
            PaperTradeRow(
                created_at=day_a,
                realized_at=day_b,
                market_id="m1",
                tif="FAK",
                delay_ms=500,
                status="merged",
                realized_pnl="7",
                pnl="7",
            )
        )
    out12 = generate_daily_report("2026-08-12")
    out13 = generate_daily_report("2026-08-13")
    html12 = (tmp_path / "reports" / "daily_2026-08-12.html").read_text(encoding="utf-8")
    html13 = (tmp_path / "reports" / "daily_2026-08-13.html").read_text(encoding="utf-8")
    assert Decimal(str(out12["daily_realized_pnl"])) == Decimal("0")
    assert Decimal(str(out13["daily_realized_pnl"])) == Decimal("7")
    assert "Unique episodes" in html12
    assert html12.count('<div class="n">1</div>') >= 1
    assert "Unique episodes" in html13
    assert html13.count("m1") >= 1


def test_report_empty_and_insufficient_sample(tmp_path, monkeypatch) -> None:
    _db(tmp_path, monkeypatch)
    out = generate_daily_report("2026-08-13")
    html = (tmp_path / "reports" / "daily_2026-08-13.html").read_text(encoding="utf-8")
    assert Decimal(str(out["daily_realized_pnl"])) == Decimal("0")
    assert "INSUFFICIENT SAMPLE" in html
    with session_scope() as session:
        row = session.scalar(
            select(DailyReportRow).where(DailyReportRow.report_date == "2026-08-13")
        )
        assert row is not None
        summary = json.loads(row.summary_json or "{}")
        assert summary.get("insufficient_sample") is True
        assert summary.get("unique_episodes") == 0


def test_mark_market_books_does_not_change_status(tmp_path, monkeypatch) -> None:
    _db(tmp_path, monkeypatch)
    t1 = datetime(2026, 8, 13, 12, 0, tzinfo=timezone.utc)
    open_or_increase_position(
        market_id="m1",
        token_id="y",
        outcome="YES",
        quantity=Decimal("5"),
        cost_basis=Decimal("2"),
        mark_price=Decimal("0.39"),
        status="RESIDUAL_OPEN",
    )
    yes = _book("y", OutcomeSide.YES, t1, hash_="h1", bid="0.55")
    no = _book("n", OutcomeSide.NO, t1, hash_="h2", bid="0.45")
    mark_market_books("m1", yes, no)
    with session_scope() as session:
        pos = session.scalar(select(PositionRow))
        assert pos is not None
        assert pos.status == "RESIDUAL_OPEN"
        assert Decimal(pos.marked_value) == Decimal("5") * Decimal("0.55")
