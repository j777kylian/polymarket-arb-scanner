from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from decimal import Decimal

from polymarket_scanner.config import get_config
from polymarket_scanner.database import (
    MarketRow,
    PaperAccountRow,
    PaperTradeRow,
    ScannerRunRow,
    init_db,
    session_scope,
    utcnow,
)
from polymarket_scanner.reporting.html_report import generate_daily_report
from polymarket_scanner.scheduler import get_dashboard_stats


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


def test_daily_pnl_not_equal_cumulative(tmp_path, monkeypatch) -> None:
    _db(tmp_path, monkeypatch)
    today = utcnow().replace(hour=12, minute=0, second=0, microsecond=0)
    yesterday = today - timedelta(days=1)
    with session_scope() as session:
        from sqlalchemy import select

        acc = session.scalar(select(PaperAccountRow).limit(1))
        assert acc is not None
        acc.realized_pnl = "100"
        acc.cash = "1100"
        acc.occupied = "0"
        acc.marked_inventory = "0"
        session.add(
            PaperTradeRow(
                created_at=yesterday,
                market_id="old",
                tif="FAK",
                status="merged",
                pnl="80",
                realized_pnl="80",
                realized_at=yesterday,
            )
        )
        session.add(
            PaperTradeRow(
                created_at=today,
                market_id="new",
                tif="FAK",
                status="merged",
                pnl="20",
                realized_pnl="20",
                realized_at=today,
            )
        )
    report_date = today.date().isoformat()
    out = generate_daily_report(report_date)
    html = (tmp_path / "reports" / f"daily_{report_date}.html").read_text(encoding="utf-8")
    assert "20" in html
    with session_scope() as session:
        from sqlalchemy import select

        from polymarket_scanner.database import DailyReportRow

        row = session.scalar(select(DailyReportRow).where(DailyReportRow.report_date == report_date))
        assert row is not None
        assert row.total_sim_profit == "20.000" or Decimal(row.total_sim_profit) == Decimal("20")
        summary = json.loads(row.summary_json or "{}")
        assert Decimal(summary["daily_realized_pnl"]) == Decimal("20")
        assert Decimal(summary["cumulative_realized_pnl"]) == Decimal("100")
        assert summary["daily_realized_pnl"] != summary["cumulative_realized_pnl"]
    assert out["report_date"] == report_date


def test_realtime_market_count_uses_subscribed_not_all_db(tmp_path, monkeypatch) -> None:
    _db(tmp_path, monkeypatch)
    now = datetime.now(timezone.utc)
    with session_scope() as session:
        for i in range(10):
            session.add(
                MarketRow(
                    market_id=f"m{i}",
                    condition_id=f"c{i}",
                    active=True,
                    closed=False,
                    accepting_orders=True,
                    enable_order_book=True,
                )
            )
        session.add(
            ScannerRunRow(
                started_at=now,
                status="running",
                mode="live",
                markets_synced=50,
                subscribed_markets=50,
                subscribed_tokens=100,
                discovered_markets=200,
            )
        )
    stats = get_dashboard_stats()
    assert stats["markets"] == 50
    report_date = now.date().isoformat()
    generate_daily_report(report_date)
    with session_scope() as session:
        from sqlalchemy import select

        from polymarket_scanner.database import DailyReportRow

        row = session.scalar(select(DailyReportRow).where(DailyReportRow.report_date == report_date))
        assert row is not None
        assert row.markets_scanned == 50
