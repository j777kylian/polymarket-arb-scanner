"""Scanner orchestration with single-instance lock."""

from __future__ import annotations

import asyncio
import os
import time
from decimal import Decimal
from typing import Any

from filelock import FileLock, Timeout
from sqlalchemy import desc, func, select

from polymarket_scanner.config import get_config
from polymarket_scanner.database import (
    LatencySampleRow,
    OpportunityEpisodeRow,
    OpportunityRow,
    PaperAccountRow,
    PaperTradeRow,
    ScannerRunRow,
    ensure_utc,
    init_db,
    session_scope,
    utcnow,
)
from polymarket_scanner.discovery.market_discovery import (
    discover_and_store_markets,
    load_markets_from_db,
)
from polymarket_scanner.discovery.orderbook_collector import collect_orderbooks
from polymarket_scanner.logging_config import get_logger, setup_logging
from polymarket_scanner.runtime_settings import apply_runtime_to_config, set_scanner_process_status
from polymarket_scanner.safety import assert_trading_disabled
from polymarket_scanner.scanners.binary_complete_set import scan_binary_market
from polymarket_scanner.scanners.opportunity_tracker import sync_episodes
from polymarket_scanner.scanners.pipeline import persist_signals
from polymarket_scanner.simulation.execution_simulator import simulate_all_profiles

logger = get_logger(__name__)


class ScannerService:
    def __init__(self) -> None:
        self.cfg = get_config()
        self._paused = False
        self._running = False
        lock_path = self.cfg.resolve_path(self.cfg.scanner.lock_file)
        lock_path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = FileLock(str(lock_path), timeout=0)

    @property
    def is_running(self) -> bool:
        return self._running and not self._paused

    def pause(self) -> None:
        self._paused = True

    def resume(self) -> None:
        self._paused = False

    async def run_once(
        self,
        *,
        max_market_pages: int | None = None,
        market_limit: int | None = None,
        sync_markets: bool = True,
    ) -> dict[str, Any]:
        assert_trading_disabled()
        init_db()
        started = utcnow()
        with session_scope() as session:
            run = ScannerRunRow(started_at=started, status="running")
            session.add(run)
            session.flush()
            run_id = run.id

        markets_synced = 0
        books_fetched = 0
        signals_found = 0
        api_errors = 0

        try:
            if sync_markets:
                markets = await discover_and_store_markets(max_pages=max_market_pages)
            else:
                markets = load_markets_from_db(tradable_only=True)
            markets = [m for m in markets if m.yes_token_id and m.no_token_id]
            if market_limit is not None:
                markets = markets[:market_limit]
            markets_synced = len(markets)

            books = await collect_orderbooks(markets)
            books_fetched = sum(
                1 for y, n in books.values() if y is not None and n is not None
            )

            for market in markets:
                yes_book, no_book = books.get(market.market_id, (None, None))
                if yes_book is None or no_book is None:
                    api_errors += 1
                    continue
                signals = scan_binary_market(market, yes_book, no_book)
                sims = simulate_all_profiles(market, yes_book, no_book)
                episode_ids, _st = sync_episodes(
                    signals, scanned_market_ids={market.market_id}
                )
                n = persist_signals(market, signals, sims, episode_ids=episode_ids)
                signals_found += n

            status = "ok"
        except Exception as exc:
            logger.exception("Scanner run failed: %s", exc)
            status = f"error: {exc}"
            api_errors += 1

        with session_scope() as session:
            run = session.get(ScannerRunRow, run_id)
            if run:
                run.finished_at = utcnow()
                run.status = status
                run.markets_synced = markets_synced
                run.books_fetched = books_fetched
                run.signals_found = signals_found
                run.api_errors = api_errors

        summary = {
            "run_id": run_id,
            "status": status,
            "markets_synced": markets_synced,
            "books_fetched": books_fetched,
            "signals_found": signals_found,
            "api_errors": api_errors,
        }
        logger.info("Scanner run complete: %s", summary)
        return summary

    def _refresh_cfg(self) -> None:
        self.cfg = apply_runtime_to_config(self.cfg)

    async def run_daemon(self, *, mode: str | None = None, paper: bool = False) -> None:
        assert_trading_disabled()
        setup_logging()
        init_db()
        self._refresh_cfg()
        mode = (mode or self.cfg.scanner.mode or "static").lower()
        try:
            self._lock.acquire(timeout=0)
        except Timeout:
            logger.error("Another scanner instance holds the lock; exiting")
            return

        self._running = True
        self._paused = False
        set_scanner_process_status(
            mode=mode,
            paper=paper,
            pid=os.getpid(),
            started_at=utcnow().isoformat(),
        )
        logger.info("Scanner daemon started mode=%s paper=%s (read-only)", mode, paper)
        try:
            if mode == "realtime":
                from polymarket_scanner.realtime import RealtimeScanner
                from polymarket_scanner.reporting.html_report import generate_daily_report

                rt = RealtimeScanner(paper=paper)
                last_report_date = ""

                async def report_loop() -> None:
                    nonlocal last_report_date
                    while self._running:
                        if self.cfg.scanner.auto_daily_report:
                            today = utcnow().date().isoformat()
                            hour = utcnow().hour
                            if today != last_report_date and hour >= self.cfg.reporting.report_hour_utc:
                                try:
                                    generate_daily_report(today)
                                    last_report_date = today
                                except Exception:
                                    logger.exception("Auto daily report failed")
                        await asyncio.sleep(60)

                report_task = asyncio.create_task(report_loop())
                try:
                    await rt.run()
                finally:
                    report_task.cancel()
                return

            last_market_sync = 0.0
            last_report_date = ""
            while self._running:
                if self._paused:
                    await asyncio.sleep(1)
                    continue
                self._refresh_cfg()
                now = time.time()
                sync = (now - last_market_sync) >= self.cfg.scanner.market_sync_interval_seconds
                try:
                    await self.run_once(sync_markets=sync, market_limit=None)
                    if sync:
                        last_market_sync = now
                    if self.cfg.scanner.auto_daily_report:
                        today = utcnow().date().isoformat()
                        if today != last_report_date and utcnow().hour >= self.cfg.reporting.report_hour_utc:
                            from polymarket_scanner.reporting.html_report import generate_daily_report

                            generate_daily_report(today)
                            last_report_date = today
                except Exception as exc:
                    logger.exception("Daemon iteration failed: %s", exc)
                await asyncio.sleep(self.cfg.scanner.orderbook_poll_interval_seconds)
        finally:
            self._running = False
            set_scanner_process_status(mode=None)
            try:
                self._lock.release()
            except Exception:
                pass


def get_dashboard_stats() -> dict[str, Any]:
    from polymarket_scanner.database import ApiErrorRow, MarketRow, OrderBookSnapshotRow

    with session_scope() as session:
        markets = session.scalar(
            select(func.count()).select_from(MarketRow).where(
                MarketRow.active.is_(True),
                MarketRow.closed.is_(False),
                MarketRow.accepting_orders.is_(True),
                MarketRow.enable_order_book.is_(True),
            )
        ) or 0
        last_run = session.scalar(
            select(ScannerRunRow).order_by(desc(ScannerRunRow.started_at)).limit(1)
        )
        today = utcnow().date().isoformat()
        todays = session.scalar(
            select(func.count()).select_from(OpportunityRow).where(
                OpportunityRow.discovered_at >= utcnow().replace(
                    hour=0, minute=0, second=0, microsecond=0
                )
            )
        ) or 0
        active_ops = session.scalar(
            select(func.count()).select_from(OpportunityRow).where(
                OpportunityRow.net_profitable.is_(True),
                OpportunityRow.stale.is_(False),
            )
        ) or 0
        last_book = session.scalar(
            select(OrderBookSnapshotRow)
            .order_by(desc(OrderBookSnapshotRow.fetched_at))
            .limit(1)
        )
        api_errors_today = session.scalar(
            select(func.count()).select_from(ApiErrorRow).where(
                ApiErrorRow.created_at >= utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
            )
        ) or 0

        def sum_net(col) -> float:
            rows = session.scalars(
                select(col).where(
                    OpportunityRow.discovered_at
                    >= utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
                )
            ).all()
            total = Decimal("0")
            for v in rows:
                if v:
                    total += Decimal(v)
            return float(total)

        open_eps = session.scalar(
            select(func.count()).select_from(OpportunityEpisodeRow).where(
                OpportunityEpisodeRow.is_open.is_(True)
            )
        ) or 0
        lat_rows = session.scalars(
            select(LatencySampleRow.latency_ms)
            .order_by(desc(LatencySampleRow.created_at))
            .limit(500)
        ).all()
        lat_sorted = sorted(float(x) for x in lat_rows)
        p50 = p95 = None
        sufficient = None
        if lat_sorted:
            p50 = lat_sorted[int(0.50 * (len(lat_sorted) - 1))]
            p95 = lat_sorted[int(0.95 * (len(lat_sorted) - 1))]
            cfg = get_config()
            sufficient = (
                p50 <= cfg.scanner.latency_sufficient_p50_ms
                and p95 <= cfg.scanner.latency_sufficient_p95_ms
            )
        paper = session.scalar(select(PaperAccountRow).limit(1))
        paper_trades = session.scalar(select(func.count()).select_from(PaperTradeRow)) or 0

        return {
            "markets": markets,
            "last_run_started_at": ensure_utc(last_run.started_at) if last_run else None,
            "last_run_status": last_run.status if last_run else None,
            "signals_today": todays,
            "active_opportunities": active_ops,
            "open_episodes": open_eps,
            "last_book_at": ensure_utc(last_book.fetched_at) if last_book else None,
            "api_errors_today": api_errors_today,
            "optimistic_profit_today": sum_net(OpportunityRow.optimistic_net),
            "base_profit_today": sum_net(OpportunityRow.base_net),
            "pessimistic_profit_today": sum_net(OpportunityRow.pessimistic_net),
            "latency_p50_ms": p50,
            "latency_p95_ms": p95,
            "latency_sufficient": sufficient,
            "paper_cash": paper.cash if paper else None,
            "paper_pnl": paper.realized_pnl if paper else None,
            "paper_trades": paper_trades,
            "date": today,
        }
