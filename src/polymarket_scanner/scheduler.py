"""Scanner orchestration with single-instance lock."""

from __future__ import annotations

import asyncio
import os
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

CLOCK_SKEW_WARNING = (
    "Host clock skew detected: feed event timestamps are ahead of local received_at. "
    "Signed WS latency is not trustworthy; VPS latency is not sufficient."
)


def latency_sufficiency_label(stats: dict[str, Any]) -> str:
    if stats.get("clock_skew_detected"):
        return "clock skew"
    suff = stats.get("latency_sufficient")
    if suff is True:
        return "sufficient"
    if suff is False:
        return "insufficient"
    return "no WS samples"


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
            run = ScannerRunRow(started_at=started, status="running", mode="snapshot")
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
                signals_found += len(n)

            status = "ok"
        except Exception as exc:
            logger.exception("Scanner run failed: %s", exc)
            status = f"error: {exc}"
            api_errors += 1

        with session_scope() as session:
            finished = session.get(ScannerRunRow, run_id)
            if finished is not None:
                finished.finished_at = utcnow()
                finished.status = status
                finished.mode = "snapshot"
                finished.markets_synced = markets_synced
                finished.discovered_markets = markets_synced
                finished.subscribed_markets = markets_synced
                finished.books_fetched = books_fetched
                finished.signals_found = signals_found
                finished.api_errors = api_errors

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

    async def run_daemon(
        self,
        *,
        mode: str | None = None,
        paper: bool = False,
        max_market_pages: int | None = None,
        market_limit: int | None = None,
        sync_markets: bool | None = None,
        duration_seconds: float | None = None,
    ) -> None:
        assert_trading_disabled()
        setup_logging()
        init_db()
        self._refresh_cfg()
        if max_market_pages is not None:
            self.cfg.scanner.max_pages = max_market_pages
        if market_limit is not None:
            self.cfg.scanner.market_limit = market_limit
        if sync_markets is not None:
            self.cfg.scanner.sync_markets = sync_markets
        from polymarket_scanner.config import normalize_scanner_mode

        mode = normalize_scanner_mode(mode or self.cfg.scanner.mode or "live")
        if mode == "snapshot":
            logger.error(
                "Snapshot Audit is --once only. Use --daemon for Live Research (WebSocket). "
                "Static REST polling daemon has been removed."
            )
            return

        try:
            self._lock.acquire(timeout=0)
        except Timeout:
            logger.error("Another scanner instance holds the lock; exiting")
            return

        self._running = True
        self._paused = False
        set_scanner_process_status(
            mode="live",
            paper=paper,
            pid=os.getpid(),
            started_at=utcnow().isoformat(),
        )
        logger.info("Live Research started paper=%s (read-only)", paper)
        from zoneinfo import ZoneInfo

        from polymarket_scanner.realtime import RealtimeScanner
        from polymarket_scanner.reporting.html_report import (
            generate_daily_report,
            previous_report_date_due,
        )

        rt = RealtimeScanner(config=self.cfg, paper=paper)
        try:
            report_tz = ZoneInfo(self.cfg.reporting.timezone or "UTC")
        except Exception:
            report_tz = ZoneInfo("UTC")
        last_report_date = utcnow().astimezone(report_tz).date().isoformat()

        async def report_loop() -> None:
            nonlocal last_report_date
            while self._running:
                if self.cfg.scanner.auto_daily_report:
                    due = previous_report_date_due(
                        now=utcnow(),
                        last_report_date=last_report_date,
                        timezone_name=self.cfg.reporting.timezone,
                        report_hour=self.cfg.reporting.report_hour_utc,
                    )
                    if due:
                        try:
                            generate_daily_report(due)
                            last_report_date = utcnow().astimezone(report_tz).date().isoformat()
                        except Exception:
                            logger.exception("Auto daily report failed")
                await asyncio.sleep(60)

        report_task = asyncio.create_task(report_loop())
        try:
            if duration_seconds is None:
                await rt.run()
            else:
                run_task = asyncio.create_task(rt.run())
                try:
                    # Shield so timeout cancels only the waiter; rt.run() exits via _running
                    # and existing finally blocks still record stopped status / stop WS / paper / latency.
                    await asyncio.wait_for(asyncio.shield(run_task), timeout=duration_seconds)
                except TimeoutError:
                    logger.info(
                        "Live Research duration elapsed (%s seconds); stopping normally",
                        duration_seconds,
                    )
                    rt._running = False
                    await run_task
        finally:
            report_task.cancel()
            try:
                await report_task
            except (asyncio.CancelledError, Exception):
                pass
            if self.cfg.scanner.auto_daily_report:
                try:
                    generate_daily_report()
                except Exception:
                    logger.exception("Stop-time daily report failed")
            self._running = False
            set_scanner_process_status(mode=None)
            try:
                self._lock.release()
            except Exception:
                pass


def get_dashboard_stats() -> dict[str, Any]:
    from polymarket_scanner.database import ApiErrorRow, MarketRow, OrderBookSnapshotRow

    with session_scope() as session:
        last_run = session.scalar(
            select(ScannerRunRow).order_by(desc(ScannerRunRow.started_at)).limit(1)
        )
        markets = session.scalar(
            select(func.count()).select_from(MarketRow).where(
                MarketRow.active.is_(True),
                MarketRow.closed.is_(False),
                MarketRow.accepting_orders.is_(True),
                MarketRow.enable_order_book.is_(True),
            )
        ) or 0
        if last_run is not None and last_run.mode in {"live", "realtime"}:
            if last_run.subscribed_markets:
                markets = last_run.subscribed_markets
            elif last_run.markets_synced:
                markets = last_run.markets_synced
        today = utcnow().date().isoformat()
        todays = session.scalar(
            select(func.count()).select_from(OpportunityRow).where(
                OpportunityRow.discovered_at >= utcnow().replace(
                    hour=0, minute=0, second=0, microsecond=0
                )
            )
        ) or 0
        active_ops = session.scalar(
            select(func.count()).select_from(OpportunityEpisodeRow).where(
                OpportunityEpisodeRow.is_open.is_(True)
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

        open_eps = session.scalar(
            select(func.count()).select_from(OpportunityEpisodeRow).where(
                OpportunityEpisodeRow.is_open.is_(True)
            )
        ) or 0
        lat_rows = session.scalars(
            select(LatencySampleRow.latency_ms)
            .where(LatencySampleRow.event_type.in_(("price_change", "last_trade_price")))
            .order_by(desc(LatencySampleRow.created_at))
            .limit(500)
        ).all()
        lat_sorted = sorted(float(x) for x in lat_rows)
        p50 = p95 = None
        sufficient = None
        clock_skew_detected = False
        if lat_sorted:
            p50 = lat_sorted[int(0.50 * (len(lat_sorted) - 1))]
            p95 = lat_sorted[int(0.95 * (len(lat_sorted) - 1))]
            cfg = get_config()
            clock_skew_detected = any(
                sample < -cfg.scanner.observed_delay_tolerance_ms for sample in lat_sorted
            )
            sufficient = (
                not clock_skew_detected
                and p50 <= cfg.scanner.latency_sufficient_p50_ms
                and p95 <= cfg.scanner.latency_sufficient_p95_ms
            )
        qualified_today = session.scalar(
            select(func.count()).select_from(OpportunityRow).where(
                OpportunityRow.discovered_at
                >= utcnow().replace(hour=0, minute=0, second=0, microsecond=0),
                OpportunityRow.passes_rule_set.is_(True),
                OpportunityRow.net_profitable.is_(True),
            )
        ) or 0
        episode_first_today = session.scalar(
            select(func.count()).select_from(OpportunityEpisodeRow).where(
                OpportunityEpisodeRow.first_seen_at
                >= utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
            )
        ) or 0

        paper = session.scalar(select(PaperAccountRow).limit(1))
        paper_trades = session.scalar(select(func.count()).select_from(PaperTradeRow)) or 0
        paper_realized = paper.realized_pnl if paper else None
        day_start = utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
        daily_rows = session.scalars(
            select(PaperTradeRow).where(PaperTradeRow.created_at >= day_start)
        ).all()
        daily_realized = sum(
            (Decimal(t.realized_pnl or t.pnl or "0") for t in daily_rows),
            Decimal("0"),
        )
        occupied = paper.occupied if paper else None
        marked = (paper.marked_inventory if paper else None) or occupied
        cash = paper.cash if paper else None
        peak = paper.peak_equity if paper else None
        max_dd = paper.max_drawdown if paper else None
        equity = None
        if cash is not None:
            marked_d = Decimal(marked or occupied or "0")
            equity = format(Decimal(cash) + marked_d, "f")

        return {
            "markets": markets,
            "last_run_started_at": ensure_utc(last_run.started_at) if last_run else None,
            "last_run_status": last_run.status if last_run else None,
            "signals_today": todays,
            "raw_signals_today": todays,
            "qualified_today": qualified_today,
            "qualified_episodes_today": episode_first_today,
            "active_opportunities": active_ops,
            "open_episodes": open_eps,
            "last_book_at": ensure_utc(last_book.fetched_at) if last_book else None,
            "api_errors_today": api_errors_today,
            "optimistic_profit_today": 0.0,
            "base_profit_today": 0.0,
            "pessimistic_profit_today": 0.0,
            "theoretical_note": (
                "Do not sum per-tick base_net. Theoretical opportunity count uses "
                "first-seen episodes; realized P&L is paper only."
            ),
            "latency_p50_ms": p50,
            "latency_p95_ms": p95,
            "latency_sufficient": sufficient,
            "clock_skew_detected": clock_skew_detected,
            "paper_cash": cash,
            "paper_pnl": paper_realized,
            "paper_realized_pnl": paper_realized,
            "paper_daily_realized_pnl": format(daily_realized, "f"),
            "paper_cumulative_realized_pnl": paper_realized,
            "paper_occupied": occupied,
            "paper_marked_inventory": marked,
            "paper_equity": equity,
            "paper_max_drawdown": max_dd,
            "paper_peak_equity": peak,
            "paper_trades": paper_trades,
            "date": today,
        }
