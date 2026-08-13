"""Streamlit scanner control — Snapshot Audit and Live Research."""

from __future__ import annotations

import os
import signal
import subprocess
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Literal

from filelock import FileLock, Timeout
from sqlalchemy import desc, select

from polymarket_scanner.config import ROOT_DIR, get_config
from polymarket_scanner.database import (
    OpportunityEpisodeRow,
    OpportunityRow,
    PaperTradeRow,
    ScannerRunRow,
    ensure_utc,
    get_setting,
    session_scope,
)
from polymarket_scanner.runtime_settings import save_runtime_settings, set_scanner_process_status
from polymarket_scanner.scheduler import ScannerService

ExecutionMode = Literal["observe", "paper"]
ProductId = Literal["snapshot", "live"]


@dataclass
class ScannerParams:
    market_sync_s: int = 300
    max_pages: int | None = None
    market_limit: int | None = None
    sync_markets: bool = True
    paper_delay_ms: int = 500
    paper_tif: str = "FAK"
    paper_min_net_profit: float = 0.50
    auto_daily_report: bool = True
    poll_interval_s: int = 45  # unused for live; kept for settings compatibility

    def to_runtime_settings(self, *, paper: bool) -> dict[str, Any]:
        return {
            "orderbook_poll_interval_seconds": self.poll_interval_s,
            "market_sync_interval_seconds": self.market_sync_s,
            "paper_delay_ms": self.paper_delay_ms,
            "paper_time_in_force": self.paper_tif,
            "paper_min_net_profit": self.paper_min_net_profit,
            "paper_enabled": paper,
            "scanner_max_pages": self.max_pages,
            "scanner_market_limit": self.market_limit,
            "scanner_sync_markets": self.sync_markets,
        }


# Backward-compatible alias used by older tests/imports.
PhaseParams = ScannerParams


@dataclass
class ScannerStatus:
    running: bool = False
    source: str = "stopped"  # stopped | ui_subprocess | lock_file | db
    mode: str | None = None  # snapshot | live
    product: str | None = None
    execution: ExecutionMode | None = None
    paper: bool = False
    pid: int | None = None
    started_at: str | None = None
    lock_held: bool = False
    ui_proc_alive: bool = False
    message: str = ""
    phase: str | None = None  # unused; kept so old UI bindings do not crash


def _product_label(product: ProductId, execution: ExecutionMode = "observe") -> str:
    if product == "snapshot":
        return "Snapshot Audit"
    if execution == "paper":
        return "Live Research — Paper Trading"
    return "Live Research — Observe Only"


def is_lock_held() -> bool:
    cfg = get_config()
    lock_path = cfg.resolve_path(cfg.scanner.lock_file)
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        lock = FileLock(str(lock_path), timeout=0)
        lock.acquire(timeout=0)
        lock.release()
        return False
    except Timeout:
        return True
    except Exception:
        return lock_path.exists()


def build_daemon_cmd(execution: ExecutionMode, params: ScannerParams) -> list[str]:
    cmd = [
        sys.executable,
        str(ROOT_DIR / "scripts" / "run_scanner.py"),
        "--daemon",
        "--mode",
        "live",
        "--execution",
        execution,
    ]
    if execution == "paper":
        cmd.append("--paper")
    if params.max_pages is not None:
        cmd.extend(["--max-pages", str(params.max_pages)])
    if params.market_limit is not None:
        cmd.extend(["--market-limit", str(params.market_limit)])
    if not params.sync_markets:
        cmd.append("--no-sync")
    return cmd


def get_scanner_status(ui_proc: subprocess.Popen | None = None) -> ScannerStatus:
    cfg = get_config()
    lock_held = is_lock_held()
    ui_alive = ui_proc is not None and ui_proc.poll() is None

    with session_scope() as session:
        mode = get_setting(session, "scanner_active_mode")
        paper = bool(get_setting(session, "scanner_paper") or False)
        pid = get_setting(session, "scanner_pid")
        started_at = get_setting(session, "scanner_started_at")

    if mode in {"static", "snapshot"}:
        mode = "snapshot"
        product: str | None = "snapshot"
        execution: ExecutionMode | None = "observe"
    elif mode in {"realtime", "live"}:
        mode = "live"
        product = "live"
        execution = "paper" if paper else "observe"
    else:
        product = None
        execution = None

    running = ui_alive or lock_held
    source = "stopped"
    if ui_alive:
        source = "ui_subprocess"
        pid = ui_proc.pid if ui_proc else pid
    elif lock_held:
        source = "lock_file"
        running = True

    msg_parts: list[str] = []
    if lock_held:
        msg_parts.append(f"Lock held ({cfg.scanner.lock_file})")
    if ui_alive and ui_proc is not None:
        msg_parts.append(f"UI subprocess pid={ui_proc.pid}")
    if mode:
        msg_parts.append(f"mode={mode} execution={execution} paper={paper}")

    return ScannerStatus(
        running=running,
        source=source,
        mode=mode,
        product=product,
        execution=execution,
        paper=paper,
        pid=int(pid) if pid else (ui_proc.pid if ui_alive and ui_proc else None),
        started_at=str(started_at) if started_at else None,
        lock_held=lock_held,
        ui_proc_alive=ui_alive,
        message=" · ".join(msg_parts) if msg_parts else "Scanner stopped",
        phase=product,
    )


def read_log_tail(lines: int = 40) -> str:
    cfg = get_config()
    log_path = cfg.resolve_path(cfg.logging.dir) / cfg.logging.file
    if not log_path.exists():
        return "(log file not found)"
    try:
        content = log_path.read_text(encoding="utf-8", errors="replace").splitlines()
        return "\n".join(content[-lines:]) if content else "(empty log)"
    except OSError as exc:
        return f"(cannot read log: {exc})"


def get_recent_live_data(*, limit: int = 12) -> dict[str, list[dict[str, Any]]]:
    with session_scope() as session:
        opps = [
            {
                "discovered_at": ensure_utc(o.discovered_at),
                "market_id": o.market_id,
                "base_net": float(o.base_net or 0),
                "stale": o.stale,
            }
            for o in session.scalars(
                select(OpportunityRow)
                .where(OpportunityRow.net_profitable.is_(True))
                .order_by(desc(OpportunityRow.discovered_at))
                .limit(limit)
            ).all()
        ]
        episodes = [
            {
                "first_seen": ensure_utc(e.first_seen_at),
                "market_id": e.market_id,
                "open": e.is_open,
                "duration_s": e.duration_seconds,
            }
            for e in session.scalars(
                select(OpportunityEpisodeRow)
                .order_by(desc(OpportunityEpisodeRow.first_seen_at))
                .limit(limit)
            ).all()
        ]
        trades = [
            {
                "created_at": ensure_utc(t.created_at),
                "market_id": t.market_id,
                "status": t.status,
                "tif": t.tif,
                "yes_qty": float(t.yes_qty or 0),
                "no_qty": float(t.no_qty or 0),
                "pnl": float(t.realized_pnl or t.pnl or 0),
                "reject_reason": t.reject_reason,
            }
            for t in session.scalars(
                select(PaperTradeRow).order_by(desc(PaperTradeRow.created_at)).limit(limit)
            ).all()
        ]
        runs = [
            {
                "started_at": ensure_utc(r.started_at),
                "status": r.status,
                "signals": r.signals_found,
                "markets": r.subscribed_markets or r.markets_synced,
            }
            for r in session.scalars(
                select(ScannerRunRow).order_by(desc(ScannerRunRow.started_at)).limit(5)
            ).all()
        ]

    return {
        "opportunities": opps,
        "episodes": episodes,
        "paper_trades": trades,
        "scanner_runs": runs,
    }


async def run_snapshot_once(params: ScannerParams) -> dict[str, Any]:
    save_runtime_settings(params.to_runtime_settings(paper=False))
    return await ScannerService().run_once(
        max_market_pages=params.max_pages,
        market_limit=params.market_limit,
        sync_markets=params.sync_markets,
    )


run_phase1_once = run_snapshot_once


def start_live_daemon(
    execution: ExecutionMode, params: ScannerParams
) -> tuple[subprocess.Popen | None, str]:
    status = get_scanner_status()
    if status.running:
        return None, f"Scanner already running ({status.message})"

    paper = execution == "paper"
    save_runtime_settings(params.to_runtime_settings(paper=paper))
    cmd = build_daemon_cmd(execution, params)
    proc = subprocess.Popen(cmd, cwd=str(ROOT_DIR))
    set_scanner_process_status(
        mode="live",
        paper=paper,
        pid=proc.pid,
        started_at=datetime.now(timezone.utc).isoformat(),
    )
    return proc, f"Started {_product_label('live', execution)} · pid={proc.pid}"


def start_phase_daemon(phase: str, params: ScannerParams) -> tuple[subprocess.Popen | None, str]:
    """Compatibility wrapper — Phase 1 daemon is removed; live maps to WebSocket."""
    if phase in {"phase1", "snapshot", "static"}:
        return None, "Snapshot Audit is --once only. Static REST polling daemon was removed."
    execution: ExecutionMode = "paper" if phase in {"phase3", "paper"} else "observe"
    return start_live_daemon(execution, params)


def stop_scanner(
    ui_proc: subprocess.Popen | None,
    *,
    generate_report: bool = False,
) -> tuple[subprocess.Popen | None, str]:
    messages: list[str] = []

    if ui_proc and ui_proc.poll() is None:
        ui_proc.terminate()
        try:
            ui_proc.wait(timeout=8)
            messages.append(f"Terminated UI subprocess pid={ui_proc.pid}")
        except subprocess.TimeoutExpired:
            ui_proc.kill()
            messages.append(f"Killed UI subprocess pid={ui_proc.pid}")

    with session_scope() as session:
        pid = get_setting(session, "scanner_pid")

    if pid and (not ui_proc or ui_proc.pid != int(pid)):
        try:
            os.kill(int(pid), signal.SIGTERM)
            messages.append(f"Sent SIGTERM to pid={pid}")
        except (ProcessLookupError, PermissionError, ValueError):
            pass

    if is_lock_held():
        messages.append("Waiting for lock release…")
        for _ in range(20):
            if not is_lock_held():
                break
            import time

            time.sleep(0.25)

    set_scanner_process_status(mode=None)
    if generate_report:
        from polymarket_scanner.reporting.html_report import generate_daily_report

        out = generate_daily_report()
        messages.append(f"Report: {out}")

    if not messages:
        messages.append("Scanner was not running")
    return None, " · ".join(messages)


def load_params_from_settings(default: ScannerParams | None = None) -> ScannerParams:
    base = default or ScannerParams()
    cfg = get_config()
    with session_scope() as session:
        poll = get_setting(session, "orderbook_poll_interval_seconds")
        sync = get_setting(session, "market_sync_interval_seconds")
        delay = get_setting(session, "paper_delay_ms")
        tif = get_setting(session, "paper_time_in_force")
        min_p = get_setting(session, "paper_min_net_profit")
        max_p = get_setting(session, "scanner_max_pages")
        mlim = get_setting(session, "scanner_market_limit")
        sync_m = get_setting(session, "scanner_sync_markets")
    return ScannerParams(
        poll_interval_s=int(poll) if poll is not None else cfg.scanner.orderbook_poll_interval_seconds,
        market_sync_s=int(sync) if sync is not None else cfg.scanner.market_sync_interval_seconds,
        max_pages=int(max_p) if max_p is not None else base.max_pages,
        market_limit=int(mlim) if mlim is not None else base.market_limit,
        sync_markets=bool(sync_m) if sync_m is not None else base.sync_markets,
        paper_delay_ms=int(delay) if delay is not None else cfg.paper.delay_ms,
        paper_tif=str(tif) if tif else cfg.paper.time_in_force,
        paper_min_net_profit=float(min_p) if min_p is not None else float(cfg.paper.min_net_profit),
    )
