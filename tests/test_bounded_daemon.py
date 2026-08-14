"""Bounded Live Research runtime: CLI flag, threading, and normal timeout cleanup."""

from __future__ import annotations

import asyncio
import importlib.util
import sys
from pathlib import Path
from types import ModuleType

import pytest
from filelock import FileLock
from sqlalchemy import desc, select

from polymarket_scanner.config import get_config
from polymarket_scanner.database import ScannerRunRow, get_setting, init_db, session_scope
from polymarket_scanner.models import MarketInfo
from polymarket_scanner.realtime import RealtimeScanner
from polymarket_scanner.scheduler import ScannerService

ROOT = Path(__file__).resolve().parents[1]


def _tmp_db(tmp_path, monkeypatch) -> None:
    db = tmp_path / "t.db"
    monkeypatch.setenv("DATABASE_URL", f"sqlite:///{db}")
    get_config.cache_clear()
    from polymarket_scanner import database as dbmod

    dbmod._engine = None
    dbmod._SessionLocal = None
    init_db(f"sqlite:///{db}")


def _load_run_scanner() -> ModuleType:
    spec = importlib.util.spec_from_file_location(
        "run_scanner_script", ROOT / "scripts" / "run_scanner.py"
    )
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _markets(n: int) -> list[MarketInfo]:
    return [
        MarketInfo(
            market_id=f"m{i}",
            condition_id=f"c{i}",
            yes_token_id=f"y{i}",
            no_token_id=f"n{i}",
        )
        for i in range(n)
    ]


def test_module_cli_exposes_positive_duration_seconds() -> None:
    from polymarket_scanner.__main__ import parse_args

    args = parse_args(["--daemon", "--mode", "live", "--duration-seconds", "43200"])
    assert args.duration_seconds == 43200
    omitted = parse_args(["--daemon", "--mode", "live"])
    assert omitted.duration_seconds is None


def test_module_cli_rejects_non_positive_duration_seconds() -> None:
    from polymarket_scanner.__main__ import parse_args

    for value in ("0", "-1"):
        with pytest.raises(SystemExit) as exc:
            parse_args(["--daemon", "--mode", "live", "--duration-seconds", value])
        assert exc.value.code == 2


def test_run_scanner_cli_exposes_and_rejects_duration_seconds() -> None:
    mod = _load_run_scanner()
    args = mod.parse_args(["--daemon", "--mode", "live", "--duration-seconds", "43200"])
    assert args.duration_seconds == 43200
    omitted = mod.parse_args(["--daemon", "--mode", "live"])
    assert omitted.duration_seconds is None
    for value in ("0", "-1"):
        with pytest.raises(SystemExit) as exc:
            mod.parse_args(["--daemon", "--mode", "live", "--duration-seconds", value])
        assert exc.value.code == 2


_NON_LIVE_DURATION_ARGV = (
    ["--once", "--duration-seconds", "60"],
    ["--once", "--mode", "snapshot", "--duration-seconds", "60"],
    ["--once", "--mode", "static", "--duration-seconds", "60"],
    ["--once", "--mode", "live", "--duration-seconds", "60"],
    ["--once", "--mode", "realtime", "--duration-seconds", "60"],
    ["--mode", "snapshot", "--duration-seconds", "60"],
    ["--mode", "static", "--duration-seconds", "60"],
)

_LIVE_DURATION_ARGV = (
    ["--daemon", "--mode", "live", "--duration-seconds", "60"],
    ["--daemon", "--mode", "realtime", "--duration-seconds", "60"],
    ["--mode", "live", "--duration-seconds", "60"],
    ["--mode", "realtime", "--duration-seconds", "60"],
    ["--daemon", "--duration-seconds", "60"],
)


def test_module_cli_rejects_duration_seconds_outside_live_daemon() -> None:
    from polymarket_scanner.__main__ import parse_args

    for argv in _NON_LIVE_DURATION_ARGV:
        with pytest.raises(SystemExit) as exc:
            parse_args(argv)
        assert exc.value.code == 2
    for argv in _LIVE_DURATION_ARGV:
        args = parse_args(argv)
        assert args.duration_seconds == 60
    parse_args(["--once"])
    parse_args(["--mode", "snapshot"])


def test_run_scanner_cli_rejects_duration_seconds_outside_live_daemon() -> None:
    mod = _load_run_scanner()
    for argv in _NON_LIVE_DURATION_ARGV:
        with pytest.raises(SystemExit) as exc:
            mod.parse_args(argv)
        assert exc.value.code == 2
    for argv in _LIVE_DURATION_ARGV:
        args = mod.parse_args(argv)
        assert args.duration_seconds == 60
    mod.parse_args(["--once"])
    mod.parse_args(["--mode", "snapshot"])


def test_module_main_threads_duration_seconds(tmp_path, monkeypatch) -> None:
    _tmp_db(tmp_path, monkeypatch)
    captured: dict[str, object] = {}

    async def fake_run_daemon(self, **kwargs):
        captured.update(kwargs)

    monkeypatch.setattr(ScannerService, "run_daemon", fake_run_daemon)
    monkeypatch.setattr(
        sys,
        "argv",
        ["polymarket_scanner", "--daemon", "--mode", "live", "--duration-seconds", "43200"],
    )
    from polymarket_scanner.__main__ import main

    main()
    assert captured["duration_seconds"] == 43200


def test_module_main_omits_duration_when_flag_absent(tmp_path, monkeypatch) -> None:
    _tmp_db(tmp_path, monkeypatch)
    captured: dict[str, object] = {}

    async def fake_run_daemon(self, **kwargs):
        captured.update(kwargs)

    monkeypatch.setattr(ScannerService, "run_daemon", fake_run_daemon)
    monkeypatch.setattr(sys, "argv", ["polymarket_scanner", "--daemon", "--mode", "live"])
    from polymarket_scanner.__main__ import main

    main()
    assert captured.get("duration_seconds") is None


def test_run_scanner_main_threads_duration_seconds(tmp_path, monkeypatch) -> None:
    _tmp_db(tmp_path, monkeypatch)
    captured: dict[str, object] = {}

    async def fake_run_daemon(self, **kwargs):
        captured.update(kwargs)

    monkeypatch.setattr(ScannerService, "run_daemon", fake_run_daemon)
    monkeypatch.setattr(
        sys,
        "argv",
        ["run_scanner.py", "--daemon", "--mode", "live", "--paper", "--duration-seconds", "43200"],
    )
    mod = _load_run_scanner()
    mod.main()
    assert captured["duration_seconds"] == 43200


def _patch_live_scanner(monkeypatch) -> dict[str, object]:
    captured: dict[str, object] = {}
    flushed: list[bool] = []
    paper_shutdowns: list[bool] = []
    reports: list[object] = []

    async def fake_discover(*, max_pages=None, **_kwargs):
        captured["max_pages"] = max_pages
        return _markets(2)

    class FakeWS:
        def __init__(self, *args, **kwargs) -> None:
            captured["ws"] = self
            self.stopped = False
            self.subscribed_tokens: set[str] = set()

        def stop(self) -> None:
            self.stopped = True

        async def run(self, token_ids: list[str]) -> None:
            captured["tokens"] = list(token_ids)
            await asyncio.Event().wait()

        async def update_subscriptions(self, token_ids: list[str]) -> tuple[set[str], set[str]]:
            return set(), set()

    orig_flush = RealtimeScanner._shutdown_paper_tasks

    async def spy_paper(self) -> None:
        paper_shutdowns.append(True)
        await orig_flush(self)

    def spy_flush() -> None:
        flushed.append(True)
        from polymarket_scanner.scanners.pipeline import flush_latency as real_flush

        real_flush()

    created: dict[str, RealtimeScanner] = {}
    orig_rt = RealtimeScanner

    class TrackingScanner(orig_rt):  # type: ignore[valid-type,misc]
        def __init__(self, *args, **kwargs) -> None:
            super().__init__(*args, **kwargs)
            created["rt"] = self

    monkeypatch.setattr("polymarket_scanner.realtime.discover_and_store_markets", fake_discover)
    monkeypatch.setattr("polymarket_scanner.realtime.MarketWebsocketClient", FakeWS)
    monkeypatch.setattr("polymarket_scanner.realtime.flush_latency", spy_flush)
    monkeypatch.setattr(RealtimeScanner, "_shutdown_paper_tasks", spy_paper)
    monkeypatch.setattr("polymarket_scanner.realtime.RealtimeScanner", TrackingScanner)
    monkeypatch.setattr(
        "polymarket_scanner.reporting.html_report.generate_daily_report",
        lambda *a, **k: reports.append(a[0] if a else None) or {"ok": True},
    )
    captured["flushed"] = flushed
    captured["paper_shutdowns"] = paper_shutdowns
    captured["reports"] = reports
    captured["created"] = created
    return captured


def _service(tmp_path) -> ScannerService:
    svc = ScannerService()
    svc._lock = FileLock(str(tmp_path / "scanner.lock"), timeout=0)
    return svc


@pytest.mark.asyncio
async def test_run_daemon_timeout_is_normal_completion(tmp_path, monkeypatch, capsys) -> None:
    _tmp_db(tmp_path, monkeypatch)
    captured = _patch_live_scanner(monkeypatch)
    svc = _service(tmp_path)

    await svc.run_daemon(
        mode="live",
        paper=True,
        duration_seconds=0.05,
        sync_markets=False,
        max_market_pages=1,
        market_limit=2,
    )

    logged = capsys.readouterr().out.lower()
    assert "duration elapsed" in logged
    assert "stopping normally" in logged
    assert svc._running is False
    assert svc._lock.is_locked is False

    with session_scope() as session:
        run = session.scalar(select(ScannerRunRow).order_by(desc(ScannerRunRow.id)).limit(1))
        assert run is not None
        status = run.status
        finished_at = run.finished_at
        mode = get_setting(session, "scanner_active_mode")
    assert status == "stopped"
    assert finished_at is not None
    assert mode is None

    rt = captured["created"]["rt"]
    assert rt._running is False
    assert captured["flushed"]
    assert captured["paper_shutdowns"]
    ws = captured.get("ws")
    assert ws is not None
    assert ws.stopped is True
    assert captured["reports"]  # stop-time daily report


@pytest.mark.asyncio
async def test_run_daemon_without_duration_stays_indefinite(tmp_path, monkeypatch) -> None:
    _tmp_db(tmp_path, monkeypatch)
    started = asyncio.Event()
    hang = asyncio.Event()
    created: dict[str, object] = {}

    class FakeRT:
        def __init__(self, *args, **kwargs) -> None:
            self._running = False
            created["rt"] = self

        async def run(self) -> None:
            self._running = True
            started.set()
            await hang.wait()
            self._running = False

    monkeypatch.setattr("polymarket_scanner.realtime.RealtimeScanner", FakeRT)
    monkeypatch.setattr(
        "polymarket_scanner.reporting.html_report.generate_daily_report",
        lambda *a, **k: {"ok": True},
    )
    svc = _service(tmp_path)
    task = asyncio.create_task(svc.run_daemon(mode="live", paper=True, sync_markets=False))
    await asyncio.wait_for(started.wait(), timeout=2)
    await asyncio.sleep(0.05)
    assert task.done() is False
    hang.set()
    await asyncio.wait_for(task, timeout=2)
    assert task.exception() is None
    assert svc._running is False
    assert svc._lock.is_locked is False
