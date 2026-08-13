"""Tests for scanner control helpers."""

from __future__ import annotations

from polymarket_scanner.ui.scanner_control import (
    ScannerParams,
    build_daemon_cmd,
    start_phase_daemon,
)


def test_build_daemon_cmd_observe_only():
    params = ScannerParams(max_pages=2, market_limit=50)
    cmd = build_daemon_cmd("observe", params)
    assert "--mode" in cmd
    assert "live" in cmd
    assert "--daemon" in cmd
    assert "--paper" not in cmd
    assert "--max-pages" in cmd
    assert "2" in cmd
    assert "--market-limit" in cmd
    assert "static" not in cmd


def test_build_daemon_cmd_paper():
    params = ScannerParams()
    cmd = build_daemon_cmd("paper", params)
    assert "live" in cmd
    assert "--paper" in cmd
    assert "--execution" in cmd
    assert "paper" in cmd


def test_phase_params_runtime_settings():
    params = ScannerParams(paper_delay_ms=500, paper_tif="FOK")
    settings = params.to_runtime_settings(paper=True)
    assert settings["paper_enabled"] is True
    assert settings["paper_time_in_force"] == "FOK"
    assert settings["paper_delay_ms"] == 500


def test_static_phase_daemon_rejected():
    proc, msg = start_phase_daemon("phase1", ScannerParams())
    assert proc is None
    assert "Snapshot Audit" in msg or "removed" in msg.lower()
