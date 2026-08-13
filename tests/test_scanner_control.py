"""Tests for scanner control helpers."""

from __future__ import annotations

from polymarket_scanner.ui.scanner_control import PhaseParams, build_daemon_cmd


def test_build_daemon_cmd_phase1():
    params = PhaseParams(poll_interval_s=45, max_pages=2, market_limit=50)
    cmd = build_daemon_cmd("phase1", params)
    assert "--mode" in cmd
    assert "static" in cmd
    assert "--paper" not in cmd
    assert "--max-pages" in cmd
    assert "2" in cmd
    assert "--market-limit" in cmd


def test_build_daemon_cmd_phase3():
    params = PhaseParams()
    cmd = build_daemon_cmd("phase3", params)
    assert "realtime" in cmd
    assert "--paper" in cmd


def test_phase_params_runtime_settings():
    params = PhaseParams(poll_interval_s=60, paper_delay_ms=500, paper_tif="FOK")
    settings = params.to_runtime_settings(phase="phase3", daemon=True)
    assert settings["orderbook_poll_interval_seconds"] == 60
    assert settings["paper_enabled"] is True
    assert settings["paper_time_in_force"] == "FOK"
