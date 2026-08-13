"""Runtime overrides stored in app_settings (UI → scanner daemon)."""

from __future__ import annotations

from decimal import Decimal
from typing import Any

from polymarket_scanner.config import AppConfig, get_config
from polymarket_scanner.database import get_setting, session_scope, set_setting


RUNTIME_KEYS = (
    "orderbook_poll_interval_seconds",
    "market_sync_interval_seconds",
    "max_concurrent_requests",
    "http_timeout_seconds",
    "max_data_age_seconds",
    "paper_delay_ms",
    "paper_time_in_force",
    "paper_min_net_profit",
    "paper_enabled",
    "scanner_max_pages",
    "scanner_market_limit",
    "scanner_sync_markets",
)


def save_runtime_settings(values: dict[str, Any]) -> None:
    with session_scope() as session:
        for key, value in values.items():
            if key in RUNTIME_KEYS or key.startswith("scanner_"):
                set_setting(session, key, value)


def load_runtime_settings() -> dict[str, Any]:
    with session_scope() as session:
        out: dict[str, Any] = {}
        for key in RUNTIME_KEYS:
            val = get_setting(session, key)
            if val is not None:
                out[key] = val
        for extra in ("scanner_active_mode", "scanner_paper", "scanner_pid", "scanner_started_at"):
            val = get_setting(session, extra)
            if val is not None:
                out[extra] = val
        return out


def apply_runtime_to_config(cfg: AppConfig | None = None) -> AppConfig:
    """Return config with app_settings overrides applied (does not mutate cache)."""
    base = cfg or get_config()
    overrides = load_runtime_settings()
    if not overrides:
        return base

    data = base.model_dump()
    if poll := overrides.get("orderbook_poll_interval_seconds"):
        data.setdefault("scanner", {})["orderbook_poll_interval_seconds"] = int(poll)
    if sync := overrides.get("market_sync_interval_seconds"):
        data.setdefault("scanner", {})["market_sync_interval_seconds"] = int(sync)
    if conc := overrides.get("max_concurrent_requests"):
        data.setdefault("api", {})["max_concurrent_requests"] = int(conc)
    if timeout := overrides.get("http_timeout_seconds"):
        data.setdefault("api", {})["http_timeout_seconds"] = float(timeout)
    if max_age := overrides.get("max_data_age_seconds"):
        data.setdefault("scanner", {})["max_data_age_seconds"] = int(max_age)
    if overrides.get("paper_enabled") is not None:
        data.setdefault("paper", {})["enabled"] = bool(overrides["paper_enabled"])
    if delay := overrides.get("paper_delay_ms"):
        data.setdefault("paper", {})["delay_ms"] = int(delay)
    if tif := overrides.get("paper_time_in_force"):
        data.setdefault("paper", {})["time_in_force"] = str(tif)
    if min_profit := overrides.get("paper_min_net_profit"):
        data.setdefault("paper", {})["min_net_profit"] = Decimal(str(min_profit))
    return AppConfig.model_validate(data)


def set_scanner_process_status(
    *,
    mode: str | None,
    paper: bool = False,
    pid: int | None = None,
    started_at: str | None = None,
) -> None:
    with session_scope() as session:
        if mode is None:
            for key in ("scanner_active_mode", "scanner_paper", "scanner_pid", "scanner_started_at"):
                row_val = get_setting(session, key)
                if row_val is not None:
                    set_setting(session, key, None)
            return
        set_setting(session, "scanner_active_mode", mode)
        set_setting(session, "scanner_paper", paper)
        set_setting(session, "scanner_pid", pid)
        set_setting(session, "scanner_started_at", started_at)
