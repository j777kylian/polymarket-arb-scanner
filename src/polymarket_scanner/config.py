"""Configuration loading."""

from __future__ import annotations

import os
from decimal import Decimal
from functools import lru_cache
from pathlib import Path
from typing import Any

import yaml
from dotenv import load_dotenv
from pydantic import BaseModel, Field

from polymarket_scanner.safety import TRADING_ENABLED, assert_trading_disabled

ROOT_DIR = Path(__file__).resolve().parents[2]
CONFIG_DIR = ROOT_DIR / "config"


class ApiConfig(BaseModel):
    gamma_url: str = "https://gamma-api.polymarket.com"
    clob_url: str = "https://clob.polymarket.com"
    geoblock_url: str = "https://polymarket.com/api/geoblock"
    market_ws_url: str = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
    http_timeout_seconds: float = 30.0
    max_concurrent_requests: int = 8
    retry_max_attempts: int = 5
    retry_min_wait_seconds: float = 0.5
    retry_max_wait_seconds: float = 30.0
    market_page_limit: int = 100


class ScannerConfig(BaseModel):
    mode: str = "live"  # snapshot | live (realtime alias accepted)
    market_sync_interval_seconds: int = 300
    orderbook_poll_interval_seconds: int = 45
    max_data_age_seconds: int = 60
    default_rule_set: str = "Balanced"
    retention_days: int = 14
    lock_file: str = "data/scanner.lock"
    auto_daily_report: bool = True
    ws_ping_interval_seconds: int = 10
    ws_subscribe_chunk: int = 0  # 0 = send all tokens in one initial type=market frame
    ws_recalc_debounce_ms: int = 75
    latency_sufficient_p50_ms: float = 200.0
    latency_sufficient_p95_ms: float = 500.0
    max_book_skew_ms: float = 250.0
    observed_delay_tolerance_ms: float = 250.0
    max_pages: int | None = None
    market_limit: int | None = None
    sync_markets: bool = True
    ws_persist_min_interval_ms: int = 400
    min_walk_forward_trades: int = 30
    new_market_resync_cooldown_seconds: int = 30


class PaperConfig(BaseModel):
    enabled: bool = False
    starting_capital: Decimal = Decimal("1000")
    delay_ms: int = 500  # alias of signal_to_first_leg_ms
    signal_to_first_leg_ms: int = 500
    inter_leg_delay_ms: int = 100
    force_close_delay_ms: int = 200
    time_in_force: str = "FAK"  # FOK | FAK
    first_leg: str = "YES"
    force_close_unhedged: bool = True
    residual_close_retries: int = 3
    min_net_profit: Decimal = Decimal("0.50")
    min_profit_per_share: Decimal = Decimal("0")
    minimum_quantity: Decimal = Decimal("0")
    strategy_id: str = "live_default"
    strategy_version: int = 1

    @property
    def first_leg_delay_ms(self) -> int:
        return int(self.signal_to_first_leg_ms or self.delay_ms)


class ScenarioProfileConfig(BaseModel):
    delay_ms: int = 0
    slippage_ticks: int = 0
    depth_factor: Decimal = Decimal("1.0")
    sequential_legs: bool = False
    partial_second_leg_ratio: Decimal = Decimal("1.0")
    force_close_unhedged: bool = False


class SimulationConfig(BaseModel):
    default_profile: str = "base"
    operational_cost: Decimal = Decimal("0")
    safety_buffer: Decimal = Decimal("0.01")
    profiles: dict[str, ScenarioProfileConfig] = Field(default_factory=dict)


class ReportingConfig(BaseModel):
    timezone: str = "UTC"
    report_hour_utc: int = 0
    reports_dir: str = "reports"


class DatabaseConfig(BaseModel):
    url: str = "sqlite:///./data/scanner.db"


class LoggingConfig(BaseModel):
    level: str = "INFO"
    dir: str = "logs"
    file: str = "scanner.log"


class UiConfig(BaseModel):
    title: str = "Polymarket Structural Arb Scanner"
    port: int = 8501


class AppConfig(BaseModel):
    trading_enabled: bool = False
    api: ApiConfig = Field(default_factory=ApiConfig)
    scanner: ScannerConfig = Field(default_factory=ScannerConfig)
    simulation: SimulationConfig = Field(default_factory=SimulationConfig)
    paper: PaperConfig = Field(default_factory=PaperConfig)
    reporting: ReportingConfig = Field(default_factory=ReportingConfig)
    database: DatabaseConfig = Field(default_factory=DatabaseConfig)
    logging: LoggingConfig = Field(default_factory=LoggingConfig)
    ui: UiConfig = Field(default_factory=UiConfig)

    def resolve_path(self, relative: str) -> Path:
        path = Path(relative)
        if path.is_absolute():
            return path
        return ROOT_DIR / path


def _deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    out = dict(base)
    for key, value in override.items():
        if key in out and isinstance(out[key], dict) and isinstance(value, dict):
            out[key] = _deep_merge(out[key], value)
        else:
            out[key] = value
    return out


def _env_overrides() -> dict[str, Any]:
    overrides: dict[str, Any] = {}
    if gamma := os.getenv("POLYMARKET_GAMMA_URL"):
        overrides.setdefault("api", {})["gamma_url"] = gamma
    if clob := os.getenv("POLYMARKET_CLOB_URL"):
        overrides.setdefault("api", {})["clob_url"] = clob
    if geo := os.getenv("POLYMARKET_GEOBLOCK_URL"):
        overrides.setdefault("api", {})["geoblock_url"] = geo
    if db := os.getenv("DATABASE_URL"):
        overrides.setdefault("database", {})["url"] = db
    if level := os.getenv("LOG_LEVEL"):
        overrides.setdefault("logging", {})["level"] = level
    if tz := os.getenv("TIMEZONE"):
        overrides.setdefault("reporting", {})["timezone"] = tz
    return overrides


@lru_cache(maxsize=1)
def get_config(config_path: str | None = None) -> AppConfig:
    load_dotenv(ROOT_DIR / ".env")
    path = Path(config_path) if config_path else CONFIG_DIR / "default.yaml"
    raw: dict[str, Any] = {}
    if path.exists():
        with path.open("r", encoding="utf-8") as fh:
            raw = yaml.safe_load(fh) or {}
    raw = _deep_merge(raw, _env_overrides())
    cfg = AppConfig.model_validate(raw)
    # Enforce safety regardless of yaml
    if cfg.trading_enabled or TRADING_ENABLED:
        assert_trading_disabled()
    cfg.trading_enabled = False
    return cfg


def reload_config(config_path: str | None = None) -> AppConfig:
    get_config.cache_clear()
    return get_config(config_path)


def normalize_scanner_mode(mode: str | None) -> str:
    """Map CLI aliases to snapshot | live. Static daemon polling is not a product mode."""
    value = (mode or "live").strip().lower()
    if value in {"static", "snapshot", "once", "audit"}:
        return "snapshot"
    if value in {"realtime", "live", "daemon", "research"}:
        return "live"
    return value
