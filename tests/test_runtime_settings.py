"""Runtime settings and CLI daemon args actually reach config."""

from __future__ import annotations

import argparse
from pathlib import Path

from polymarket_scanner.config import get_config
from polymarket_scanner.runtime_settings import apply_runtime_to_config, save_runtime_settings


def test_apply_runtime_pages_and_limit(tmp_path, monkeypatch) -> None:
    db = tmp_path / "s.db"
    monkeypatch.setenv("DATABASE_URL", f"sqlite:///{db}")
    get_config.cache_clear()
    from polymarket_scanner import database as dbmod

    dbmod._engine = None
    dbmod._SessionLocal = None
    dbmod.init_db(f"sqlite:///{db}")
    save_runtime_settings(
        {
            "scanner_max_pages": 2,
            "scanner_market_limit": 15,
            "scanner_sync_markets": False,
        }
    )
    cfg = apply_runtime_to_config(get_config())
    assert cfg.scanner.max_pages == 2
    assert cfg.scanner.market_limit == 15
    assert cfg.scanner.sync_markets is False
    save_runtime_settings({"scanner_sync_markets": "false"})
    cfg2 = apply_runtime_to_config(get_config())
    assert cfg2.scanner.sync_markets is False
    dbmod._engine = None
    dbmod._SessionLocal = None
    get_config.cache_clear()


def test_run_scanner_cli_exposes_daemon_limits() -> None:
    text = Path("scripts/run_scanner.py").read_text(encoding="utf-8")
    assert "max_market_pages=args.max_pages" in text
    assert "market_limit=args.market_limit" in text
    assert "sync_markets=not args.no_sync" in text
    parser = argparse.ArgumentParser()
    parser.add_argument("--max-pages", type=int, default=None)
    parser.add_argument("--market-limit", type=int, default=None)
    args = parser.parse_args(["--max-pages", "3", "--market-limit", "9"])
    assert args.max_pages == 3
    assert args.market_limit == 9
