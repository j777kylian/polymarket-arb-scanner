#!/usr/bin/env python3
"""Run the scanner once or as a daemon (static poll or realtime WebSocket)."""

from __future__ import annotations

import argparse
import asyncio
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from polymarket_scanner.logging_config import setup_logging
from polymarket_scanner.safety import assert_trading_disabled
from polymarket_scanner.scheduler import ScannerService


def main() -> None:
    assert_trading_disabled()
    setup_logging()
    parser = argparse.ArgumentParser(description="Polymarket read-only arb scanner")
    parser.add_argument("--once", action="store_true", help="Run a single static scan cycle")
    parser.add_argument("--daemon", action="store_true", help="Run continuously")
    parser.add_argument(
        "--mode",
        choices=["static", "realtime"],
        default="static",
        help="static = REST poll 30–60s; realtime = public market WebSocket",
    )
    parser.add_argument(
        "--paper",
        action="store_true",
        help="Enable paper trading (no real orders): 500ms delay, FOK/FAK, merge, capital",
    )
    parser.add_argument("--max-pages", type=int, default=None, help="Limit Gamma keyset pages")
    parser.add_argument("--market-limit", type=int, default=None, help="Limit markets scanned")
    parser.add_argument("--no-sync", action="store_true", help="Skip market discovery sync")
    args = parser.parse_args()

    service = ScannerService()
    if args.daemon or args.mode == "realtime":
        asyncio.run(
            service.run_daemon(
                mode=args.mode,
                paper=args.paper,
                max_market_pages=args.max_pages,
                market_limit=args.market_limit,
                sync_markets=not args.no_sync,
            )
        )
    else:
        summary = asyncio.run(
            service.run_once(
                max_market_pages=args.max_pages,
                market_limit=args.market_limit,
                sync_markets=not args.no_sync,
            )
        )
        print(summary)


if __name__ == "__main__":
    main()
