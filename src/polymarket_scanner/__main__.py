"""python -m polymarket_scanner"""

from __future__ import annotations

import argparse
import asyncio

from polymarket_scanner.database import init_db
from polymarket_scanner.logging_config import setup_logging
from polymarket_scanner.safety import TRADING_ENABLED, assert_trading_disabled
from polymarket_scanner.scheduler import ScannerService


def main() -> None:
    assert_trading_disabled()
    setup_logging()
    init_db()
    parser = argparse.ArgumentParser(description="Polymarket structural arb scanner (read-only)")
    parser.add_argument("--once", action="store_true", default=True)
    parser.add_argument("--daemon", action="store_true")
    parser.add_argument("--mode", choices=["static", "realtime"], default="static")
    parser.add_argument("--paper", action="store_true")
    parser.add_argument("--max-pages", type=int, default=2)
    parser.add_argument("--market-limit", type=int, default=30)
    args = parser.parse_args()
    print(f"READ-ONLY MODE — TRADING_ENABLED={TRADING_ENABLED}")
    service = ScannerService()
    if args.daemon or args.mode == "realtime":
        asyncio.run(service.run_daemon(mode=args.mode, paper=args.paper))
    else:
        summary = asyncio.run(
            service.run_once(max_market_pages=args.max_pages, market_limit=args.market_limit)
        )
        print(summary)


if __name__ == "__main__":
    main()
