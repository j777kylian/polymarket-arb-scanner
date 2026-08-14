"""python -m polymarket_scanner"""

from __future__ import annotations

import argparse
import asyncio

from polymarket_scanner.config import normalize_scanner_mode
from polymarket_scanner.database import init_db
from polymarket_scanner.logging_config import setup_logging
from polymarket_scanner.safety import TRADING_ENABLED, assert_trading_disabled
from polymarket_scanner.scheduler import ScannerService


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Polymarket structural arb scanner (read-only)")
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--daemon", action="store_true")
    parser.add_argument(
        "--mode",
        choices=["snapshot", "live", "static", "realtime"],
        default=None,
    )
    parser.add_argument("--execution", choices=["observe", "paper"], default="observe")
    parser.add_argument("--paper", action="store_true")
    parser.add_argument("--max-pages", type=int, default=None)
    parser.add_argument("--market-limit", type=int, default=None)
    parser.add_argument(
        "--duration-seconds",
        type=int,
        default=None,
        help=(
            "Live Research only: stop after N seconds and exit through normal cleanup "
            "(omit to run indefinitely; 43200 = 12 hours)"
        ),
    )
    args = parser.parse_args(argv)
    if args.duration_seconds is not None and args.duration_seconds <= 0:
        parser.error("--duration-seconds must be a positive integer")
    mode = normalize_scanner_mode(args.mode)
    live = bool(args.daemon or mode == "live" or args.mode in {"live", "realtime"})
    if args.duration_seconds is not None and not (live and not args.once):
        parser.error("--duration-seconds is Live Research only")
    return args


def main() -> None:
    assert_trading_disabled()
    setup_logging()
    init_db()
    args = parse_args()
    print(f"READ-ONLY MODE — TRADING_ENABLED={TRADING_ENABLED}")
    service = ScannerService()
    mode = normalize_scanner_mode(args.mode)
    paper = bool(args.paper or args.execution == "paper")
    live = bool(args.daemon or mode == "live" or args.mode in {"live", "realtime"})
    if live and not args.once:
        asyncio.run(
            service.run_daemon(
                mode="live",
                paper=paper,
                max_market_pages=args.max_pages,
                market_limit=args.market_limit,
                duration_seconds=args.duration_seconds,
            )
        )
        return
    summary = asyncio.run(
        service.run_once(
            max_market_pages=args.max_pages if args.max_pages is not None else 2,
            market_limit=args.market_limit if args.market_limit is not None else 30,
        )
    )
    print(summary)


if __name__ == "__main__":
    main()
