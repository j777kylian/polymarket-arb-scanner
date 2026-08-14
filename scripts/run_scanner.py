#!/usr/bin/env python3
"""Run Snapshot Audit once, or Live Research as a WebSocket daemon."""

from __future__ import annotations

import argparse
import asyncio
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from polymarket_scanner.config import normalize_scanner_mode  # noqa: E402
from polymarket_scanner.logging_config import setup_logging  # noqa: E402
from polymarket_scanner.safety import assert_trading_disabled  # noqa: E402
from polymarket_scanner.scheduler import ScannerService  # noqa: E402


def main() -> None:
    assert_trading_disabled()
    setup_logging()
    parser = argparse.ArgumentParser(
        description="Polymarket read-only arb scanner — Snapshot Audit or Live Research"
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="Snapshot Audit: run a single REST scan (API/fee/book/formula diagnostics)",
    )
    parser.add_argument(
        "--daemon",
        action="store_true",
        help="Live Research: public market WebSocket (no static REST polling)",
    )
    parser.add_argument(
        "--mode",
        choices=["snapshot", "live", "static", "realtime"],
        default=None,
        help="snapshot/static = Snapshot Audit; live/realtime = Live Research",
    )
    parser.add_argument(
        "--execution",
        choices=["observe", "paper"],
        default="observe",
        help="Live Research execution: Observe Only or Paper Trading (simulated)",
    )
    parser.add_argument(
        "--paper",
        action="store_true",
        help="Alias for --execution paper (local simulation only; no real orders)",
    )
    parser.add_argument("--max-pages", type=int, default=None, help="Limit Gamma keyset pages")
    parser.add_argument("--market-limit", type=int, default=None, help="Limit markets scanned/subscribed")
    parser.add_argument("--no-sync", action="store_true", help="Skip market discovery sync")
    args = parser.parse_args()

    mode = normalize_scanner_mode(args.mode)
    if args.once and args.daemon:
        parser.error("Use either --once (Snapshot Audit) or --daemon (Live Research), not both.")
    if args.daemon and mode == "snapshot":
        parser.error(
            "Snapshot Audit is --once only. Static REST polling daemon was removed. "
            "Use --daemon --mode live for Live Research."
        )
    paper = bool(args.paper or args.execution == "paper")
    live = bool(args.daemon or mode == "live" or args.mode in {"live", "realtime"})
    if live and not args.once:
        service = ScannerService()
        asyncio.run(
            service.run_daemon(
                mode="live",
                paper=paper,
                max_market_pages=args.max_pages,
                market_limit=args.market_limit,
                sync_markets=not args.no_sync,
            )
        )
        return

    service = ScannerService()
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
