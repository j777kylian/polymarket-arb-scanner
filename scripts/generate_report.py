#!/usr/bin/env python3
"""Generate HTML/CSV daily report."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from polymarket_scanner.database import init_db
from polymarket_scanner.logging_config import setup_logging
from polymarket_scanner.reporting.html_report import generate_daily_report


def main() -> None:
    setup_logging()
    init_db()
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True, help="YYYY-MM-DD")
    args = parser.parse_args()
    out = generate_daily_report(args.date)
    print(out)


if __name__ == "__main__":
    main()
