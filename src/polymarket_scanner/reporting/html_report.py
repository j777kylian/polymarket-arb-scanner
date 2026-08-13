"""HTML daily report and CSV export."""

from __future__ import annotations

import csv
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

from jinja2 import Template
from sqlalchemy import select

from polymarket_scanner.config import ROOT_DIR, get_config
from polymarket_scanner.database import DailyReportRow, OpportunityRow, session_scope, utcnow
from polymarket_scanner.logging_config import get_logger

logger = get_logger(__name__)

DISCLAIMER = """
DISCLAIMER: These are historical or simulated results only. They do not represent
real executable trades. Multi-leg orders are not atomic; partial fills can turn an
apparent arbitrage into directional risk. This is not investment advice.
"""

HTML_TEMPLATE = Template(
    """
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8"/>
  <title>Polymarket Arb Scanner Daily Report — {{ report_date }}</title>
  <style>
    body { font-family: Georgia, serif; margin: 2rem; color: #1a1a1a; background: #fafafa; }
    h1,h2 { font-family: "Helvetica Neue", Arial, sans-serif; }
    .banner { background: #111; color: #f5f5f5; padding: 0.75rem 1rem; margin-bottom: 1.5rem; }
    .grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr)); gap: 1rem; }
    .stat { background: #fff; border: 1px solid #ddd; padding: 1rem; }
    .stat .n { font-size: 1.6rem; font-weight: 700; }
    table { border-collapse: collapse; width: 100%; background: #fff; margin: 1rem 0; }
    th, td { border: 1px solid #ddd; padding: 0.4rem 0.6rem; text-align: left; font-size: 0.9rem; }
    th { background: #eee; }
    .warn { background: #fff3cd; border: 1px solid #ffecb5; padding: 1rem; margin-top: 2rem; }
  </style>
</head>
<body>
  <div class="banner">READ-ONLY MODE — REAL TRADING DISABLED — SIMULATED / THEORETICAL VALUES ONLY</div>
  <h1>Daily Structural Arb Report</h1>
  <p>Date: <strong>{{ report_date }}</strong> · Generated (UTC): {{ generated_at }}</p>

  <h2>Summary</h2>
  <div class="grid">
    <div class="stat"><div>Markets scanned</div><div class="n">{{ markets_scanned }}</div></div>
    <div class="stat"><div>Raw signals</div><div class="n">{{ raw_signals }}</div></div>
    <div class="stat"><div>Net profitable</div><div class="n">{{ net_signals }}</div></div>
    <div class="stat"><div>Base profitable</div><div class="n">{{ base_profitable }}</div></div>
    <div class="stat"><div>Pessimistic profitable</div><div class="n">{{ pessimistic_profitable }}</div></div>
    <div class="stat"><div>Sim total profit (Base)</div><div class="n">{{ total_sim_profit }}</div></div>
    <div class="stat"><div>Max single profit</div><div class="n">{{ max_single_profit }}</div></div>
    <div class="stat"><div>Max one-leg loss</div><div class="n">{{ max_one_leg_loss }}</div></div>
  </div>

  <h2>Scenario profits</h2>
  <ul>
    <li>Optimistic (theoretical): {{ optimistic_profit }}</li>
    <li>Base (estimated/observed): {{ base_profit }}</li>
    <li>Pessimistic (estimated/observed): {{ pessimistic_profit }}</li>
  </ul>

  <h2>By fee status</h2>
  <ul>
    {% for k,v in by_fee.items() %}
      <li>{{ k }}: {{ v }}</li>
    {% endfor %}
  </ul>

  <h2>By category</h2>
  <ul>
    {% for k,v in by_category.items() %}
      <li>{{ k }}: {{ v }}</li>
    {% endfor %}
  </ul>

  <h2>Top 20 opportunities</h2>
  <table>
    <tr>
      <th>Time</th><th>Market</th><th>Qty</th><th>Gross</th><th>Fees</th>
      <th>Net</th><th>Base</th><th>Pess</th><th>Quality</th><th>Tags</th>
    </tr>
    {% for o in top20 %}
    <tr>
      <td>{{ o.discovered_at }}</td>
      <td>{{ o.question }}</td>
      <td>{{ o.quantity }}</td>
      <td>{{ o.gross_profit }}</td>
      <td>{{ o.fee_total }}</td>
      <td>{{ o.net_profit }}</td>
      <td>{{ o.base_net }}</td>
      <td>{{ o.pessimistic_net }}</td>
      <td>{{ o.simulation_quality }}</td>
      <td>{{ o.risk_tags }}</td>
    </tr>
    {% endfor %}
  </table>

  <h2>API errors / data gaps</h2>
  <p>API errors recorded this day: {{ api_errors }}</p>

  <div class="warn"><pre>{{ disclaimer }}</pre></div>
</body>
</html>
"""
)


def _day_bounds(report_date: str) -> tuple[datetime, datetime]:
    start = datetime.fromisoformat(report_date).replace(tzinfo=timezone.utc)
    end = start.replace(hour=23, minute=59, second=59, microsecond=999999)
    return start, end


def generate_daily_report(report_date: str | None = None) -> dict[str, Any]:
    cfg = get_config()
    report_date = report_date or utcnow().date().isoformat()
    start, end = _day_bounds(report_date)
    reports_dir = cfg.resolve_path(cfg.reporting.reports_dir)
    reports_dir.mkdir(parents=True, exist_ok=True)

    with session_scope() as session:
        ops = session.scalars(
            select(OpportunityRow).where(
                OpportunityRow.discovered_at >= start,
                OpportunityRow.discovered_at <= end,
            )
        ).all()
        from polymarket_scanner.database import ApiErrorRow, MarketRow, ScannerRunRow

        markets_scanned = session.scalar(
            select(ScannerRunRow).where(ScannerRunRow.started_at >= start).limit(1)
        )
        market_count = 0
        if markets_scanned:
            market_count = markets_scanned.markets_synced
        else:
            market_count = len(session.scalars(select(MarketRow)).all())

        api_errors = len(
            session.scalars(
                select(ApiErrorRow).where(
                    ApiErrorRow.created_at >= start, ApiErrorRow.created_at <= end
                )
            ).all()
        )

        raw_signals = len(ops)
        net_signals = sum(1 for o in ops if o.net_profitable)
        base_profitable = sum(
            1 for o in ops if o.base_net and Decimal(o.base_net) > 0
        )
        pessimistic_profitable = sum(
            1 for o in ops if o.pessimistic_net and Decimal(o.pessimistic_net) > 0
        )

        def sum_col(attr: str) -> Decimal:
            total = Decimal("0")
            for o in ops:
                v = getattr(o, attr)
                if v:
                    total += Decimal(v)
            return total

        opt_p = sum_col("optimistic_net")
        base_p = sum_col("base_net")
        pes_p = sum_col("pessimistic_net")
        max_single = max((Decimal(o.net_profit) for o in ops), default=Decimal("0"))
        max_one_leg = Decimal("0")
        for o in ops:
            tags = o.risk_tags_json or ""
            if "one-leg" in tags and o.pessimistic_net:
                max_one_leg = min(max_one_leg, Decimal(o.pessimistic_net))

        by_fee: dict[str, int] = {"fees_on": 0, "fees_off": 0, "unknown": 0}
        by_category: dict[str, int] = {}
        for o in ops:
            if o.fees_enabled is True:
                by_fee["fees_on"] += 1
            elif o.fees_enabled is False:
                by_fee["fees_off"] += 1
            else:
                by_fee["unknown"] += 1
            # category unknown on opportunity — bucket as unknown
            by_category["unknown"] = by_category.get("unknown", 0) + 1

        top20 = sorted(ops, key=lambda o: Decimal(o.net_profit), reverse=True)[:20]
        top_rows = [
            {
                "discovered_at": o.discovered_at.isoformat(),
                "question": (o.question or o.market_id)[:80],
                "quantity": o.quantity,
                "gross_profit": o.gross_profit,
                "fee_total": o.fee_total,
                "net_profit": o.net_profit,
                "base_net": o.base_net,
                "pessimistic_net": o.pessimistic_net,
                "simulation_quality": o.simulation_quality,
                "risk_tags": o.risk_tags_json,
            }
            for o in top20
        ]

        html = HTML_TEMPLATE.render(
            report_date=report_date,
            generated_at=utcnow().isoformat(),
            markets_scanned=market_count,
            raw_signals=raw_signals,
            net_signals=net_signals,
            base_profitable=base_profitable,
            pessimistic_profitable=pessimistic_profitable,
            total_sim_profit=format(base_p, "f"),
            max_single_profit=format(max_single, "f"),
            max_one_leg_loss=format(max_one_leg, "f"),
            optimistic_profit=format(opt_p, "f"),
            base_profit=format(base_p, "f"),
            pessimistic_profit=format(pes_p, "f"),
            by_fee=by_fee,
            by_category=by_category,
            top20=top_rows,
            api_errors=api_errors,
            disclaimer=DISCLAIMER.strip(),
        )

        html_path = reports_dir / f"daily_{report_date}.html"
        csv_path = reports_dir / f"daily_{report_date}.csv"
        html_path.write_text(html, encoding="utf-8")

        with csv_path.open("w", newline="", encoding="utf-8") as fh:
            writer = csv.DictWriter(
                fh,
                fieldnames=[
                    "discovered_at",
                    "market_id",
                    "question",
                    "direction",
                    "quantity",
                    "gross_profit",
                    "fee_total",
                    "net_profit",
                    "optimistic_net",
                    "base_net",
                    "pessimistic_net",
                    "simulation_quality",
                    "stale",
                    "risk_tags",
                ],
            )
            writer.writeheader()
            for o in ops:
                writer.writerow(
                    {
                        "discovered_at": o.discovered_at.isoformat(),
                        "market_id": o.market_id,
                        "question": o.question,
                        "direction": o.direction,
                        "quantity": o.quantity,
                        "gross_profit": o.gross_profit,
                        "fee_total": o.fee_total,
                        "net_profit": o.net_profit,
                        "optimistic_net": o.optimistic_net,
                        "base_net": o.base_net,
                        "pessimistic_net": o.pessimistic_net,
                        "simulation_quality": o.simulation_quality,
                        "stale": o.stale,
                        "risk_tags": o.risk_tags_json,
                    }
                )

        existing = session.scalar(
            select(DailyReportRow).where(DailyReportRow.report_date == report_date)
        )
        fields = dict(
            markets_scanned=market_count,
            raw_signals=raw_signals,
            net_signals=net_signals,
            base_profitable=base_profitable,
            pessimistic_profitable=pessimistic_profitable,
            total_sim_profit=format(base_p, "f"),
            max_single_profit=format(max_single, "f"),
            max_one_leg_loss=format(max_one_leg, "f"),
            html_path=str(html_path),
            csv_path=str(csv_path),
        )
        if existing:
            for k, v in fields.items():
                setattr(existing, k, v)
        else:
            session.add(DailyReportRow(report_date=report_date, **fields))

    logger.info("Generated daily report %s", report_date)
    return {"report_date": report_date, "html": str(html_path), "csv": str(csv_path)}
