"""HTML daily report and CSV export."""

from __future__ import annotations

import csv
import json
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any
from zoneinfo import ZoneInfo

from jinja2 import Template
from sqlalchemy import desc, select

from polymarket_scanner.config import get_config
from polymarket_scanner.database import (
    ApiErrorRow,
    DailyReportRow,
    MarketRow,
    OpportunityEpisodeRow,
    OpportunityRow,
    PaperAccountRow,
    PaperTradeRow,
    ScannerRunRow,
    session_scope,
    utcnow,
)
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
    <div class="stat"><div>Daily realized P&amp;L (simulated)</div><div class="n">{{ daily_realized_pnl }}</div></div>
    <div class="stat"><div>Cumulative realized P&amp;L (simulated)</div><div class="n">{{ cumulative_realized_pnl }}</div></div>
    <div class="stat"><div>Available cash</div><div class="n">{{ available_cash }}</div></div>
    <div class="stat"><div>Occupied inventory cost</div><div class="n">{{ occupied_inventory }}</div></div>
    <div class="stat"><div>Marked inventory value</div><div class="n">{{ marked_inventory }}</div></div>
    <div class="stat"><div>Account equity</div><div class="n">{{ account_equity }}</div></div>
    <div class="stat"><div>Max drawdown</div><div class="n">{{ max_drawdown }}</div></div>
    <div class="stat"><div>Qualified episodes (first-seen)</div><div class="n">{{ qualified_episodes }}</div></div>
    <div class="stat"><div>Max single profit</div><div class="n">{{ max_single_profit }}</div></div>
    <div class="stat"><div>Max one-leg loss</div><div class="n">{{ max_one_leg_loss }}</div></div>
  </div>

  <h2>Scenario notes</h2>
  <ul>
    <li>Optimistic / Base / Pessimistic tick sums are <strong>not</strong> profit.</li>
    <li>Daily realized P&amp;L is the sum of PaperTradeRow.realized_pnl for this date (simulated).</li>
    <li>Cumulative realized P&amp;L is the paper account ledger, not the daily sum.</li>
    <li>Occupied inventory cost is not treated as realized loss.</li>
    <li>Qualified first-seen episodes: {{ pessimistic_profit }}</li>
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


def _report_tz() -> ZoneInfo:
    name = get_config().reporting.timezone or "UTC"
    try:
        return ZoneInfo(name)
    except Exception:
        return ZoneInfo("UTC")


def _day_bounds(report_date: str) -> tuple[datetime, datetime]:
    tz = _report_tz()
    start_local = datetime.fromisoformat(report_date).replace(tzinfo=tz)
    end_local = start_local + timedelta(days=1)
    return start_local.astimezone(timezone.utc), end_local.astimezone(timezone.utc)


def previous_report_date_due(
    *,
    now: datetime,
    last_report_date: str,
    timezone_name: str,
    report_hour: int,
) -> str | None:
    """If local date rolled past last_report_date, return that previous date to generate."""
    try:
        tz = ZoneInfo(timezone_name or "UTC")
    except Exception:
        tz = ZoneInfo("UTC")
    local = now.astimezone(tz) if now.tzinfo else now.replace(tzinfo=tz)
    today = local.date().isoformat()
    if today != last_report_date and local.hour >= int(report_hour):
        return last_report_date
    return None


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
                OpportunityRow.discovered_at < end,
            )
        ).all()

        runs = session.scalars(
            select(ScannerRunRow)
            .where(ScannerRunRow.started_at >= start)
            .order_by(desc(ScannerRunRow.started_at))
        ).all()
        live_run = next((r for r in runs if r.mode in {"live", "realtime"}), None)
        latest = live_run or (runs[0] if runs else None)
        if latest is not None and latest.mode in {"live", "realtime"} and latest.subscribed_markets:
            market_count = latest.subscribed_markets
        elif latest is not None:
            market_count = latest.subscribed_markets or latest.markets_synced or 0
        else:
            market_count = 0

        market_cat = {
            r.market_id: r.category or "unknown"
            for r in session.scalars(select(MarketRow)).all()
        }

        api_errors = len(
            session.scalars(
                select(ApiErrorRow).where(
                    ApiErrorRow.created_at >= start, ApiErrorRow.created_at < end
                )
            ).all()
        )

        raw_signals = len(ops)
        net_signals = sum(1 for o in ops if o.passes_rule_set and o.net_profitable)
        qualified_ops = [o for o in ops if o.passes_rule_set]
        base_profitable = sum(
            1 for o in qualified_ops if o.base_net and Decimal(o.base_net) > 0
        )
        pessimistic_profitable = sum(
            1 for o in qualified_ops if o.pessimistic_net and Decimal(o.pessimistic_net) > 0
        )

        paper = session.scalar(select(PaperAccountRow).limit(1))
        cumulative_pnl = paper.realized_pnl if paper else "0"
        cash = paper.cash if paper else "0"
        occupied = paper.occupied if paper else "0"
        marked = (paper.marked_inventory if paper else None) or occupied
        max_dd = paper.max_drawdown if paper else "0"
        equity = format(Decimal(cash or "0") + Decimal(marked or "0"), "f")
        day_trades = session.scalars(
            select(PaperTradeRow).where(
                PaperTradeRow.created_at >= start,
                PaperTradeRow.created_at < end,
            )
        ).all()
        daily_pnl = sum(
            (Decimal(t.realized_pnl or t.pnl or "0") for t in day_trades),
            Decimal("0"),
        )
        paper_pnl = format(daily_pnl, "f")
        ep_count = len(
            session.scalars(
                select(OpportunityEpisodeRow).where(
                    OpportunityEpisodeRow.first_seen_at >= start,
                    OpportunityEpisodeRow.first_seen_at < end,
                )
            ).all()
        )

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
            cat = market_cat.get(o.market_id) or "unknown"
            by_category[cat] = by_category.get(cat, 0) + 1

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
            total_sim_profit=paper_pnl,
            daily_realized_pnl=paper_pnl,
            cumulative_realized_pnl=cumulative_pnl,
            available_cash=cash,
            occupied_inventory=occupied,
            marked_inventory=marked,
            account_equity=equity,
            max_drawdown=max_dd,
            qualified_episodes=ep_count,
            max_single_profit=format(max_single, "f"),
            max_one_leg_loss=format(max_one_leg, "f"),
            optimistic_profit="n/a (do not sum ticks)",
            base_profit=f"daily realized {paper_pnl}; cumulative {cumulative_pnl}",
            pessimistic_profit=f"qualified episodes {ep_count}",
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
            total_sim_profit=paper_pnl,
            max_single_profit=format(max_single, "f"),
            max_one_leg_loss=format(max_one_leg, "f"),
            html_path=str(html_path),
            csv_path=str(csv_path),
            summary_json=json.dumps(
                {
                    "daily_realized_pnl": paper_pnl,
                    "cumulative_realized_pnl": cumulative_pnl,
                    "available_cash": cash,
                    "occupied_inventory": occupied,
                    "marked_inventory": marked,
                    "account_equity": equity,
                    "max_drawdown": max_dd,
                }
            ),
        )
        if existing:
            for k, v in fields.items():
                setattr(existing, k, v)
        else:
            session.add(DailyReportRow(report_date=report_date, **fields))

    logger.info("Generated daily report %s", report_date)
    return {
        "report_date": report_date,
        "html": str(html_path),
        "csv": str(csv_path),
        "daily_realized_pnl": paper_pnl,
    }
