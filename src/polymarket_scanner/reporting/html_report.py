"""Self-contained offline HTML research report (Plotly JS inlined, no CDN)."""

from __future__ import annotations

import csv
import json
import re
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any
from zoneinfo import ZoneInfo

from jinja2 import Template
from sqlalchemy import desc, select

from polymarket_scanner.config import get_config
from polymarket_scanner.database import (
    AccountSnapshotRow,
    ApiErrorRow,
    DailyReportRow,
    MarketRow,
    OpportunityEpisodeRow,
    OpportunityRow,
    PaperAccountRow,
    PaperTradeRow,
    ScannerRunRow,
    StrategyAccountRow,
    StrategyEvalRow,
    StrategyTradeRow,
    session_scope,
    utcnow,
)
from polymarket_scanner.logging_config import get_logger

logger = get_logger(__name__)

DISCLAIMER = """
DISCLAIMER: These are historical or simulated results only. They do not represent
real executable trades. Multi-leg orders are not atomic; partial fills can turn an
apparent arbitrage into directional risk. This is not investment advice.
ALL CHARTS: SIMULATED / NOT EXECUTABLE PROFIT.
"""

SIM_LABEL = "SIMULATED / NOT EXECUTABLE PROFIT"

HTML_SHELL = Template(
    """
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8"/>
  <title>Polymarket Arb Research Report — {{ report_date }}</title>
  <style>
    :root { --bg:#f7f5f1; --ink:#1c1b19; --muted:#5c574f; --line:#d4cfc4; --accent:#0f5c4c; --warn:#7a3b12; }
    body { font-family: "IBM Plex Sans", "Segoe UI", sans-serif; margin:0; color:var(--ink); background:
      radial-gradient(circle at 10% 0%, #e8efe9 0%, transparent 40%),
      linear-gradient(180deg, #fbfaf7 0%, var(--bg) 40%); }
    .banner { background:#111; color:#f5f5f5; padding:0.85rem 1.25rem; font-size:0.92rem; letter-spacing:0.02em; }
    main { max-width:1180px; margin:0 auto; padding:1.5rem 1.25rem 3rem; }
    h1 { font-family:"IBM Plex Serif", Georgia, serif; font-size:2rem; margin:1rem 0 0.25rem; }
    h2 { margin-top:2rem; border-bottom:1px solid var(--line); padding-bottom:0.35rem; }
    h3 { margin-top:1.25rem; color:var(--muted); font-size:1rem; text-transform:uppercase; letter-spacing:0.04em; }
    .sub { color:var(--muted); margin-bottom:1.25rem; }
    .grid { display:grid; grid-template-columns:repeat(auto-fit,minmax(160px,1fr)); gap:0.75rem; }
    .stat { background:#fff; border:1px solid var(--line); padding:0.85rem; }
    .stat .n { font-size:1.35rem; font-weight:700; color:var(--accent); }
    .stat .l { font-size:0.78rem; color:var(--muted); }
    .sim { color:var(--warn); font-size:0.75rem; font-weight:600; }
    .chart { background:#fff; border:1px solid var(--line); margin:1rem 0; padding:0.5rem; overflow:auto; }
    table { border-collapse:collapse; width:100%; background:#fff; margin:0.75rem 0; font-size:0.85rem; }
    th, td { border:1px solid var(--line); padding:0.35rem 0.5rem; text-align:left; }
    th { background:#efeae2; position:sticky; top:0; }
    .warn { background:#fff4e8; border:1px solid #efd2b3; padding:1rem; margin-top:2rem; white-space:pre-wrap; }
    .insuf { background:#f3e8e4; border:1px solid #e0bdb0; padding:0.75rem 1rem; color:var(--warn); font-weight:700; }
    input[type=search] { width:100%; max-width:420px; padding:0.45rem 0.6rem; border:1px solid var(--line); }
    .mono { font-family: ui-monospace, SFMono-Regular, Menlo, monospace; font-size:0.8rem; }
  </style>
</head>
<body>
  <div class="banner">READ-ONLY · PAPER-ONLY · TRADING_ENABLED=False · {{ sim_label }}</div>
  <main>
    <h1>Research Report</h1>
    <p class="sub">Date <strong>{{ report_date }}</strong> · Generated UTC {{ generated_at }} · {{ sim_label }}</p>

    <h2>1. Data quality &amp; scan coverage</h2>
    <div class="grid">
      <div class="stat"><div class="l">Markets scanned</div><div class="n">{{ markets_scanned }}</div></div>
      <div class="stat"><div class="l">Raw ticks (OpportunityRow)</div><div class="n">{{ raw_ticks }}</div></div>
      <div class="stat"><div class="l">Unique episodes</div><div class="n">{{ unique_episodes }}</div></div>
      <div class="stat"><div class="l">Trade attempts</div><div class="n">{{ trade_attempts }}</div></div>
      <div class="stat"><div class="l">API / data-quality errors</div><div class="n">{{ api_errors }}</div></div>
      <div class="stat"><div class="l">Stale / skewed ticks</div><div class="n">{{ stale_skewed }}</div></div>
    </div>

    <h2>2. Unique episode opportunity funnel</h2>
    <div class="grid">
      {% for label, n in episode_funnel %}
      <div class="stat"><div class="l">{{ label }}</div><div class="n">{{ n }}</div><div class="sim">{{ sim_label }}</div></div>
      {% endfor %}
    </div>

    <h2>3. Paper execution funnel</h2>
    <div class="grid">
      {% for label, n in exec_funnel %}
      <div class="stat"><div class="l">{{ label }}</div><div class="n">{{ n }}</div><div class="sim">{{ sim_label }}</div></div>
      {% endfor %}
    </div>

    <h2>4. Equity / cash / inventory / drawdown</h2>
    <p class="sim">{{ sim_label }}</p>
    <div class="chart">{{ equity_chart|safe }}</div>

    <h2>5. Realized / unrealized P&amp;L</h2>
    <div class="grid">
      <div class="stat"><div class="l">Daily realized (by realized_at)</div><div class="n">{{ daily_realized_pnl }}</div><div class="sim">{{ sim_label }}</div></div>
      <div class="stat"><div class="l">Cumulative realized</div><div class="n">{{ cumulative_realized_pnl }}</div><div class="sim">{{ sim_label }}</div></div>
      <div class="stat"><div class="l">Unrealized (open marks)</div><div class="n">{{ unrealized_pnl }}</div><div class="sim">{{ sim_label }}</div></div>
      <div class="stat"><div class="l">Equity</div><div class="n">{{ account_equity }}</div></div>
      <div class="stat"><div class="l">Cash</div><div class="n">{{ available_cash }}</div></div>
      <div class="stat"><div class="l">Marked inventory</div><div class="n">{{ marked_inventory }}</div></div>
      <div class="stat"><div class="l">Max drawdown</div><div class="n">{{ max_drawdown }}</div></div>
    </div>
    <div class="chart">{{ pnl_chart|safe }}</div>

    <h2>6. Expected edge vs realized P&amp;L</h2>
    <p class="sim">{{ sim_label }}</p>
    <div class="chart">{{ edge_chart|safe }}</div>

    <h2>7. Latency vs edge retention / P&amp;L</h2>
    <p class="sim">{{ sim_label }}</p>
    <div class="chart">{{ latency_chart|safe }}</div>

    <h2>8. Rejection reason distribution</h2>
    <div class="chart">{{ reject_chart|safe }}</div>

    <h2>9. Residual inventory risk</h2>
    <div class="grid">
      <div class="stat"><div class="l">Residual trades</div><div class="n">{{ residual_count }}</div></div>
      <div class="stat"><div class="l">Residual qty</div><div class="n">{{ residual_qty }}</div></div>
      <div class="stat"><div class="l">Avg hold hours (open)</div><div class="n">{{ residual_hold_h }}</div></div>
      <div class="stat"><div class="l">Max one-leg loss (actual trades)</div><div class="n">{{ max_one_leg_loss }}</div><div class="sim">{{ sim_label }}</div></div>
    </div>

    <h2>10. Groupings</h2>
    <h3>Category</h3><ul>{% for k,v in by_category.items() %}<li>{{ k }}: {{ v }}</li>{% endfor %}</ul>
    <h3>Fee status</h3><ul>{% for k,v in by_fee.items() %}<li>{{ k }}: {{ v }}</li>{% endfor %}</ul>
    <h3>Price band</h3><ul>{% for k,v in by_price_band.items() %}<li>{{ k }}: {{ v }}</li>{% endfor %}</ul>
    <h3>Liquidity band</h3><ul>{% for k,v in by_liq.items() %}<li>{{ k }}: {{ v }}</li>{% endfor %}</ul>
    <h3>Hour of day (local)</h3><ul>{% for k,v in by_hour.items() %}<li>{{ k }}: {{ v }}</li>{% endfor %}</ul>

    <h2>11. Live vs shadow strategies</h2>
    <p class="sim">{{ sim_label }}</p>
    <table>
      <tr><th>Account</th><th>Trades</th><th>Realized</th><th>Cash</th><th>Equity-ish</th><th>Rejects</th></tr>
      {% for r in strategy_compare %}
      <tr>
        <td>{{ r.name }}</td><td>{{ r.trades }}</td><td>{{ r.realized }}</td>
        <td>{{ r.cash }}</td><td>{{ r.equity }}</td><td>{{ r.rejects }}</td>
      </tr>
      {% endfor %}
    </table>

    <h2>12. Walk-forward training / validation</h2>
    {% if walk_forward_insufficient %}
    <div class="insuf">INSUFFICIENT SAMPLE — no parameter recommendations generated.</div>
    {% endif %}
    <table>
      <tr><th>Created</th><th>Train window</th><th>Val window</th><th>Samples</th><th>Insufficient</th><th>Recommended</th><th>Note</th></tr>
      {% for e in walk_forward_rows %}
      <tr>
        <td class="mono">{{ e.created_at }}</td>
        <td class="mono">{{ e.train }}</td>
        <td class="mono">{{ e.val }}</td>
        <td>{{ e.sample_count }}</td>
        <td>{{ e.insufficient }}</td>
        <td>{{ e.recommended }}</td>
        <td>{{ e.note }}</td>
      </tr>
      {% endfor %}
    </table>

    <h2>13. Trade audit (filterable)</h2>
    <input id="auditFilter" type="search" placeholder="Filter market / status / reject / strategy…" oninput="filterAudit()"/>
    <table id="auditTable">
      <tr>
        <th>ID</th><th>Kind</th><th>Strategy</th><th>Market</th><th>Status</th><th>Leg</th>
        <th>Realized</th><th>Expected</th><th>Reject</th><th>s2f_ms</th><th>f2s_ms</th><th>realized_at</th>
      </tr>
      {% for t in audit_trades %}
      <tr>
        <td>{{ t.id }}</td><td>{{ t.kind }}</td><td>{{ t.strategy }}</td><td>{{ t.market }}</td>
        <td>{{ t.status }}</td><td>{{ t.leg }}</td><td>{{ t.realized }}</td><td>{{ t.expected }}</td>
        <td>{{ t.reject }}</td><td>{{ t.s2f }}</td><td>{{ t.f2s }}</td><td class="mono">{{ t.realized_at }}</td>
      </tr>
      {% endfor %}
    </table>

    <h2>14. Strategy recommendations</h2>
    {% if recommendations_blocked %}
    <div class="insuf">INSUFFICIENT SAMPLE — hard thresholds not met; no strategy parameter recommendations.</div>
    {% else %}
    <ul>{% for r in recommendations %}<li>{{ r }}</li>{% endfor %}</ul>
    {% endif %}

    <h2>Top unique episodes (no tick duplicates)</h2>
    <table>
      <tr><th>First seen</th><th>Market</th><th>Qty</th><th>Net</th><th>Base</th><th>Pess</th><th>Quality</th></tr>
      {% for o in top_episodes %}
      <tr>
        <td class="mono">{{ o.first_seen }}</td><td>{{ o.question }}</td><td>{{ o.quantity }}</td>
        <td>{{ o.net_profit }}</td><td>{{ o.base_net }}</td><td>{{ o.pessimistic_net }}</td><td>{{ o.quality }}</td>
      </tr>
      {% endfor %}
    </table>

    <div class="warn">{{ disclaimer }}</div>
  </main>
  <script>
  function filterAudit(){
    const q=(document.getElementById('auditFilter').value||'').toLowerCase();
    const rows=document.querySelectorAll('#auditTable tr');
    for(let i=1;i<rows.length;i++){
      const t=rows[i].innerText.toLowerCase();
      rows[i].style.display=t.includes(q)?'':'none';
    }
  }
  </script>
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
    try:
        tz = ZoneInfo(timezone_name or "UTC")
    except Exception:
        tz = ZoneInfo("UTC")
    local = now.astimezone(tz) if now.tzinfo else now.replace(tzinfo=tz)
    today = local.date().isoformat()
    if today != last_report_date and local.hour >= int(report_hour):
        return last_report_date
    return None


def _d(v: str | None, default: str = "0") -> Decimal:
    return Decimal(v or default)


def _fig_html(fig: Any) -> str:
    try:
        return fig.to_html(include_plotlyjs=True, full_html=False, config={"displayModeBar": False})
    except Exception as exc:  # pragma: no cover
        return f"<pre>chart unavailable: {exc}</pre>"


def _empty_fig(title: str) -> str:
    try:
        import plotly.graph_objects as go

        fig = go.Figure()
        fig.update_layout(
            title=f"{title} — {SIM_LABEL}",
            annotations=[
                dict(text="No data", showarrow=False, xref="paper", yref="paper", x=0.5, y=0.5)
            ],
            height=280,
            margin=dict(l=40, r=20, t=50, b=40),
        )
        return _fig_html(fig)
    except Exception:
        return f"<p>{title}: no data ({SIM_LABEL})</p>"


def _price_band(yes: Decimal | None, no: Decimal | None) -> str:
    mid: Decimal | None = None
    if yes is not None and no is not None:
        mid = (yes + no) / 2
    elif yes is not None:
        mid = yes
    elif no is not None:
        mid = no
    if mid is None:
        return "unknown"
    if mid < Decimal("0.2"):
        return "0-0.2"
    if mid < Decimal("0.4"):
        return "0.2-0.4"
    if mid < Decimal("0.6"):
        return "0.4-0.6"
    if mid < Decimal("0.8"):
        return "0.6-0.8"
    return "0.8-1.0"


ZERO = Decimal("0")


def _aware(dt: datetime | None) -> datetime | None:
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt


def _in_range(dt: datetime | None, start: datetime, end: datetime) -> bool:
    adt = _aware(dt)
    return adt is not None and start <= adt < end


def generate_daily_report(report_date: str | None = None) -> dict[str, Any]:
    cfg = get_config()
    report_date = report_date or utcnow().date().isoformat()
    start, end = _day_bounds(report_date)
    reports_dir = cfg.resolve_path(cfg.reporting.reports_dir)
    reports_dir.mkdir(parents=True, exist_ok=True)
    min_samples = int(cfg.scanner.min_walk_forward_trades)

    with session_scope() as session:
        ops = list(
            session.scalars(
                select(OpportunityRow).where(
                    OpportunityRow.discovered_at >= start,
                    OpportunityRow.discovered_at < end,
                )
            ).all()
        )
        episodes = list(
            session.scalars(
                select(OpportunityEpisodeRow).where(
                    OpportunityEpisodeRow.first_seen_at >= start,
                    OpportunityEpisodeRow.first_seen_at < end,
                )
            ).all()
        )
        paper_trades = list(session.scalars(select(PaperTradeRow)).all())
        day_paper_created = [t for t in paper_trades if _in_range(t.created_at, start, end)]
        # Daily realized by realized_at / settled_at (NOT created_at)
        day_realized_trades = [
            t
            for t in paper_trades
            if _in_range(t.realized_at, start, end) or _in_range(t.settled_at, start, end)
        ]
        strat_trades = list(session.scalars(select(StrategyTradeRow)).all())
        day_strat = [t for t in strat_trades if _in_range(t.created_at, start, end)]
        snapshots = list(
            session.scalars(select(AccountSnapshotRow).order_by(AccountSnapshotRow.created_at)).all()
        )
        snapshots = [s for s in snapshots if _in_range(s.created_at, start, end)]
        evals = list(
            session.scalars(select(StrategyEvalRow).order_by(desc(StrategyEvalRow.created_at)).limit(20)).all()
        )
        runs = list(
            session.scalars(
                select(ScannerRunRow)
                .where(ScannerRunRow.started_at >= start)
                .order_by(desc(ScannerRunRow.started_at))
            ).all()
        )
        live_run = next((r for r in runs if r.mode in {"live", "realtime"}), None)
        latest = live_run or (runs[0] if runs else None)
        if latest is not None and latest.mode in {"live", "realtime"} and latest.subscribed_markets:
            market_count = latest.subscribed_markets
        elif latest is not None:
            market_count = latest.subscribed_markets or latest.markets_synced or 0
        else:
            market_count = 0

        market_cat = {
            r.market_id: r.category or "unknown" for r in session.scalars(select(MarketRow)).all()
        }
        api_errors = len(
            session.scalars(
                select(ApiErrorRow).where(ApiErrorRow.created_at >= start, ApiErrorRow.created_at < end)
            ).all()
        )
        paper = session.scalar(select(PaperAccountRow).limit(1))
        strat_accts = list(session.scalars(select(StrategyAccountRow)).all())

        # Unique episode funnel (episode-level, not tick-level)
        qualified_eps = 0
        net_pos_eps = 0
        base_pos_eps = 0
        pess_pos_eps = 0
        for e in episodes:
            oid = e.last_opportunity_id
            o = session.get(OpportunityRow, oid) if oid else None
            if o is None:
                continue
            if o.passes_rule_set:
                qualified_eps += 1
            if o.net_profitable:
                net_pos_eps += 1
            if o.base_net and _d(o.base_net) > 0:
                base_pos_eps += 1
            if o.pessimistic_net and _d(o.pessimistic_net) > 0:
                pess_pos_eps += 1

        episode_funnel = [
            ("Unique episodes", len(episodes)),
            ("Rule-qualified episodes", qualified_eps),
            ("Net>0 episodes", net_pos_eps),
            ("Base>0 episodes", base_pos_eps),
            ("Pess>0 episodes", pess_pos_eps),
        ]

        reject_counter: Counter[str] = Counter()
        status_counter: Counter[str] = Counter()
        for pt in day_paper_created:
            status_counter[pt.status or "unknown"] += 1
            if pt.reject_reason:
                reject_counter[pt.reject_reason] += 1
        for st_row in day_strat:
            status_counter[f"shadow:{st_row.status or 'unknown'}"] += 1
            if st_row.reject_reason:
                reject_counter[st_row.reject_reason] += 1

        exec_funnel = [
            ("Attempts (live)", len(day_paper_created)),
            ("Rejected", status_counter.get("rejected", 0)),
            ("First filled+", sum(1 for t in day_paper_created if (t.leg_state or "") not in {"", "SIGNALLED", None} and t.status != "rejected")),
            ("Merged/closed", status_counter.get("merged", 0) + status_counter.get("closed", 0)),
            ("One-leg / residual", status_counter.get("one_leg", 0) + status_counter.get("one_leg_merged", 0)),
        ]

        daily_pnl = sum((_d(t.realized_pnl or t.pnl) for t in day_realized_trades), ZERO)
        # Also include strategy day realized by realized_at
        day_strat_realized = [
            t
            for t in strat_trades
            if _in_range(t.realized_at, start, end) or _in_range(t.settled_at, start, end)
        ]
        daily_pnl += sum((_d(t.realized_pnl) for t in day_strat_realized), ZERO)
        paper_pnl = format(daily_pnl, "f")

        cash = paper.cash if paper else "0"
        occupied = paper.occupied if paper else "0"
        marked = (paper.marked_inventory if paper else None) or occupied
        max_dd = paper.max_drawdown if paper else "0"
        cumulative_pnl = paper.realized_pnl if paper else "0"
        equity = format(_d(cash) + _d(marked), "f")
        unrealized_pnl = format(_d(marked) - _d(occupied), "f")

        stale_skewed = sum(
            1
            for o in ops
            if o.stale or (o.book_skew_ms is not None and o.book_skew_ms > 0 and getattr(o, "books_ready", True) is False)
            or (o.book_skew_ms is not None and float(o.book_skew_ms) > float(cfg.scanner.max_book_skew_ms))
        )

        # Top unique episodes (dedupe by episode id / market+direction)
        top_episode_rows: list[dict[str, Any]] = []
        seen_ep: set[Any] = set()
        ep_candidates: list[tuple[Decimal, OpportunityEpisodeRow, OpportunityRow | None]] = []
        for e in episodes:
            o = session.get(OpportunityRow, e.last_opportunity_id) if e.last_opportunity_id else None
            net = _d(o.net_profit) if o else ZERO
            ep_candidates.append((net, e, o))
        for net, e, o in sorted(ep_candidates, key=lambda x: x[0], reverse=True):
            key = e.id
            if key in seen_ep:
                continue
            seen_ep.add(key)
            top_episode_rows.append(
                {
                    "first_seen": e.first_seen_at.isoformat() if e.first_seen_at else "",
                    "question": ((o.question if o else None) or e.market_id)[:80],
                    "quantity": o.quantity if o else "",
                    "net_profit": o.net_profit if o else "0",
                    "base_net": o.base_net if o else "",
                    "pessimistic_net": o.pessimistic_net if o else "",
                    "quality": o.simulation_quality if o else "",
                }
            )
            if len(top_episode_rows) >= 20:
                break

        # one-leg loss from actual trades
        max_one_leg = ZERO
        all_trades: list[Any] = list(paper_trades) + list(strat_trades)
        for tr in all_trades:
            st_status = (tr.status or "").lower()
            ls = (getattr(tr, "leg_state", None) or "").upper()
            if "one_leg" in st_status or "RESIDUAL" in ls or st_status == "one_leg":
                pnl = _d(getattr(tr, "realized_pnl", None) or getattr(tr, "pnl", None))
                if pnl < max_one_leg:
                    max_one_leg = pnl

        by_fee: dict[str, int] = {"fees_on": 0, "fees_off": 0, "unknown": 0}
        by_category: dict[str, int] = {}
        by_price_band: dict[str, int] = defaultdict(int)
        by_liq: dict[str, int] = defaultdict(int)
        by_hour: dict[str, int] = defaultdict(int)
        tz = _report_tz()
        for o in ops:
            if o.fees_enabled is True:
                by_fee["fees_on"] += 1
            elif o.fees_enabled is False:
                by_fee["fees_off"] += 1
            else:
                by_fee["unknown"] += 1
            cat = market_cat.get(o.market_id) or "unknown"
            by_category[cat] = by_category.get(cat, 0) + 1
            y = _d(o.yes_vwap) if o.yes_vwap else None
            n = _d(o.no_vwap) if o.no_vwap else None
            by_price_band[_price_band(y, n)] += 1
            qty = _d(o.quantity)
            if qty < 10:
                by_liq["qty<10"] += 1
            elif qty < 50:
                by_liq["qty 10-50"] += 1
            else:
                by_liq["qty>=50"] += 1
            if o.discovered_at:
                local = o.discovered_at.astimezone(tz)
                by_hour[f"{local.hour:02d}:00"] += 1

        # Strategy compare
        strategy_compare: list[dict[str, Any]] = []
        live_rejects = sum(1 for t in day_paper_created if t.status == "rejected")
        strategy_compare.append(
            {
                "name": "live",
                "trades": len(day_paper_created),
                "realized": cumulative_pnl,
                "cash": cash,
                "equity": equity,
                "rejects": live_rejects,
            }
        )
        for acct in strat_accts:
            shadow_day = [
                t
                for t in day_strat
                if t.strategy_id == acct.strategy_id and t.strategy_version == acct.version
            ]
            strategy_compare.append(
                {
                    "name": f"{acct.strategy_id}@v{acct.version}",
                    "trades": len(shadow_day),
                    "realized": acct.realized_pnl,
                    "cash": acct.cash,
                    "equity": format(_d(acct.cash) + _d(acct.marked_inventory), "f"),
                    "rejects": sum(1 for t in shadow_day if t.status == "rejected"),
                }
            )

        walk_forward_rows: list[dict[str, Any]] = []
        walk_forward_insufficient = False
        for ev in evals:
            if ev.insufficient_sample:
                walk_forward_insufficient = True
            walk_forward_rows.append(
                {
                    "created_at": ev.created_at.isoformat() if ev.created_at else "",
                    "train": f"{ev.training_start} → {ev.training_end}",
                    "val": f"{ev.validation_start} → {ev.validation_end}",
                    "sample_count": ev.sample_count,
                    "insufficient": ev.insufficient_sample,
                    "recommended": f"{ev.recommended_strategy_id}@v{ev.recommended_version}"
                    if ev.recommended_strategy_id
                    else "—",
                    "note": ev.note or "",
                }
            )

        recommendations_blocked = (
            walk_forward_insufficient
            or len(day_paper_created) + len(day_strat) < min_samples
            or (bool(evals[:1]) and bool(evals[0].insufficient_sample))
        )
        if not evals and (len(day_paper_created) + len(day_strat) < min_samples):
            recommendations_blocked = True
        recommendations: list[str] = []
        if not recommendations_blocked and evals and evals[0].recommended_strategy_id:
            recommendations.append(
                f"Hard-threshold pick: {evals[0].recommended_strategy_id}@v{evals[0].recommended_version} "
                f"(train samples={evals[0].sample_count})"
            )
        elif recommendations_blocked:
            recommendations = []

        residual_trades: list[Any] = [
            t
            for t in all_trades
            if (t.status or "") in {"one_leg", "one_leg_merged"}
            or "RESIDUAL" in (getattr(t, "leg_state", None) or "").upper()
        ]
        residual_qty = sum((_d(t.remaining_inventory) for t in residual_trades), ZERO)
        hold_hours: list[float] = []
        now = utcnow()
        for t in residual_trades:
            start_t = getattr(t, "first_leg_time", None) or t.created_at
            if start_t:
                hold_hours.append((now - start_t).total_seconds() / 3600.0)
        residual_hold = format(Decimal(str(sum(hold_hours) / len(hold_hours))) if hold_hours else ZERO, "f")

        audit_trades: list[dict[str, Any]] = []
        for pt in sorted(day_paper_created, key=lambda x: x.id or 0, reverse=True)[:500]:
            audit_trades.append(
                {
                    "id": pt.id,
                    "kind": "live",
                    "strategy": pt.strategy_id or "live",
                    "market": pt.market_id,
                    "status": pt.status,
                    "leg": pt.leg_state,
                    "realized": pt.realized_pnl or pt.pnl,
                    "expected": pt.expected_net_profit,
                    "reject": pt.reject_reason or "",
                    "s2f": pt.signal_to_first_ms,
                    "f2s": pt.first_to_second_ms,
                    "realized_at": pt.realized_at.isoformat() if pt.realized_at else "",
                }
            )
        for st_row in sorted(day_strat, key=lambda x: x.id or 0, reverse=True)[:500]:
            audit_trades.append(
                {
                    "id": st_row.id,
                    "kind": "strategy",
                    "strategy": f"{st_row.strategy_id}@v{st_row.strategy_version}",
                    "market": st_row.market_id,
                    "status": st_row.status,
                    "leg": st_row.leg_state,
                    "realized": st_row.realized_pnl,
                    "expected": "",
                    "reject": st_row.reject_reason or "",
                    "s2f": st_row.signal_to_first_ms,
                    "f2s": st_row.first_to_second_ms,
                    "realized_at": st_row.realized_at.isoformat() if st_row.realized_at else "",
                }
            )

        # Charts
        try:
            import plotly.graph_objects as go
            from plotly.subplots import make_subplots

            live_snaps = [s for s in snapshots if s.account_kind == "live"]
            if live_snaps:
                fig_eq = make_subplots(specs=[[{"secondary_y": True}]])
                xs = [s.created_at.isoformat() for s in live_snaps]
                fig_eq.add_trace(go.Scatter(x=xs, y=[float(_d(s.equity)) for s in live_snaps], name="equity"), secondary_y=False)
                fig_eq.add_trace(go.Scatter(x=xs, y=[float(_d(s.cash)) for s in live_snaps], name="cash"), secondary_y=False)
                fig_eq.add_trace(
                    go.Scatter(x=xs, y=[float(_d(s.marked_inventory)) for s in live_snaps], name="marked_inv"),
                    secondary_y=False,
                )
                fig_eq.add_trace(
                    go.Scatter(x=xs, y=[float(_d(s.drawdown)) for s in live_snaps], name="drawdown"),
                    secondary_y=True,
                )
                fig_eq.update_layout(title=f"Account time series — {SIM_LABEL}", height=360, margin=dict(l=40, r=20, t=50, b=40))
                equity_chart = _fig_html(fig_eq)
                fig_pnl = go.Figure()
                fig_pnl.add_trace(
                    go.Scatter(x=xs, y=[float(_d(s.realized_pnl)) for s in live_snaps], name="realized")
                )
                fig_pnl.add_trace(
                    go.Scatter(x=xs, y=[float(_d(s.unrealized_pnl)) for s in live_snaps], name="unrealized")
                )
                fig_pnl.update_layout(title=f"P&L decomposition — {SIM_LABEL}", height=320)
                pnl_chart = _fig_html(fig_pnl)
            else:
                equity_chart = _empty_fig("Equity/cash/inventory")
                pnl_chart = _empty_fig("P&L")

            filled = [t for t in day_paper_created if t.status != "rejected"]
            if filled:
                fig_edge = go.Figure()
                fig_edge.add_trace(
                    go.Scatter(
                        x=[float(_d(t.expected_net_profit)) for t in filled],
                        y=[float(_d(t.realized_pnl or t.pnl)) for t in filled],
                        mode="markers",
                        name="trades",
                    )
                )
                fig_edge.update_layout(
                    title=f"Expected edge vs realized — {SIM_LABEL}",
                    xaxis_title="expected_net_profit",
                    yaxis_title="realized_pnl",
                    height=320,
                )
                edge_chart = _fig_html(fig_edge)
                fig_lat = go.Figure()
                fig_lat.add_trace(
                    go.Scatter(
                        x=[t.signal_to_first_ms or 0 for t in filled],
                        y=[float(_d(t.realized_pnl or t.pnl)) for t in filled],
                        mode="markers",
                        name="s2f",
                    )
                )
                fig_lat.add_trace(
                    go.Scatter(
                        x=[t.first_to_second_ms or 0 for t in filled],
                        y=[float(_d(t.realized_pnl or t.pnl)) for t in filled],
                        mode="markers",
                        name="f2s",
                    )
                )
                fig_lat.update_layout(title=f"Latency vs P&L — {SIM_LABEL}", xaxis_title="ms", height=320)
                latency_chart = _fig_html(fig_lat)
            else:
                edge_chart = _empty_fig("Expected vs realized")
                latency_chart = _empty_fig("Latency vs P&L")

            if reject_counter:
                fig_rj = go.Figure(
                    data=[
                        go.Bar(x=list(reject_counter.keys()), y=list(reject_counter.values()), name="rejects")
                    ]
                )
                fig_rj.update_layout(title=f"Rejection reasons — {SIM_LABEL}", height=320)
                reject_chart = _fig_html(fig_rj)
            else:
                reject_chart = _empty_fig("Rejection reasons")
        except Exception as exc:
            logger.warning("plotly charts failed: %s", exc)
            equity_chart = pnl_chart = edge_chart = latency_chart = reject_chart = (
                f"<pre>charts unavailable offline build: {exc}</pre>"
            )

        html = HTML_SHELL.render(
            report_date=report_date,
            generated_at=utcnow().isoformat(),
            sim_label=SIM_LABEL,
            markets_scanned=market_count,
            raw_ticks=len(ops),
            unique_episodes=len(episodes),
            trade_attempts=len(day_paper_created) + len(day_strat),
            api_errors=api_errors,
            stale_skewed=stale_skewed,
            episode_funnel=episode_funnel,
            exec_funnel=exec_funnel,
            equity_chart=equity_chart,
            pnl_chart=pnl_chart,
            edge_chart=edge_chart,
            latency_chart=latency_chart,
            reject_chart=reject_chart,
            daily_realized_pnl=paper_pnl,
            cumulative_realized_pnl=cumulative_pnl,
            unrealized_pnl=unrealized_pnl,
            account_equity=equity,
            available_cash=cash,
            marked_inventory=marked,
            max_drawdown=max_dd,
            residual_count=len(residual_trades),
            residual_qty=format(residual_qty, "f"),
            residual_hold_h=residual_hold,
            max_one_leg_loss=format(max_one_leg, "f"),
            by_category=by_category,
            by_fee=by_fee,
            by_price_band=dict(by_price_band),
            by_liq=dict(by_liq),
            by_hour=dict(sorted(by_hour.items())),
            strategy_compare=strategy_compare,
            walk_forward_rows=walk_forward_rows,
            walk_forward_insufficient=walk_forward_insufficient or recommendations_blocked,
            audit_trades=audit_trades,
            recommendations_blocked=recommendations_blocked,
            recommendations=recommendations,
            top_episodes=top_episode_rows,
            disclaimer=DISCLAIMER.strip(),
        )

        # Offline guarantee: no external script/cdn refs
        if re.search(r"https?://cdn\.|cdn\.plot\.ly|unpkg\.com|jsdelivr", html, re.I):
            html = re.sub(
                r"<script[^>]+src=[\"']https?://[^\"']+[\"'][^>]*></script>",
                "<!-- external script stripped for offline -->",
                html,
                flags=re.I,
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

        existing = session.scalar(select(DailyReportRow).where(DailyReportRow.report_date == report_date))
        fields = dict(
            markets_scanned=market_count,
            raw_signals=len(ops),
            net_signals=net_pos_eps,
            base_profitable=base_pos_eps,
            pessimistic_profitable=pess_pos_eps,
            total_sim_profit=paper_pnl,
            max_single_profit=format(max((_d(o.net_profit) for o in ops), default=ZERO), "f"),
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
                    "unique_episodes": len(episodes),
                    "raw_ticks": len(ops),
                    "trade_attempts": len(day_paper_created) + len(day_strat),
                    "insufficient_sample": recommendations_blocked,
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
