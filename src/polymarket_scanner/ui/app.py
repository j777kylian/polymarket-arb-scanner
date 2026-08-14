"""Streamlit UI — read-only Polymarket structural arb scanner."""

from __future__ import annotations

import asyncio
import json
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from sqlalchemy import desc, select

from polymarket_scanner.config import get_config
from polymarket_scanner.database import (
    DailyReportRow,
    OpportunityEpisodeRow,
    OpportunityRow,
    PaperTradeRow,
    SimulationRunRow,
    ensure_utc,
    init_db,
    session_scope,
    set_setting,
    utcnow,
)
from polymarket_scanner.discovery.market_discovery import load_markets_from_db
from polymarket_scanner.discovery.orderbook_collector import latest_books_for_market
from polymarket_scanner.logging_config import setup_logging
from polymarket_scanner.models import RuleCondition, RuleSetModel
from polymarket_scanner.polymarket_links import polymarket_market_url
from polymarket_scanner.reporting.html_report import generate_daily_report
from polymarket_scanner.safety import TRADING_ENABLED, assert_trading_disabled
from polymarket_scanner.scanners.rule_engine import (
    delete_rule_set,
    duplicate_rule_set,
    explain_filter,
    export_rule_set_json,
    filter_opportunities,
    import_rule_set_json,
    load_rule_sets_from_db,
    save_rule_set,
)
from polymarket_scanner.scheduler import (
    CLOCK_SKEW_WARNING,
    get_dashboard_stats,
    latency_sufficiency_label,
)
from polymarket_scanner.simulation.execution_simulator import simulate_forward
from polymarket_scanner.simulation.scenario_profiles import ScenarioProfile
from polymarket_scanner.ui.scanner_control import (
    ScannerParams,
    build_daemon_cmd,
    get_recent_live_data,
    get_scanner_status,
    load_params_from_settings,
    read_log_tail,
    run_snapshot_once,
    start_live_daemon,
    stop_scanner,
)

assert_trading_disabled()
setup_logging()
init_db()
cfg = get_config()

st.set_page_config(
    page_title=cfg.ui.title,
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown(
    """
    <div style="background:#111;color:#f5f5f5;padding:0.6rem 1rem;margin-bottom:1rem;
    font-family:monospace;letter-spacing:0.04em;">
    READ-ONLY MODE — REAL TRADING DISABLED · No wallets · No private keys · No order placement
    </div>
    """,
    unsafe_allow_html=True,
)

PAGES = [
    "Dashboard",
    "Markets",
    "Opportunities",
    "Simulator",
    "Paper",
    "Strategies",
    "Rules",
    "Reports",
    "Settings",
]
page = st.sidebar.radio("Navigation", PAGES)

if "scanner_proc" not in st.session_state:
    st.session_state.scanner_proc = None
if "live_refresh" not in st.session_state:
    st.session_state.live_refresh = False
if "scanner_params" not in st.session_state:
    st.session_state.scanner_params = load_params_from_settings()
if "phase_params" not in st.session_state:
    st.session_state.phase_params = st.session_state.scanner_params

_scan_status = get_scanner_status(st.session_state.scanner_proc)
if _scan_status.running and not st.session_state.live_refresh:
    st.session_state.live_refresh = True

st.sidebar.markdown("### Scanner status")
if _scan_status.running:
    st.sidebar.success(
        f"Running · {_scan_status.product or _scan_status.mode} · pid={_scan_status.pid or '?'}"
    )
else:
    st.sidebar.info("Stopped")
st.sidebar.caption(_scan_status.message)
st.session_state.live_refresh = st.sidebar.checkbox(
    "Live refresh (5s)", value=st.session_state.live_refresh
)


def _run_async(coro):
    return asyncio.run(coro)


def _render_metrics(stats: dict) -> None:
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Monitored markets", stats["markets"])
    c2.metric("Open episodes (active)", stats["active_opportunities"])
    c3.metric("Raw signals today", stats.get("raw_signals_today", stats["signals_today"]))
    c4.metric("API errors today", stats["api_errors_today"])

    c5, c6, c7 = st.columns(3)
    c5.metric("Qualified signals today", stats.get("qualified_today") or 0)
    c6.metric("First-seen episodes today", stats.get("qualified_episodes_today") or 0)
    c7.metric("Paper realized P&L", str(stats.get("paper_realized_pnl") or stats.get("paper_pnl") or "0"))
    st.caption(
        "Active = currently open episodes, not historical OpportunityRow rows. "
        "Do not treat summed per-tick base_net as profit. Paper P&L is simulated only."
    )
    p1, p2, p3, p4 = st.columns(4)
    p1.metric("Daily realized P&L", str(stats.get("paper_daily_realized_pnl") or "0"))
    p2.metric("Cumulative realized P&L", str(stats.get("paper_cumulative_realized_pnl") or stats.get("paper_realized_pnl") or "0"))
    p3.metric("Available cash", str(stats.get("paper_cash") or "n/a"))
    p4.metric("Account equity", str(stats.get("paper_equity") or "n/a"))
    q1, q2, q3 = st.columns(3)
    q1.metric("Occupied inventory cost", str(stats.get("paper_occupied") or "0"))
    q2.metric("Marked inventory value", str(stats.get("paper_marked_inventory") or "0"))
    q3.metric("Max drawdown", str(stats.get("paper_max_drawdown") or "0"))

    l1, l2, l3, l4 = st.columns(4)
    l1.metric("Open episodes", stats.get("open_episodes") or 0)
    p50 = stats.get("latency_p50_ms")
    p95 = stats.get("latency_p95_ms")
    l2.metric("WS latency p50 (ms)", f"{p50:.1f}" if p50 is not None else "n/a")
    l3.metric("WS latency p95 (ms)", f"{p95:.1f}" if p95 is not None else "n/a")
    l4.metric(
        "VPS latency vs 500ms paper delay",
        latency_sufficiency_label(stats),
    )
    if stats.get("clock_skew_detected"):
        st.warning(CLOCK_SKEW_WARNING)
    st.write(
        f"**Paper cash:** {stats.get('paper_cash')} · **realized PnL (paper):** "
        f"{stats.get('paper_pnl')} · **paper trades:** {stats.get('paper_trades')}"
    )
    st.write(
        f"**Last scanner run:** "
        f"{stats.get('last_run_started_at') or 'never'} · "
        f"status={stats.get('last_run_status')}"
    )
    st.write(f"**Last orderbook update:** {stats['last_book_at']}")


def _render_phase_params_form(key_prefix: str) -> ScannerParams:
    p = st.session_state.scanner_params
    with st.expander("Scan parameters", expanded=True):
        c2, c3 = st.columns(2)
        sync = c2.number_input(
            "Market sync interval (s)", min_value=60, value=p.market_sync_s, key=f"{key_prefix}_sync"
        )
        sync_m = c3.checkbox("Sync markets from Gamma", value=p.sync_markets, key=f"{key_prefix}_syncm")
        c4, c5 = st.columns(2)
        max_pages = c4.number_input(
            "Max Gamma pages (0 = all)",
            min_value=0,
            value=p.max_pages or 0,
            key=f"{key_prefix}_pages",
        )
        mlimit = c5.number_input(
            "Market limit (0 = no limit)",
            min_value=0,
            value=p.market_limit or 0,
            key=f"{key_prefix}_mlim",
        )
        c6, c7, c8 = st.columns(3)
        delay = c6.number_input(
            "Signal to first-leg delay (ms)", min_value=0, value=p.paper_delay_ms, key=f"{key_prefix}_delay"
        )
        tif = c7.selectbox("Paper TIF", ["FAK", "FOK"], index=0 if p.paper_tif == "FAK" else 1, key=f"{key_prefix}_tif")
        min_p = c8.number_input(
            "Min net profit ($)", min_value=0.0, value=float(p.paper_min_net_profit), key=f"{key_prefix}_minp"
        )
    return ScannerParams(
        market_sync_s=int(sync),
        max_pages=int(max_pages) if max_pages > 0 else None,
        market_limit=int(mlimit) if mlimit > 0 else None,
        sync_markets=sync_m,
        paper_delay_ms=int(delay),
        paper_tif=tif,
        paper_min_net_profit=float(min_p),
    )


def _render_scanner_control() -> None:
    st.subheader("Scanner control — Snapshot Audit / Live Research")
    st.caption(
        "Snapshot Audit runs a single REST scan in-process. "
        "Live Research spawns a WebSocket daemon; enable **Live refresh** to stream DB updates. "
        "Paper Trading reuses the realtime scanner. No wallets, no POST /order."
    )
    status = get_scanner_status(st.session_state.scanner_proc)
    if status.running:
        st.info(f"Active: {status.message}")
    else:
        st.warning("No scanner daemon running")

    tab1, tab2, tab_stop = st.tabs(
        ["Snapshot Audit", "Live Research", "Stop / Report"]
    )

    with tab1:
        st.markdown(
            "One REST scan of Gamma + CLOB. Use this for API, fee, book, and formula diagnostics. "
            "There is no static polling daemon."
        )
        params = _render_phase_params_form("p1")
        if st.button("Run Snapshot Audit once (in-process)", type="primary", key="p1_once"):
            with st.spinner("Running snapshot scan…"):
                summary = _run_async(run_snapshot_once(params))
            st.session_state.scanner_params = params
            st.session_state.phase_params = params
            st.success(summary)
            st.session_state.live_refresh = True
            st.rerun()
        st.code(
            "python scripts/run_scanner.py --once --max-pages "
            f"{params.max_pages or 1} --market-limit {params.market_limit or 50}",
            language="bash",
        )

    with tab2:
        st.markdown(
            "Public market WebSocket, incremental recalc, episode tracking, latency samples. "
            "Execution Mode is Observe Only or Paper Trading (simulated)."
        )
        params = _render_phase_params_form("p2")
        execution = st.selectbox(
            "Execution Mode",
            ["observe", "paper"],
            format_func=lambda x: "Observe Only" if x == "observe" else "Paper Trading",
            key="live_execution",
        )
        if st.button("Start Live Research daemon", type="primary", key="live_daemon"):
            proc, msg = start_live_daemon(execution, params)
            if proc:
                st.session_state.scanner_proc = proc
                st.session_state.scanner_params = params
                st.session_state.phase_params = params
                st.session_state.live_refresh = True
                st.success(msg)
                st.rerun()
            else:
                st.error(msg)
        st.code(" ".join(build_daemon_cmd(execution, params)), language="bash")

    with tab_stop:
        c1, c2 = st.columns(2)
        if c1.button("Stop scanner", key="stop_scan"):
            proc, msg = stop_scanner(st.session_state.scanner_proc, generate_report=False)
            st.session_state.scanner_proc = proc
            st.session_state.live_refresh = False
            st.success(msg)
            st.rerun()
        if c2.button("Stop + generate daily report", key="stop_report"):
            proc, msg = stop_scanner(st.session_state.scanner_proc, generate_report=True)
            st.session_state.scanner_proc = proc
            st.session_state.live_refresh = False
            st.success(msg)
            st.rerun()
        if st.button("Refresh markets only (Gamma sync)", key="sync_markets"):
            from polymarket_scanner.discovery.market_discovery import discover_and_store_markets

            with st.spinner("Syncing markets…"):
                markets = _run_async(discover_and_store_markets(max_pages=3))
            st.success(f"Synced {len(markets)} markets")
            st.rerun()


@st.fragment(run_every=timedelta(seconds=5))
def _live_data_fragment() -> None:
    if not st.session_state.live_refresh:
        return
    stats = get_dashboard_stats()
    _render_metrics(stats)
    live = get_recent_live_data(limit=8)
    st.markdown("#### Live feed (auto-refresh 5s)")
    c1, c2 = st.columns(2)
    with c1:
        st.markdown("**Recent net-profitable signals**")
        if live["opportunities"]:
            st.dataframe(pd.DataFrame(live["opportunities"]), use_container_width=True, hide_index=True)
        else:
            st.caption("No signals yet")
        st.markdown("**Recent scanner runs**")
        if live["scanner_runs"]:
            st.dataframe(pd.DataFrame(live["scanner_runs"]), use_container_width=True, hide_index=True)
    with c2:
        st.markdown("**Episodes**")
        if live["episodes"]:
            st.dataframe(pd.DataFrame(live["episodes"]), use_container_width=True, hide_index=True)
        else:
            st.caption("No episodes yet")
        st.markdown("**Paper trades**")
        if live["paper_trades"]:
            st.dataframe(pd.DataFrame(live["paper_trades"]), use_container_width=True, hide_index=True)
        else:
            st.caption("No paper trades yet")
    status = get_scanner_status(st.session_state.scanner_proc)
    st.caption(f"Scanner: {status.message} · refreshed {datetime.now(timezone.utc).strftime('%H:%M:%S')} UTC")


def page_dashboard() -> None:
    st.title("Dashboard")
    _render_scanner_control()

    if st.session_state.live_refresh:
        _live_data_fragment()
    else:
        stats = get_dashboard_stats()
        _render_metrics(stats)

    with st.expander("Scanner log (tail)"):
        st.text(read_log_tail(50))

    try:
        from polymarket_scanner.api.geoblock_client import GeoblockClient

        geo = _run_async(GeoblockClient().check())
        st.info(
            f"Geoblock status (display only): blocked={geo.blocked} country={geo.country} "
            f"region={geo.region} error={geo.error}"
        )
    except Exception as exc:
        st.warning(f"Geoblock check failed: {exc}")


def page_markets() -> None:
    st.title("Markets")
    st.caption(
        "Data comes from Gamma/CLOB APIs cached in SQLite. "
        "Use the Polymarket URL (event slug) to open the same market on the website — "
        "searching by market_id or a truncated question often fails."
    )
    markets = load_markets_from_db(tradable_only=True)
    q = st.text_input("Search keyword (question / slug / event_slug / market_id)")
    fee_free = st.checkbox("Fee-free only")
    neg_only = st.checkbox("negRisk only")
    rows = []
    for m in markets:
        haystack = " ".join(
            filter(
                None,
                [m.question, m.slug, m.event_slug, m.market_id, m.condition_id],
            )
        ).lower()
        if q and q.lower() not in haystack:
            continue
        if fee_free and m.fees_enabled is not False:
            continue
        if neg_only and not m.neg_risk:
            continue
        yes_book, no_book = (None, None)
        if m.yes_token_id and m.no_token_id:
            yes_book, no_book = latest_books_for_market(
                m.condition_id, m.yes_token_id, m.no_token_id
            )
        age = None
        if yes_book:
            fetched = ensure_utc(yes_book.fetched_at)
            if fetched is not None:
                age = (utcnow() - fetched).total_seconds()
        ya = float(yes_book.best_ask) if yes_book and yes_book.best_ask else None
        yb = float(yes_book.best_bid) if yes_book and yes_book.best_bid else None
        na = float(no_book.best_ask) if no_book and no_book.best_ask else None
        nb = float(no_book.best_bid) if no_book and no_book.best_bid else None
        pm_url = polymarket_market_url(m.slug, m.event_slug)
        rows.append(
            {
                "market_id": m.market_id,
                "question": m.question,
                "event_slug": m.event_slug,
                "slug": m.slug,
                "polymarket_url": pm_url,
                "category": m.category or "unknown",
                "yes_bid": yb,
                "yes_ask": ya,
                "no_bid": nb,
                "no_ask": na,
                "yes_no_ask": (ya + na) if ya is not None and na is not None else None,
                "fees_enabled": m.fees_enabled if m.fees_enabled is not None else "unknown",
                "fee_rate": float(m.fee_schedule.rate) if m.fee_schedule else None,
                "neg_risk": m.neg_risk,
                "end_date": m.end_date,
                "data_age_s": age,
                "condition_id": m.condition_id,
            }
        )
    df = pd.DataFrame(rows)
    if not df.empty and "polymarket_url" in df.columns:
        st.dataframe(
            df,
            use_container_width=True,
            column_config={
                "polymarket_url": st.column_config.LinkColumn(
                    "Polymarket",
                    help="Open the event page on polymarket.com",
                    display_text="Open",
                )
            },
        )
    else:
        st.dataframe(df, use_container_width=True)
    st.download_button(
        "Export CSV",
        df.to_csv(index=False),
        file_name="markets.csv",
        mime="text/csv",
    )

    ids = [r["market_id"] for r in rows]
    if ids:
        mid = st.selectbox("Market detail", ids)
        m = next(x for x in markets if x.market_id == mid)
        st.subheader(m.question or mid)
        pm_url = polymarket_market_url(m.slug, m.event_slug)
        meta_cols = st.columns(3)
        meta_cols[0].write(f"**market_id:** `{m.market_id}`")
        meta_cols[1].write(f"**event_slug:** `{m.event_slug or 'unknown'}`")
        meta_cols[2].write(f"**slug:** `{m.slug or 'unknown'}`")
        st.write(f"**condition_id:** `{m.condition_id}`")
        if pm_url:
            st.markdown(f"[Open on Polymarket]({pm_url})")
        else:
            st.warning("No event_slug/slug in DB for this market — refresh markets and retry.")
        st.write(m.description or "Resolution rules: unknown")
        st.write(
            f"feesEnabled={m.fees_enabled if m.fees_enabled is not None else 'unknown'} "
            f"feeSchedule={m.fee_schedule}"
        )
        yes_book, no_book = latest_books_for_market(
            m.condition_id, m.yes_token_id or "", m.no_token_id or ""
        )
        if yes_book and no_book:
            c1, c2 = st.columns(2)
            c1.write("YES asks (low→high)")
            c1.dataframe(
                pd.DataFrame([{"price": float(x.price), "size": float(x.size)} for x in yes_book.asks[:20]])
            )
            c2.write("NO asks (low→high)")
            c2.dataframe(
                pd.DataFrame([{"price": float(x.price), "size": float(x.size)} for x in no_book.asks[:20]])
            )
            fig = go.Figure()
            fig.add_trace(
                go.Bar(
                    name="YES ask depth",
                    x=[float(x.price) for x in yes_book.asks[:30]],
                    y=[float(x.size) for x in yes_book.asks[:30]],
                )
            )
            fig.add_trace(
                go.Bar(
                    name="NO ask depth",
                    x=[float(x.price) for x in no_book.asks[:30]],
                    y=[float(x.size) for x in no_book.asks[:30]],
                )
            )
            fig.update_layout(barmode="overlay", title="Depth (theoretical display)")
            st.plotly_chart(fig, use_container_width=True)


def page_opportunities() -> None:
    st.title("Opportunities")
    st.caption("All profits are theoretical or simulated — not guaranteed executable.")
    market_by_id = {m.market_id: m for m in load_markets_from_db(tradable_only=False)}
    with session_scope() as session:
        ops = session.scalars(
            select(OpportunityRow).order_by(desc(OpportunityRow.discovered_at)).limit(500)
        ).all()
        rows = []
        for o in ops:
            m = market_by_id.get(o.market_id)
            pm_url = polymarket_market_url(
                m.slug if m else None,
                m.event_slug if m else None,
            )
            rows.append(
                {
                    "discovered_at": o.discovered_at,
                    "market": (o.question or o.market_id)[:80],
                    "market_id": o.market_id,
                    "event_slug": m.event_slug if m else None,
                    "polymarket_url": pm_url,
                    "direction": o.direction,
                    "yes_vwap": float(o.yes_vwap),
                    "no_vwap": float(o.no_vwap),
                    "quantity": float(o.quantity),
                    "gross": float(o.gross_profit),
                    "fees": float(o.fee_total),
                    "net": float(o.net_profit),
                    "optimistic": float(o.optimistic_net or 0),
                    "base": float(o.base_net or 0),
                    "pessimistic": float(o.pessimistic_net or 0),
                    "quality": o.simulation_quality,
                    "data_age": o.data_age_seconds,
                    "stale": o.stale,
                    "passes_rule": o.passes_rule_set,
                    "books_ready": getattr(o, "books_ready", True),
                    "tags": o.risk_tags_json,
                }
            )
    df = pd.DataFrame(rows)
    sort = st.selectbox("Sort by", ["net", "base", "pessimistic", "quantity", "data_age"])
    if not df.empty:
        df = df.sort_values(sort, ascending=False)
        st.dataframe(
            df,
            use_container_width=True,
            column_config={
                "polymarket_url": st.column_config.LinkColumn(
                    "Polymarket",
                    help="Open the event page on polymarket.com",
                    display_text="Open",
                )
            },
        )
    else:
        st.dataframe(df, use_container_width=True)


def page_simulator() -> None:
    st.title("Simulator")
    st.caption("Custom trade simulation — theoretical / estimated unless delayed snapshots exist.")
    markets = load_markets_from_db(tradable_only=True)
    labels = {
        f"{m.market_id} | {(m.event_slug or m.slug or '')[:40]} | {(m.question or '')[:50]}": m
        for m in markets[:500]
    }
    if not labels:
        st.warning("No markets in DB — refresh markets first.")
        return
    choice = st.selectbox("Market", list(labels.keys()))
    m = labels[choice]
    col1, col2, col3 = st.columns(3)
    qty = Decimal(str(col1.number_input("Target quantity", min_value=0.0, value=50.0)))
    delay = int(col2.number_input("Delay ms", min_value=0, value=500))
    slip = int(col3.number_input("Slippage ticks", min_value=0, value=1))
    depth = Decimal(str(st.slider("Depth factor", 0.1, 1.0, 0.9)))
    partial = Decimal(str(st.slider("Second leg fill ratio", 0.0, 1.0, 1.0)))
    force_close = st.checkbox("Force close unhedged", value=True)
    first_leg = st.selectbox("First leg", ["YES", "NO"])
    op_cost = Decimal(str(st.number_input("Operational cost", value=0.0)))
    buffer = Decimal(str(st.number_input("Safety buffer", value=0.01)))
    is_taker = st.checkbox("Taker fees", value=True)
    if st.button("Run simulation"):
        yes_book, no_book = latest_books_for_market(
            m.condition_id, m.yes_token_id or "", m.no_token_id or ""
        )
        if not yes_book or not no_book:
            st.error("Missing order books — run a scan first.")
            return
        profile = ScenarioProfile(
            name="custom",
            delay_ms=delay,
            slippage_ticks=slip,
            depth_factor=depth,
            sequential_legs=True,
            partial_second_leg_ratio=partial,
            force_close_unhedged=force_close,
            operational_cost=op_cost,
            safety_buffer=buffer,
            is_taker=is_taker,
            first_leg=first_leg,
        )
        result = simulate_forward(
            m, yes_book, no_book, profile, target_quantity=qty if qty > 0 else None
        )
        if result.quality.value != "observed_snapshot":
            st.warning(
                f"Simulation quality = **{result.quality.value}** (not a real delayed observation). "
                "No matching historical snapshot was found in the delay window; this is an estimate."
            )
        else:
            st.success("Used observed delayed snapshots from stored books (still simulated, not live fills).")
        st.json(json.loads(result.model_dump_json()))
        st.write(result.details)
        with session_scope() as session:
            session.add(
                SimulationRunRow(
                    market_id=m.market_id,
                    profile=result.profile,
                    quality=result.quality.value,
                    quantity=format(result.quantity, "f"),
                    gross_profit=format(result.gross_profit, "f"),
                    fees=format(result.fees, "f"),
                    operational_cost=format(result.operational_cost, "f"),
                    safety_buffer=format(result.safety_buffer, "f"),
                    net_profit=format(result.net_profit, "f"),
                    worst_loss=format(result.worst_loss, "f"),
                    unhedged_quantity=format(result.unhedged_quantity, "f"),
                    still_arbitrage=result.still_arbitrage,
                    one_leg_risk=result.one_leg_risk,
                    details=result.details,
                    params_json=profile.model_dump_json(),
                )
            )
        st.success("Simulation saved")


def page_paper() -> None:
    st.title("Paper trading")
    st.caption(
        "Local simulation only — no wallet, no API keys, no POST /order. "
        "On a new episode the engine waits 500ms, then FOK/FAK-fills YES then NO, "
        "merges matched complete sets at $1, and recycles cash."
    )
    from polymarket_scanner.database import PositionRow
    from polymarket_scanner.simulation.paper_trader import get_paper_account

    cash, occupied, pnl = get_paper_account()
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Cash (paper)", f"{float(cash):.4f}")
    c2.metric("Cost basis (occupied)", f"{float(occupied):.4f}")
    c3.metric("Realized PnL", f"{float(pnl):.4f}")
    with session_scope() as session:
        pos = session.scalars(select(PositionRow).where(PositionRow.status.in_(["open", "RESIDUAL_OPEN", "residual_open"]))).all()
        marked = sum((Decimal(p.marked_value or "0") for p in pos), Decimal("0"))
    c4.metric("Marked value", f"{float(marked):.4f}")
    st.caption("Equity = cash + marked value. Occupied cost is not market value.")

    st.subheader("Opportunity episodes")
    with session_scope() as session:
        eps = session.scalars(
            select(OpportunityEpisodeRow)
            .order_by(desc(OpportunityEpisodeRow.first_seen_at))
            .limit(200)
        ).all()
        ep_rows = [
            {
                "market_id": e.market_id,
                "question": (e.question or "")[:80],
                "direction": e.direction,
                "first_seen": e.first_seen_at,
                "last_seen": e.last_seen_at,
                "disappeared": e.disappeared_at,
                "duration_s": e.duration_seconds,
                "open": e.is_open,
                "peak_net": e.peak_net_profit,
            }
            for e in eps
        ]
        trades = session.scalars(
            select(PaperTradeRow).order_by(desc(PaperTradeRow.created_at)).limit(200)
        ).all()
        t_rows = [
            {
                "created": t.created_at,
                "market_id": t.market_id,
                "tif": t.tif,
                "delay_ms": t.delay_ms,
                "status": t.status,
                "yes_qty": t.yes_qty,
                "no_qty": t.no_qty,
                "matched": t.matched_qty,
                "unhedged": t.unhedged_qty,
                "cash_used": t.cash_used,
                "merge": t.merge_proceeds,
                "pnl": t.pnl,
                "cash_after": t.cash_after,
                "details": t.details,
            }
            for t in trades
        ]
    st.dataframe(pd.DataFrame(ep_rows), use_container_width=True)
    st.subheader("Paper fills")
    st.dataframe(pd.DataFrame(t_rows), use_container_width=True)
    st.subheader("Open positions")
    with session_scope() as session:
        from polymarket_scanner.database import PositionRow

        pos = session.scalars(select(PositionRow).order_by(desc(PositionRow.acquired_at)).limit(200)).all()
        p_rows = [
            {
                "market_id": p.market_id,
                "token_id": p.token_id,
                "outcome": p.outcome,
                "qty": p.quantity,
                "cost_basis": p.cost_basis,
                "mark": p.last_mark_price,
                "marked_value": p.marked_value,
                "unrealized": p.unrealized_pnl,
                "status": p.status,
            }
            for p in pos
        ]
    st.dataframe(pd.DataFrame(p_rows), use_container_width=True)


def page_strategies() -> None:
    st.title("Strategies")
    st.caption(
        "Immutable versions — creating a version inserts a new row. "
        "Walk-forward recommends only; it never auto-applies live params. Paper-only."
    )
    from polymarket_scanner.database import (
        StrategyAccountRow,
        StrategyEvalRow,
        StrategyPositionRow,
        StrategyRunRow,
        StrategyTradeRow,
    )
    from polymarket_scanner.strategy.params import params_from_json
    from polymarket_scanner.strategy.store import (
        create_strategy_version,
        list_strategy_configs,
        set_strategy_enabled,
    )

    rows = list_strategy_configs()
    if not rows:
        st.info("No strategies seeded yet. Restart scanner / init_db.")
        return
    labels = [f"{r.strategy_id} v{r.version} ({'live' if r.is_live else 'shadow'})" for r in rows]
    idx = st.selectbox("Strategy version", range(len(labels)), format_func=lambda i: labels[i])
    current = rows[idx]
    params = params_from_json(current.params_json)
    st.write(f"Enabled: {current.enabled} · live={current.is_live}")
    st.json(params.model_dump(mode="json"))
    c1, c2, c3 = st.columns(3)
    if c1.button("Enable"):
        set_strategy_enabled(current.strategy_id, current.version, True)
        st.rerun()
    if c2.button("Disable"):
        set_strategy_enabled(current.strategy_id, current.version, False)
        st.rerun()
    with st.expander("Create new version (does not mutate this version)"):
        delay = st.number_input("delay_ms", value=int(params.delay_ms))
        min_net = st.text_input("min_net_profit", value=str(params.min_net_profit))
        tif = st.selectbox("tif", ["FAK", "FOK"], index=0 if params.tif == "FAK" else 1)
        if st.button("Insert new version"):
            new_p = params.model_copy(
                update={"delay_ms": int(delay), "min_net_profit": Decimal(min_net), "tif": tif}
            )
            ver = create_strategy_version(
                current.strategy_id, current.name, new_p, is_live=current.is_live, enabled=True
            )
            st.success(f"Created {current.strategy_id} v{ver}")
            st.rerun()

    with session_scope() as session:
        acct = session.scalar(
            select(StrategyAccountRow).where(
                StrategyAccountRow.strategy_id == current.strategy_id,
                StrategyAccountRow.version == current.version,
            )
        )
        trades = session.scalars(
            select(StrategyTradeRow)
            .where(
                StrategyTradeRow.strategy_id == current.strategy_id,
                StrategyTradeRow.strategy_version == current.version,
            )
            .order_by(desc(StrategyTradeRow.created_at))
            .limit(100)
        ).all()
        pos = session.scalars(
            select(StrategyPositionRow).where(
                StrategyPositionRow.strategy_id == current.strategy_id,
                StrategyPositionRow.strategy_version == current.version,
            )
        ).all()
        runs = session.scalars(
            select(StrategyRunRow)
            .where(StrategyRunRow.strategy_id == current.strategy_id)
            .order_by(desc(StrategyRunRow.started_at))
            .limit(20)
        ).all()
        evals = session.scalars(select(StrategyEvalRow).order_by(desc(StrategyEvalRow.id)).limit(10)).all()
    if acct:
        a1, a2, a3, a4 = st.columns(4)
        a1.metric("Cash", acct.cash)
        a2.metric("Cost basis", acct.occupied)
        a3.metric("Marked inventory", acct.marked_inventory)
        a4.metric("Realized P&L", acct.realized_pnl)
        st.caption("Equity = cash + marked inventory, not occupied cost.")
    st.subheader("Runs")
    st.dataframe(
        pd.DataFrame(
            [
                {
                    "id": r.id,
                    "version": r.strategy_version,
                    "started": r.started_at,
                    "finished": r.finished_at,
                    "status": r.status,
                    "trades": r.trade_count,
                }
                for r in runs
            ]
        ),
        use_container_width=True,
    )
    st.subheader("Trades")
    st.dataframe(
        pd.DataFrame(
            [
                {
                    "created": t.created_at,
                    "status": t.status,
                    "pnl": t.realized_pnl,
                    "first_qty": t.first_qty,
                    "second_qty": t.second_qty,
                    "first_vwap": t.first_vwap,
                    "second_vwap": t.second_vwap,
                    "signal_to_first_ms": t.signal_to_first_ms,
                    "first_to_second_ms": t.first_to_second_ms,
                    "opp_id": t.signal_opportunity_id,
                    "cash_after": t.cash_after,
                }
                for t in trades
            ]
        ),
        use_container_width=True,
    )
    st.subheader("Positions")
    st.dataframe(
        pd.DataFrame(
            [
                {
                    "market_id": p.market_id,
                    "outcome": p.outcome,
                    "qty": p.quantity,
                    "cost_basis": p.cost_basis,
                    "marked_value": p.marked_value,
                    "unrealized": p.unrealized_pnl,
                    "status": p.status,
                }
                for p in pos
            ]
        ),
        use_container_width=True,
    )
    st.subheader("Walk-forward")
    st.dataframe(
        pd.DataFrame(
            [
                {
                    "id": e.id,
                    "insufficient": e.insufficient_sample,
                    "recommended": e.recommended_strategy_id,
                    "version": e.recommended_version,
                    "note": e.note,
                }
                for e in evals
            ]
        ),
        use_container_width=True,
    )


def page_rules() -> None:
    st.title("Rules")
    sets = load_rule_sets_from_db()
    names = [s.name for s in sets] or ["Balanced"]
    name = st.selectbox("Rule set", names)
    current = next((s for s in sets if s.name == name), RuleSetModel(name=name))
    st.write(current.description or "")
    enabled = st.checkbox("Enabled", value=current.enabled)
    for i, cond in enumerate(current.conditions):
        with st.expander(f"Condition {i+1}: {cond.field} {cond.operator} {cond.value}"):
            cond.enabled = st.checkbox("enabled", value=cond.enabled, key=f"en{i}")
            cond.field = st.text_input("field", value=cond.field, key=f"f{i}")
            ops = [
                ">",
                ">=",
                "<",
                "<=",
                "==",
                "!=",
                "contains",
                "not contains",
                "in",
                "not in",
            ]
            cond.operator = st.selectbox(
                "operator",
                ops,
                index=ops.index(cond.operator) if cond.operator in ops else 0,
                key=f"o{i}",
            )
            cond.value = st.text_input("value", value=str(cond.value), key=f"v{i}")

    if st.button("Add condition"):
        current.conditions.append(
            RuleCondition(field="net_profit", operator=">=", value=0.5, enabled=True)
        )
        save_rule_set(current)
        st.rerun()

    c1, c2, c3, c4, c5 = st.columns(5)
    if c1.button("Save"):
        current.enabled = enabled
        # coerce numeric-looking values
        for cond in current.conditions:
            try:
                if isinstance(cond.value, str) and cond.value.replace(".", "", 1).isdigit():
                    cond.value = float(cond.value)
                elif isinstance(cond.value, str) and cond.value.lower() in {"true", "false"}:
                    cond.value = cond.value.lower() == "true"
            except Exception:
                pass
        save_rule_set(current)
        st.success("Saved")
    if c2.button("Duplicate"):
        new_name = f"{current.name} copy"
        duplicate_rule_set(current.name, new_name)
        st.success(f"Duplicated as {new_name}")
        st.rerun()
    if c3.button("Delete"):
        delete_rule_set(current.name)
        st.warning("Deleted")
        st.rerun()
    export = export_rule_set_json(current)
    c4.download_button("Export JSON", export, file_name=f"{current.name}.json")
    uploaded = c5.file_uploader("Import JSON", type=["json"])
    if uploaded:
        imported = import_rule_set_json(uploaded.read().decode("utf-8"))
        save_rule_set(imported)
        st.success(f"Imported {imported.name}")

    if st.button("Test against current markets/opportunities"):
        with session_scope() as session:
            ops = session.scalars(select(OpportunityRow).limit(1000)).all()
            objs = [
                {
                    "net_profit": float(o.net_profit),
                    "gross_profit": float(o.gross_profit),
                    "quantity": float(o.quantity),
                    "data_age_seconds": o.data_age_seconds,
                    "stale": o.stale,
                    "fees_enabled": o.fees_enabled,
                    "neg_risk": o.neg_risk,
                    "base_net_profit": float(o.base_net or 0),
                    "pessimistic_net_profit": float(o.pessimistic_net or 0),
                    "question": o.question,
                }
                for o in ops
            ]
        st.write(explain_filter(objs, current))
        st.write(f"Filtered opportunities: {len(filter_opportunities(objs, current))}")

    new_name = st.text_input("New rule set name")
    if st.button("Add rule set") and new_name:
        save_rule_set(RuleSetModel(name=new_name, enabled=False, conditions=[]))
        st.rerun()


def page_reports() -> None:
    st.title("Reports")
    with session_scope() as session:
        rows = session.scalars(
            select(DailyReportRow).order_by(desc(DailyReportRow.report_date))
        ).all()
        data = [
            {
                "date": r.report_date,
                "markets": r.markets_scanned,
                "raw": r.raw_signals,
                "net": r.net_signals,
                "base_ok": r.base_profitable,
                "pess_ok": r.pessimistic_profitable,
                "sim_profit": r.total_sim_profit,
                "max_profit": r.max_single_profit,
                "max_one_leg_loss": r.max_one_leg_loss,
                "html": r.html_path,
                "csv": r.csv_path,
            }
            for r in rows
        ]
    st.dataframe(pd.DataFrame(data), use_container_width=True)
    d = st.date_input("Report date", value=datetime.now(timezone.utc).date())
    if st.button("Regenerate"):
        out = generate_daily_report(d.isoformat())
        st.success(out)
        if out.get("html") and Path(out["html"]).exists():
            st.download_button(
                "Download HTML",
                Path(out["html"]).read_text(encoding="utf-8"),
                file_name=Path(out["html"]).name,
            )
            st.download_button(
                "Download CSV",
                Path(out["csv"]).read_text(encoding="utf-8"),
                file_name=Path(out["csv"]).name,
            )


def page_settings() -> None:
    st.title("Settings")
    st.error("READ-ONLY MODE — REAL TRADING DISABLED")
    st.write(f"TRADING_ENABLED constant = {TRADING_ENABLED}")
    cfg = get_config()
    market_sync = st.number_input(
        "Market sync interval (s)", value=cfg.scanner.market_sync_interval_seconds
    )
    book_poll = st.number_input(
        "Orderbook poll interval (s)", value=cfg.scanner.orderbook_poll_interval_seconds
    )
    concurrency = st.number_input(
        "Max concurrent requests", value=cfg.api.max_concurrent_requests
    )
    timeout = st.number_input("HTTP timeout (s)", value=cfg.api.http_timeout_seconds)
    max_age = st.number_input("Max data age (s)", value=cfg.scanner.max_data_age_seconds)
    retention = st.number_input("Retention days", value=cfg.scanner.retention_days)
    tz = st.text_input("Timezone", value=cfg.reporting.timezone)
    st.caption("Used for daily-report date bounds and auto-report rollover (not display-only).")
    log_level = st.selectbox("Log level", ["DEBUG", "INFO", "WARNING", "ERROR"], index=1)
    st.text_input("Database path", value=cfg.database.url, disabled=True)
    if st.button("Save settings"):
        with session_scope() as session:
            set_setting(session, "market_sync_interval_seconds", int(market_sync))
            set_setting(session, "orderbook_poll_interval_seconds", int(book_poll))
            set_setting(session, "max_concurrent_requests", int(concurrency))
            set_setting(session, "http_timeout_seconds", float(timeout))
            set_setting(session, "max_data_age_seconds", int(max_age))
            set_setting(session, "retention_days", int(retention))
            set_setting(session, "timezone", tz)
            set_setting(session, "log_level", log_level)
        st.success("Saved to app_settings (runtime yaml still used for process defaults)")
    st.info("Private key / wallet connect controls are intentionally absent.")


if page == "Dashboard":
    page_dashboard()
elif page == "Markets":
    page_markets()
elif page == "Opportunities":
    page_opportunities()
elif page == "Simulator":
    page_simulator()
elif page == "Paper":
    page_paper()
elif page == "Strategies":
    page_strategies()
elif page == "Rules":
    page_rules()
elif page == "Reports":
    page_reports()
else:
    page_settings()
