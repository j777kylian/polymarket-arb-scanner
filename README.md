# Polymarket Structural Arbitrage Scanner (Read-Only)

Research and simulation platform for **structural complete-set arbitrage** on Polymarket public market data.

**READ-ONLY MODE — REAL TRADING DISABLED**

- No wallet connection
- No private keys
- No order placement (`POST /order` is not implemented and is blocked)
- Public GET APIs + local SQLite only

This tool produces **theoretical / simulated** results. Multi-leg fills are **not atomic**. Partial fills can turn an apparent arb into directional risk. **Not investment advice.**

## What it does

1. Discovers open, order-book-enabled markets via Gamma `/markets/keyset`
2. Fetches YES/NO CLOB order books
3. Reads per-market `feesEnabled` / `feeSchedule` (no category hardcoding)
4. Detects binary complete-set arb (Ask YES + Ask NO < 1)
5. Walks multi-level depth for VWAP, gross/net profit
6. Simulates Optimistic / Base / Pessimistic scenarios (delay, slippage, one-leg risk)
7. Stores history in SQLite
8. Streamlit UI for dashboard, markets, opportunities, simulator, rules, reports, settings
9. HTML daily reports + CSV export

## Architecture

```
Gamma API ──► market discovery ──► SQLite
CLOB API  ──► order books     ──► scanners ──► opportunities
                                      │
                                      ├── fee calculator (Decimal)
                                      ├── orderbook walker
                                      └── execution simulator
UI (Streamlit) ◄── SQLite ◄── reports (HTML/CSV)
```

Scanner daemon uses a file lock (`data/scanner.lock`) so Streamlit reloads do not spawn duplicate scanners.

## Install

Requires Python 3.11+.

```bash
cd polymarket-arb-scanner
python3.12 -m venv .venv
source .venv/bin/activate
pip install -U pip
pip install -e ".[dev]"
cp .env.example .env
```

## Local start

Initialize DB + one scan (limited pages for smoke):

```bash
python -m polymarket_scanner --max-pages 2 --market-limit 30
```

UI:

```bash
streamlit run src/polymarket_scanner/ui/app.py
```

Scanner once / daemon:

```bash
Scanner once / daemon:

```bash
# Phase 1 — static poll every ~45s, auto daily HTML/CSV
python scripts/run_scanner.py --once --max-pages 2 --market-limit 50
python scripts/run_scanner.py --daemon --mode static

# Phase 2+3 — public WebSocket + optional paper trading (still no real orders)
python scripts/run_scanner.py --daemon --mode realtime --paper
```

Daily report:

```bash
python scripts/generate_report.py --date YYYY-MM-DD
```

## Docker

```bash
docker compose up -d
```

- UI: http://localhost:8501
- Volumes: `data/`, `reports/`, `logs/`
- Runs as non-root uid 10001
- No trading credentials

## UI pages

| Page | Purpose |
|------|---------|
| Dashboard | Status, geoblock display, run once / daemon / report |
| Markets | Sortable market table + book depth |
| Opportunities | Signals with scenario nets + risk tags |
| Simulator | Custom delay/slippage/partial-fill simulation |
| Paper | Episode lifetimes, FOK/FAK paper fills, cash recycle |
| Rules | Add/edit/enable/import/export rule sets (SQLite) |
| Reports | HTML/CSV daily reports |
| Settings | Intervals, concurrency, retention (read-only banner) |

## Configuration

See `config/default.yaml` and `.env.example`.

Important knobs:

- `scanner.market_sync_interval_seconds`
- `scanner.orderbook_poll_interval_seconds`
- `scanner.max_data_age_seconds`
- `api.max_concurrent_requests`
- `simulation.profiles.*`

## Rules editor

Rule sets are stored in SQLite (`rule_sets` / `rules`), not only session state.

Default sets: **Conservative**, **Balanced**, **Exploratory**, **Fee-free only**.

Operators: `>`, `>=`, `<`, `<=`, `==`, `!=`, `contains`, `not contains`, `in`, `not in` (AND in v1; OR groups reserved).

## Simulation scenarios

| Profile | Delay | Slippage | Depth | Notes |
|---------|-------|----------|-------|-------|
| Optimistic | 0 ms | 0 | 100% | Signal-time book |
| Base | 500 ms | 1 tick | 90% | Sequential legs + buffer |
| Pessimistic | 2000 ms | 2 ticks | 70% | Partial 2nd leg + force close |

`simulation_quality`:

- `observed_snapshot` — used a real later book
- `estimated` — no delayed snapshot; marked in UI/reports

## Database

SQLite schema includes: markets, tokens, orderbook_snapshots/levels, fee_schedules, opportunities, simulation_runs/legs, rule_sets/rules, scanner_runs, api_errors, daily_reports, app_settings.

Init: `init_db()` on startup (creates tables + default rule sets).

## Tests

```bash
pytest -q
```

## Common issues

- **Deprecation warning on `/markets`**: use `/markets/keyset` (this project does).
- **HTTP 429**: exponential backoff via tenacity.
- **Missing fees on Gamma**: enrich via CLOB `GET /clob-markets/{condition_id}` (fee curve in `fd`); legacy `/markets/{id}` lacks fee schedule details.
- **Stale books**: opportunities tagged `stale data`; default rules exclude them.
- **Geoblock**: shown for awareness only — no bypass/proxy support.

## API rate limits

Keep `max_concurrent_requests` modest (default 8). Daemon polls books on an interval; market sync is less frequent.

## Streamlit dashboard control

The Dashboard wraps all three phases with parameters and live updates:

| Action | Behavior |
|--------|----------|
| **Phase 1 once** | Runs in-process; results appear immediately in metrics + SQLite |
| **Phase 1/2/3 daemon** | Spawns `scripts/run_scanner.py`; parameters saved to `app_settings` |
| **Live refresh (5s)** | Auto-refreshes metrics, recent signals, episodes, paper trades |
| **Stop** | Terminates subprocess / lock; optional daily report |

Sidebar shows scanner pid, mode, and lock status. Phase 2/3 cannot run fully in-process (WebSocket blocks Streamlit); use daemon + live refresh instead.

## Three operating phases

**Phase 1 — static scan (no keys):** daemon polls Gamma + CLOB every 30–60s (`orderbook_poll_interval_seconds: 45`), writes SQLite, and auto-generates HTML/CSV daily reports.

**Phase 2 — realtime:** `--mode realtime` subscribes to the public market WebSocket (`wss://ws-subscriptions-clob.polymarket.com/ws/market`), applies `book` / `price_change` to an in-memory cache, recalculates **only dirty markets**, records episode first-seen/disappear times, and stores WS latency samples. Dashboard flags whether p50/p95 are sufficient vs a 500ms paper delay.

**Phase 3 — paper trading (still not live):** `--paper` waits 500ms after a new episode, then simulates FOK/FAK taker fills, one-leg residual, YES+NO merge at $1, and cash recycle. No wallet, no CLOB credentials, no `POST /order`.

Do not run both `scanner` and `scanner-realtime` compose services against one SQLite file at the same time (file lock). Pick one.

## Completely read-only

- `TRADING_ENABLED = False` in `safety.py`
- Enabling it still raises `NotImplementedError`
- HTTP helper blocks trading write paths
- No signing / wallet code imported

## Multi-leg non-atomic risk

Buying YES and NO are separate executions. The second leg may miss, partially fill, or move. Pessimistic mode models this; it does **not** make the strategy safe.

## Disclaimer

Historical and simulated outputs are **not** guarantees of executable profit. This software is for research and education only and does **not** constitute financial, trading, or investment advice.
