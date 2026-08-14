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
# Snapshot Audit — single REST scan (API / fee / book / formula diagnostics)
python scripts/run_scanner.py --once --max-pages 2 --market-limit 50

# Live Research — public WebSocket. Execution Mode: Observe Only (default) or Paper Trading
python scripts/run_scanner.py --daemon --mode live
python scripts/run_scanner.py --daemon --mode live --paper --max-pages 1 --market-limit 50

# Bounded 12-hour paper run (43200 seconds); exits through normal cleanup
python scripts/run_scanner.py --daemon --mode live --paper --duration-seconds 43200
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
- Default `scanner` service is Live Research (Observe Only). Paper profile: `docker compose --profile paper up`

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
- `scanner.max_data_age_seconds`
- `scanner.max_pages` / `scanner.market_limit` (Live Research subscription cap)
- `api.max_concurrent_requests`
- `paper.signal_to_first_leg_ms` / `paper.inter_leg_delay_ms`
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

SQLite schema includes: markets, tokens, orderbook_snapshots/levels, fee_schedules, opportunities, simulation_runs/legs, rule_sets/rules, scanner_runs, api_errors, daily_reports, app_settings, paper_account/trades, strategy_configs/runs/accounts/trades/evals.

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
- **Geoblock**: shown for awareness only. HTTP client honors `HTTPS_PROXY` / `HTTP_PROXY` / `ALL_PROXY`. SOCKS requires optional extra: `pip install 'httpx[socks]'` (see `pyproject.toml` optional `socks`).
- **All P&L figures are simulated** and must not be treated as executable profit.

## API rate limits

Keep `max_concurrent_requests` modest (default 8). Daemon polls books on an interval; market sync is less frequent.

## Streamlit dashboard control

The Dashboard wraps Snapshot Audit and Live Research with parameters and live updates:

| Action | Behavior |
|--------|----------|
| **Snapshot Audit once** | Runs in-process REST scan; results appear immediately in metrics + SQLite |
| **Live Research daemon** | Spawns `scripts/run_scanner.py --daemon --mode live`; Execution Mode is Observe Only or Paper Trading |
| **Live refresh (5s)** | Auto-refreshes metrics, recent signals, episodes, paper trades |
| **Stop** | Terminates subprocess / lock; optional daily report (re-queries the database) |

Sidebar shows scanner pid, mode, and lock status. Live Research cannot run fully in-process (WebSocket blocks Streamlit); use daemon + live refresh instead.

## Operating modes

**Snapshot Audit — REST once (no keys):** `--once` fetches Gamma + CLOB books a single time. Keep this for diagnosing APIs, fees, order books, and the arb formula. There is **no** static REST polling daemon.

**Live Research — realtime WebSocket:** `--daemon --mode live` subscribes to the public market channel (`wss://ws-subscriptions-clob.polymarket.com/ws/market`). First connection sends `type=market` with `initial_dump` and `custom_feature_enabled`. Dynamic adds use `operation=subscribe`; removes use `operation=unsubscribe` (not `type=unsubscribe`). Recalc is **dirty-market only**. Execution Mode is **Observe Only** or **Paper Trading**.

**Paper Trading (still not live):** `--paper` reuses the same realtime scanner and live book cache. After a new episode it waits `signal_to_first_leg_ms`, recaptures books, simulates the first leg, waits `inter_leg_delay_ms`, recaptures a **new** second-leg book, and optionally force-closes leftover inventory from a third snapshot. Residual inventory is occupied capital, not realized P&L. No wallet, no CLOB credentials, no `POST /order`.

Do not run both `scanner` and `scanner-paper` compose services against one SQLite file at the same time (file lock). Pick one.

`--mode static` / `--mode realtime` remain as aliases for snapshot / live. `--daemon --mode static` is rejected.

## Completely read-only

- `TRADING_ENABLED = False` in `safety.py`
- Enabling it still raises `NotImplementedError`
- HTTP helper blocks trading write paths
- No signing / wallet code imported

## Multi-leg non-atomic risk

Buying YES and NO are separate executions. The second leg may miss, partially fill, or move. Pessimistic mode models this; it does **not** make the strategy safe.

## Disclaimer

Historical and simulated outputs are **not** guarantees of executable profit. This software is for research and education only and does **not** constitute financial, trading, or investment advice.
