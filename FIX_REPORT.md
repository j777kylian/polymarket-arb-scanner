# FIX_REPORT — polymarket-arb-scanner audit

All results in this project remain **simulated / paper-only**. Nothing here is executable live P&L. `TRADING_ENABLED=False`. There is no wallet, private key, signing, or `POST /order` implementation.

## Phase 1 — false P&L / fake arb

### 1. Paper cashflow double-count

| | |
|---|---|
| **Files** | `src/polymarket_scanner/simulation/paper_trader.py`, `tests/test_paper_cashflow.py`, `tests/test_paper_trader.py` |
| **Defect** | Settlement mixed `merge_proceeds - cash_used + close_pnl`, booking the same cash twice. Partial force-close treated leftover inventory as realized loss or dropped it. |
| **Fix** | `settle_complete_set_cashflow()` books buy cost, buy fees, merge proceeds, sell proceeds, and sell fees once. Residual inventory is `inventory_cost` / `remaining_inventory`, not realized P&L. `cash_after = start + realized_pnl - inventory_cost`. Affordable size is checked against the **desired** qty first so binary search cannot shrink a fully funded fill. |
| **Tests** | `test_settle_full_merge_exact`, `test_settle_partial_second_leg_full_close`, `test_settle_partial_force_close_keeps_inventory`, `test_settle_fees_on_buy_and_sell`, `test_paper_execute_full_merge_and_reconcile`, `test_paper_fees_both_sides_execute`, `test_paper_merge_recycles_capital` |
| **Residual risk** | Leftover inventory is marked occupied but never automatically closed later. Occupied capital accumulates per trade and is not marked-to-market. |

### 2. LiveBookCache lifecycle

| | |
|---|---|
| **Files** | `src/polymarket_scanner/discovery/book_cache.py`, `src/polymarket_scanner/scanners/binary_complete_set.py`, `tests/test_book_lifecycle.py`, `tests/test_book_cache.py` |
| **Defect** | `price_change` could create a scannable book before a full `book` snapshot. Reconnects could splice old and new books. YES/NO skew was ignored for `net_profitable`. |
| **Fix** | Each token stores `initialized_from_full_snapshot`, `connection_generation`, `last_full_snapshot_at`, `last_update_at`. `begin_generation()` / `mark_disconnected()` mark all books not-ready. `price_change` before snapshot is dropped (counted) and queued. `pair_ready` requires both ready and the same generation. `max_book_skew_ms` (default 250) blocks `net_profitable`. Scan returns `[]` until `books_ready`. |
| **Tests** | `test_price_change_before_book_does_not_create_scannable_book`, `test_reconnect_marks_old_generation_not_ready`, `test_pair_ready_requires_same_generation`, `test_does_not_splice_old_book_after_reconnect`, `test_scan_skips_until_books_ready`, `test_skewed_not_net_profitable` |
| **Residual risk** | Pending `price_change`s are only replayed on `apply_book_event`, not `upsert_snapshot`. REST static mode does not use generation gating. |

### 3. Dynamic market subscribe / resync

| | |
|---|---|
| **Files** | `src/polymarket_scanner/api/market_ws.py`, `src/polymarket_scanner/realtime.py`, `src/polymarket_scanner/scanners/opportunity_tracker.py`, `tests/test_market_ws.py`, `tests/test_episode_close_and_stats.py` |
| **Defect** | Token universe was static after start. Removed markets kept scanning / open episodes. |
| **Fix** | Resync diffs `added_tokens` / `removed_tokens`, sends official subscribe/unsubscribe, and `request_rebuild()` (close socket → reconnect + `initial_dump`) on send failure. Removed markets close episodes with `reason=market_removed`. WS disconnect closes episodes with `ws_disconnected`; stale books with `stale_books`. |
| **Tests** | `test_diff_tokens_added_removed`, `test_subscribe_unsubscribe_payloads`, `test_close_episodes_reason` |
| **Residual risk** | Official unsubscribe payload is best-effort; rebuild is the safety path. No live WS integration test against Polymarket. |

## Phase 2 — paper account and simulation

### 4. Paper capital constraints and task tracking

| | |
|---|---|
| **Files** | `src/polymarket_scanner/simulation/paper_trader.py`, `src/polymarket_scanner/realtime.py`, `tests/test_paper_concurrent.py`, `tests/test_paper_cashflow.py` |
| **Defect** | Paper fills ignored cash. Concurrent episodes could lost-update the account. Background tasks were not cancelled on shutdown. |
| **Fix** | `affordable_quantity()` walks real asks + fees + safety buffer. SQLite transaction in `_apply_account` plus `threading.Lock` and `asyncio.Lock` (`execute_paper_complete_set_async`). Qty below `minimum_order_size` → `rejected_insufficient_capital`. Realtime tracks `_paper_tasks` and cancels/awaits them on shutdown. Paper only if forward, net>0, not stale/skewed, books ready, and `passes_rule_set is not False`. |
| **Tests** | `test_paper_insufficient_capital`, `test_concurrent_threads_do_not_go_negative`, `test_async_lock_serializes_paper` |
| **Residual risk** | Affordable binary search is still approximate when the full desired size does not fit. Capital is reserved only for sequential YES+NO buys, not for later residual sells. |

### 5. execution_simulator residual inventory

| | |
|---|---|
| **Files** | `src/polymarket_scanner/simulation/execution_simulator.py`, `src/polymarket_scanner/models.py`, `tests/test_simulator.py`, `tests/test_simulator_residual.py` |
| **Defect** | Force-close omitted residual buy fees. Partial close mixed full acquisition cost with partial proceeds. |
| **Fix** | Close path allocates acquisition cost **and** original buy fee by fill fraction. Leftover qty stays in `remaining_inventory` / `unrealized_inventory_cost`. `SimulationResult` adds `realized_pnl`, `unrealized_inventory_cost`, `remaining_inventory` (old `net_profit` kept). `realized_pnl` equals completed (matched+closed) net; leftover cost is not treated as realized loss. |
| **Tests** | `test_residual_partial_close_exact_no_fees`, `test_residual_includes_original_buy_fee`, `test_partial_second_leg_one_leg_risk` |
| **Residual risk** | `net_profit` still subtracts operational_cost / safety_buffer; it is a scenario metric, not a cash ledger. |

### 6. Observed-delay simulation

| | |
|---|---|
| **Files** | `src/polymarket_scanner/simulation/execution_simulator.py`, `src/polymarket_scanner/discovery/orderbook_collector.py`, `src/polymarket_scanner/ui/app.py`, `tests/test_observed_delay.py` |
| **Defect** | Delay profiles reused t0 books and copied base quality. UI could look like a real wait. |
| **Fix** | `select_delayed_books` requires `target <= fetched_at <= target + tolerance` and YES/NO skew ≤ `max_book_skew_ms`. Missing later books → `estimated`; too early → `stale`; skew → `unavailable`. Each profile keeps its own quality. Simulator UI warns when quality is not `observed_snapshot`. Details record t0 hashes / timestamps / generations. |
| **Tests** | `test_observed_window_requires_target_to_tolerance`, `test_observed_skew_unavailable`, `test_missing_delayed_is_estimated_not_observed`, `test_profiles_keep_independent_quality`, `test_estimated_quality_without_delayed_snapshot` |
| **Residual risk** | Observed delay needs persisted snapshots in the window. Hash-deduped books update `last_seen_at` without a new row, so sparse history can force `estimated`. |

## Phase 3 — rules, runtime settings, stats

### 7. Rule engine in the pipeline

| | |
|---|---|
| **Files** | `src/polymarket_scanner/scanners/pipeline.py`, `src/polymarket_scanner/scanners/rule_engine.py`, `src/polymarket_scanner/database.py`, `src/polymarket_scanner/realtime.py`, `tests/test_integrity.py`, `tests/test_rule_engine.py` |
| **Defect** | Rules were not applied on persist. Paper could fire on raw ticks. |
| **Fix** | All raw opportunities are stored. `OpportunityRow` has `episode_id`, `passes_rule_set`, `rule_set_id`, `rule_set_version`. Paper requires `passes_rule_set is not False`. Saving a rule set increments `version`. |
| **Tests** | `test_persist_raw_and_rule_flags`, existing `test_rule_engine.py` |
| **Residual risk** | If no rule set exists, `passes` defaults to True (research-open). Default DB seeds Balanced as enabled. |

### 8. scanner_max_pages / market_limit / sync_markets

| | |
|---|---|
| **Files** | `src/polymarket_scanner/runtime_settings.py`, `src/polymarket_scanner/config.py`, `src/polymarket_scanner/scheduler.py`, `src/polymarket_scanner/realtime.py`, `scripts/run_scanner.py`, `src/polymarket_scanner/ui/scanner_control.py`, `tests/test_runtime_settings.py`, `tests/test_scanner_control.py` |
| **Defect** | UI/CLI flags were stored but daemon/realtime ignored them. `bool("false")` would have been True. |
| **Fix** | `apply_runtime_to_config` maps `scanner_max_pages`, `scanner_market_limit`, `scanner_sync_markets` with `parse_strict_bool`. Static daemon `run_once` and realtime discovery/resync read `cfg.scanner.*` each loop. CLI `--max-pages` / `--market-limit` / `--no-sync` are passed into `run_daemon`. |
| **Tests** | `test_apply_runtime_pages_and_limit`, `test_run_scanner_cli_exposes_daemon_limits`, `test_phase_params_runtime_settings` |
| **Residual risk** | Changing pages/limit does not drop in-memory WS state until the next resync interval. |

### 9. Dashboard / daily report semantics

| | |
|---|---|
| **Files** | `src/polymarket_scanner/scheduler.py`, `src/polymarket_scanner/reporting/html_report.py`, `src/polymarket_scanner/ui/app.py` |
| **Defect** | Active count summed historical `OpportunityRow`. Per-tick `base_net` was treated as profit. Categories were all `unknown`. |
| **Fix** | Active = open `OpportunityEpisodeRow`. Theoretical count = first-seen episodes. Realized = `PaperAccountRow.realized_pnl` / `PaperTrade.pnl`. Daily report joins `MarketRow.category`. UI labels: raw signals, qualified signals, first-seen episodes, paper realized P&L (simulated). |
| **Tests** | `test_close_episodes_reason` (active → 0 after close) |
| **Residual risk** | Qualified-today still counts OpportunityRow ticks that passed rules, not unique episodes (episodes are a separate metric). |

## Phase 4 — integrity and performance

### 10. WS book persist throttle + hash dedup

| | |
|---|---|
| **Files** | `src/polymarket_scanner/discovery/orderbook_collector.py`, `src/polymarket_scanner/realtime.py`, `tests/test_integrity.py` |
| **Defect** | Every WS update wrote a full level copy. |
| **Fix** | Same `(token_id, hash)` updates `last_seen_at` only. Realtime persist is throttled by `ws_persist_min_interval_ms`. |
| **Tests** | `test_persist_same_hash_updates_last_seen` |
| **Residual risk** | Throttle + hash dedup reduce delayed-sim snapshot density (see item 6). |

### 11. Gamma reconcile vs max_pages

| | |
|---|---|
| **Files** | `src/polymarket_scanner/discovery/market_discovery.py`, `tests/test_integrity.py` |
| **Defect** | Partial page fetches could delist live markets not on the first pages. |
| **Fix** | `reconcile_unseen_markets(..., full_sync=)` only when `max_pages is None`. Limited scans never delist. |
| **Tests** | `test_reconcile_skips_when_not_full_sync` |
| **Residual risk** | A failed full Gamma fetch that returns a short list would still delist. There is no completeness checksum beyond `max_pages is None`. |

### 12. Strict bool parser

| | |
|---|---|
| **Files** | `src/polymarket_scanner/parse_bool.py`, `src/polymarket_scanner/api/gamma_client.py`, `src/polymarket_scanner/runtime_settings.py`, `tests/test_parse_bool.py` |
| **Defect** | Python `bool("false")` is True. Missing `acceptingOrders` / `enableOrderBook` defaulted tradable. |
| **Fix** | `parse_strict_bool` / `parse_bool_conservative`. Missing flags → False + `parse_reasons`. Runtime `"false"` → False. |
| **Tests** | `test_string_false_is_false`, `test_missing_accepting_orders_conservative`, `test_gamma_string_false_not_tradable` |
| **Residual risk** | `active`/`closed` missing still default True/False (legacy Gamma payloads). |

### 13. Episode close reasons on data gaps

| | |
|---|---|
| **Files** | `src/polymarket_scanner/scanners/opportunity_tracker.py`, `src/polymarket_scanner/realtime.py` |
| **Fix** | `close_reason` values: `signal_gone`, `market_removed`, `ws_disconnected`, `stale_books`. |
| **Tests** | `test_close_episodes_reason`, `test_episode_open_and_close` |
| **Residual risk** | Static daemon does not close on HTTP book fetch failure for a previously open market unless that market is scanned with no signal. |

### 14. Latency / signal batching

| | |
|---|---|
| **Files** | `src/polymarket_scanner/scanners/pipeline.py`, `tests/test_integrity.py` |
| **Fix** | WS latency samples buffer (flush 32 or shutdown). Opportunity rows for one persist call share one SQLite transaction. |
| **Tests** | `test_latency_batches_until_flush` |
| **Residual risk** | Dirty-market recalc still writes opportunities synchronously per market, not a global queue. |

### 15. minimum_order_size / tick_size / fee coverage

| | |
|---|---|
| **Files** | `src/polymarket_scanner/scanners/binary_complete_set.py`, `src/polymarket_scanner/simulation/paper_trader.py`, `src/polymarket_scanner/simulation/fee_calculator.py`, `src/polymarket_scanner/api/clob_client.py` |
| **Fix** | Scanner tags `below min order size` and `fee schedule missing`. Paper rejects below min size. Tick size drives slippage. Missing fee schedule uses conservative 0.07 when fees are not explicitly off. |
| **Tests** | `test_missing_schedule_not_treated_as_free_when_fees_unknown`, `test_explicit_fee_free_still_zero` |
| **Residual risk** | Walker still sizes below venue min for raw signals (tagged, not blocked). |

### 16. Fee enrichment concurrency

| | |
|---|---|
| **Files** | `src/polymarket_scanner/discovery/market_discovery.py` |
| **Fix** | `asyncio.Semaphore(8)` plus counts `successful` / `missing` / `fallback` / `errors`. Endpoint is `GET /clob-markets/{condition_id}`. |
| **Tests** | `tests/test_clob_fees.py` |
| **Residual risk** | Enrichment capped at 500 markets per sync. |

### 17. HTTP proxy

| | |
|---|---|
| **Files** | `src/polymarket_scanner/api/http_base.py`, `pyproject.toml`, `README.md` |
| **Fix** | Honors `HTTPS_PROXY` / `HTTP_PROXY` / `ALL_PROXY`. Optional extra: `pip install '.[socks]'` → `httpx[socks]`. |
| **Tests** | None (env-dependent). |
| **Residual risk** | SOCKS without the extra extra will fail at client construction. |

## Tooling

| | |
|---|---|
| **Files** | `pyproject.toml` |
| **Fix** | mypy: `mypy_path=src`, `packages=["polymarket_scanner"]`, numpy/pandas/streamlit overrides, exclude Streamlit `app.py`. Ruff lint select `E4,E7,E9,F,I,W` so `ruff check src tests` is a real gate without FURB noise on Decimal string constructors. |

## Trading surface search

Repo-wide search found **no** wallet, private key, `eth_account`, `web3`, `sign_order`, or `POST /order` client. Mentions are documentation / UI disclaimers and `safety.guard_write_endpoint`, which raises `NotImplementedError` on write `/order` paths. `.env` has no keys.

## Verification

```
pytest -q          # 81 passed
ruff check src tests
mypy src           # Success: no issues found in 39 source files
```

## Unresolved product risks (not claimed as fixed)

1. Paper and simulator P&L are **not** live fills. Multi-leg YES+NO is not atomic on Polymarket.
2. Observed-delay quality degrades to estimated whenever history is thin.
3. Residual inventory has no later mark-to-market or forced flatten job.
4. Full Gamma reconcile trusts “no max_pages” as completeness.
5. Official WS unsubscribe may still require a reconnect (`request_rebuild`).
