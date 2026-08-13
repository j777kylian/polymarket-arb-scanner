# FIX_REPORT — polymarket-arb-scanner (post-21bcea7)

All results remain **simulated / paper-only**. `TRADING_ENABLED=False`. No wallet, private key, signing, or `POST /order`.

This round continues from commit `21bcea7`. State machine, cash, and residual behaviour are proven by database tests, not by this report alone.

---

## 已修复

### 1. Paper 两腿状态机（立即第一腿成交）

`run_delayed_paper_trade` 不再等第二腿验证后再一次性提交两腿。

第一腿 snapshot 有效后立即：模拟成交、扣现金、写入 `PositionRow`、`leg_state=FIRST_LEG_FILLED`。

状态：`SIGNALLED`、`FIRST_LEG_FILLED`、`SECOND_LEG_FILLED`、`SECOND_LEG_FAILED`、`CLOSE_PENDING`、`RESIDUAL_OPEN`、`MERGED`、`CLOSED`。

第二腿 snapshot 无效时保留第一腿敞口（`one_leg` / `SECOND_LEG_FAILED`），整笔不为 0。测试：`test_second_snapshot_invalid_after_first_fill_leaves_exposure` 断言 `cash < 1000`、`cash >= 0`、`PositionRow.quantity > 0`。

### 2. 资本约束（不预支 merge proceeds）

第一腿只按单腿 `affordable_single_leg` 从当前现金计价；扣款后再按剩余现金给第二腿重新计价。中间任意时刻 `cash >= 0`。

回归：`starting_cash=1000`，YES ask 0.01 × 10000（成本 100），NO ask 0.99 × 10000（成本 9900）禁止完整成交 10,000 对。测试：`test_second_leg_cannot_spend_future_merge_proceeds`。

### 3. 强平 snapshot 无效 → 残仓

禁止回退到第二腿旧盘口。`reject_reason=close_snapshot_unavailable`，`leg_state=RESIDUAL_OPEN`。`simulation/inventory.py` 在后续有效盘口上 mark-to-market，在 `market_resolved` 时按胜负 1/0 更新残仓。测试：`test_invalid_close_snapshot_preserves_residual`。

### 4. PositionRow / StrategyPositionRow 与 equity

字段：`market_id`、`token_id`、`outcome`、`quantity`、`cost_basis`、`acquired_at`、`last_mark_price`、`marked_value`、`unrealized_pnl`、`status`。

保守市值 = 当前 best bid。`equity = cash + marked_value`，occupied cost 不再冒充市值。UI Paper / Strategies 分开显示 cost basis 与 marked value。测试：`test_residual_mark_to_market_changes_equity_drawdown`。

### 5. Shadow 独立候选宇宙

Shadow 吃同一批 raw forward episodes（books ready、非 stale/skew、net>0），不受 live Balanced 或主 `--paper` 开关控制。Observe Only 仍跑 shadow。

每策略独立跟踪 eligibility False→True，每个 episode 只 attempt 一次。`shadow_fast`（`min_net_profit=0.25`）能看到 0.25–0.50 的机会。测试：`test_shadow_candidate_universe_independent_from_live_rule`、`test_eligibility_false_to_true_triggers_once`。

### 6. Walk-forward 无 validation leakage

训练窗口只选候选；验证窗口只评估训练期选出的策略。禁止用验证 P&L 重选。窗口指标只来自该窗口 `StrategyTradeRow`。禁止用累计 `StrategyAccountRow` 覆盖 validation P&L。测试：`test_walk_forward_has_no_validation_leakage`（训练赢家 A，验证赢家 B，推荐仍为 A；A 的 validation realized=1 而不是账户 999）。

### 7. `market_resolved` condition ID

`RealtimeScanner.condition_to_market_id`。官方 payload `{"event_type":"market_resolved","market":"<condition_id>"}` 能关 episode、退订、更新残仓。测试：`test_condition_id_market_resolved`。

### 8. 延迟指标拆分

`book` / `market_book` 记为 `initial_snapshot_age`，不进 feed p50/p95。Feed 只统计 `price_change`（及 `last_trade_price`）。另记 `receive_to_recalc`、`signal_to_first_leg`、`first_to_second_leg`。测试：`test_initial_book_age_excluded_from_feed_latency`。

### 9. `new_market` resync debounce

订阅后 `new_market_resync_cooldown_seconds`（默认 30s）内的批量 `new_market` 不立刻触发全量 Gamma。

### 10. 自动日报跨日

`previous_report_date_due`：日期滚动时生成 **previous** 本地日期，而不是今天的空报。`reporting.timezone` 用于日界和 rollover（Settings 中有说明）。测试：`test_previous_day_daily_report`。

### 11. Docker profiles 互斥

Observe scanner：`profiles: ["observe"]`。Paper scanner：`profiles: ["paper"]`。UI 无 profile。

```bash
docker compose --profile paper up     # UI + Paper scanner
docker compose --profile observe up   # UI + Observe scanner
```

`docker compose up` 只起 UI。测试：`test_docker_paper_profile_does_not_start_observe_scanner`。

### 12. persist_signals → opportunity IDs

返回 `list[int]`；写入 `OpportunityEpisodeRow.last_opportunity_id`；`PaperTrade.signal_opportunity_id` 指向触发的 `OpportunityRow`。测试：`test_paper_trade_links_exact_opportunity_row`。

### 13. Strategy UI 与 Run 生命周期

Strategies 页：查看/创建新版本（只 INSERT，不 UPDATE `params_json`）、启用/停用、账户、交易、持仓、walk-forward。Live Research 启动 `start_strategy_runs()`，停止 `finish_open_strategy_runs()`。`StrategyTrade` 保存两腿时间、数量、价格、费用、hash、延迟、`signal_opportunity_id`。

---

## 测试覆盖

`pytest -q`：110 passed。新增 `tests/test_round3_fixes.py`：

| 要求 | 测试 |
|------|------|
| 第二腿 snapshot 无效保留敞口 | `test_second_snapshot_invalid_after_first_fill_leaves_exposure` |
| 第二腿不能花未来 merge 款 | `test_second_leg_cannot_spend_future_merge_proceeds` |
| 无效强平 snapshot 留残仓 | `test_invalid_close_snapshot_preserves_residual` |
| 残仓 MTM 改 equity/drawdown | `test_residual_mark_to_market_changes_equity_drawdown` |
| Shadow 宇宙独立于 live rule | `test_shadow_candidate_universe_independent_from_live_rule` |
| eligibility False→True 一次 | `test_eligibility_false_to_true_triggers_once` |
| walk-forward 无 validation leakage | `test_walk_forward_has_no_validation_leakage` |
| condition-id market_resolved | `test_condition_id_market_resolved` |
| 初始 book age 不进 feed latency | `test_initial_book_age_excluded_from_feed_latency` |
| 跨日生成前一日日报 | `test_previous_day_daily_report` |
| Docker paper 不起 observe | `test_docker_paper_profile_does_not_start_observe_scanner` |
| PaperTrade 链到 OpportunityRow | `test_paper_trade_links_exact_opportunity_row` |

`ruff check src tests` 通过。`mypy src` 通过。

---

## 真实 API 验证

本轮没有对 Polymarket 生产 WebSocket / Gamma / CLOB 做联机验证。

需要真人只读环境确认：`docker compose --profile paper up` 只有 UI + paper scanner；Observe Only 下 shadow 仍写入 `strategy_*` 表。

---

## 仍为模拟的限制

- Paper 成交、P&L、残仓平仓全部是本地盘口模拟，不能当作可执行利润。
- 两腿在真实 CLOB 上不是原子的；模拟在锁内记账，等待期间其他任务可以穿插。
- Walk-forward 只建议，不会改 `live_default` 或 YAML。

永久只读：`src/polymarket_scanner/safety.py` `TRADING_ENABLED = False`。
