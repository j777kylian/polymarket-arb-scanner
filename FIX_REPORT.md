# FIX_REPORT — polymarket-arb-scanner (accounting + research report round)

All results remain **simulated / paper-only**. `TRADING_ENABLED=False`. No wallet, private key, signing, or `POST /order`.

---

## 本轮已修复

### 1. `settle_market_resolved` 真结算（cash + realized）

此前只把 mark 打到 1/0 并刷新 equity，**没有**把胜方 payout 计入 cash，也没有按 `payout - cost_basis` 记 realized。

现在在同一 `session_scope` 事务内分别处理 live `PaperAccount` 与每个 `StrategyAccount`：

- 胜方 payout = `quantity * 1`，败方 = 0
- `cash += payout`；`realized_pnl += payout - remaining cost_basis`
- occupied / marked_inventory / equity / peak / drawdown 一致更新
- `PositionRow` / `StrategyPositionRow` 清零并 `status=settled`
- 关联 `PaperTradeRow` / `StrategyTradeRow` 更新 realized、`remaining_inventory=0`、`settled_at` / `realized_at`
- 支持 `winningTokenId` / `winning_token_id` / `winningAssetId` / `winning_asset_id` / `winningOutcome` / `winning_outcome`
- 无明确胜方时 **不** 结算、不删仓，写入 `ApiErrorRow` data-quality

回归：`tests/test_round4_accounting.py`（winner/loser/equity/winningTokenId/no_winner）

### 2. Live / strategy 持仓隔离

`set_positions_status(..., account_kind=, strategy_id=, strategy_version=)`：

- live 只改 `PositionRow`
- strategy 只改对应 `StrategyPositionRow`
- `mark_market_books` **只改 mark price，不改 status**
- 同 `trade_id` 的 shadow 不会改 live；一个 shadow 不会改另一个

### 3. 第一腿后状态机

`validate_execution_snapshot(..., require_episode_open=True|False)`：

- 第一腿前仍要求 episode open
- 第一腿成交后，第二腿 / emergency-close **不再**因 `episode_closed` 中止
- 第二腿失败且 `force_close_unhedged` → 进入 emergency close
- close snapshot 无效 → `RESIDUAL_OPEN`
- 后续有效盘口：`attempt_residual_closes`（`residual_close_retries`，默认 3）并记录 `close_attempts`

### 4. `AccountSnapshotRow` 可审计时间序列

字段：timestamp、account_kind、strategy_id、version、trade_id、event_type、cash、occupied_cost、marked_inventory、equity、realized_pnl、unrealized_pnl、drawdown。

事件：`first_leg_fill`、`second_leg_fill`、`merge`、`close`、`residual_mark`、`market_settlement`（reject 可选）。

### 5. 自包含 HTML research report

`reporting/html_report.py` 重写：Plotly JS **inline**，无 CDN。含 funnel、equity 曲线、edge vs realized、latency、reject 分布、residual、分组、live vs shadow、walk-forward、可过滤 audit、硬门槛 recommendations。

统计口径：

- raw ticks / unique episodes / trade attempts 分开
- Top opportunities 按 **episode** 去重
- one-leg loss 来自实际 Paper/Strategy trades
- 每日 realized 按 `realized_at` / `settled_at`（非 `created_at`）
- 样本不足 → `INSUFFICIENT SAMPLE`，不给参数推荐
- 图表标注 `SIMULATED / NOT EXECUTABLE PROFIT`

### 6. 验证

```text
pytest -q          → 122 passed
ruff check src tests → All checks passed
mypy src           → Success: no issues found in 45 source files
```

---

## 安全不变式

- `TRADING_ENABLED=False`（permanent read-only）
- 无钱包 / 私钥 / 签名 / `POST /order`
- Paper 仅为本地模拟记账
