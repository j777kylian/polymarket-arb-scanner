# FIX_REPORT — polymarket-arb-scanner (post-92c5a2f)

All results remain **simulated / paper-only**. `TRADING_ENABLED=False`. No wallet, private key, signing, or `POST /order`.

This round continues from commit `92c5a2f`. Prior cashflow / book-lifecycle / rule-engine fixes are unchanged.

---

## 已修复

### 1. 产品结构：Snapshot Audit / Live Research

- UI / CLI / README / Docker 不再使用 Phase 1/2/3。
- **Snapshot Audit** = `--once` 单次 REST scan（保留，用于 API / 费用 / 盘口 / 公式诊断）。
- **Live Research** = `--daemon --mode live`，Execution Mode = Observe Only / Paper Trading。
- 删除 static REST **持续轮询 daemon**。`--daemon --mode static|snapshot` 直接拒绝。
- Paper Trading **复用** `RealtimeScanner` + `LiveBookCache`，没有第二套盘口逻辑。
- Docker 默认 `scanner` 为 Live Research（Observe Only）；`--profile paper` 为 Paper Trading。

### 2. WebSocket 官方 payload

首次连接：

```json
{"assets_ids": [...], "type": "market", "initial_dump": true, "custom_feature_enabled": true}
```

动态新增：`operation=subscribe` + `custom_feature_enabled`（**禁止** `type=market`）。

动态删除：`operation=unsubscribe`（**禁止** `type=unsubscribe`）。

`ws_subscribe_chunk: 0` 时一次发送全部 token。若仍分块：仅第一块 `type=market`，后续块 `operation=subscribe`。

处理 `tick_size_change` / `new_market` / `market_resolved`。`market_resolved` 与 removed 关闭对应 episode。

### 3. CLI 配置传递

`ScannerService.run_daemon` 在 overlay `--max-pages` / `--market-limit` 之后构造：

```python
RealtimeScanner(config=self.cfg, paper=paper)
```

`RealtimeScanner` 不再 `apply_runtime_to_config(get_config())`，运行循环也不再覆盖 CLI。

`ScannerRunRow` 记录 `discovered_markets`、`subscribed_markets`、`subscribed_tokens`、`ready_market_pairs`、`fee_schedule_coverage`、`mode`。

### 4. Paper 分腿真实性

Signal 后等待 `signal_to_first_leg_ms`，校验 episode open / pair ready / generation / stale / skew / `snapshot_time >= target_time`。模拟第一腿；等待 `inter_leg_delay_ms` 后取**新**第二腿快照；残仓则在 `force_close_delay_ms` 后取第三本簿平仓。每次拒绝写入明确 `reject_reason`。残仓成本仍是 occupied inventory，不是已实现亏损。

`PaperTradeRow` 增加 strategy / 时间戳 / book hash / expected vs realized 字段。账户跟踪 peak equity、max drawdown、marked inventory。

### 5. 日报

- Daily realized P&L = 当天 `PaperTradeRow.created_at` 的 `realized_pnl` 之和。
- 同时展示 cumulative realized、cash、occupied inventory cost、marked inventory、equity、max drawdown。
- Live Research 受 `market_limit` 限制时，Markets scanned = `subscribed_markets`，不是数据库全部市场。
- daemon 启动把 `last_report_date` 设为今天，避免启动瞬间空报告；停止时重新查库生成。

### 6. Shadow 策略比较（只建议，不自动改参）

表：`strategy_configs` / `strategy_runs` / `strategy_accounts` / `strategy_trades` / `strategy_evals`。版本不可变；历史交易保留当时 `strategy_version`。多 shadow 共用同一组 LiveBookCache 快照，账户资金互不占用。Walk-forward 训练/验证窗口严格分离；样本不足（默认 &lt; 30 笔验证交易）不推荐。推荐不会写入运行中 paper 参数。

---

## 测试覆盖

`pytest -q` 新增/更新：

| 要求 | 测试 |
|------|------|
| 官方动态 WS 订阅 payload | `tests/test_market_ws.py` |
| CLI realtime 参数不丢失 | `tests/test_cli_realtime_config.py` |
| 500ms 后 episode 消失不成交 | `test_episode_closed_after_delay_does_not_fill` |
| 延迟后 stale / skewed 拒绝 | `test_stale_or_skewed_books_rejected` |
| 两腿使用不同时间快照 | `test_two_legs_use_different_time_snapshots` |
| tick_size_change 更新 | `tests/test_tick_size_ws.py` |
| daily P&L ≠ 累计 P&L | `test_daily_pnl_not_equal_cumulative` |
| realtime market count | `test_realtime_market_count_uses_subscribed_not_all_db` |
| shadow 账户隔离 | `test_shadow_strategy_accounts_are_isolated` |
| 样本不足不推荐 | `test_walk_forward_insufficient_sample_does_not_recommend` |

既有 paper cashflow / book lifecycle / rule engine 测试保留。

---

## 真实 API 验证

本轮**没有**对 Polymarket 生产 WebSocket 或 Gamma/CLOB 做联机验证。

未验证：

- `operation=subscribe` / `unsubscribe` 在当前 CLOB 网关上的接受情况（按官方文档构造；发送失败仍会 `request_rebuild` + `initial_dump`）。
- `tick_size_change` / `new_market` / `market_resolved` 的线上字段名是否与解析别名完全一致。
- `--max-pages 1 --market-limit 50` 在真实 Gamma keyset 下的实际返回条数（单测 mock discovery，不断网）。
- 延迟后的真实盘口是否总会在 `snapshot_time >= target_time` 窗口内更新。

需要真人用只读环境跑：

```bash
python scripts/run_scanner.py --daemon --mode live --paper --max-pages 1 --market-limit 50
```

确认日志中的 `subscribed_markets=50` 与 WS 订阅成功。

---

## 仍为模拟的限制

- Paper 成交、P&amp;L、fill、残仓平仓全部是本地盘口模拟，**不能**当作可执行利润。
- 两腿在真实 CLOB 上不是原子的；模拟在锁内一次记账，等待期间其他任务可以穿插（不持有锁睡眠）。
- 残仓用成本占用资金，不是逐笔 mark-to-market（marked inventory 目前等于 occupied cost）。
- Shadow 策略只写 `strategy_*` 表；walk-forward 只输出建议，**不会**改 `live_default` 或 YAML。
- Snapshot Audit 仍是 REST 瞬时截面，没有 WebSocket 生命周期。
- 官方 100-token 限制取消后默认一次订阅全部 token；若节点仍拒绝大帧，可把 `ws_subscribe_chunk` 调回正数（仅第一块 `type=market`）。

永久只读约束未改：`src/polymarket_scanner/safety.py` `TRADING_ENABLED = False`。
