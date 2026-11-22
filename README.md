# DeSSS

便携式桌面量化交易工具 — Tauri v2 (Rust + React + TypeScript + Tailwind + SQLite)

> 当前阶段：项目骨架 + 核心接口契约。**不接任何真实交易所 API**，只有 MockAdapter。

## 工作流（分支）

1. 功能在分支上开发（如 `feat/scaffold-v1`），不直接推 `main`
2. 推分支后到 GitHub Actions 下载 Windows 便携包实机测试：
   仓库 → Actions → **Build (portable test)** → 最新 run → Artifacts → `DeSSS-windows-portable`
   也可以在 Actions 页面点 **Run workflow** 手动触发（workflow_dispatch）
3. 实机验证通过后，再决定是否合并到 `main`；合并后再开新分支做下一阶段

## 便携式目录

所有持久化文件只读写 **可执行文件同级** 的 `./data`：

```
DeSSS/
  desss.exe
  data/
    desss.db
    logs/
```

不使用 `%APPDATA%` / `~/.config` 等系统全局配置目录。实现：`src-tauri/src/portable.rs` 用 `std::env::current_exe()` 解析。

## 本地启动验证

前置：Node 20+、Rust stable、（Linux 还需 webkit2gtk-4.1 等 Tauri 系统依赖）

```bash
npm install
npm run tauri dev    # 开发运行，自动启动 Mock 行情
# 或仅前端
npm run build        # tsc + vite build 验证前端编译
cargo check --manifest-path src-tauri/Cargo.toml
```

打开后默认进「交易」页：应看到 BTCUSDT / ETHUSDT 每 500ms 跳动；切到「账户」页看 Mock 余额每 2s 推送。

## 目录规划

```
src/                        # React + TS
  components/Sidebar.tsx    # 左侧固定导航
  pages/                    # 账户 / 策略 / 交易 / 数据 / 风控 / 系统（6 空壳）
  store/                    # Zustand: useAppStore, useMarketStore
  lib/                      # Tauri invoke/listen 封装 + 类型
src-tauri/src/
  portable.rs               # ./data 解析
  adapters/                 # ExchangeAdapter trait + types + MockAdapter
  bus/                      # EventBus (tokio broadcast) + AppEvent
  data_engine/              # SQLite (rusqlite bundled) 初始化
  strategy_engine/          # LegBinding / StrategyDefinition / Strategy trait
  risk/                     # RiskConfig + check_order
  vault/                    # Vault trait（占位，后续加密存 ./data）
  commands/                 # Tauri commands + Bus→前端事件转发
.github/workflows/build.yml # 分支构建 + Windows 便携包 Artifact
```

## 核心契约

- `ExchangeAdapter`（async trait）：`connect/disconnect`、`subscribe_ticks/unsubscribe_ticks`、`get_balances/get_positions/get_open_orders`、`place_order/cancel_order`
- 策略腿：`Vec<LegBinding>`，`LegBinding { leg_id, adapter_id, account_id, symbol, ratio, side, enabled }` — 1 条=单腿，N 条=多腿，不写死 A/B
- `MockAdapter`：随机游走 BTC/ETH 行情 500ms、余额 2s → `EventBus` → Tauri 事件 `market://tick` / `account://balance` → Zustand → 页面
