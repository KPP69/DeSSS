# DeSSS

便携式桌面量化交易工具 — Tauri v2 (Rust + React + TypeScript + Tailwind + SQLite)

> 当前阶段：Phase 1 后端收口 + Phase 2 前端收口已完成。六所交易所只读接入已打通，
> 下单仅 Hyperliquid / Entropy（后端管线在，交易页无手动下单入口，留给策略模块调用）。

## 交易所与只读范围

| 交易所 | 读内容 | 下单 |
| --- | --- | --- |
| Hyperliquid | 权益 / 持仓 / 挂单 / 盘口 / 品种目录 | 支持（后端管线） |
| Entropy（HIP-3，dex=`io`） | 同上（与 Hyperliquid 共用主钱包资金池） | 支持（后端管线） |
| Lighter（主网） | 权益 / 持仓 / 品种目录 | 不支持 |
| Lighter（Robinhood Chain） | 同上 | 不支持 |
| AsterDEX（V3，仅 Agent 钱包） | 权益 / 持仓 / 品种目录 | 不支持 |
| Arcus | 权益 / 持仓 / 品种目录 | 不支持 |

所有数字都来自真实交易所读取，没有模拟数据。前端从不直连交易所（CSP
connect-src 只有 `'self'` 与 `ipc:`），全部流量走 Rust 后端。

## 品种目录与刷新

品种目录（交易所 / 合约 / 标记价 / 资金费率 / 杠杆等）落库保存，数据页搜索
只读本地库、不走网络。后台刷新时机：启动时、每 12 小时、搜索未命中时、
手动点「刷新」。盘口（买一 / 卖一）走 WebSocket 实时流，5 秒无帧回退到
目录里的值。

## 便携式目录

所有持久化文件只读写 **可执行文件同级** 的 `./data`：

```
DeSSS/
  desss.exe
  data/
    desss.db        # 账户元数据 + 品种目录（SQLite）
    vault.key       # 凭证加密密钥
    vault.enc       # AES-256-GCM 加密后的账户凭证
    proxy.json      # 自定义代理（可选）
    logs/
      app.log       # 应用日志，5 MB × 2 滚动（app.log / app.log.1）
      crash.log     # 崩溃日志（panic 时写）
    webview/        # WebView 缓存
```

不使用 `%APPDATA%` / `~/.config` 等系统全局配置目录。整个 DeSSS 文件夹拷到
另一台 Windows 电脑仍可恢复凭证（`vault.key` 与 `vault.enc` 须一起拷）。

## 如何看日志

- 应用内：底部控制台抽屉实时显示日志（与 `app.log` 同源）。
- 文件：打开 `./data/logs/app.log`，最新内容在文件末尾；`app.log.1` 是上一轮。
- 报错时请贴 `app.log` 末尾 50 行（不要贴整文件），并注明操作步骤。
- 崩溃（窗口直接消失）时看 `./data/logs/crash.log`。

## 工作流（分支）

1. 功能在分支上开发（如 `feat/realtime-ws-and-order-core`），不直接推 `main`
2. 推分支后到 GitHub Actions 下载 Windows 便携包实机测试：
   仓库 → Actions → **Build (portable test)** → 最新 run → Artifacts → `DeSSS-windows-portable`
3. 实机验证通过后，再决定是否合并；合并后再开新分支做下一阶段

## 本地开发

```bash
npm install
npm run tauri dev   # 桌面应用（需 Rust 工具链）
npm run build       # 仅前端构建
npx tsc --noEmit    # 类型检查
```

后端测试用 scratch crate（`rebuild-desss-test.sh`，不含 Tauri 依赖的
commands / lib.rs / logging / strategy_engine 由 CI 的 `cargo check` 覆盖）。
