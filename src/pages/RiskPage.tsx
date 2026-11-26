import React, { useEffect, useState } from "react";
import { Button, PageHeader } from "../components/ui";
import PageState from "../components/PageState";
import { useInvoke } from "../hooks/useInvoke";
import { api } from "../lib/tauri";
import type { RiskConfig } from "../lib/types";

/**
 * Risk page (phase 5A §3): the kill switch toggle and the hard limits.
 * Every new order passes the gateway first (kill switch, account
 * status, book freshness, qty/price validity, per-order max notional,
 * available funds) — no path bypasses it. Cancels are never blocked by
 * the kill switch: it stops new orders only, so you can always exit.
 *
 * The toggle takes effect immediately and persists across restarts (a
 * kill switch left on stays on). Limits save with the 保存 button and
 * are validated backend-side (a bad value is rejected, never clamped).
 */

const CHECKS: { title: string; desc: string }[] = [
  { title: "总开关", desc: "开启时禁止一切新下单" },
  { title: "回撤", desc: "会话回撤超过上限时停止开仓，直到权益恢复" },
  { title: "账户状态", desc: "账户必须处于已连接，未知状态按未连接处理" },
  { title: "行情新鲜度", desc: "标的盘口超过 5 秒未更新时禁止下单" },
  { title: "数量 / 价格", desc: "数量大于 0，限价大于 0，且严格对齐步长（Decimal 计算）" },
  { title: "交易方向", desc: "只做多时禁止开空，只做空时禁止开多" },
  { title: "单笔名义上限", desc: "一笔订单的名义价值不得超过上限" },
  { title: "最大仓位", desc: "当前持仓名义 + 本单名义不得超过上限" },
  { title: "可用资金", desc: "单笔名义不得超过账户可用资金" },
];

const inputCls =
  "w-40 bg-ds-panel-2 border border-ds-line rounded-lg px-3 h-8 text-ds-text-1 outline-none focus:border-ds-accent num";

export default function RiskPage() {
  // Config + drawdown both come from useInvoke (Phase 2, I11); the
  // form fields sync from the loaded config below. Save / toggle are
  // click mutations and refresh the config afterwards.
  const cfg = useInvoke(() => api.getRiskConfig(), []);
  const dd = useInvoke(() => api.getDrawdownStatus(), []);
  const config = cfg.data;
  const drawdown = dd.data;

  useEffect(() => {
    const t = setInterval(() => dd.refresh(), 10_000);
    return () => clearInterval(t);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  const [maxOrder, setMaxOrder] = useState("");
  const [maxPosition, setMaxPosition] = useState("");
  const [maxDrawdown, setMaxDrawdown] = useState("");
  const [allowedSides, setAllowedSides] = useState<RiskConfig["allowedSides"]>("both");
  const [autoPause, setAutoPause] = useState("");
  const [saving, setSaving] = useState(false);
  const [toggling, setToggling] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [savedAt, setSavedAt] = useState<number | null>(null);

  useEffect(() => {
    if (!config) return;
    setMaxOrder(String(config.maxOrderNotionalUsd));
    setMaxPosition(String(config.maxPositionUsd));
    setMaxDrawdown(String(config.maxDrawdownPct));
    setAllowedSides(config.allowedSides);
    setAutoPause(String(config.autoPauseAfterFailures));
  }, [config]);

  const onToggle = async () => {
    if (!config || toggling) return;
    setToggling(true);
    setError(null);
    try {
      await api.setKillSwitch(!config.killSwitch);
      cfg.refresh();
    } catch (e) {
      setError(String(e));
    } finally {
      setToggling(false);
    }
  };

  const onSave = async () => {
    if (!config || saving) return;
    setSaving(true);
    setError(null);
    try {
      await api.setRiskConfig({
        killSwitch: config.killSwitch,
        maxOrderNotionalUsd: Number(maxOrder),
        maxPositionUsd: Number(maxPosition),
        maxDrawdownPct: Number(maxDrawdown),
        allowedSides,
        autoPauseAfterFailures: Number(autoPause),
      });
      cfg.refresh();
      setSavedAt(Date.now());
    } catch (e) {
      setError(String(e));
    } finally {
      setSaving(false);
    }
  };

  if (cfg.loading && !config) {
    return <PageState loading loadingText="加载风控配置…" />;
  }
  if (cfg.error || !config) {
    return <PageState error={`风控配置加载失败：${cfg.error ?? "未知错误"}`} />;
  }

  return (
    <div>
      <PageHeader title="风控">
        <Button onClick={onSave} disabled={saving}>
          {saving ? "保存中…" : "保存限额"}
        </Button>
      </PageHeader>

      {/* Kill switch: one big toggle. Red = killing (no new orders). */}
      <div
        className={`mb-4 rounded-lg border p-4 ${
          config.killSwitch
            ? "border-ds-down/60 bg-ds-down/10"
            : "border-ds-line bg-ds-panel"
        }`}
      >
        <div className="flex items-center justify-between gap-3">
          <div>
            <div className="text-[14px] font-semibold text-ds-text-1">
              {config.killSwitch ? "总开关已开启 — 禁止一切新下单" : "总开关已关闭 — 允许下单"}
            </div>
            <div className="mt-1 text-xs text-ds-text-2">
              撤单永远不受总开关影响；开关状态会保存，重启后仍然生效。
            </div>
          </div>
          <Button onClick={onToggle} disabled={toggling}>
            {toggling ? "切换中…" : config.killSwitch ? "关闭总开关" : "开启总开关"}
          </Button>
        </div>
      </div>

      {/* Hard limits. */}
      <div className="mb-4 rounded-lg border border-ds-line bg-ds-panel p-4">
        <div className="mb-3 text-[13px] font-semibold text-ds-text-1">硬性限额</div>
        <div className="flex flex-col gap-3 text-[13px] text-ds-text-2">
          <label className="flex items-center gap-3">
            <span className="w-36 shrink-0">单笔最大名义（USD）</span>
            <input
              className={inputCls}
              type="number"
              min={0}
              step="any"
              value={maxOrder}
              onChange={(e) => setMaxOrder(e.target.value)}
            />
          </label>
          <label className="flex items-center gap-3">
            <span className="w-36 shrink-0">最大持仓名义（USD）</span>
            <input
              className={inputCls}
              type="number"
              min={0}
              step="any"
              value={maxPosition}
              onChange={(e) => setMaxPosition(e.target.value)}
            />
          </label>
          <label className="flex items-center gap-3">
            <span className="w-36 shrink-0">最大回撤（%）</span>
            <input
              className={inputCls}
              type="number"
              min={0}
              max={100}
              step="any"
              value={maxDrawdown}
              onChange={(e) => setMaxDrawdown(e.target.value)}
            />
          </label>
          <label className="flex items-center gap-3">
            <span className="w-36 shrink-0">允许方向</span>
            <select
              className={inputCls}
              value={allowedSides}
              onChange={(e) =>
                setAllowedSides(e.target.value as RiskConfig["allowedSides"])
              }
            >
              <option value="both">双向</option>
              <option value="long_only">只做多</option>
              <option value="short_only">只做空</option>
            </select>
          </label>
          <label className="flex items-center gap-3">
            <span className="w-36 shrink-0">连续异常自动暂停（次）</span>
            <input
              className={inputCls}
              type="number"
              min={0}
              max={10}
              step={1}
              value={autoPause}
              onChange={(e) => setAutoPause(e.target.value)}
            />
            <span className="text-xs text-ds-text-3">0 = 不自动暂停</span>
          </label>
        </div>
        {error && <div className="mt-3 text-xs text-ds-down">{error}</div>}
        {savedAt != null && !error && (
          <div className="mt-3 text-xs text-ds-text-3">
            已保存 · {new Date(savedAt).toLocaleTimeString("zh-CN", { hour12: false })}
          </div>
        )}
      </div>

      {/* Drawdown: session equity vs the high-water mark. */}
      <div className="mb-4 rounded-lg border border-ds-line bg-ds-panel p-4">
        <div className="mb-3 text-[13px] font-semibold text-ds-text-1">回撤控制</div>
        {drawdown == null || drawdown.initialEquity == null ? (
          <div className="text-[13px] text-ds-text-2">
            尚未采样到权益（连接账户后每 10 秒采样一次）。
          </div>
        ) : (
          <div className="flex flex-col gap-2 text-[13px]">
            <div className="flex gap-3">
              <span className="w-24 shrink-0 text-ds-text-2">初始权益</span>
              <span className="num text-ds-text-1">${drawdown.initialEquity.toLocaleString()}</span>
            </div>
            <div className="flex gap-3">
              <span className="w-24 shrink-0 text-ds-text-2">会话最高</span>
              <span className="num text-ds-text-1">${drawdown.sessionHigh.toLocaleString()}</span>
            </div>
            <div className="flex gap-3">
              <span className="w-24 shrink-0 text-ds-text-2">当前权益</span>
              <span className="num text-ds-text-1">
                ${drawdown.lastEquity?.toLocaleString() ?? "—"}
              </span>
            </div>
            <div className="flex gap-3">
              <span className="w-24 shrink-0 text-ds-text-2">当前回撤</span>
              <span
                className={`num ${
                  drawdown.drawdownPct > config.maxDrawdownPct
                    ? "text-ds-down"
                    : "text-ds-text-1"
                }`}
              >
                {drawdown.drawdownPct.toFixed(2)}%
                {drawdown.drawdownPct > config.maxDrawdownPct && " — 已超限，停止开仓"}
              </span>
            </div>
          </div>
        )}
      </div>

      {/* What the gateway checks, before every order. */}
      <div className="rounded-lg border border-ds-line bg-ds-panel p-4">
        <div className="mb-3 text-[13px] font-semibold text-ds-text-1">下单前硬校验（全部通过才发送）</div>
        <div className="flex flex-col gap-2">
          {CHECKS.map((c) => (
            <div key={c.title} className="flex gap-3 text-[13px]">
              <span className="w-24 shrink-0 text-ds-text-1">{c.title}</span>
              <span className="text-ds-text-2">{c.desc}</span>
            </div>
          ))}
        </div>
        <div className="mt-3 text-xs text-ds-text-3">
          超时或结果不明的下单永远不算成交，会记为“待核对”并触发订单状态查询。
        </div>
      </div>
    </div>
  );
}
