import React, { useEffect, useLayoutEffect, useState } from "react";
import { Button, ConfirmDialog, Input, SelectDropdown, StatCard, Switch } from "../components/ui";
import PageState from "../components/PageState";
import { useInvoke } from "../hooks/useInvoke";
import { api } from "../lib/tauri";
import type { RiskConfig } from "../lib/types";
import { useShellActions } from "../store/useShellActions";
import { toast } from "../store/useToastStore";

/**
 * Risk page (F3, phase 5A §3): the kill switch toggle and the hard
 * limits. Every new order passes the gateway first (kill switch,
 * account status, book freshness, qty/price validity, per-order max
 * notional, available funds) — no path bypasses it. Cancels are never
 * blocked by the kill switch: it stops new orders only, so you can
 * always exit.
 *
 * The toggle takes effect immediately and persists across restarts (a
 * kill switch left on stays on). Limits save with the 保存 button and
 * are validated backend-side (a bad value is rejected, never clamped);
 * the frontend intercepts the same ranges first and shows the error
 * under the field it belongs to.
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

/** Map a backend validation error to the field it names, if any. */
function fieldForBackendError(message: string): string | null {
  if (message.includes("单笔最大名义")) return "maxOrder";
  if (message.includes("最大持仓名义")) return "maxPosition";
  if (message.includes("最大回撤")) return "maxDrawdown";
  if (message.includes("连续异常")) return "autoPause";
  return null;
}

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
  const [pendingEnable, setPendingEnable] = useState(false);
  const [fieldErrors, setFieldErrors] = useState<Record<string, string>>({});
  const [formError, setFormError] = useState<string | null>(null);

  useEffect(() => {
    if (!config) return;
    setMaxOrder(String(config.maxOrderNotionalUsd));
    setMaxPosition(String(config.maxPositionUsd));
    setMaxDrawdown(String(config.maxDrawdownPct));
    setAllowedSides(config.allowedSides);
    setAutoPause(String(config.autoPauseAfterFailures));
  }, [config]);

  const onToggle = async (next: boolean) => {
    if (!config || toggling) return;
    setToggling(true);
    try {
      await api.setKillSwitch(next);
      cfg.refresh();
      toast.success(next ? "总开关已开启，禁止一切新下单" : "总开关已关闭，允许下单");
    } catch (e) {
      toast.error(e instanceof Error ? e.message : String(e));
    } finally {
      setToggling(false);
    }
  };

  const validate = (): Record<string, string> | null => {
    const errors: Record<string, string> = {};
    const check = (key: string, raw: string, label: string) => {
      const n = Number(raw);
      if (raw.trim() === "" || !Number.isFinite(n)) {
        errors[key] = `${label}必须是数字。`;
      } else if (n < 0) {
        errors[key] = `${label}不能为负数。`;
      }
    };
    check("maxOrder", maxOrder, "单笔最大名义");
    check("maxPosition", maxPosition, "最大持仓名义");
    check("maxDrawdown", maxDrawdown, "最大回撤");
    check("autoPause", autoPause, "连续失败自动暂停");
    const dd = Number(maxDrawdown);
    if (Number.isFinite(dd) && dd > 100) errors.maxDrawdown = "最大回撤不能超过 100%。";
    const ap = Number(autoPause);
    if (Number.isFinite(ap) && (!Number.isInteger(ap) || ap > 10)) {
      errors.autoPause = "连续失败自动暂停必须是 0–10 的整数。";
    }
    return Object.keys(errors).length > 0 ? errors : null;
  };

  const onSave = async () => {
    if (!config || saving) return;
    const errors = validate();
    if (errors) {
      setFieldErrors(errors);
      setFormError(null);
      return;
    }
    setFieldErrors({});
    setFormError(null);
    setSaving(true);
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
      toast.success("限额已保存");
    } catch (e) {
      const message = e instanceof Error ? e.message : String(e);
      toast.error(message);
      const fieldKey = fieldForBackendError(message);
      if (fieldKey) {
        setFieldErrors({ [fieldKey]: message });
      } else {
        setFormError(message);
      }
    } finally {
      setSaving(false);
    }
  };

  // Toolbar (F1): save-limits action + kill-switch state. Registered
  // before the loading early-returns below, so the bar is never empty.
  const register = useShellActions((s) => s.register);
  const unregister = useShellActions((s) => s.unregister);
  useLayoutEffect(() => {
    register("risk", {
      actions: (
        <Button onClick={onSave} loading={saving} disabled={!config}>
          保存限额
        </Button>
      ),
    });
    return () => unregister("risk");
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [register, unregister, config, maxOrder, maxPosition, maxDrawdown, allowedSides, autoPause, saving]);

  if (cfg.loading && !config) {
    return <PageState loading loadingText="加载风控配置…" />;
  }
  if (cfg.error || !config) {
    return (
      <PageState
        error={`风控配置加载失败：${cfg.error ?? "未知错误"}`}
        onRetry={() => cfg.refresh()}
      />
    );
  }

  const limitRow = (
    key: string,
    label: string,
    desc: string,
    unit: string,
    control: React.ReactNode,
  ) => (
    <div key={key} className="flex items-start gap-3">
      <div className="w-52 shrink-0 pt-1.5">
        <div className="text-[13px] text-ds-text-1">{label}</div>
        <div className="mt-0.5 text-xs text-ds-text-3">{desc}</div>
      </div>
      <div className="w-40 shrink-0">{control}</div>
      {unit && <span className="pt-2 text-xs text-ds-text-3">{unit}</span>}
    </div>
  );

  return (
    <div>
      {/* Kill switch: one big toggle. Red = killing (no new orders). */}
      <div
        className={`mb-4 rounded-[10px] border p-4 ${
          config.killSwitch
            ? "border-ds-down/60 bg-ds-down/10"
            : "border-ds-control-line bg-ds-panel"
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
          <Switch
            size="lg"
            checked={config.killSwitch}
            disabled={toggling}
            label="风控总开关"
            onChange={(next) => {
              if (next) setPendingEnable(true);
              else void onToggle(false);
            }}
          />
        </div>
      </div>

      {/* Hard limits: one row per item — label + note, input, unit. */}
      <div className="mb-4 rounded-[10px] border border-ds-line bg-ds-panel p-4">
        <div className="mb-3 text-[13px] font-semibold text-ds-text-1">硬性限额</div>
        <div className="flex flex-col gap-3">
          {limitRow(
            "maxOrder",
            "单笔最大名义",
            "一笔订单的名义价值不得超过上限",
            "USD",
            <Input
              type="number"
              value={maxOrder}
              onChange={(e) => setMaxOrder(e.target.value)}
              error={fieldErrors.maxOrder}
            />,
          )}
          {limitRow(
            "maxPosition",
            "最大持仓名义",
            "当前持仓名义 + 本单名义不得超过上限",
            "USD",
            <Input
              type="number"
              value={maxPosition}
              onChange={(e) => setMaxPosition(e.target.value)}
              error={fieldErrors.maxPosition}
            />,
          )}
          {limitRow(
            "maxDrawdown",
            "最大回撤",
            "会话回撤超过上限时停止开仓，直到权益恢复",
            "%",
            <Input
              type="number"
              value={maxDrawdown}
              onChange={(e) => setMaxDrawdown(e.target.value)}
              error={fieldErrors.maxDrawdown}
            />,
          )}
          {limitRow(
            "allowedSides",
            "允许方向",
            "只做多时禁止开空，只做空时禁止开多",
            "",
            <SelectDropdown
              options={[
                { value: "both", label: "双向" },
                { value: "long_only", label: "只做多" },
                { value: "short_only", label: "只做空" },
              ]}
              value={allowedSides}
              onChange={(v) => setAllowedSides(v as RiskConfig["allowedSides"])}
            />,
          )}
          {limitRow(
            "autoPause",
            "连续失败自动暂停",
            "连续失败达到该次数后自动暂停开仓，0 = 不自动暂停",
            "次",
            <Input
              type="number"
              value={autoPause}
              onChange={(e) => setAutoPause(e.target.value)}
              error={fieldErrors.autoPause}
            />,
          )}
        </div>
        {formError && <div className="mt-3 text-[13px] text-ds-down">{formError}</div>}
        <div className="mt-4">
          <Button onClick={onSave} loading={saving}>
            保存限额
          </Button>
        </div>
      </div>

      {/* Drawdown: session equity vs the high-water mark (read-only). */}
      <div className="mb-4 rounded-[10px] border border-ds-line bg-ds-panel p-4">
        <div className="mb-3 text-[13px] font-semibold text-ds-text-1">回撤控制</div>
        {drawdown == null || drawdown.initialEquity == null ? (
          <div className="grid grid-cols-3 gap-3">
            <StatCard title="当前回撤" value="—" sub="尚未采样到权益" compact />
            <StatCard title="会话高点" value="—" sub="尚未采样到权益" compact />
            <StatCard title="初始权益" value="—" sub="连接账户后每 10 秒采样一次" compact />
          </div>
        ) : (
          <div className="grid grid-cols-3 gap-3">
            <StatCard
              title="当前回撤"
              value={`${drawdown.drawdownPct.toFixed(2)}%`}
              tone={drawdown.drawdownPct > config.maxDrawdownPct ? "loss" : "default"}
              sub={drawdown.drawdownPct > config.maxDrawdownPct ? "已超限，停止开仓" : "会话回撤"}
              compact
            />
            <StatCard
              title="会话高点"
              value={`$${drawdown.sessionHigh.toLocaleString()}`}
              sub="会话权益最高点"
              compact
            />
            <StatCard
              title="初始权益"
              value={`$${drawdown.initialEquity.toLocaleString()}`}
              sub="会话开始时权益"
              compact
            />
          </div>
        )}
      </div>

      {/* What the gateway checks, before every order. */}
      <div className="rounded-[10px] border border-ds-line bg-ds-panel p-4">
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

      <ConfirmDialog
        open={pendingEnable}
        title="确认开启总开关"
        confirmText="确认开启"
        message="开启后将立即禁止所有新开仓，已有挂单与仓位不受影响。"
        onCancel={() => setPendingEnable(false)}
        onConfirm={async () => {
          setPendingEnable(false);
          await onToggle(true);
        }}
      />
    </div>
  );
}
