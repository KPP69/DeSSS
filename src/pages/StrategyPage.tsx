import React, { useEffect, useLayoutEffect, useState } from "react";
import { Button, Checkbox, ConfirmDialog, DataTable, Input, SelectDropdown, Switch, Tooltip } from "../components/ui";
import PageState from "../components/PageState";
import { useInvoke } from "../hooks/useInvoke";
import { useVenueSchemas } from "../hooks/useVenueSchemas";
import { api } from "../lib/tauri";
import type { LegRef, SaveStrategyInput, SpreadParams, StrategyView } from "../lib/types";
import { useAccountStore } from "../store/useAccountStore";
import { useShellActions } from "../store/useShellActions";
import { toast } from "../store/useToastStore";
import { useVenueStore } from "../store/useVenueStore";

/**
 * Strategy page (F3, Phase 3.1): the mean-reversion spread host.
 * Instances are listed with their live view state (last evaluation,
 * last error, orders placed) and created / edited through the inline
 * form below the toolbar. Only venues whose schema says
 * `supportsTrading` appear in the leg exchange dropdowns; everything
 * else (param ranges, account-connected checks, the midline warning)
 * is enforced backend-side and surfaced here verbatim, never silently.
 *
 * Columns: 启用（开关）· A 腿 · B 腿 · 参数摘要 · 最近评估 ·
 * 最近错误 · 累计下单 · 操作（编辑）. Numbers are tabular; the
 * order count is right-aligned. Disabling an instance asks first
 * (it stops new opens and closes; protection orders already on the
 * exchange are unaffected).
 */

function fmtClock(ms: number | null): string {
  if (ms == null) return "—";
  return new Date(ms).toLocaleTimeString("zh-CN", { hour12: false });
}

type LegForm = { exchangeId: string; accountId: string; symbol: string };

const DEFAULT_PARAMS: SpreadParams = {
  midlineBps: 0,
  upperBps: 10,
  lowerBps: 10,
  persistSec: 0,
  takeFraction: 1,
  maxNotionalUsd: 5000,
  feeBpsA: 0,
  feeBpsB: 0,
  tpBps: 0,
  slBps: 0,
};

const PARAM_KEYS = Object.keys(DEFAULT_PARAMS) as (keyof SpreadParams)[];

function legCell(
  leg: LegRef,
  names: Record<string, string>,
  accounts: { id: string; label: string }[],
) {
  const account = accounts.find((a) => a.id === leg.accountId);
  return (
    <div>
      <div>
        {names[leg.exchangeId] ?? leg.exchangeId} {leg.symbol}
      </div>
      <div className="text-ds-text-2 text-xs">{account?.label ?? leg.accountId}</div>
    </div>
  );
}

export default function StrategyPage() {
  // The instance list comes from useInvoke (Phase 2, I11); a 10s
  // refresh keeps last-eval / last-error / orders live, same pattern
  // as the Risk page drawdown poll.
  const list = useInvoke(() => api.listStrategies(), []);
  const { schemas } = useVenueSchemas();
  const names = useVenueStore((s) => s.names);
  const accounts = useAccountStore((s) => s.accounts);
  const fetchAll = useAccountStore((s) => s.fetchAll);

  useEffect(() => {
    fetchAll();
  }, [fetchAll]);

  useEffect(() => {
    const t = setInterval(() => list.refresh(), 10_000);
    return () => clearInterval(t);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  const tradable = (schemas ?? []).filter((s) => s.supportsTrading);
  const instances = list.data ?? [];

  // Toolbar (F1): new-strategy action + running / total counts.
  const register = useShellActions((s) => s.register);
  const unregister = useShellActions((s) => s.unregister);
  const running = instances.filter((v) => v.enabled).length;
  useLayoutEffect(() => {
    register("strategy", {
      actions: <Button onClick={openNew}>新建策略</Button>,
      status: `运行中 ${running} / 共 ${instances.length}`,
    });
    return () => unregister("strategy");
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [register, unregister, instances, schemas]);

  const [formOpen, setFormOpen] = useState(false);
  const [editingId, setEditingId] = useState<string | null>(null);
  const [legA, setLegA] = useState<LegForm>({ exchangeId: "hyperliquid", accountId: "", symbol: "BTC" });
  const [legB, setLegB] = useState<LegForm>({ exchangeId: "entropy", accountId: "", symbol: "io:BTC" });
  const [params, setParams] = useState<Record<keyof SpreadParams, string>>({
    midlineBps: "0",
    upperBps: "10",
    lowerBps: "10",
    persistSec: "0",
    takeFraction: "1",
    maxNotionalUsd: "5000",
    feeBpsA: "0",
    feeBpsB: "0",
    tpBps: "0",
    slBps: "0",
  });
  const [enabled, setEnabled] = useState(true);
  const [saving, setSaving] = useState(false);
  const [togglingId, setTogglingId] = useState<string | null>(null);
  const [pendingDisable, setPendingDisable] = useState<StrategyView | null>(null);
  const [fieldErrors, setFieldErrors] = useState<Record<string, string>>({});
  const [formWarning, setFormWarning] = useState<string | null>(null);

  const openNew = () => {
    setEditingId(null);
    setLegA({ exchangeId: tradable[0]?.id ?? "hyperliquid", accountId: "", symbol: "BTC" });
    setLegB({ exchangeId: tradable[1]?.id ?? tradable[0]?.id ?? "entropy", accountId: "", symbol: "io:BTC" });
    setParams({
      midlineBps: String(DEFAULT_PARAMS.midlineBps),
      upperBps: String(DEFAULT_PARAMS.upperBps),
      lowerBps: String(DEFAULT_PARAMS.lowerBps),
      persistSec: String(DEFAULT_PARAMS.persistSec),
      takeFraction: String(DEFAULT_PARAMS.takeFraction),
      maxNotionalUsd: String(DEFAULT_PARAMS.maxNotionalUsd),
      feeBpsA: String(DEFAULT_PARAMS.feeBpsA),
      feeBpsB: String(DEFAULT_PARAMS.feeBpsB),
      tpBps: String(DEFAULT_PARAMS.tpBps),
      slBps: String(DEFAULT_PARAMS.slBps),
    });
    setEnabled(true);
    setFieldErrors({});
    setFormWarning(null);
    setFormOpen(true);
  };

  const openEdit = (v: StrategyView) => {
    setEditingId(v.id);
    setLegA({ ...v.legA });
    setLegB({ ...v.legB });
    setParams({
      midlineBps: String(v.params.midlineBps),
      upperBps: String(v.params.upperBps),
      lowerBps: String(v.params.lowerBps),
      persistSec: String(v.params.persistSec),
      takeFraction: String(v.params.takeFraction),
      maxNotionalUsd: String(v.params.maxNotionalUsd),
      feeBpsA: String(v.params.feeBpsA),
      feeBpsB: String(v.params.feeBpsB),
      tpBps: String(v.params.tpBps),
      slBps: String(v.params.slBps),
    });
    setEnabled(v.enabled);
    setFieldErrors({});
    setFormWarning(null);
    setFormOpen(true);
  };

  const setParam = (key: keyof SpreadParams, value: string) =>
    setParams((p) => ({ ...p, [key]: value }));

  /** Midline warning (same rule + wording as the backend): live, from
   * the form values — shown in the 提示 section, never blocks saving. */
  const midlineWarning =
    Number(params.midlineBps) === 0 &&
    Number(params.feeBpsA) + Number(params.feeBpsB) > 0
      ? "midline 未标定，建议先录数据校准"
      : null;

  const onSave = async () => {
    if (saving) return;
    const errors: Record<string, string> = {};
    if (!legA.accountId) errors.legA = "A 腿请选择账户。";
    if (!legA.symbol.trim()) errors.legA = "A 腿请填写品种。";
    if (!legB.accountId) errors.legB = "B 腿请选择账户。";
    if (!legB.symbol.trim()) errors.legB = "B 腿请填写品种。";
    const parsed = {} as Record<keyof SpreadParams, number>;
    for (const k of PARAM_KEYS) {
      const n = Number(params[k]);
      if (!Number.isFinite(n)) {
        errors[k] = "必须是数字。";
        continue;
      }
      if (n < 0) {
        errors[k] = "不能为负数。";
        continue;
      }
      parsed[k] = n;
    }
    if (parsed.takeFraction !== undefined && (parsed.takeFraction <= 0 || parsed.takeFraction > 1)) {
      errors.takeFraction = "仓位比例必须在 (0, 1] 之间。";
    }
    if (Object.keys(errors).length > 0) {
      setFieldErrors(errors);
      return;
    }
    setFieldErrors({});
    const input: SaveStrategyInput = {
      id: editingId,
      kind: "spread",
      legA: { ...legA, symbol: legA.symbol.trim() },
      legB: { ...legB, symbol: legB.symbol.trim() },
      params: parsed,
      enabled,
    };
    setSaving(true);
    try {
      const res = await api.saveStrategy(input);
      list.refresh();
      toast.success("策略已保存");
      if (res.warning) {
        // Keep the form open so the warning is actually seen.
        setFormWarning(res.warning);
      } else {
        setFormOpen(false);
        setEditingId(null);
      }
    } catch (e) {
      // Backend failure: toast the backend's own words (F1).
      toast.error(e instanceof Error ? e.message : String(e));
    } finally {
      setSaving(false);
    }
  };

  const onToggle = async (v: StrategyView, next: boolean) => {
    if (togglingId) return;
    setTogglingId(v.id);
    try {
      await api.setStrategyEnabled(v.id, next);
      list.refresh();
      toast.success(next ? "策略已启用" : "策略已停用");
    } catch (e) {
      toast.error(e instanceof Error ? e.message : String(e));
    } finally {
      setTogglingId(null);
    }
  };

  const legForm = (
    title: string,
    leg: LegForm,
    setLeg: (l: LegForm) => void,
    errorKey: "legA" | "legB",
  ) => {
    const legAccounts = accounts.filter((a) => a.exchangeId === leg.exchangeId);
    return (
      <div className="rounded-lg border border-ds-line bg-ds-panel-2/50 p-4">
        <div className="mb-3 text-[13px] font-semibold text-ds-text-1">{title}</div>
        <div className="grid grid-cols-3 gap-3">
          <label className="block">
            <span className="mb-1 block text-xs text-ds-text-2">交易所</span>
            <SelectDropdown
              options={tradable.map((s) => ({ value: s.id, label: s.name }))}
              value={leg.exchangeId}
              onChange={(v) => setLeg({ ...leg, exchangeId: v, accountId: "" })}
            />
          </label>
          <label className="block">
            <span className="mb-1 block text-xs text-ds-text-2">账户</span>
            <SelectDropdown
              options={legAccounts.map((a) => ({ value: a.id, label: a.label }))}
              value={leg.accountId}
              onChange={(v) => setLeg({ ...leg, accountId: v })}
              placeholder={legAccounts.length === 0 ? "该交易所暂无账户" : "请选择"}
            />
          </label>
          <label className="block">
            <span className="mb-1 block text-xs text-ds-text-2">品种</span>
            <Input
              value={leg.symbol}
              onChange={(e) => setLeg({ ...leg, symbol: e.target.value })}
              placeholder="BTC"
            />
            {leg.exchangeId === "entropy" && (
              <span className="mt-1 block text-xs text-ds-text-2">Entropy 品种带 io: 前缀，如 io:BTC</span>
            )}
          </label>
        </div>
        {fieldErrors[errorKey] && (
          <div className="mt-2 text-xs text-ds-down">{fieldErrors[errorKey]}</div>
        )}
      </div>
    );
  };

  const paramField = (key: keyof SpreadParams, label: string, unit: string) => (
    <label key={key} className="block">
      <span className="mb-1 block text-xs text-ds-text-2">{label}</span>
      <span className="flex items-center gap-1.5">
        <span className="min-w-0 flex-1">
          <Input
            type="number"
            value={params[key]}
            onChange={(e) => setParam(key, e.target.value)}
            inputMode="decimal"
            error={fieldErrors[key]}
          />
        </span>
        {unit && <span className="w-8 shrink-0 text-xs text-ds-text-3">{unit}</span>}
      </span>
    </label>
  );

  return (
    <div>
      {formOpen && (
        <div className="mb-5 rounded-xl border border-ds-line bg-ds-panel p-4">
          <div className="mb-4 text-[13px] font-semibold text-ds-text-1">
            {editingId ? "编辑实例" : "新建实例"}
          </div>
          <div className="mb-4 text-[13px] font-semibold text-ds-text-1">两条腿</div>
          <div className="grid grid-cols-2 gap-4">
            {legForm("A 腿", legA, setLegA, "legA")}
            {legForm("B 腿", legB, setLegB, "legB")}
          </div>
          <div className="mt-5 mb-3 text-[13px] font-semibold text-ds-text-1">参数</div>
          <div className="grid grid-cols-3 gap-3">
            {paramField("midlineBps", "中线", "bps")}
            {paramField("upperBps", "上轨", "bps")}
            {paramField("lowerBps", "下轨", "bps")}
            {paramField("persistSec", "持续秒数", "秒")}
            {paramField("takeFraction", "仓位比例", "")}
            {paramField("maxNotionalUsd", "最大名义", "USD")}
            {paramField("feeBpsA", "A 腿费率", "bps")}
            {paramField("feeBpsB", "B 腿费率", "bps")}
          </div>
          <div className="mt-5 mb-3 text-[13px] font-semibold text-ds-text-1">保护</div>
          <div className="grid grid-cols-3 gap-3">
            {paramField("tpBps", "止盈", "bps")}
            {paramField("slBps", "止损", "bps")}
          </div>
          <div className="mt-5 mb-3 text-[13px] font-semibold text-ds-text-1">提示</div>
          <div className="space-y-1 text-xs text-ds-text-2">
            <p>止盈 / 止损为 0 表示不挂保护单；保护单为 reduce-only 触发单。</p>
            <p>仓位比例 (0, 1]：每轮最多用该比例的名义开仓。</p>
            {midlineWarning && <p className="text-ds-warn">{midlineWarning}</p>}
            {formWarning && <p className="text-ds-warn">{formWarning}</p>}
          </div>
          <Checkbox
            className="mt-4"
            label="保存后启用"
            checked={enabled}
            onChange={(e) => setEnabled(e.target.checked)}
          />
          <div className="mt-4 flex gap-2">
            <Button onClick={onSave} loading={saving}>
              保存
            </Button>
            <Button variant="secondary" onClick={() => setFormOpen(false)}>
              取消
            </Button>
          </div>
        </div>
      )}

      {list.loading && list.data === null ? (
        <PageState loading loadingText="加载策略…" />
      ) : list.error ? (
        <PageState error={`策略列表加载失败：${list.error}`} onRetry={() => list.refresh()} />
      ) : instances.length === 0 && !formOpen ? (
        <PageState
          empty
          action={<Button onClick={openNew}>新建策略</Button>}
        >
          <div>
            <div className="text-[13px] font-semibold text-ds-text-1">暂无策略</div>
            <div className="mt-1">新建一个均值回归策略，跑在 Hyperliquid 与 Entropy 之间。</div>
          </div>
        </PageState>
      ) : instances.length > 0 ? (
        <DataTable
          headers={["启用", "A 腿", "B 腿", "参数摘要", "最近评估", "最近错误", "累计下单", "操作"]}
          colWidths={["8%", "18%", "18%", "18%", "10%", "14%", "7%", "7%"]}
          aligns={["center", "center", "center", "center", "center", "center", "right", "center"]}
          rows={instances.map((v) => [
            <Switch
              checked={v.enabled}
              disabled={togglingId === v.id}
              label={v.enabled ? `停用策略 ${v.id}` : `启用策略 ${v.id}`}
              onChange={(next) => {
                if (next) void onToggle(v, true);
                else setPendingDisable(v);
              }}
            />,
            legCell(v.legA, names, accounts),
            legCell(v.legB, names, accounts),
            <span className="whitespace-nowrap num">
              中 {v.params.midlineBps} / 上 {v.params.upperBps} / 下 {v.params.lowerBps} bps
            </span>,
            <span className="num">{fmtClock(v.lastEvalMs)}</span>,
            v.lastError ? (
              <Tooltip content={v.lastError}>
                <span className="inline-block max-w-40 truncate align-middle text-ds-down">
                  {v.lastError}
                </span>
              </Tooltip>
            ) : (
              "—"
            ),
            <span className="num">{v.ordersPlaced}</span>,
            <Button size="sm" variant="ghost" onClick={() => openEdit(v)}>
              编辑
            </Button>,
          ])}
        />
      ) : null}

      <ConfirmDialog
        open={pendingDisable !== null}
        title="确认停用策略"
        confirmText="确认停用"
        message={
          pendingDisable && (
            <>
              停用后该实例不再开仓，正在执行的平仓逻辑也会停止。
              已有仓位的保护单在交易所不受影响，仍需手动处理。
            </>
          )
        }
        onCancel={() => setPendingDisable(null)}
        onConfirm={async () => {
          if (!pendingDisable) return;
          await onToggle(pendingDisable, false);
          setPendingDisable(null);
        }}
      />
    </div>
  );
}
