import React, { useEffect, useState } from "react";
import { Badge, Button, DataTable, SelectDropdown } from "../components/ui";
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
 * Strategy page (Phase 3.1, commit 2): the mean-reversion spread
 * host. Instances are listed with their live view state (last
 * evaluation, last error, orders placed) and created / edited through
 * the form below. Only venues whose schema says `supportsTrading`
 * appear in the leg exchange dropdowns; everything else (param
 * ranges, account-connected checks, the midline warning) is enforced
 * backend-side and surfaced here verbatim, never silently.
 */

const inputCls =
  "w-full bg-ds-panel-2 border border-ds-line rounded-lg px-3 h-8 text-ds-text-1 outline-none focus:border-ds-accent focus:ring-2 focus:ring-ds-focus-ring/50 disabled:bg-ds-bg-disabled disabled:text-ds-text-disabled num";

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
  useEffect(() => {
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
  const [formError, setFormError] = useState<string | null>(null);
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
    setFormError(null);
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
    setFormError(null);
    setFormWarning(null);
    setFormOpen(true);
  };

  const setParam = (key: keyof SpreadParams, value: string) =>
    setParams((p) => ({ ...p, [key]: value }));

  const onSave = async () => {
    if (saving) return;
    setFormError(null);
    setFormWarning(null);
    if (!legA.accountId || !legA.symbol.trim() || !legB.accountId || !legB.symbol.trim()) {
      setFormError("两条腿都要选账户并填写品种。");
      return;
    }
    const parsed = {} as Record<keyof SpreadParams, number>;
    for (const k of Object.keys(params) as (keyof SpreadParams)[]) {
      const n = Number(params[k]);
      if (!Number.isFinite(n)) {
        setFormError("参数必须是数字。");
        return;
      }
      parsed[k] = n;
    }
    if (parsed.tpBps < 0 || parsed.slBps < 0) {
      setFormError("止盈/止损 bps 不能为负数（0 表示不挂）。");
      return;
    }
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
      setFormWarning(res.warning);
      list.refresh();
      setFormOpen(false);
      setEditingId(null);
    } catch (e) {
      // Backend failure: toast the backend's own words (F1).
      // formError stays for client-side validation only.
      toast.error(e instanceof Error ? e.message : String(e));
    } finally {
      setSaving(false);
    }
  };

  const onToggle = async (v: StrategyView) => {
    if (togglingId) return;
    setTogglingId(v.id);
    try {
      await api.setStrategyEnabled(v.id, !v.enabled);
      list.refresh();
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
            <input
              className={inputCls}
              value={leg.symbol}
              onChange={(e) => setLeg({ ...leg, symbol: e.target.value })}
              placeholder="BTC"
            />
            {leg.exchangeId === "entropy" && (
              <span className="mt-1 block text-xs text-ds-text-2">Entropy 品种带 io: 前缀，如 io:BTC</span>
            )}
          </label>
        </div>
      </div>
    );
  };

  const paramField = (key: keyof SpreadParams, label: string) => (
    <label key={key} className="block">
      <span className="mb-1 block text-xs text-ds-text-2">{label}</span>
      <input
        className={inputCls}
        value={params[key]}
        onChange={(e) => setParam(key, e.target.value)}
        inputMode="decimal"
      />
    </label>
  );

  return (
    <div>
      {formOpen && (
        <div className="mb-5 rounded-xl border border-ds-line bg-ds-panel p-4">
          <div className="mb-4 text-[13px] font-semibold text-ds-text-1">
            {editingId ? "编辑策略" : "新建均值回归策略"}
          </div>
          <div className="grid grid-cols-2 gap-4">
            {legForm("A 腿", legA, setLegA)}
            {legForm("B 腿", legB, setLegB)}
          </div>
          <div className="mt-4 grid grid-cols-4 gap-3">
            {paramField("midlineBps", "中线 bps")}
            {paramField("upperBps", "上轨 bps")}
            {paramField("lowerBps", "下轨 bps")}
            {paramField("persistSec", "持续秒数")}
            {paramField("takeFraction", "仓位比例 0–1")}
            {paramField("maxNotionalUsd", "最大名义 USD")}
            {paramField("feeBpsA", "A 腿费率 bps")}
            {paramField("feeBpsB", "B 腿费率 bps")}
            {paramField("tpBps", "止盈 bps（0 不挂）")}
            {paramField("slBps", "止损 bps（0 不挂）")}
          </div>
          <label className="mt-4 flex items-center gap-2 text-[13px] text-ds-text-1">
            <input
              type="checkbox"
              checked={enabled}
              onChange={(e) => setEnabled(e.target.checked)}
            />
            保存后启用
          </label>
          {formError && <div className="mt-3 text-[13px] text-ds-down">{formError}</div>}
          {formWarning && <div className="mt-3 text-[13px] text-ds-warn">{formWarning}</div>}
          <div className="mt-4 flex gap-2">
            <Button onClick={onSave} disabled={saving}>
              {saving ? "保存中…" : "保存"}
            </Button>
            <Button variant="secondary" onClick={() => setFormOpen(false)}>
              取消
            </Button>
          </div>
        </div>
      )}

      <PageState loading={list.loading && list.data === null} error={list.error} empty={instances.length === 0 && !formOpen}>
        <div>
          <div className="text-[13px] font-semibold text-ds-text-1">暂无策略</div>
          <div className="mt-1">新建一个均值回归策略，跑在 Hyperliquid 与 Entropy 之间。</div>
        </div>
      </PageState>

      {instances.length > 0 && (
        <div>
          <DataTable
            headers={["状态", "A 腿", "B 腿", "参数", "最近评估", "最近错误", "累计下单", "操作"]}
            rows={instances.map((v) => [
              <Badge tone={v.enabled ? "online" : "offline"}>
                {v.enabled ? "启用" : "停用"}
              </Badge>,
              legCell(v.legA, names, accounts),
              legCell(v.legB, names, accounts),
              <div>
                <div>
                  中线 {v.params.midlineBps} · 上 {v.params.upperBps} · 下 {v.params.lowerBps} bps
                </div>
                <div className="text-ds-text-2 text-xs">
                  持续 {v.params.persistSec}s · 比例 {v.params.takeFraction} · 上限 ${v.params.maxNotionalUsd}
                </div>
              </div>,
              fmtClock(v.lastEvalMs),
              v.lastError ? <span className="text-ds-down">{v.lastError}</span> : "—",
              String(v.ordersPlaced),
              <div className="flex justify-center gap-2">
                <Button
                  size="sm"
                  variant="secondary"
                  disabled={togglingId === v.id}
                  onClick={() => void onToggle(v)}
                >
                  {v.enabled ? "停用" : "启用"}
                </Button>
                <Button size="sm" variant="ghost" onClick={() => openEdit(v)}>
                  编辑
                </Button>
              </div>,
            ])}
          />
        </div>
      )}
    </div>
  );
}
