import React, { useEffect, useLayoutEffect, useRef, useState } from "react";
import { Badge, Button, Checkbox, ConfirmDialog, DataTable, Input, SelectDropdown, Switch, Tooltip } from "../components/ui";
import PageState from "../components/PageState";
import { useInvoke } from "../hooks/useInvoke";
import { useVenueSchemas } from "../hooks/useVenueSchemas";
import { api } from "../lib/tauri";
import { displaySymbol } from "../lib/symbol";
import { fmtClock } from "../lib/format";
import type { LegRef, MarketInfo, SaveStrategyInput, SpreadParams, StrategyView } from "../lib/types";
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
 * 最近错误 · 累计下单 · 操作（编辑 · 删除）. Numbers are tabular; the
 * order count is right-aligned. Disabling an instance asks first
 * (it stops new opens and closes; protection orders already on the
 * exchange are unaffected). Deleting asks first too (irreversible;
 * positions and protection orders on the exchange are unaffected).
 */

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

/**
 * Strategy types (F4.1): the picker shows display names only — the
 * backend knows a single kind ("spread") today and the instant spread
 * has no backend kind yet, so it stays disabled. Mean reversion is
 * enabled (F4.4): the runner already runs kind "spread".
 */
const STRATEGY_TYPES = [
  { kind: "spread", name: "均值回归", note: "可用", desc: "在 Hyperliquid 与 Entropy 之间做均值回归价差", available: true },
  { kind: "spread_instant", name: "即时价差", note: "未实现", desc: "跨所即时价差套利", available: false },
] as const;

const KIND_NAMES: Record<string, string> = {
  spread: "均值回归",
  spread_instant: "即时价差",
};
const kindName = (kind: string) => KIND_NAMES[kind] ?? kind;

/** Native wire symbol for a display name (Entropy carries the io: prefix). */
const toNativeSymbol = (exchangeId: string, display: string) =>
  exchangeId === "entropy" ? `io:${display}` : display;

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

  // Toolbar (F1): new-strategy action (F4.4: the running / total
  // counts on the right are gone).
  const register = useShellActions((s) => s.register);
  const unregister = useShellActions((s) => s.unregister);
  useLayoutEffect(() => {
    register("strategy", {
      actions: <Button onClick={openNew}>新建策略</Button>,
    });
    return () => unregister("strategy");
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [register, unregister, schemas]);

  const [formOpen, setFormOpen] = useState(false);
  const [formStep, setFormStep] = useState<"type" | "config">("type");
  const [selectedKind, setSelectedKind] = useState<string | null>(null);
  const [editingId, setEditingId] = useState<string | null>(null);
  // Symbol intersection (F4.1): display names both legs share, from the
  // catalog. The backend refuses empty queries and never dumps the whole
  // market over IPC, so the list is built by sweeping a-z0-9 — every
  // base starts with a letter or digit — and intersecting the two sets.
  // Cached per exchange; a sweep is one parallel batch of local reads.
  const [symbolOptions, setSymbolOptions] = useState<string[] | null>(null);
  const [symbolsLoading, setSymbolsLoading] = useState(false);
  const symbolCache = useRef(new Map<string, string[]>());

  const fetchVenueSymbols = async (exchangeId: string): Promise<string[]> => {
    const cached = symbolCache.current.get(exchangeId);
    if (cached) return cached;
    const probes = "abcdefghijklmnopqrstuvwxyz0123456789".split("");
    const lists = await Promise.all(
      probes.map((q) =>
        api.searchMarketInfo(q, [exchangeId]).catch(() => [] as MarketInfo[]),
      ),
    );
    const set = new Set<string>();
    for (const rows of lists) {
      for (const r of rows) set.add(displaySymbol(r.symbol));
    }
    const out = [...set].sort();
    symbolCache.current.set(exchangeId, out);
    return out;
  };

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
  const [pendingDelete, setPendingDelete] = useState<StrategyView | null>(null);
  const [fieldErrors, setFieldErrors] = useState<Record<string, string>>({});
  const [formWarning, setFormWarning] = useState<string | null>(null);

  // Load the symbol intersection once the config step is shown for a leg pair.
  useEffect(() => {
    if (!formOpen || formStep !== "config") return;
    const a = legA.exchangeId;
    const b = legB.exchangeId;
    if (!a || !b) {
      setSymbolOptions(null);
      return;
    }
    let cancelled = false;
    setSymbolsLoading(true);
    (async () => {
      const [sa, sb] = await Promise.all([fetchVenueSymbols(a), fetchVenueSymbols(b)]);
      if (cancelled) return;
      const setB = new Set(sb);
      setSymbolOptions(sa.filter((s) => setB.has(s)));
      setSymbolsLoading(false);
    })();
    return () => {
      cancelled = true;
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [formOpen, formStep, legA.exchangeId, legB.exchangeId]);

  const openNew = () => {
    setEditingId(null);
    setSelectedKind(null);
    setFormStep("type");
    setSymbolOptions(null);
    setLegA({ exchangeId: tradable[0]?.id ?? "hyperliquid", accountId: "", symbol: "BTC" });
    setLegB({ exchangeId: tradable[1]?.id ?? tradable[0]?.id ?? "entropy", accountId: "", symbol: "BTC" });
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
    setSelectedKind(v.kind);
    setFormStep("config");
    setSymbolOptions(null);
    setLegA({ ...v.legA, symbol: displaySymbol(v.legA.symbol) });
    setLegB({ ...v.legB, symbol: displaySymbol(v.legB.symbol) });
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

  const noIntersection = symbolOptions !== null && symbolOptions.length === 0;

  const onSave = async () => {
    if (saving || noIntersection) return;
    const errors: Record<string, string> = {};
    if (!legA.accountId) errors.legA = "A 腿请选择账户。";
    if (!legA.symbol.trim()) errors.legA = "A 腿请选择品种。";
    else if (symbolOptions !== null && !symbolOptions.includes(legA.symbol.trim())) {
      errors.legA = "A 腿品种不在两所交集中。";
    }
    if (!legB.accountId) errors.legB = "B 腿请选择账户。";
    if (!legB.symbol.trim()) errors.legB = "B 腿请选择品种。";
    else if (symbolOptions !== null && !symbolOptions.includes(legB.symbol.trim())) {
      errors.legB = "B 腿品种不在两所交集中。";
    }
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
      kind: selectedKind ?? "spread",
      legA: { ...legA, symbol: toNativeSymbol(legA.exchangeId, legA.symbol.trim()) },
      legB: { ...legB, symbol: toNativeSymbol(legB.exchangeId, legB.symbol.trim()) },
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

  const onDelete = async (v: StrategyView) => {
    try {
      await api.deleteStrategy(v.id);
      list.refresh();
      toast.success("策略已删除");
    } catch (e) {
      // Backend failure: toast the backend's own words (F1).
      toast.error(e instanceof Error ? e.message : String(e));
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
      <div className="rounded-[10px] border border-ds-line bg-ds-panel-2/50 p-4">
        <div className="mb-3 text-[13px] font-semibold text-ds-text-1">{title}</div>
        {/* Exchange column is 130px (F4.3): wide enough for the
            longest venue name, Hyperliquid, without truncating the
            trigger. Account / symbol share the rest. */}
        <div className="grid grid-cols-[130px_minmax(0,1fr)_minmax(0,1fr)] gap-3">
          <label className="block">
            <span className="mb-1 block text-xs text-ds-text-2">交易所</span>
            <SelectDropdown
              options={tradable.map((s) => ({ value: s.id, label: s.name }))}
              value={leg.exchangeId}
              onChange={(v) => setLeg({ ...leg, exchangeId: v, accountId: "", symbol: "" })}
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
            <SelectDropdown
              options={(symbolOptions ?? []).map((s) => ({ value: s, label: s }))}
              value={leg.symbol}
              onChange={(v) => setLeg({ ...leg, symbol: v })}
              placeholder={
                symbolsLoading
                  ? "加载品种…"
                  : symbolOptions !== null && symbolOptions.length === 0
                    ? "两所无共同品种"
                    : "请选择品种"
              }
              disabled={symbolsLoading || symbolOptions === null || symbolOptions.length === 0}
            />
          </label>
        </div>
        {fieldErrors[errorKey] && (
          <div className="mt-2 text-xs text-ds-down">{fieldErrors[errorKey]}</div>
        )}
      </div>
    );
  };

  const paramField = (key: keyof SpreadParams, label: string, unit: string, desc: string) => (
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
      <span className="mt-1 block text-xs text-ds-text-2">{desc}</span>
    </label>
  );

  return (
    <div>
      {formOpen && formStep === "type" && !editingId && (
        <div className="mb-5 rounded-[10px] border border-ds-line bg-ds-panel p-4">
          <div className="mb-4 text-[13px] font-semibold text-ds-text-1">选择策略类型</div>
          <div className="space-y-2">
            {STRATEGY_TYPES.map((t) => (
              <div
                key={t.kind}
                onClick={
                  t.available
                    ? () => {
                        setSelectedKind(t.kind);
                        setFormStep("config");
                      }
                    : undefined
                }
                className={`flex items-center gap-3 rounded-[10px] border border-ds-line bg-ds-panel-2/50 px-4 py-3 ${
                  t.available ? "cursor-pointer hover:border-ds-accent" : "opacity-60"
                }`}
              >
                <div className="min-w-0 flex-1">
                  <div className="text-[13px] font-semibold text-ds-text-1">{t.name}</div>
                  <div className="text-xs text-ds-text-2">{t.desc}</div>
                </div>
                <Badge tone={t.available ? "up" : "neutral"}>{t.note}</Badge>
              </div>
            ))}
          </div>
          <div className="mt-4 flex gap-2">
            <Button variant="secondary" onClick={() => setFormOpen(false)}>
              取消
            </Button>
          </div>
        </div>
      )}

      {formOpen && formStep === "config" && (
        <div className="mb-5 rounded-[10px] border border-ds-line bg-ds-panel p-4">
          <div className="mb-4 text-[13px] font-semibold text-ds-text-1">
            {editingId ? "编辑实例" : "新建实例"} · {kindName(selectedKind ?? "spread")}
          </div>
          <div className="mb-4 text-[13px] font-semibold text-ds-text-1">两条腿</div>
          <div className="grid grid-cols-2 gap-4">
            {legForm("A 腿", legA, setLegA, "legA")}
            {legForm("B 腿", legB, setLegB, "legB")}
          </div>
          <div className="mt-5 mb-3 text-[13px] font-semibold text-ds-text-1">参数</div>
          <div className="grid grid-cols-3 gap-3">
            {paramField("midlineBps", "中线", "bps", "两所价差长期均值，需实测")}
            {paramField("upperBps", "上轨", "bps", "偏离多少触发")}
            {paramField("lowerBps", "下轨", "bps", "偏离多少触发")}
            {paramField("persistSec", "持续秒数", "秒", "信号需保持多久")}
            {paramField("takeFraction", "仓位比例", "", "吃可成交深度的比例")}
            {paramField("maxNotionalUsd", "最大名义", "USD", "单次开仓上限")}
            {paramField("feeBpsA", "A 腿费率", "bps", "taker 费率，计入门槛")}
            {paramField("feeBpsB", "B 腿费率", "bps", "taker 费率，计入门槛")}
          </div>
          <div className="mt-5 mb-3 text-[13px] font-semibold text-ds-text-1">保护</div>
          <div className="grid grid-cols-3 gap-3">
            {paramField("tpBps", "止盈", "bps", "开仓后挂的保护")}
            {paramField("slBps", "止损", "bps", "开仓后挂的保护")}
          </div>
          <div className="mt-5 mb-3 text-[13px] font-semibold text-ds-text-1">提示</div>
          <div className="space-y-1 text-xs text-ds-text-2">
            <p>止盈 / 止损为 0 表示不挂保护单；保护单为 reduce-only 触发单。</p>
            <p>仓位比例 (0, 1]：每轮最多用该比例的名义开仓。</p>
            {noIntersection && <p className="text-ds-warn">两所无共同品种，无法保存。</p>}
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
            <Button onClick={onSave} loading={saving} disabled={noIntersection}>
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
            <div className="text-[13px] font-semibold text-ds-text-1">当前没有可用策略</div>
            <div className="mt-1">即时价差尚未实现。</div>
          </div>
        </PageState>
      ) : instances.length > 0 ? (
        <DataTable
          headers={["启用", "A 腿", "B 腿", "参数摘要", "最近评估", "最近错误", "累计下单", "操作"]}
          colWidths={["7%", "17%", "17%", "16%", "10%", "13%", "7%", "13%"]}
          aligns={["center", "left", "left", "center", "right", "center", "right", "center"]}
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
            <span className="num">
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
            <span className="flex items-center justify-center gap-1">
              <Button size="sm" variant="ghost" onClick={() => openEdit(v)}>
                编辑
              </Button>
              <Button size="sm" variant="danger" onClick={() => setPendingDelete(v)}>
                删除
              </Button>
            </span>,
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

      <ConfirmDialog
        open={pendingDelete !== null}
        title="确认删除策略"
        confirmText="确认删除"
        message={
          pendingDelete && (
            <>
              即将删除：{kindName(pendingDelete.kind)}（
              {displaySymbol(pendingDelete.legA.symbol)} ↔{" "}
              {displaySymbol(pendingDelete.legB.symbol)}）。
              删除后不可恢复；已有仓位与其保护单在交易所不受影响。
            </>
          )
        }
        onCancel={() => setPendingDelete(null)}
        onConfirm={async () => {
          if (!pendingDelete) return;
          await onDelete(pendingDelete);
          setPendingDelete(null);
        }}
      />
    </div>
  );
}
