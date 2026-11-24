import React, { useEffect, useMemo, useRef, useState } from "react";
import AccountModal from "../components/AccountModal";
import { Badge, Button, ConfirmDialog, DataTable, StatCard, tdClsCompact, trCls } from "../components/ui";
import { getExchange } from "../lib/exchanges";
import { api } from "../lib/tauri";
import { useAccountStore } from "../store/useAccountStore";
import type { AccountView } from "../lib/types";
import { useAccountUiStore } from "../store/useAccountUiStore";
import { useLiveStore, latencyTone } from "../store/useLiveStore";
import { useMarketStore } from "../store/useMarketStore";

function fmt(n: number): string {
  return n.toLocaleString("en-US", { minimumFractionDigits: 2, maximumFractionDigits: 2 });
}

/** Exchanges with a read-only adapter in phase 3. */
const SYNCABLE = new Set(["hyperliquid", "entropy", "lighter", "lighter_robinhood"]);

const STATUS_LABEL: Record<string, string> = {
  connecting: "连接中",
  connected: "已连接",
  "rest-only": "仅 REST（WS 断线）",
  reconnecting: "重连中",
  error: "连接异常",
  disconnected: "已断开",
};

function latencyDot(tone: string): string {
  switch (tone) {
    case "online":
      return "bg-profit";
    case "warn":
      return "bg-warn";
    case "danger":
      return "bg-loss";
    default:
      return "bg-t3";
  }
}

const HEADERS = ["账户名", "账户信息", "交易所", "权益 / 可用", "盈亏", "操作"];

export default function AccountPage() {
  const accounts = useAccountStore((s) => s.accounts);
  const loading = useAccountStore((s) => s.loading);
  const error = useAccountStore((s) => s.error);
  const vaultError = useAccountStore((s) => s.vaultError);
  const fetchAll = useAccountStore((s) => s.fetchAll);
  const remove = useAccountStore((s) => s.remove);
  const balances = useMarketStore((s) => s.balances);
  const live = useLiveStore((s) => s.live);
  const setLive = useLiveStore((s) => s.setLive);
  const removeLive = useLiveStore((s) => s.removeLive);

  const modalOpen = useAccountUiStore((s) => s.modalOpen);
  const editing = useAccountUiStore((s) => s.editing);
  const openEdit = useAccountUiStore((s) => s.openEdit);
  const closeModal = useAccountUiStore((s) => s.close);
  const [pendingDelete, setPendingDelete] = useState<AccountView | null>(null);
  const connectTried = useRef<Set<string>>(new Set());

  useEffect(() => {
    fetchAll();
  }, [fetchAll]);

  // Hydrate live states, then auto-connect every supported account that has
  // credentials and is not already live. Private keys are never involved:
  // the backend only reads the public address / account index from the vault.
  useEffect(() => {
    api
      .listAccountLive()
      .then((list) => list.forEach(setLive))
      .catch(() => {});
  }, [setLive]);

  useEffect(() => {
    for (const a of accounts) {
      if (!SYNCABLE.has(a.exchangeId) || !a.credentialConfigured) continue;
      if (live[a.id] || connectTried.current.has(a.id)) continue;
      connectTried.current.add(a.id);
      api
        .connectAccount(a.id)
        .then(setLive)
        .catch(() => {});
    }
  }, [accounts, live, setLive]);

  // Overview cards show Mock data only, clearly labelled as simulated.
  const mock = useMemo(() => {
    const totalEquity = balances.reduce((sum, b) => sum + b.equityUsd, 0);
    const available = balances
      .filter((b) => b.asset === "USDT")
      .reduce((sum, b) => sum + b.free, 0);
    return { totalEquity, available, pnl: 0 };
  }, [balances]);

  return (
    <div>
      {vaultError && (
        <div className="mb-4 text-loss bg-panel border border-line rounded px-4 py-3">
          Vault 初始化失败，账户凭证功能不可用：{vaultError}
          <span className="text-t3">（原有 vault 文件未被修改，请检查 data 目录下的 vault.key / vault.enc）</span>
        </div>
      )}
      {error && !vaultError && (
        <div className="mb-4 text-loss bg-panel border border-line rounded px-4 py-3">
          {error}
        </div>
      )}

      {/* Overview: mock data, clearly labelled */}
      <div className="mb-2 text-xs text-t3 flex items-center gap-2">
        以下为模拟账户数据，非真实资产；下方真实账户会单独显示交易所同步到的真实权益。
        <Badge tone="mock">Mock 模拟</Badge>
      </div>
      <div className="grid grid-cols-3 gap-3 mb-3 mt-4">
        <StatCard compact title="总权益" value={`$${fmt(mock.totalEquity)}`} sub="模拟数据 · Mock" />
        <StatCard compact title="可用权益" value={`$${fmt(mock.available)}`} sub="模拟数据 · Mock" />
        <StatCard compact title="总盈亏" value={`$${fmt(mock.pnl)}`} sub="模拟数据 · Mock" />
      </div>

      <DataTable headers={HEADERS} minWidth={970} compact colWidths={[150, 250, 120, 170, 100, 180]}>
        {/* Built-in mock account row */}
        <tr className={trCls}>
          <td className={tdClsCompact}>
            <span className="inline-flex items-center gap-2">
              模拟账户 <Badge tone="mock">Mock</Badge>
            </span>
          </td>
          <td className={`${tdClsCompact} text-t3`}>模拟数据</td>
          <td className={tdClsCompact}>MockAdapter</td>
          <td className={`${tdClsCompact} num`}>
            <div>${fmt(mock.totalEquity)}</div>
            <div className="text-t3 text-xs">可用 ${fmt(mock.available)}</div>
          </td>
          <td className={`${tdClsCompact} num`}>${fmt(mock.pnl)}</td>
          <td className={`${tdClsCompact} text-t3`}>—</td>
        </tr>
        {accounts.map((a) => {
          const lv = live[a.id];
          const syncable = SYNCABLE.has(a.exchangeId);
          const tone = lv ? latencyTone(lv.latencyMs, lv.status) : "offline";
          return (
          <tr key={a.id} className={trCls}>
            <td className={tdClsCompact}>
              <span className="block truncate">{a.label}</span>
            </td>
            <td className={tdClsCompact}>
              <div className="flex flex-col items-center gap-0.5 leading-tight w-full">
                <span className="block w-full truncate text-xs text-t3 num">{a.maskedIdentifier}</span>
                <Badge tone={a.credentialConfigured ? "configured" : "warn"}>
                  {a.credentialConfigured ? "凭证已配置" : "未配置凭证"}
                </Badge>
                {lv && (
                  <Badge tone={tone}>
                    <span className={`inline-block w-1.5 h-1.5 rounded-full ${latencyDot(tone)}`} />
                    {STATUS_LABEL[lv.status] ?? lv.status}
                    {lv.latencyMs !== null && (lv.status === "connected" || lv.status === "rest-only")
                      ? ` · ${lv.latencyMs}ms`
                      : ""}
                  </Badge>
                )}
              </div>
            </td>
            <td className={tdClsCompact}>{getExchange(a.exchangeId).name}</td>
            {lv ? (
              <>
                <td className={`${tdClsCompact} num`}>
                  <div>${fmt(lv.equityUsd)} <span className="text-t3 text-xs">{lv.currency}</span></div>
                  <div className="text-t3 text-xs">可用 ${fmt(lv.availableUsd)}</div>
                </td>
                <td className={`${tdClsCompact} num ${lv.pnlUsd > 0 ? "text-profit" : lv.pnlUsd < 0 ? "text-loss" : ""}`}>
                  {lv.pnlUsd > 0 ? "+" : ""}${fmt(lv.pnlUsd)}
                </td>
              </>
            ) : (
              <>
                <td className={`${tdClsCompact} text-t3`}>{syncable ? "未连接" : "本阶段不支持"}</td>
                <td className={`${tdClsCompact} text-t3`}>—</td>
              </>
            )}
            <td className={tdClsCompact}>
              <div className="flex items-center justify-center gap-2">
                {syncable && a.credentialConfigured && !lv && (
                  <Button
                    variant="secondary"
                    size="sm"
                    onClick={() => api.connectAccount(a.id).then(setLive).catch(() => {})}
                  >
                    连接
                  </Button>
                )}
                {lv && (
                  <Button
                    variant="ghost"
                    size="sm"
                    onClick={() =>
                      api
                        .disconnectAccount(a.id)
                        .then(() => removeLive(a.id))
                        .catch(() => {})
                    }
                  >
                    断开
                  </Button>
                )}
                <Button
                  variant="secondary"
                  size="sm"
                  onClick={() => openEdit(a)}
                >
                  编辑
                </Button>
                <Button variant="ghost" size="sm" onClick={() => setPendingDelete(a)}>
                  删除
                </Button>
              </div>
            </td>
          </tr>
          );
        })}
        {accounts.length === 0 && (
          <tr className="border-t border-line">
            <td colSpan={HEADERS.length} className="text-center px-4 py-8 text-t3">
              {loading ? "加载中…" : "暂无真实账户，点击上方「添加账户」创建。账户凭证只加密保存在本机。"}
            </td>
          </tr>
        )}
      </DataTable>

      <AccountModal open={modalOpen} editing={editing} onClose={closeModal} />

      <ConfirmDialog
        open={pendingDelete !== null}
        title="确认删除账户"
        message={
          pendingDelete && (
            <>
              确定要删除账户「{pendingDelete.label}」（{getExchange(pendingDelete.exchangeId).name}）吗？
              该账户保存在本机 Vault 中的凭证将一并删除，此操作不可恢复。
            </>
          )
        }
        onCancel={() => setPendingDelete(null)}
        onConfirm={async () => {
          if (!pendingDelete) return;
          await api.disconnectAccount(pendingDelete.id).catch(() => {});
          removeLive(pendingDelete.id);
          const ok = await remove(pendingDelete.id);
          if (ok) setPendingDelete(null);
        }}
      />
    </div>
  );
}
