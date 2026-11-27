import React, { useEffect, useMemo, useState } from "react";
import AccountModal from "../components/AccountModal";
import { Badge, Button, ConfirmDialog, DataTable, StatCard, tdClsCompact, trCls } from "../components/ui";
import { useVenueSchemas } from "../hooks/useVenueSchemas";
import { useAccountStore } from "../store/useAccountStore";
import type { AccountView } from "../lib/types";
import { useAccountUiStore } from "../store/useAccountUiStore";
import { useLiveStore, latencyTone } from "../store/useLiveStore";
import { useShellActions } from "../store/useShellActions";
import { useVenueStore } from "../store/useVenueStore";

function fmt(n: number): string {
  return n.toLocaleString("en-US", { minimumFractionDigits: 2, maximumFractionDigits: 2 });
}

/**
 * Account page. Live sync lives in the account store (Phase 2, I11):
 * the list load hydrates live states and auto-connects, so this page
 * only reads state and fires store actions. The connectable set is
 * the backend's schema list, not a frontend table (Phase 2, I10).
 */

const STATUS_LABEL: Record<string, string> = {
  connecting: "连接中",
  connected: "已连接",
  error: "连接异常",
  disconnected: "已断开",
};

const HEADERS = ["账户名", "账户信息", "交易所", "权益 / 可用", "盈亏", "操作"];

export default function AccountPage() {
  const accounts = useAccountStore((s) => s.accounts);
  const loading = useAccountStore((s) => s.loading);
  const fetchAll = useAccountStore((s) => s.fetchAll);
  const remove = useAccountStore((s) => s.remove);
  const connect = useAccountStore((s) => s.connect);
  const disconnect = useAccountStore((s) => s.disconnect);
  const live = useLiveStore((s) => s.live);
  const { schemas } = useVenueSchemas();
  const names = useVenueStore((s) => s.names);
  const venueName = (id: string) => names[id] ?? id;
  // Every venue the backend reports has a read-only adapter.
  const syncable = useMemo(
    () => new Set((schemas ?? []).map((s) => s.id)),
    [schemas],
  );

  const modalOpen = useAccountUiStore((s) => s.modalOpen);
  const editing = useAccountUiStore((s) => s.editing);
  const openAdd = useAccountUiStore((s) => s.openAdd);
  const openEdit = useAccountUiStore((s) => s.openEdit);
  const closeModal = useAccountUiStore((s) => s.close);
  const [pendingDelete, setPendingDelete] = useState<AccountView | null>(null);

  // Toolbar (F1): add-account action + account / connected counts.
  const register = useShellActions((s) => s.register);
  const unregister = useShellActions((s) => s.unregister);
  const connected = accounts.filter((a) => live[a.id]?.status === "connected").length;
  useEffect(() => {
    register("account", {
      actions: <Button onClick={openAdd}>添加账户</Button>,
      status: `${accounts.length} 账户 · ${connected} 已连接`,
    });
    return () => unregister("account");
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [register, unregister, openAdd, accounts, live]);

  // Top overview: sum live equity / available / PnL across accounts.
  // Hyperliquid and Entropy share one master-wallet pool (the backend
  // aggregates perp + io + spot into the same snapshot for both), so
  // accounts of those two exchanges with the same masked wallet address
  // count once — never double. Every other exchange counts per account.
  const summary = useMemo(() => {
    const seen = new Set<string>();
    let equity = 0;
    let available = 0;
    let pnl = 0;
    let count = 0;
    for (const a of accounts) {
      const lv = live[a.id];
      if (!lv) continue;
      const key =
        a.exchangeId === "hyperliquid" || a.exchangeId === "entropy"
          ? `wallet:${a.maskedIdentifier}`
          : `account:${a.id}`;
      if (seen.has(key)) continue;
      seen.add(key);
      equity += lv.equityUsd;
      available += lv.availableUsd;
      if (lv.pnlUsd != null) pnl += lv.pnlUsd;
      count++;
    }
    return { equity, available, pnl, count };
  }, [accounts, live]);

  useEffect(() => {
    fetchAll();
  }, [fetchAll]);

  return (
    <div>
      <div className="grid grid-cols-3 gap-3 mb-4">
        <StatCard
          title="总权益"
          value={`$${fmt(summary.equity)}`}
          sub={`已连接 ${summary.count} 个账户（按主钱包去重）`}
          compact
        />
        <StatCard
          title="可用权益"
          value={`$${fmt(summary.available)}`}
          sub="各所可用保证金合计"
          compact
        />
        <StatCard
          title="总盈亏"
          value={`${summary.pnl > 0 ? "+" : ""}$${fmt(summary.pnl)}`}
          tone={summary.pnl > 0 ? "profit" : summary.pnl < 0 ? "loss" : "default"}
          sub="未实现盈亏合计"
          compact
        />
      </div>
      {/* Account columns, strictly locked by percentage (sum 100%):
          账户名 14% / 账户信息 22% / 交易所 14% / 权益 16% / 盈亏 12% /
          操作 22%. table-fixed + w-full keeps the ratios at any window
          size; min-w-700px is the floor, overflow-x-auto is the fallback. */}
      <DataTable headers={HEADERS} minWidth={700} compact colWidths={["14%", "22%", "14%", "16%", "12%", "22%"]}>
        {accounts.map((a) => {
          const lv = live[a.id];
          const isSyncable = syncable.has(a.exchangeId);
          const tone = lv ? latencyTone(lv.latencyMs, lv.status) : "offline";
          return (
          <tr key={a.id} className={trCls}>
            <td className={tdClsCompact}>
              <span className="block truncate">{a.label}</span>
            </td>
            <td className={tdClsCompact}>
              <div className="flex flex-col items-center gap-0.5 leading-tight w-full">
                <span className="block w-full truncate text-xs text-ds-text-3 num">{a.maskedIdentifier}</span>
                <Badge tone={a.credentialConfigured ? "configured" : "warn"}>
                  {a.credentialConfigured ? "凭证已配置" : "未配置凭证"}
                </Badge>
                {lv && (
                  <Badge tone={tone}>
                    {STATUS_LABEL[lv.status] ?? lv.status}
                    {lv.latencyMs !== null && lv.status === "connected"
                      ? ` · ${lv.latencyMs}ms`
                      : ""}
                  </Badge>
                )}
                {lv && lv.degraded && (
                  <span className="block w-full truncate text-[11px] text-ds-text-3">
                    数据降级：REST 兜底
                  </span>
                )}
              </div>
            </td>
            <td className={`${tdClsCompact} leading-tight`}>{venueName(a.exchangeId)}</td>
            {lv ? (
              <>
                <td className={`${tdClsCompact} num`}>
                  <div>${fmt(lv.equityUsd)} <span className="text-ds-text-3 text-xs">{lv.currency}</span></div>
                  <div className="text-ds-text-3 text-xs">可用 ${fmt(lv.availableUsd)}</div>
                </td>
                <td className={`${tdClsCompact} num ${lv.pnlUsd != null && lv.pnlUsd > 0 ? "text-ds-up" : lv.pnlUsd != null && lv.pnlUsd < 0 ? "text-ds-down" : ""}`}>
                  {lv.pnlUsd == null ? "—" : `${lv.pnlUsd > 0 ? "+" : ""}$${fmt(lv.pnlUsd)}`}
                </td>
              </>
            ) : (
              <>
                <td className={`${tdClsCompact} text-ds-text-3`}>{isSyncable ? "未连接" : "本阶段不支持"}</td>
                <td className={`${tdClsCompact} text-ds-text-3`}>—</td>
              </>
            )}
            <td className="text-center px-2 py-1.5 text-[13px] text-ds-text-1 whitespace-nowrap">
              <div className="flex items-center justify-center gap-1.5">
                {isSyncable && a.credentialConfigured && !lv && (
                  <Button
                    variant="secondary"
                    size="sm"
                    onClick={() => void connect(a.id)}
                  >
                    连接
                  </Button>
                )}
                {lv && (
                  <Button
                    variant="ghost"
                    size="sm"
                    onClick={() => void disconnect(a.id)}
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
          <tr className="border-t border-ds-line">
            <td colSpan={HEADERS.length} className="text-center px-4 py-8 text-ds-text-3">
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
              确定要删除账户「{pendingDelete.label}」（{venueName(pendingDelete.exchangeId)}）吗？
              该账户保存在本机 Vault 中的凭证将一并删除，此操作不可恢复。
            </>
          )
        }
        onCancel={() => setPendingDelete(null)}
        onConfirm={async () => {
          if (!pendingDelete) return;
          await disconnect(pendingDelete.id);
          const ok = await remove(pendingDelete.id);
          if (ok) {
            setPendingDelete(null);
          }
        }}
      />
    </div>
  );
}
