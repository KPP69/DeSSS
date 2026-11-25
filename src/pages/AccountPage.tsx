import React, { useEffect, useRef, useState } from "react";
import AccountModal from "../components/AccountModal";
import { Badge, Button, ConfirmDialog, DataTable, tdClsCompact, trCls } from "../components/ui";
import { getExchange } from "../lib/exchanges";
import { api } from "../lib/tauri";
import { useAccountStore } from "../store/useAccountStore";
import type { AccountView } from "../lib/types";
import { useAccountUiStore } from "../store/useAccountUiStore";
import { useLiveStore, latencyTone } from "../store/useLiveStore";

function fmt(n: number): string {
  return n.toLocaleString("en-US", { minimumFractionDigits: 2, maximumFractionDigits: 2 });
}

/** Exchanges with a read-only adapter in phase 3. */
const SYNCABLE = new Set(["hyperliquid", "entropy", "lighter", "lighter_robinhood", "asterdex", "arcus"]);

const STATUS_LABEL: Record<string, string> = {
  connecting: "连接中",
  connected: "已连接",
  "rest-only": "仅 REST（WS 断线）",
  reconnecting: "重连中",
  error: "连接异常",
  disconnected: "已断开",
};

const HEADERS = ["账户名", "账户信息", "交易所", "权益 / 可用", "盈亏", "操作"];

export default function AccountPage() {
  const accounts = useAccountStore((s) => s.accounts);
  const loading = useAccountStore((s) => s.loading);
  const fetchAll = useAccountStore((s) => s.fetchAll);
  const remove = useAccountStore((s) => s.remove);
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
        // Connect failures are logged to the console by the backend;
        // nothing is shown here so the list never shifts.
        .catch(() => {});
    }
  }, [accounts, live, setLive]);

  return (
    <div>
      {/* Account columns, strictly locked by percentage (sum 100%):
          账户名 14% / 账户信息 22% / 交易所 14% / 权益 16% / 盈亏 12% /
          操作 22%. table-fixed + w-full keeps the ratios at any window
          size; min-w-700px is the floor, overflow-x-auto is the fallback. */}
      <DataTable headers={HEADERS} minWidth={700} compact colWidths={["14%", "22%", "14%", "16%", "12%", "22%"]}>
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
                <span className="block w-full truncate text-xs text-ds-text-3 num">{a.maskedIdentifier}</span>
                <Badge tone={a.credentialConfigured ? "configured" : "warn"}>
                  {a.credentialConfigured ? "凭证已配置" : "未配置凭证"}
                </Badge>
                {lv && (
                  <Badge tone={tone}>
                    {STATUS_LABEL[lv.status] ?? lv.status}
                    {lv.latencyMs !== null && (lv.status === "connected" || lv.status === "rest-only")
                      ? ` · ${lv.latencyMs}ms`
                      : ""}
                  </Badge>
                )}
              </div>
            </td>
            <td className={`${tdClsCompact} leading-tight`}>{getExchange(a.exchangeId).name}</td>
            {lv ? (
              <>
                <td className={`${tdClsCompact} num`}>
                  <div>${fmt(lv.equityUsd)} <span className="text-ds-text-3 text-xs">{lv.currency}</span></div>
                  <div className="text-ds-text-3 text-xs">可用 ${fmt(lv.availableUsd)}</div>
                </td>
                <td className={`${tdClsCompact} num ${lv.pnlUsd > 0 ? "text-ds-up" : lv.pnlUsd < 0 ? "text-ds-down" : ""}`}>
                  {lv.pnlUsd > 0 ? "+" : ""}${fmt(lv.pnlUsd)}
                </td>
              </>
            ) : (
              <>
                <td className={`${tdClsCompact} text-ds-text-3`}>{syncable ? "未连接" : "本阶段不支持"}</td>
                <td className={`${tdClsCompact} text-ds-text-3`}>—</td>
              </>
            )}
            <td className="text-center px-2 py-1.5 text-[13px] text-ds-text-1 whitespace-nowrap">
              <div className="flex items-center justify-center gap-1.5">
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
          if (ok) {
            // Clear the frontend pending-connect queue for this account so
            // the auto-connect effect never retries a deleted account.
            connectTried.current.delete(pendingDelete.id);
            setPendingDelete(null);
          }
        }}
      />
    </div>
  );
}
