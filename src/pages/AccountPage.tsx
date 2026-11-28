import React, { useEffect, useLayoutEffect, useMemo, useState } from "react";
import AccountModal from "../components/AccountModal";
import PageState from "../components/PageState";
import { Badge, Button, ConfirmDialog, DataTable, StatCard, tdClsCompact, tdClsCompactRight, trCls } from "../components/ui";
import { useVenueSchemas } from "../hooks/useVenueSchemas";
import { useAccountStore } from "../store/useAccountStore";
import type { AccountView } from "../lib/types";
import { useAccountUiStore } from "../store/useAccountUiStore";
import { useLiveStore } from "../store/useLiveStore";
import { useShellActions } from "../store/useShellActions";
import { toast } from "../store/useToastStore";
import { useVenueStore } from "../store/useVenueStore";

function fmt(n: number): string {
  return n.toLocaleString("en-US", { minimumFractionDigits: 2, maximumFractionDigits: 2 });
}

/**
 * Account page (F3): one row per account. Live sync lives in the
 * account store (Phase 2, I11): the list load hydrates live states
 * and auto-connects, so this page only reads state and fires store
 * actions. The connectable set is the backend's schema list, not a
 * frontend table (Phase 2, I10).
 *
 * Columns: 名称 · 交易所 · 标识（脱敏）· 状态 · 权益 · 可用 · 操作.
 * Status is text + Badge (connected up, connecting info, error down,
 * degraded gets an extra warn badge, no live entry = neutral 未连接).
 * Amounts are right-aligned tabular numerals. Actions converge by
 * state: disconnected shows 连接 + 编辑, connected shows 断开 +
 * 编辑, 删除 is always last (danger + confirm).
 */

const STATUS_LABEL: Record<string, string> = {
  connecting: "连接中",
  connected: "已连接",
  error: "连接异常",
  disconnected: "已断开",
};

const STATUS_TONE: Record<string, "up" | "info" | "down" | "neutral"> = {
  connecting: "info",
  connected: "up",
  error: "down",
  disconnected: "neutral",
};

const HEADERS = ["名称", "交易所", "标识", "状态", "权益", "可用", "操作"];

export default function AccountPage() {
  const accounts = useAccountStore((s) => s.accounts);
  const loading = useAccountStore((s) => s.loading);
  const listError = useAccountStore((s) => s.listError);
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
  const [busy, setBusy] = useState<{ id: string; kind: "connect" | "disconnect" } | null>(null);

  // Toolbar (F1): add-account action + account / connected counts.
  const register = useShellActions((s) => s.register);
  const unregister = useShellActions((s) => s.unregister);
  const connected = accounts.filter((a) => live[a.id]?.status === "connected").length;
  useLayoutEffect(() => {
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

  // A list-load failure with accounts already on screen has no
  // full-page state to show — surface it as a toast instead.
  useEffect(() => {
    if (listError) toast.error(listError);
  }, [listError]);

  const onConnect = async (a: AccountView) => {
    if (busy) return;
    setBusy({ id: a.id, kind: "connect" });
    const ok = await connect(a.id);
    setBusy(null);
    if (ok) toast.success(`账户「${a.label}」已连接`);
  };

  const onDisconnect = async (a: AccountView) => {
    if (busy) return;
    setBusy({ id: a.id, kind: "disconnect" });
    const ok = await disconnect(a.id);
    setBusy(null);
    if (ok) toast.success(`账户「${a.label}」已断开`);
  };

  // The modal lives outside the state branches (P0, F4.1): it used to
  // render only in the table branch, so "添加账户" did nothing on an
  // empty account list — openAdd set the flag and nothing observed it.
  // `body` picks the state view; exactly one modal is mounted below.
  const body =
    loading && accounts.length === 0 ? (
      <PageState loading loadingText="加载账户…" />
    ) : listError && accounts.length === 0 ? (
      <PageState
        error={`账户列表加载失败：${listError}`}
        onRetry={() => void fetchAll()}
      />
    ) : accounts.length === 0 ? (
      <PageState
        empty
        action={<Button onClick={openAdd}>添加账户</Button>}
      >
        <div>
          <div className="text-[13px] font-semibold text-ds-text-1">暂无账户</div>
          <div className="mt-1">添加第一个账户，凭证只加密保存在本机。</div>
        </div>
      </PageState>
    ) : (
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
          名称 14% / 交易所 12% / 标识 20% / 状态 14% / 权益 12% /
          可用 11% / 操作 17%. table-fixed + w-full keeps the ratios
          at any window size; min-w-800px is the floor. */}
      <DataTable
        headers={HEADERS}
        minWidth={800}
        compact
        colWidths={["14%", "12%", "20%", "14%", "12%", "11%", "17%"]}
        aligns={["center", "center", "center", "center", "right", "right", "center"]}
      >
        {accounts.map((a) => {
          const lv = live[a.id];
          const isSyncable = syncable.has(a.exchangeId);
          const connecting =
            lv?.status === "connecting" ||
            (busy?.id === a.id && busy.kind === "connect");
          const disconnecting = busy?.id === a.id && busy.kind === "disconnect";
          return (
          <tr key={a.id} className={trCls}>
            <td className={tdClsCompact}>
              <span className="block truncate">{a.label}</span>
              {!a.credentialConfigured && (
                <span className="block text-[11px] text-ds-warn">未配置凭证</span>
              )}
            </td>
            <td className={`${tdClsCompact} leading-tight`}>{venueName(a.exchangeId)}</td>
            <td className={`${tdClsCompact} num`}>
              <span className="block truncate">{a.maskedIdentifier}</span>
            </td>
            <td className={tdClsCompact}>
              <div className="flex flex-col items-center gap-1">
                {lv ? (
                  <Badge tone={STATUS_TONE[lv.status] ?? "neutral"}>
                    {STATUS_LABEL[lv.status] ?? lv.status}
                  </Badge>
                ) : (
                  <Badge tone="neutral">{isSyncable ? "未连接" : "本阶段不支持"}</Badge>
                )}
                {lv && lv.degraded && lv.status === "connected" && (
                  <Badge tone="warn">数据降级</Badge>
                )}
                {lv && lv.status === "connected" && lv.latencyMs !== null && (
                  <span className="text-[11px] text-ds-text-3 num">{lv.latencyMs}ms</span>
                )}
              </div>
            </td>
            <td className={tdClsCompactRight}>
              {lv ? `$${fmt(lv.equityUsd)}` : <span className="text-ds-text-3">—</span>}
            </td>
            <td className={tdClsCompactRight}>
              {lv ? `$${fmt(lv.availableUsd)}` : <span className="text-ds-text-3">—</span>}
            </td>
            <td className="text-center px-2 py-1.5 text-[13px] text-ds-text-1 whitespace-nowrap">
              <div className="flex flex-wrap items-center justify-center gap-1.5">
                {connecting ? (
                  <Button variant="secondary" size="sm" loading disabled>
                    连接中
                  </Button>
                ) : lv && lv.status === "connected" ? (
                  <Button
                    variant="ghost"
                    size="sm"
                    loading={disconnecting}
                    disabled={busy !== null}
                    onClick={() => void onDisconnect(a)}
                  >
                    断开
                  </Button>
                ) : isSyncable && a.credentialConfigured ? (
                  <Button
                    variant="secondary"
                    size="sm"
                    disabled={busy !== null}
                    onClick={() => void onConnect(a)}
                  >
                    连接
                  </Button>
                ) : null}
                <Button
                  variant="secondary"
                  size="sm"
                  disabled={busy !== null}
                  onClick={() => openEdit(a)}
                >
                  编辑
                </Button>
                <Button
                  variant="danger"
                  size="sm"
                  disabled={busy !== null}
                  onClick={() => setPendingDelete(a)}
                >
                  删除
                </Button>
              </div>
            </td>
          </tr>
          );
        })}
      </DataTable>

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
            toast.success("账户已删除");
            setPendingDelete(null);
          }
        }}
      />
    </div>
  );

  return (
    <>
      {body}
      <AccountModal open={modalOpen} editing={editing} onClose={closeModal} />
    </>
  );
}
