import React, { useEffect, useMemo, useState } from "react";
import AccountModal from "../components/AccountModal";
import { Badge, Button, ConfirmDialog, DataTable, PageHeader, StatCard, tdCls, trCls } from "../components/ui";
import { getExchange } from "../lib/exchanges";
import type { AccountView } from "../lib/types";
import { useAccountStore } from "../store/useAccountStore";
import { useMarketStore } from "../store/useMarketStore";

function fmt(n: number): string {
  return n.toLocaleString("en-US", { minimumFractionDigits: 2, maximumFractionDigits: 2 });
}

const HEADERS = ["账户名", "交易所", "权益", "可用权益", "盈亏", "操作"];

export default function AccountPage() {
  const accounts = useAccountStore((s) => s.accounts);
  const loading = useAccountStore((s) => s.loading);
  const error = useAccountStore((s) => s.error);
  const vaultError = useAccountStore((s) => s.vaultError);
  const fetchAll = useAccountStore((s) => s.fetchAll);
  const remove = useAccountStore((s) => s.remove);
  const balances = useMarketStore((s) => s.balances);

  const [modalOpen, setModalOpen] = useState(false);
  const [editing, setEditing] = useState<AccountView | null>(null);
  const [pendingDelete, setPendingDelete] = useState<AccountView | null>(null);

  useEffect(() => {
    fetchAll();
  }, [fetchAll]);

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
      <PageHeader title="账户中心">
        <Button
          onClick={() => {
            setEditing(null);
            setModalOpen(true);
          }}
        >
          添加账户
        </Button>
      </PageHeader>

      {vaultError && (
        <div className="mb-4 text-sm text-loss bg-loss/10 border border-loss/30 rounded px-4 py-3">
          Vault 初始化失败，账户凭证功能不可用：{vaultError}
          <span className="text-loss/70">（原有 vault 文件未被修改，请检查 data 目录下的 vault.key / vault.enc）</span>
        </div>
      )}
      {error && !vaultError && (
        <div className="mb-4 text-sm text-loss bg-loss/10 border border-loss/30 rounded px-4 py-3">
          {error}
        </div>
      )}

      {/* Overview: mock data, clearly labelled */}
      <div className="mb-2 text-xs text-t3 flex items-center gap-2">
        以下为模拟账户数据，非真实资产；真实账户本阶段尚未连接交易所。
        <Badge tone="mock">Mock 模拟</Badge>
      </div>
      <div className="grid grid-cols-3 gap-4 mb-6">
        <StatCard title="总权益" value={`$${fmt(mock.totalEquity)}`} sub="模拟数据 · Mock" />
        <StatCard title="可用权益" value={`$${fmt(mock.available)}`} sub="模拟数据 · Mock" />
        <StatCard title="总盈亏" value={`$${fmt(mock.pnl)}`} sub="模拟数据 · Mock" />
      </div>

      <DataTable headers={HEADERS} minWidth={760}>
        {/* Built-in mock account row */}
        <tr className={trCls}>
          <td className={tdCls}>
            <span className="inline-flex items-center gap-2">
              模拟账户 <Badge tone="mock">Mock</Badge>
            </span>
          </td>
          <td className={tdCls}>MockAdapter</td>
          <td className={`${tdCls} num`}>${fmt(mock.totalEquity)}</td>
          <td className={`${tdCls} num`}>${fmt(mock.available)}</td>
          <td className={`${tdCls} num`}>${fmt(mock.pnl)}</td>
          <td className={`${tdCls} text-t3`}>—</td>
        </tr>
        {accounts.map((a) => (
          <tr key={a.id} className={trCls}>
            <td className={tdCls}>
              {a.label}
              <div className="text-[11px] text-t3 num">{a.maskedIdentifier}</div>
              <div className="mt-0.5">
                <Badge tone={a.credentialConfigured ? "configured" : "warn"}>
                  {a.credentialConfigured ? "凭证已配置" : "未配置凭证"}
                </Badge>
              </div>
            </td>
            <td className={tdCls}>{getExchange(a.exchangeId).name}</td>
            <td className={`${tdCls} text-t3`}>未连接</td>
            <td className={`${tdCls} text-t3`}>—</td>
            <td className={`${tdCls} text-t3`}>—</td>
            <td className={tdCls}>
              <div className="flex items-center justify-center gap-2">
                <Button
                  variant="secondary"
                  size="sm"
                  onClick={() => {
                    setEditing(a);
                    setModalOpen(true);
                  }}
                >
                  编辑
                </Button>
                <Button variant="ghost" size="sm" onClick={() => setPendingDelete(a)}>
                  <span className="text-loss">删除</span>
                </Button>
              </div>
            </td>
          </tr>
        ))}
        {accounts.length === 0 && (
          <tr className="border-t border-line">
            <td colSpan={HEADERS.length} className="text-center px-4 py-8 text-sm text-t3">
              {loading ? "加载中…" : "暂无真实账户，点击右上角「添加账户」创建。账户凭证只加密保存在本机。"}
            </td>
          </tr>
        )}
      </DataTable>

      <AccountModal open={modalOpen} editing={editing} onClose={() => setModalOpen(false)} />

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
          const ok = await remove(pendingDelete.id);
          if (ok) setPendingDelete(null);
        }}
      />
    </div>
  );
}
