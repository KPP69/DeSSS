import React, { useEffect, useMemo, useState } from "react";
import AccountModal from "../components/AccountModal";
import { getExchange } from "../lib/exchanges";
import type { AccountView } from "../lib/types";
import { useAccountStore } from "../store/useAccountStore";
import { useMarketStore } from "../store/useMarketStore";

function fmt(n: number): string {
  return n.toLocaleString("en-US", { minimumFractionDigits: 2, maximumFractionDigits: 2 });
}

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

  const cards = [
    { title: "总权益", value: `$${fmt(mock.totalEquity)}` },
    { title: "可用权益", value: `$${fmt(mock.available)}` },
    { title: "总盈亏", value: `$${fmt(mock.pnl)}` },
  ];

  const thCls = "text-center text-xs font-medium text-slate-400 px-4 py-3";
  const tdCls = "text-center px-4 py-3 text-sm";

  return (
    <div>
      {/* Header */}
      <div className="flex items-center justify-between mb-5">
        <h1 className="text-xl font-semibold">账户中心</h1>
        <button
          onClick={() => {
            setEditing(null);
            setModalOpen(true);
          }}
          className="px-4 py-2 text-sm rounded bg-sky-600 hover:bg-sky-500 text-white"
        >
          添加账户
        </button>
      </div>

      {vaultError && (
        <div className="mb-4 text-sm text-rose-300 bg-rose-950/40 border border-rose-900 rounded px-4 py-3">
          Vault 初始化失败，账户凭证功能不可用：{vaultError}
          <span className="text-rose-400/70">（原有 vault 文件未被修改，请检查 data 目录下的 vault.key / vault.enc）</span>
        </div>
      )}
      {error && !vaultError && (
        <div className="mb-4 text-sm text-rose-300 bg-rose-950/40 border border-rose-900 rounded px-4 py-3">
          {error}
        </div>
      )}

      {/* Overview: mock data, clearly labelled */}
      <div className="mb-2 text-xs text-slate-500">
        以下为模拟账户（Mock）数据，非真实资产；真实账户本阶段尚未连接交易所。
      </div>
      <div className="grid grid-cols-3 gap-4 mb-6">
        {cards.map((c) => (
          <div
            key={c.title}
            className="bg-slate-900 rounded-lg p-5 flex flex-col items-center text-center"
          >
            <div className="text-sm text-slate-400 text-center">{c.title}</div>
            <div className="text-2xl font-semibold font-mono mt-2 text-center">{c.value}</div>
            <div className="text-[11px] text-slate-600 mt-1 text-center">模拟数据 · Mock</div>
          </div>
        ))}
      </div>

      {/* Account table */}
      <div className="bg-slate-900 rounded-lg overflow-hidden">
        <table className="w-full">
          <thead className="bg-slate-800/60">
            <tr>
              <th className={thCls}>账户名</th>
              <th className={thCls}>交易所</th>
              <th className={thCls}>权益</th>
              <th className={thCls}>可用权益</th>
              <th className={thCls}>盈亏</th>
              <th className={thCls}>操作</th>
            </tr>
          </thead>
          <tbody>
            {/* Built-in mock account row */}
            <tr className="border-t border-slate-800">
              <td className={tdCls}>
                模拟账户 <span className="text-[11px] text-sky-400 border border-sky-800 rounded px-1 py-0.5 ml-1">Mock</span>
              </td>
              <td className={tdCls}>MockAdapter</td>
              <td className={`${tdCls} font-mono`}>${fmt(mock.totalEquity)}</td>
              <td className={`${tdCls} font-mono`}>${fmt(mock.available)}</td>
              <td className={`${tdCls} font-mono`}>${fmt(mock.pnl)}</td>
              <td className={`${tdCls} text-slate-600`}>—</td>
            </tr>
            {accounts.map((a) => (
              <tr key={a.id} className="border-t border-slate-800">
                <td className={tdCls}>
                  {a.label}
                  <div className="text-[11px] text-slate-500 font-mono">{a.maskedIdentifier}</div>
                </td>
                <td className={tdCls}>{getExchange(a.exchangeId).name}</td>
                <td className={`${tdCls} text-slate-500`}>未连接</td>
                <td className={`${tdCls} text-slate-500`}>—</td>
                <td className={`${tdCls} text-slate-500`}>—</td>
                <td className={tdCls}>
                  <div className="flex items-center justify-center gap-2">
                    <button
                      onClick={() => {
                        setEditing(a);
                        setModalOpen(true);
                      }}
                      className="px-3 py-1 text-xs rounded border border-slate-600 hover:bg-slate-800"
                    >
                      编辑
                    </button>
                    <button
                      onClick={() => setPendingDelete(a)}
                      className="px-3 py-1 text-xs rounded border border-rose-900 text-rose-400 hover:bg-rose-950/40"
                    >
                      删除
                    </button>
                  </div>
                </td>
              </tr>
            ))}
            {accounts.length === 0 && (
              <tr className="border-t border-slate-800">
                <td colSpan={6} className="text-center px-4 py-8 text-sm text-slate-500">
                  {loading ? "加载中…" : "暂无真实账户，点击右上角「添加账户」创建。账户凭证只加密保存在本机。"}
                </td>
              </tr>
            )}
          </tbody>
        </table>
      </div>

      <AccountModal open={modalOpen} editing={editing} onClose={() => setModalOpen(false)} />

      {/* Delete confirmation */}
      {pendingDelete && (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/60">
          <div className="bg-slate-900 border border-slate-700 rounded-lg w-[420px] max-w-[92vw] p-6">
            <h3 className="text-base font-semibold mb-2">确认删除账户</h3>
            <p className="text-sm text-slate-400">
              确定要删除账户「{pendingDelete.label}」（{getExchange(pendingDelete.exchangeId).name}）吗？
              该账户保存在本机 Vault 中的凭证将一并删除，此操作不可恢复。
            </p>
            <div className="flex justify-end gap-3 mt-5">
              <button
                onClick={() => setPendingDelete(null)}
                className="px-4 py-2 text-sm rounded border border-slate-600 hover:bg-slate-800"
              >
                取消
              </button>
              <button
                onClick={async () => {
                  const ok = await remove(pendingDelete.id);
                  if (ok) setPendingDelete(null);
                }}
                className="px-4 py-2 text-sm rounded bg-rose-600 hover:bg-rose-500 text-white"
              >
                确认删除
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
