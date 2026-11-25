import React from "react";
import { Badge, DataTable, EmptyState } from "../components/ui";
import { useLiveStore } from "../store/useLiveStore";
import { useAccountStore } from "../store/useAccountStore";
import { getExchange } from "../lib/exchanges";

/**
 * Trading page: real positions only. The live book (BBO + top-5 depth)
 * lives on the Data page now; order and trade history are clean empty
 * placeholders until the order flow lands. No mock data anywhere.
 */
export default function TradingPage() {
  const live = useLiveStore((s) => s.live);
  const accounts = useAccountStore((s) => s.accounts);
  const positions = Object.values(live).flatMap((a) =>
    a.positions.map((p) => ({ ...p, accountId: a.accountId, adapterId: a.adapterId, currency: a.currency }))
  );
  const labelOf = (accountId: string) =>
    accounts.find((a) => a.id === accountId)?.label ?? accountId;

  return (
    <div>
      {/* Real positions */}
      <div className="font-semibold text-ds-text-1 mb-2">当前持仓 · 来自已连接账户的只读同步</div>
      {positions.length === 0 ? (
        <EmptyState>暂无真实持仓 — 连接账户后，交易所同步到的当前持仓会显示在这里。</EmptyState>
      ) : (
        <div className="mb-6">
          <DataTable
            headers={["账户", "交易所", "交易对", "方向", "数量", "开仓价", "标记价", "未实现盈亏"]}
            minWidth={760}
            rows={positions.map((p, i) => [
              <span key="a">{labelOf(p.accountId)}</span>,
              <span key="e">{getExchange(p.adapterId).name}</span>,
              <span key="s" className="num">{p.symbol}</span>,
              <span key="d">
                <Badge tone={p.side === "long" ? "up" : "down"}>
                  {p.side === "long" ? "多" : "空"}
                </Badge>
              </span>,
              <span key="q" className="num">{p.qty}</span>,
              <span key="ep" className="num">{p.entryPrice.toFixed(2)}</span>,
              <span key="mp" className="num">{p.markPrice.toFixed(2)}</span>,
              <span
                key="p"
                className={`num ${p.unrealizedPnl > 0 ? "text-ds-up" : p.unrealizedPnl < 0 ? "text-ds-down" : ""}`}
              >
                {p.unrealizedPnl > 0 ? "+" : ""}
                {p.unrealizedPnl.toFixed(2)} {p.currency}
              </span>,
            ])}
          />
        </div>
      )}

      {/* Future order flow: clean placeholders, no fake rows. */}
      <div className="font-semibold text-ds-text-1 mb-2 mt-6">委托挂单</div>
      <EmptyState>委托挂单尚未接入 — 下单与撤单功能上线后，当前挂单会显示在这里。</EmptyState>

      <div className="font-semibold text-ds-text-1 mb-2 mt-6">成交记录</div>
      <EmptyState>成交记录尚未接入 — 订单成交后，成交历史会显示在这里。</EmptyState>
    </div>
  );
}
