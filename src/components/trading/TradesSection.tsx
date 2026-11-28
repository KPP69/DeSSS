import React from "react";
import { Badge, DataTable, EmptyState } from "../ui";
import PageState from "../PageState";
import { displaySymbol } from "../../lib/symbol";
import type { OrderInfo, TradeInfo } from "../../lib/types";
import { originLabel } from "../../hooks/useTradingData";
import { AccountCell } from "./OrdersSection";

/**
 * Trades segment: fill records, newest first. Fills carry no
 * origin of their own, so the source is looked up from the order
 * records by order id (falling back to the client order id); a fill
 * whose order has left the records shows "—".
 */
export default function TradesSection({
  fills,
  records,
  loading,
  error,
  onRetry,
  labelOf,
  venueName,
}: {
  fills: TradeInfo[];
  records: OrderInfo[];
  loading: boolean;
  error: string | null;
  onRetry: () => void;
  labelOf: (accountId: string) => string;
  venueName: (id: string) => string;
}) {
  if (loading) {
    return <PageState loading loadingText="加载成交记录…" />;
  }
  if (error) {
    return (
      <PageState error={`成交记录加载失败：${error}`} onRetry={onRetry} />
    );
  }

  const originByOrder = new Map<string, string>();
  for (const o of records) {
    if (o.orderId) originByOrder.set(o.orderId, o.origin);
    if (o.clientOrderId) originByOrder.set(o.clientOrderId, o.origin);
  }
  const sorted = [...fills].sort((a, b) => b.timestamp - a.timestamp);

  return (
    <div>
      <div className="font-semibold text-ds-text-1 mb-2">成交记录</div>
      {sorted.length === 0 ? (
        <EmptyState>暂无成交 — 订单成交后，成交明细（含手续费）会显示在这里。</EmptyState>
      ) : (
        <DataTable
          headers={["时间", "账户 · 交易所", "交易对", "方向", "数量", "价格", "手续费", "来源"]}
          minWidth={680}
          colWidths={["11%", "24%", "10%", "7%", "9%", "12%", "13%", "14%"]}
          aligns={["right", "left", "left", "center", "right", "right", "right", "left"]}
          rows={sorted.map((f) => {
            const origin =
              originByOrder.get(f.orderId) ?? originByOrder.get(f.clientOrderId);
            return [
              <span key="t" className="num">{new Date(f.timestamp).toLocaleTimeString()}</span>,
              <AccountCell key="a" accountId={f.accountId} exchangeId={f.exchangeId} labelOf={labelOf} venueName={venueName} maxW="max-w-[130px]" />,
              <span key="s" className="num">{displaySymbol(f.symbol)}</span>,
              <span key="d">
                <Badge tone={f.side === "buy" ? "up" : "down"}>{f.side === "buy" ? "买" : "卖"}</Badge>
              </span>,
              <span key="q" className="num">{f.quantity}</span>,
              <span key="p" className="num">{f.price}</span>,
              <span key="f" className="num">
                {f.fee != null ? `${f.fee} ${f.feeCurrency ?? ""}` : "—"}
              </span>,
              <span key="o">{origin != null ? originLabel(origin) : "—"}</span>,
            ];
          })}
        />
      )}
    </div>
  );
}
