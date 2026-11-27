import React, { useState } from "react";
import { Badge, Button, ConfirmDialog, DataTable, EmptyState, Tooltip } from "../ui";
import PageState from "../PageState";
import { displaySymbol } from "../../lib/symbol";
import type { AccountView, OrderInfo } from "../../lib/types";
import { originLabel, statusTone, STATUS_LABEL } from "../../hooks/useTradingData";

/**
 * "账户 · 交易所" cell: truncated to the column width, full text in
 * a Tooltip (account labels run up to 64 chars).
 */
export function AccountCell({
  accountId,
  exchangeId,
  labelOf,
  venueName,
}: {
  accountId: string;
  exchangeId: string;
  labelOf: (accountId: string) => string;
  venueName: (id: string) => string;
}) {
  const text = `${labelOf(accountId)} · ${venueName(exchangeId)}`;
  return (
    <Tooltip content={text}>
      <span className="inline-block max-w-[160px] truncate align-middle">{text}</span>
    </Tooltip>
  );
}

/**
 * Orders segment: live open orders on top, order records below.
 * Cancelling a strategy order asks first (it affects the strategy's
 * position judgment); cancelling a manual order goes straight
 * through. The last cancel result shows under the open-orders
 * table, exactly as before.
 */
export default function OrdersSection({
  openOrders,
  records,
  tradeAccounts,
  loading,
  error,
  onRetry,
  labelOf,
  venueName,
  cancelingId,
  onCancel,
  cancelResult,
}: {
  openOrders: OrderInfo[];
  records: OrderInfo[];
  tradeAccounts: AccountView[];
  loading: boolean;
  error: string | null;
  onRetry: () => void;
  labelOf: (accountId: string) => string;
  venueName: (id: string) => string;
  cancelingId: string | null;
  onCancel: (o: OrderInfo) => void;
  cancelResult: OrderInfo | null;
}) {
  const [pendingCancel, setPendingCancel] = useState<OrderInfo | null>(null);

  const askCancel = (order: OrderInfo) => {
    if (order.origin.startsWith("strategy:")) {
      setPendingCancel(order);
    } else {
      void onCancel(order);
    }
  };

  if (loading) {
    return <PageState loading loadingText="加载挂单与订单记录…" />;
  }
  if (error) {
    return (
      <PageState error={`挂单加载失败：${error}`} onRetry={onRetry} />
    );
  }

  return (
    <div>
      <div className="font-semibold text-ds-text-1 mb-2">委托挂单 · 与交易所实时同步</div>
      {tradeAccounts.length === 0 ? (
        <EmptyState>还没有 Hyperliquid 或 Entropy 账户 — 先在账户中心添加并连接。</EmptyState>
      ) : openOrders.length === 0 ? (
        <EmptyState>当前无挂单 — 交易所的活动挂单会显示在这里（每 10 秒同步），在官网挂的单也会出现。</EmptyState>
      ) : (
        <div className="mb-6">
          <DataTable
            headers={["时间", "账户 · 交易所", "交易对", "方向", "数量", "价格", "状态", "来源", "操作"]}
            minWidth={960}
            colWidths={["9%", "20%", "9%", "6%", "9%", "11%", "9%", "13%", "14%"]}
            aligns={["center", "center", "center", "center", "right", "right", "center", "center", "center"]}
            rows={openOrders.map((o) => [
              <span key="t" className="num">{new Date(o.createdAt).toLocaleTimeString()}</span>,
              <AccountCell key="a" accountId={o.accountId} exchangeId={o.exchangeId} labelOf={labelOf} venueName={venueName} />,
              <span key="s" className="num">{displaySymbol(o.symbol)}</span>,
              <span key="d">
                <Badge tone={o.side === "buy" ? "up" : "down"}>{o.side === "buy" ? "买" : "卖"}</Badge>
              </span>,
              <span key="q" className="num">{o.quantity}</span>,
              <span key="p" className="num">{o.price ?? "—"}</span>,
              <span key="st">
                <Badge tone={statusTone(o.status)}>{STATUS_LABEL[o.status] ?? o.status}</Badge>
              </span>,
              <span key="o">{originLabel(o.origin)}</span>,
              <span key="c">
                <Button
                  variant="ghost"
                  disabled={cancelingId === (o.orderId || o.clientOrderId)}
                  onClick={() => askCancel(o)}
                >
                  {cancelingId === (o.orderId || o.clientOrderId) ? "撤单中…" : "撤单"}
                </Button>
              </span>,
            ])}
          />
        </div>
      )}
      {cancelResult && (
        <div className="mt-3 flex flex-wrap items-center gap-3 text-[13px]">
          <Badge tone={statusTone(cancelResult.status)}>
            {STATUS_LABEL[cancelResult.status] ?? cancelResult.status}
          </Badge>
          <span className="num text-ds-text-1">
            {displaySymbol(cancelResult.symbol)} {cancelResult.side === "buy" ? "买入" : "卖出"} {cancelResult.quantity}
            {cancelResult.price != null ? ` @ ${cancelResult.price}` : ""}
          </span>
          {cancelResult.orderId && <span className="num text-ds-text-2">oid {cancelResult.orderId}</span>}
          {cancelResult.filledQuantity > 0 && (
            <span className="num text-ds-text-1">
              已成交 {cancelResult.filledQuantity}
              {cancelResult.averageFillPrice != null ? ` @ ${cancelResult.averageFillPrice}` : ""}
            </span>
          )}
          {cancelResult.error && <span className="text-ds-warn">{cancelResult.error}</span>}
          {cancelResult.status === "unresolved" && (
            <span className="text-ds-warn">结果未知：已按 cloid 对账，请查看挂单与订单记录。</span>
          )}
        </div>
      )}

      <div className="font-semibold text-ds-text-1 mb-2 mt-6">订单记录</div>
      {records.length === 0 ? (
        <EmptyState>暂无订单记录 — 下单、撤单的结果（成功 / 失败 / 未确认）会记录在这里。</EmptyState>
      ) : (
        <div className="mb-6">
          <DataTable
            headers={["时间", "账户 · 交易所", "交易对", "方向", "数量", "价格", "已成交", "状态", "来源", "说明"]}
            minWidth={960}
            colWidths={["8%", "20%", "8%", "6%", "8%", "10%", "10%", "8%", "11%", "11%"]}
            aligns={["center", "center", "center", "center", "right", "right", "right", "center", "center", "left"]}
            rows={records.map((o) => [
              <span key="t" className="num">
                {new Date(o.createdAt).toLocaleTimeString()}
              </span>,
              <AccountCell key="a" accountId={o.accountId} exchangeId={o.exchangeId} labelOf={labelOf} venueName={venueName} />,
              <span key="s" className="num">{displaySymbol(o.symbol)}</span>,
              <span key="d">
                <Badge tone={o.side === "buy" ? "up" : "down"}>{o.side === "buy" ? "买" : "卖"}</Badge>
              </span>,
              <span key="q" className="num">{o.quantity}</span>,
              <span key="p" className="num">{o.price ?? "—"}</span>,
              <span key="f" className="num">
                {o.filledQuantity > 0
                  ? `${o.filledQuantity}${o.averageFillPrice != null ? ` @ ${o.averageFillPrice}` : ""}`
                  : "—"}
              </span>,
              <span key="st">
                <Badge tone={statusTone(o.status)}>{STATUS_LABEL[o.status] ?? o.status}</Badge>
              </span>,
              <span key="o">{originLabel(o.origin)}</span>,
              <span key="e" className="text-ds-text-2">{o.error ?? ""}</span>,
            ])}
          />
        </div>
      )}

      <ConfirmDialog
        open={pendingCancel != null}
        title="确认撤单"
        confirmText="确认撤单"
        message={
          pendingCancel && (
            <div className="space-y-1">
              <div>
                {displaySymbol(pendingCancel.symbol)} ·{" "}
                {pendingCancel.side === "buy" ? "买入" : "卖出"} {pendingCancel.quantity}
                {pendingCancel.price != null ? ` @ ${pendingCancel.price}` : ""}
              </div>
              <div className="text-ds-warn">
                这笔单由{originLabel(pendingCancel.origin)}挂出，撤单会影响策略的持仓判断。
              </div>
            </div>
          )
        }
        onConfirm={() => {
          if (pendingCancel) {
            const o = pendingCancel;
            setPendingCancel(null);
            void onCancel(o);
          }
        }}
        onCancel={() => setPendingCancel(null)}
      />
    </div>
  );
}
