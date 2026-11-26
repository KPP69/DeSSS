import React, { useEffect, useState } from "react";
import { Badge, Button, DataTable, EmptyState } from "../components/ui";
import { useInvoke } from "../hooks/useInvoke";
import { displaySymbol } from "../lib/symbol";
import { api } from "../lib/tauri";
import type { OrderInfo } from "../lib/types";
import { useAccountStore } from "../store/useAccountStore";
import { useLiveStore } from "../store/useLiveStore";
import { useVenueStore } from "../store/useVenueStore";

/**
 * Trading page: real positions, live open orders (with cancel) and
 * order / fill records. There is deliberately no manual order form
 * here (phase 5A follow-up, 2026-11-26): the `place_order` pipeline,
 * risk gateway, order state machine and signer stay in the backend
 * for the Strategy module to call; this page only watches the
 * exchange state and cancels. Open orders are synced with the
 * exchange every 10s (backend poll + this page's refresh), so orders
 * placed or canceled on the website show up here too.
 *
 * The three lists come from one `useInvoke` (Phase 2, I11); cancel
 * is a click mutation and refreshes the lists after it settles.
 */

const STATUS_LABEL: Record<string, string> = {
  pending: "待发送",
  submitted: "已发送",
  open: "挂单中",
  partially_filled: "部分成交",
  filled: "已成交",
  rejected: "被拒绝",
  canceled: "已撤销",
  expired: "已过期",
  failed: "失败",
  unresolved: "未确认",
};

function statusTone(status: string): "up" | "down" | "warn" | "info" | "neutral" {
  switch (status) {
    case "filled":
      return "up";
    case "partially_filled":
      return "info";
    case "open":
    case "submitted":
    case "pending":
      return "info";
    case "unresolved":
      return "warn";
    case "rejected":
    case "canceled":
    case "expired":
    case "failed":
      return "down";
    default:
      return "neutral";
  }
}

export default function TradingPage() {
  const live = useLiveStore((s) => s.live);
  const accounts = useAccountStore((s) => s.accounts);
  const positions = Object.values(live).flatMap((a) =>
    a.positions.map((p) => ({ ...p, accountId: a.accountId, adapterId: a.adapterId, currency: a.currency }))
  );
  const labelOf = (accountId: string) =>
    accounts.find((a) => a.id === accountId)?.label ?? accountId;

  // Open orders come from every connected Hyperliquid / Entropy
  // account; the backend syncs each list with the exchange.
  const tradeAccounts = accounts.filter(
    (a) => a.exchangeId === "hyperliquid" || a.exchangeId === "entropy"
  );
  const lists = useInvoke(async () => {
    const ids = accounts
      .filter((a) => a.exchangeId === "hyperliquid" || a.exchangeId === "entropy")
      .map((a) => a.id);
    const [open, records, fills] = await Promise.all([
      Promise.all(ids.map((id) => api.listOpenOrders(id))).then((l) => l.flat()),
      api.listOrderRecords(),
      api.listOrderFills(),
    ]);
    return { open, records, fills };
  }, [accounts]);
  const openOrders = lists.data?.open ?? [];
  const records = lists.data?.records ?? [];
  const fills = lists.data?.fills ?? [];

  useEffect(() => {
    const t = setInterval(() => lists.refresh(), 10_000);
    return () => clearInterval(t);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  const names = useVenueStore((s) => s.names);
  const venueName = (id: string) => names[id] ?? id;

  const [cancelingId, setCancelingId] = useState<string | null>(null);
  const [cancelResult, setCancelResult] = useState<OrderInfo | null>(null);
  const [cancelError, setCancelError] = useState<string | null>(null);

  const cancel = async (order: OrderInfo) => {
    setCancelingId(order.orderId || order.clientOrderId);
    setCancelError(null);
    setCancelResult(null);
    try {
      const info = await api.cancelOrder(
        order.accountId,
        order.orderId || order.clientOrderId,
        order.symbol
      );
      setCancelResult(info);
    } catch (e) {
      setCancelError(String(e));
    } finally {
      setCancelingId(null);
      lists.refresh();
    }
  };

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
              <span key="e">{venueName(p.adapterId)}</span>,
              <span key="s" className="num">{displaySymbol(p.symbol)}</span>,
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

      {/* Open orders */}
      <div className="font-semibold text-ds-text-1 mb-2 mt-6">委托挂单 · 与交易所实时同步</div>
      {tradeAccounts.length === 0 ? (
        <EmptyState>还没有 Hyperliquid 或 Entropy 账户 — 先在账户中心添加并连接。</EmptyState>
      ) : openOrders.length === 0 ? (
        <EmptyState>当前无挂单 — 交易所的活动挂单会显示在这里（每 10 秒同步），在官网挂的单也会出现。</EmptyState>
      ) : (
        <div className="mb-6">
          <DataTable
            headers={["账户", "交易对", "方向", "数量", "价格", "状态", "操作"]}
            minWidth={760}
            rows={openOrders.map((o) => [
              <span key="a">{labelOf(o.accountId)}</span>,
              <span key="s" className="num">{displaySymbol(o.symbol)}</span>,
              <span key="d">
                <Badge tone={o.side === "buy" ? "up" : "down"}>{o.side === "buy" ? "买" : "卖"}</Badge>
              </span>,
              <span key="q" className="num">{o.quantity}</span>,
              <span key="p" className="num">{o.price ?? "—"}</span>,
              <span key="st">
                <Badge tone={statusTone(o.status)}>{STATUS_LABEL[o.status] ?? o.status}</Badge>
              </span>,
              <span key="c">
                <Button
                  variant="ghost"
                  disabled={cancelingId === (o.orderId || o.clientOrderId)}
                  onClick={() => void cancel(o)}
                >
                  {cancelingId === (o.orderId || o.clientOrderId) ? "撤单中…" : "撤单"}
                </Button>
              </span>,
            ])}
          />
        </div>
      )}
      {cancelError && (
        <div className="mt-3 text-[13px] text-ds-down">撤单失败：{cancelError}</div>
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

      {/* Order records */}
      <div className="font-semibold text-ds-text-1 mb-2 mt-6">订单记录</div>
      {records.length === 0 ? (
        <EmptyState>暂无订单记录 — 下单、撤单的结果（成功 / 失败 / 未确认）会记录在这里。</EmptyState>
      ) : (
        <div className="mb-6">
          <DataTable
            headers={["时间", "交易对", "方向", "数量", "价格", "已成交", "状态", "说明"]}
            minWidth={760}
            rows={records.slice(0, 20).map((o) => [
              <span key="t" className="num">
                {new Date(o.createdAt).toLocaleTimeString()}
              </span>,
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
              <span key="e" className="text-ds-text-2">{o.error ?? ""}</span>,
            ])}
          />
        </div>
      )}

      {/* Fills */}
      <div className="font-semibold text-ds-text-1 mb-2 mt-6">成交记录</div>
      {fills.length === 0 ? (
        <EmptyState>暂无成交 — 订单成交后，成交明细（含手续费）会显示在这里。</EmptyState>
      ) : (
        <DataTable
          headers={["时间", "交易对", "方向", "价格", "数量", "手续费"]}
          minWidth={640}
          rows={fills.slice(0, 20).map((f) => [
            <span key="t" className="num">{new Date(f.timestamp).toLocaleTimeString()}</span>,
            <span key="s" className="num">{displaySymbol(f.symbol)}</span>,
            <span key="d">
              <Badge tone={f.side === "buy" ? "up" : "down"}>{f.side === "buy" ? "买" : "卖"}</Badge>
            </span>,
            <span key="p" className="num">{f.price}</span>,
            <span key="q" className="num">{f.quantity}</span>,
            <span key="f" className="num">
              {f.fee != null ? `${f.fee} ${f.feeCurrency ?? ""}` : "—"}
            </span>,
          ])}
        />
      )}
    </div>
  );
}
