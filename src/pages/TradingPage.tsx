import React, { useCallback, useEffect, useState } from "react";
import { Badge, Button, DataTable, EmptyState } from "../components/ui";
import { useLiveStore } from "../store/useLiveStore";
import { useAccountStore } from "../store/useAccountStore";
import { getExchange } from "../lib/exchanges";
import { api } from "../lib/tauri";
import type { OrderInfo, TradeInfo } from "../lib/types";

/**
 * Trading page: real positions + the manual trading test area
 * (phase 5A §4). Orders go through the execution engine
 * (intent → risk → execution → adapter); results — success,
 * failure, Unresolved — are shown here and in the bottom console.
 * Only Hyperliquid / Entropy accounts can trade in this phase;
 * every other exchange answers "该交易所下单将在下一阶段接入".
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

const inputCls =
  "bg-ds-panel-2 border border-ds-line rounded-lg px-3 h-8 text-ds-text-1 outline-none focus:border-ds-accent num";

export default function TradingPage() {
  const live = useLiveStore((s) => s.live);
  const accounts = useAccountStore((s) => s.accounts);
  const positions = Object.values(live).flatMap((a) =>
    a.positions.map((p) => ({ ...p, accountId: a.accountId, adapterId: a.adapterId, currency: a.currency }))
  );
  const labelOf = (accountId: string) =>
    accounts.find((a) => a.id === accountId)?.label ?? accountId;

  // Manual trading test area: Hyperliquid / Entropy only.
  const tradeAccounts = accounts.filter(
    (a) => a.exchangeId === "hyperliquid" || a.exchangeId === "entropy"
  );
  const [accountId, setAccountId] = useState("");
  const [symbol, setSymbol] = useState("BTC");
  const [side, setSide] = useState<"buy" | "sell">("buy");
  const [price, setPrice] = useState("");
  const [qty, setQty] = useState("");
  const [orderType, setOrderType] = useState("limit");
  const [placing, setPlacing] = useState(false);
  const [result, setResult] = useState<OrderInfo | null>(null);
  const [placeError, setPlaceError] = useState<string | null>(null);
  const [openOrders, setOpenOrders] = useState<OrderInfo[]>([]);
  const [records, setRecords] = useState<OrderInfo[]>([]);
  const [fills, setFills] = useState<TradeInfo[]>([]);
  const [cancelingId, setCancelingId] = useState<string | null>(null);

  const selectedAccount = tradeAccounts.find((a) => a.id === accountId);
  const exchangeId = selectedAccount?.exchangeId ?? "";

  // Keep the traded symbol's book live (the risk gateway refuses
  // to order on a stale book); cleared when the page is left.
  useEffect(() => {
    const sym = symbol.trim().toUpperCase();
    if (!exchangeId || !sym) return;
    void api.setStreamSymbols([{ exchangeId, symbol: sym }]);
  }, [exchangeId, symbol]);
  useEffect(() => {
    return () => {
      void api.setStreamSymbols([]);
    };
  }, []);

  const refreshLists = useCallback(async () => {
    try {
      const [o, r, f] = await Promise.all([
        accountId ? api.listOpenOrders(accountId) : Promise.resolve([] as OrderInfo[]),
        api.listOrderRecords(),
        api.listOrderFills(),
      ]);
      setOpenOrders(o);
      setRecords(r);
      setFills(f);
    } catch {
      // Lists are best-effort; a failed refresh never hides the
      // positions above or the last order result.
    }
  }, [accountId]);

  useEffect(() => {
    void refreshLists();
    const t = setInterval(() => void refreshLists(), 10_000);
    return () => clearInterval(t);
  }, [refreshLists]);

  const place = async () => {
    if (!accountId || !symbol.trim() || !price || !qty) return;
    setPlacing(true);
    setPlaceError(null);
    setResult(null);
    try {
      const info = await api.placeOrder({
        accountId,
        symbol: symbol.trim().toUpperCase(),
        side,
        orderType,
        quantity: Number(qty),
        price: Number(price),
      });
      setResult(info);
    } catch (e) {
      setPlaceError(String(e));
    } finally {
      setPlacing(false);
      void refreshLists();
    }
  };

  const cancel = async (order: OrderInfo) => {
    if (!accountId) return;
    setCancelingId(order.orderId || order.clientOrderId);
    try {
      const info = await api.cancelOrder(accountId, order.orderId || order.clientOrderId, order.symbol);
      setResult(info);
    } catch (e) {
      setPlaceError(String(e));
    } finally {
      setCancelingId(null);
      void refreshLists();
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

      {/* Manual trading test area (HL / Entropy only) */}
      <div className="font-semibold text-ds-text-1 mb-2 mt-6">手动下单测试 · Hyperliquid / Entropy</div>
      {tradeAccounts.length === 0 ? (
        <EmptyState>还没有 Hyperliquid 或 Entropy 账户 — 先在账户中心添加并连接。</EmptyState>
      ) : (
        <div className="rounded-lg border border-ds-line bg-ds-panel p-4 mb-4">
          <div className="flex flex-wrap items-end gap-3">
            <label className="flex flex-col gap-1">
              <span className="text-xs text-ds-text-2">账户</span>
              <select className={inputCls} value={accountId} onChange={(e) => setAccountId(e.target.value)}>
                <option value="">选择账户</option>
                {tradeAccounts.map((a) => (
                  <option key={a.id} value={a.id}>
                    {a.label}（{getExchange(a.exchangeId).name}）
                  </option>
                ))}
              </select>
            </label>
            <label className="flex flex-col gap-1">
              <span className="text-xs text-ds-text-2">交易对</span>
              <input
                className={inputCls}
                value={symbol}
                onChange={(e) => setSymbol(e.target.value.toUpperCase())}
                placeholder="BTC"
              />
            </label>
            <div className="flex flex-col gap-1">
              <span className="text-xs text-ds-text-2">方向</span>
              <div className="flex gap-2">
                <Button variant={side === "buy" ? "primary" : "ghost"} onClick={() => setSide("buy")}>
                  买入
                </Button>
                <Button variant={side === "sell" ? "primary" : "ghost"} onClick={() => setSide("sell")}>
                  卖出
                </Button>
              </div>
            </div>
            <label className="flex flex-col gap-1">
              <span className="text-xs text-ds-text-2">价格</span>
              <input
                className={inputCls}
                type="number"
                min={0}
                step="any"
                value={price}
                onChange={(e) => setPrice(e.target.value)}
                placeholder="限价"
              />
            </label>
            <label className="flex flex-col gap-1">
              <span className="text-xs text-ds-text-2">数量</span>
              <input
                className={inputCls}
                type="number"
                min={0}
                step="any"
                value={qty}
                onChange={(e) => setQty(e.target.value)}
                placeholder="0.01"
              />
            </label>
            <label className="flex flex-col gap-1">
              <span className="text-xs text-ds-text-2">类型</span>
              <select className={inputCls} value={orderType} onChange={(e) => setOrderType(e.target.value)}>
                <option value="limit">限价 GTC</option>
                <option value="post_only">Post-Only</option>
                <option value="ioc">IOC</option>
              </select>
            </label>
            <Button
              variant="primary"
              disabled={placing || !accountId || !symbol.trim() || !price || !qty}
              onClick={() => void place()}
            >
              {placing ? "下单中…" : "下单"}
            </Button>
          </div>
          <div className="mt-3 text-xs text-ds-text-2">
            下单前经过风控网关（总开关、回撤、行情新鲜度、方向、单笔上限、仓位上限、可用资金）；价格/数量按品种步长对齐（买向下、卖向上）。请用最小数量测试。
          </div>
          {placeError && (
            <div className="mt-3 text-[13px] text-ds-down">下单失败：{placeError}</div>
          )}
          {result && (
            <div className="mt-3 flex flex-wrap items-center gap-3 text-[13px]">
              <Badge tone={statusTone(result.status)}>{STATUS_LABEL[result.status] ?? result.status}</Badge>
              <span className="num text-ds-text-1">
                {result.symbol} {result.side === "buy" ? "买入" : "卖出"} {result.quantity}
                {result.price != null ? ` @ ${result.price}` : ""}
              </span>
              {result.orderId && <span className="num text-ds-text-2">oid {result.orderId}</span>}
              {result.filledQuantity > 0 && (
                <span className="num text-ds-text-1">
                  已成交 {result.filledQuantity}
                  {result.averageFillPrice != null ? ` @ ${result.averageFillPrice}` : ""}
                </span>
              )}
              {result.error && <span className="text-ds-warn">{result.error}</span>}
              {result.status === "unresolved" && (
                <span className="text-ds-warn">结果未知：已按 cloid 对账，请查看挂单与订单记录。</span>
              )}
            </div>
          )}
        </div>
      )}

      {/* Open orders */}
      <div className="font-semibold text-ds-text-1 mb-2 mt-6">委托挂单</div>
      {openOrders.length === 0 ? (
        <EmptyState>当前无挂单 — 下单后，挂在盘上的订单会显示在这里（每 10 秒刷新）。</EmptyState>
      ) : (
        <div className="mb-6">
          <DataTable
            headers={["账户", "交易对", "方向", "数量", "价格", "状态", "操作"]}
            minWidth={760}
            rows={openOrders.map((o) => [
              <span key="a">{labelOf(o.accountId)}</span>,
              <span key="s" className="num">{o.symbol}</span>,
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
              <span key="s" className="num">{o.symbol}</span>,
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
            <span key="s" className="num">{f.symbol}</span>,
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
