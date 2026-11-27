import React, { useEffect, useState } from "react";
import { Badge, Button, ConfirmDialog, DataTable, EmptyState, Modal } from "../components/ui";
import { useInvoke } from "../hooks/useInvoke";
import { useVenueSchemas } from "../hooks/useVenueSchemas";
import { displaySymbol } from "../lib/symbol";
import { api } from "../lib/tauri";
import type { OrderBook, OrderInfo } from "../lib/types";
import { useAccountStore } from "../store/useAccountStore";
import { useLiveStore } from "../store/useLiveStore";
import { useShellActions } from "../store/useShellActions";
import { toast } from "../store/useToastStore";
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
 * PR-3.1.1 adds the one exception to "no manual orders": position
 * protection. A position row can close the position (IOC +
 * reduceOnly) or hang TP/SL trigger orders on it (reduceOnly,
 * grouping positionTpsl). Nothing here opens a position.
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

/** Where an order came from (Phase 3.1, commit 2): manual orders
 * say 手动, strategy orders show the first 8 chars of the id. */
function originLabel(origin: string): string {
  if (origin === "manual") return "手动";
  if (origin.startsWith("strategy:")) return `策略 ${origin.slice(9, 17)}`;
  return origin;
}

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

/** A trigger (TP/SL) order carries a trigger price. */
function isTriggerOrder(o: OrderInfo): boolean {
  return o.triggerPx != null;
}

/** TP/SL label for one trigger order. The exchange reports it when
 * it can ("Take Profit Market" / "Stop Loss Market"); otherwise
 * derive it from the position — for a long, a sell trigger above
 * the mark is a take-profit and below it a stop-loss (short: the
 * reverse). A trigger exactly at the mark is degenerate and reads
 * as a stop-loss. */
function tpslLabel(
  o: OrderInfo,
  position: { side: string; markPrice: number },
): "tp" | "sl" | null {
  if (o.tpsl === "tp" || o.tpsl === "sl") return o.tpsl;
  if (o.triggerPx == null) return null;
  const above = o.triggerPx > position.markPrice;
  if (position.side === "long") return above ? "tp" : "sl";
  return above ? "sl" : "tp";
}

type PositionRow = {
  accountId: string;
  adapterId: string;
  symbol: string;
  side: string;
  qty: number;
  entryPrice: number;
  markPrice: number;
  unrealizedPnl: number;
  currency: string;
};

/** The position's current protection, from the live open orders. */
function protectionFor(
  accountId: string,
  symbol: string,
  open: OrderInfo[],
  position: PositionRow,
): { tp: number | null; sl: number | null; orders: OrderInfo[] } {
  const orders = open.filter(
    (o) => o.accountId === accountId && o.symbol === symbol && isTriggerOrder(o),
  );
  let tp: number | null = null;
  let sl: number | null = null;
  for (const o of orders) {
    const label = tpslLabel(o, position);
    if (label === "tp") tp = o.triggerPx;
    else if (label === "sl") sl = o.triggerPx;
  }
  return { tp, sl, orders };
}

/** Close price: sell a long just under the best bid, buy a short
 * just over the best ask (2 bps of give, so the IOC fills). */
function closePrice(position: PositionRow, book: OrderBook | undefined): number | null {
  if (!book) return null;
  if (position.side === "long") {
    const bid = book.bids[0]?.[0];
    return bid == null ? null : bid * (1 - 0.0002);
  }
  const ask = book.asks[0]?.[0];
  return ask == null ? null : ask * (1 + 0.0002);
}

/** The book counts as fresh for 5s — the same window the risk
 * gateway enforces backend-side. */
function bookFresh(book: OrderBook | undefined): boolean {
  return book != null && Date.now() - book.ts <= 5000;
}

/** Validate the protect inputs against the position's mark price.
 * Both empty is legal: it clears all protection (cancel only).
 * Returns the error message, or null when the inputs are fine. */
function validateProtect(
  side: string,
  mark: number,
  tp: string,
  sl: string,
): string | null {
  const tpNum = tp.trim() === "" ? null : Number(tp);
  const slNum = sl.trim() === "" ? null : Number(sl);
  if (tp.trim() !== "" && !(tpNum != null && tpNum > 0)) return "止盈价必须是大于 0 的数字";
  if (sl.trim() !== "" && !(slNum != null && slNum > 0)) return "止损价必须是大于 0 的数字";
  if (tpNum == null && slNum == null) return null;
  if (side === "long") {
    if (tpNum != null && tpNum <= mark) return `多仓止盈价必须高于现价 ${mark}`;
    if (slNum != null && slNum >= mark) return `多仓止损价必须低于现价 ${mark}`;
  } else {
    if (tpNum != null && tpNum >= mark) return `空仓止盈价必须低于现价 ${mark}`;
    if (slNum != null && slNum <= mark) return `空仓止损价必须高于现价 ${mark}`;
  }
  return null;
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

  // Toolbar (F1): refresh action + position / open-order counts.
  const register = useShellActions((s) => s.register);
  const unregister = useShellActions((s) => s.unregister);
  useEffect(() => {
    register("trading", {
      actions: <Button onClick={() => lists.refresh()}>刷新</Button>,
      status: `持仓 ${positions.length} · 挂单 ${openOrders.length}`,
    });
    return () => unregister("trading");
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [register, unregister, positions.length, openOrders.length]);

  useEffect(() => {
    const t = setInterval(() => lists.refresh(), 10_000);
    return () => clearInterval(t);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  const names = useVenueStore((s) => s.names);
  const venueName = (id: string) => names[id] ?? id;

  // Position protection (PR-3.1.1): only exchanges whose backend
  // supports trading (HL / Entropy today) get the buttons.
  const books = useLiveStore((s) => s.books);
  const { schemas } = useVenueSchemas();
  const tradable = new Set(
    (schemas ?? []).filter((s) => s.supportsTrading).map((s) => s.id),
  );
  const tradableKey = [...tradable].sort().join(",");
  const positionKeys = positions
    .filter((p) => tradable.has(p.adapterId))
    .map((p) => `${p.adapterId}:${p.symbol}`)
    .sort()
    .join(",");

  // Stream the books the close price needs (same page-owns-the-set
  // pattern as the Data page: leaving the page unsubscribes). The
  // backend diffs the set, so repeat calls are no-ops; the
  // strategy's own books are re-ensured by its runner each round.
  useEffect(() => {
    const keys = positions
      .filter((p) => tradable.has(p.adapterId))
      .map((p) => ({ exchangeId: p.adapterId, symbol: p.symbol }));
    void api.setStreamSymbols(keys);
    return () => {
      void api.setStreamSymbols([]);
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [positionKeys, tradableKey]);

  // Close (ConfirmDialog) + protect (Modal) + expand state.
  const [closeTarget, setCloseTarget] = useState<{
    accountId: string;
    adapterId: string;
    symbol: string;
    side: string;
    qty: number;
    price: number;
  } | null>(null);
  const [closing, setClosing] = useState(false);
  const [protectTarget, setProtectTarget] = useState<PositionRow | null>(null);
  const [tpInput, setTpInput] = useState("");
  const [slInput, setSlInput] = useState("");
  const [protectError, setProtectError] = useState<string | null>(null);
  const [protectBusy, setProtectBusy] = useState(false);
  const [expandedKey, setExpandedKey] = useState<string | null>(null);
  const [actionResult, setActionResult] = useState<string | null>(null);
  const [actionError, setActionError] = useState<string | null>(null);

  const positionKey = (p: PositionRow) => `${p.accountId}:${p.symbol}`;
  const canAct = (p: PositionRow) =>
    tradable.has(p.adapterId) &&
    Math.abs(p.qty) > 0 &&
    bookFresh(books[`${p.adapterId}:${p.symbol}`]);

  const askClose = (p: PositionRow) => {
    const price = closePrice(p, books[`${p.adapterId}:${p.symbol}`]);
    if (price == null) {
      setActionError("盘口未就绪，无法平仓");
      return;
    }
    setActionError(null);
    setActionResult(null);
    setCloseTarget({
      accountId: p.accountId,
      adapterId: p.adapterId,
      symbol: p.symbol,
      side: p.side,
      qty: Math.abs(p.qty),
      price,
    });
  };

  const confirmClose = async () => {
    if (!closeTarget) return;
    // The dialog may have sat open while the book went stale:
    // re-check freshness, never confirm a stale price.
    const freshBook = useLiveStore.getState().books[
      `${closeTarget.adapterId}:${closeTarget.symbol}`
    ];
    if (!bookFresh(freshBook)) {
      setActionError("盘口已过期，请关闭后重试");
      setCloseTarget(null);
      return;
    }
    setClosing(true);
    try {
      const info = await api.placeOrder({
        accountId: closeTarget.accountId,
        symbol: closeTarget.symbol,
        side: closeTarget.side === "long" ? "sell" : "buy",
        orderType: "ioc",
        quantity: closeTarget.qty,
        price: closeTarget.price,
        reduceOnly: true,
      });
      setActionResult(
        `平仓单已发：${displaySymbol(info.symbol)} ${info.side === "buy" ? "买入" : "卖出"} ${info.quantity}` +
          `${info.price != null ? ` @ ${info.price}` : ""}（${STATUS_LABEL[info.status] ?? info.status}）` +
          "。仓位归零后，挂着的保护单会随之失效，请在挂单表里清除。",
      );
      setCloseTarget(null);
      lists.refresh();
    } catch (e) {
      toast.error(e instanceof Error ? e.message : String(e));
      setCloseTarget(null);
    } finally {
      setClosing(false);
    }
  };

  const openProtect = (p: PositionRow) => {
    const prot = protectionFor(p.accountId, p.symbol, openOrders, p);
    setTpInput(prot.tp != null ? String(prot.tp) : "");
    setSlInput(prot.sl != null ? String(prot.sl) : "");
    setProtectError(null);
    setProtectTarget(p);
  };

  const saveProtect = async () => {
    if (!protectTarget) return;
    const err = validateProtect(
      protectTarget.side,
      protectTarget.markPrice,
      tpInput,
      slInput,
    );
    if (err) {
      setProtectError(err);
      return;
    }
    const tpNum = tpInput.trim() === "" ? null : Number(tpInput);
    const slNum = slInput.trim() === "" ? null : Number(slInput);
    setProtectError(null);
    setProtectBusy(true);
    setActionResult(null);
    setActionError(null);
    const { accountId, symbol, side, qty } = protectTarget;
    const closeSide = side === "long" ? "sell" : "buy";
    // How many old protection orders were already cancelled when a
    // later step fails — the catch reports it, they never come back.
    let cancelled = 0;
    try {
      // "改" = 撤旧 + 建新：先撤该仓位所有手动触发单，再按输入重建。
      const manual = openOrders.filter(
        (o) =>
          o.accountId === accountId &&
          o.symbol === symbol &&
          o.origin === "manual" &&
          o.triggerPx != null,
      );
      for (const o of manual) {
        await api.cancelOrder(accountId, o.orderId || o.clientOrderId, symbol);
        cancelled++;
      }
      const placed: string[] = [];
      for (const [px, tpsl] of [
        [tpNum, "tp"],
        [slNum, "sl"],
      ] as const) {
        if (px == null) continue;
        const info = await api.placeOrder({
          accountId,
          symbol,
          side: closeSide,
          orderType: "trigger",
          quantity: Math.abs(qty),
          price: px,
          reduceOnly: true,
          triggerPx: px,
          tpsl,
          isMarket: true,
        });
        placed.push(
          `${tpsl === "tp" ? "止盈" : "止损"} ${px}` +
            `${info.status === "unresolved" ? "（未确认）" : ""}`,
        );
      }
      setActionResult(
        `保护已更新：${manual.length > 0 ? `撤掉 ${manual.length} 笔旧保护单；` : ""}` +
          `${placed.length > 0 ? `已挂 ${placed.join("、")}` : "已清除全部保护"}`,
      );
      setProtectTarget(null);
      lists.refresh();
    } catch (e) {
      toast.error(e instanceof Error ? e.message : String(e));
      if (cancelled > 0) {
        setProtectError(`已撤 ${cancelled} 笔旧单，不会自动恢复，可重试`);
      }
    } finally {
      setProtectBusy(false);
    }
  };

  const [cancelingId, setCancelingId] = useState<string | null>(null);
  const [cancelResult, setCancelResult] = useState<OrderInfo | null>(null);

  const cancel = async (order: OrderInfo) => {
    setCancelingId(order.orderId || order.clientOrderId);
    setCancelResult(null);
    try {
      const info = await api.cancelOrder(
        order.accountId,
        order.orderId || order.clientOrderId,
        order.symbol
      );
      setCancelResult(info);
    } catch (e) {
      toast.error(e instanceof Error ? e.message : String(e));
    } finally {
      setCancelingId(null);
      lists.refresh();
    }
  };

  return (
    <div>
      {/* Real positions */}
      <div className="font-semibold text-ds-text-1 mb-2">当前持仓 · 来自已连接账户的只读同步</div>
      {Object.values(live).some((a) => a.degraded) && (
        <div className="text-xs text-ds-text-3 mb-2">数据降级：REST 兜底</div>
      )}
      {positions.length === 0 ? (
        <EmptyState>暂无真实持仓 — 连接账户后，交易所同步到的当前持仓会显示在这里。</EmptyState>
      ) : (
        <div className="mb-6">
          <DataTable
            headers={["账户", "交易所", "交易对", "方向", "数量", "开仓价", "标记价", "未实现盈亏", "当前保护", "操作"]}
            minWidth={1020}
            rows={positions.map((p) => {
              const prot = protectionFor(p.accountId, p.symbol, openOrders, p);
              const act = canAct(p);
              const key = positionKey(p);
              const expanded = expandedKey === key;
              const protectionText =
                prot.tp != null || prot.sl != null
                  ? [
                      prot.tp != null ? `止盈 ${prot.tp}` : null,
                      prot.sl != null ? `止损 ${prot.sl}` : null,
                    ]
                      .filter(Boolean)
                      .join(" · ")
                  : "无";
              return [
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
                <span key="prot" className="num text-ds-text-2">{protectionText}</span>,
                <span key="ops" className="flex gap-1 justify-center">
                  <Button variant="ghost" size="sm" disabled={!act} onClick={() => void askClose(p)}>
                    平仓
                  </Button>
                  <Button variant="ghost" size="sm" disabled={!act} onClick={() => void openProtect(p)}>
                    保护
                  </Button>
                  <Button
                    variant="ghost"
                    size="sm"
                    disabled={!act}
                    onClick={() => setExpandedKey(expanded ? null : key)}
                  >
                    {expanded ? "收起" : "展开"}
                  </Button>
                </span>,
              ];
            })}
          />
          <div className="mt-2 text-[12px] text-ds-text-2">
            平仓按盘口反向 2 bps 的 IOC 限价单发出（仅减仓）；保护挂止盈 / 止损触发单（市价触发、仅减仓）。
            盘口过期、持仓为 0 或交易所不可交易时，三个按钮不可用。
          </div>
          {expandedKey != null &&
            (() => {
              const p = positions.find((x) => positionKey(x) === expandedKey);
              if (!p) return null;
              const prot = protectionFor(p.accountId, p.symbol, openOrders, p);
              return (
                <div className="mt-3">
                  <div className="font-semibold text-ds-text-1 mb-2">
                    保护单 · {labelOf(p.accountId)} {displaySymbol(p.symbol)}
                    （{p.side === "long" ? "多" : "空"} {Math.abs(p.qty)}）
                  </div>
                  {prot.orders.length === 0 ? (
                    <EmptyState>当前无保护单 — 点「保护」挂止盈 / 止损触发单。</EmptyState>
                  ) : (
                    <DataTable
                      headers={["订单号", "类型", "触发价", "数量", "状态", "操作"]}
                      minWidth={640}
                      rows={prot.orders.map((o) => [
                        <span key="o" className="num">{o.orderId || o.clientOrderId}</span>,
                        <span key="t">
                          <Badge tone={tpslLabel(o, p) === "tp" ? "up" : "down"}>
                            {tpslLabel(o, p) === "tp" ? "止盈" : "止损"}
                          </Badge>
                        </span>,
                        <span key="px" className="num">{o.triggerPx ?? "—"}</span>,
                        <span key="q" className="num">{o.quantity}</span>,
                        <span key="st">
                          <Badge tone={statusTone(o.status)}>
                            {STATUS_LABEL[o.status] ?? o.status}
                          </Badge>
                        </span>,
                        <span key="c">
                          <Button
                            variant="ghost"
                            size="sm"
                            disabled={cancelingId === (o.orderId || o.clientOrderId)}
                            onClick={() => void cancel(o)}
                          >
                            {cancelingId === (o.orderId || o.clientOrderId) ? "撤单中…" : "撤单"}
                          </Button>
                        </span>,
                      ])}
                    />
                  )}
                </div>
              );
            })()}
          {actionResult && (
            <div className="mt-3 text-[13px] text-ds-text-1">{actionResult}</div>
          )}
          {actionError && (
            <div className="mt-3 text-[13px] text-ds-down">{actionError}</div>
          )}
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
            headers={["账户", "交易对", "方向", "数量", "价格", "状态", "来源", "操作"]}
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
              <span key="o">{originLabel(o.origin)}</span>,
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
            headers={["时间", "交易对", "方向", "数量", "价格", "已成交", "状态", "来源", "说明"]}
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
              <span key="o">{originLabel(o.origin)}</span>,
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

      {/* Close confirmation: symbol / direction / quantity / price. */}
      <ConfirmDialog
        open={closeTarget != null}
        title="确认平仓"
        confirmText={closing ? "下单中…" : "确认平仓"}
        message={
          closeTarget && (
            <div className="space-y-1">
              <div>
                {displaySymbol(closeTarget.symbol)} ·{" "}
                {closeTarget.side === "long" ? "平多（卖出）" : "平空（买入）"}
              </div>
              <div className="num">数量 {closeTarget.qty}</div>
              <div className="num">价格 {closeTarget.price}（IOC 限价，仅减仓）</div>
              <div className="text-ds-warn">
                平仓后该仓位挂着的保护单会随之失效，请在挂单表里清除。
              </div>
            </div>
          )
        }
        onConfirm={() => {
          if (!closing) void confirmClose();
        }}
        onCancel={() => {
          if (!closing) setCloseTarget(null);
        }}
      />

      {/* Protect: TP / SL trigger prices. Either may be left empty;
          both empty clears all protection for the position. */}
      <Modal
        open={protectTarget != null}
        title={
          protectTarget
            ? `止盈止损 · ${displaySymbol(protectTarget.symbol)}（${protectTarget.side === "long" ? "多" : "空"} ${Math.abs(protectTarget.qty)}）`
            : "止盈止损"
        }
        onClose={() => {
          if (!protectBusy) setProtectTarget(null);
        }}
        widthClass="w-[420px]"
        footer={
          <>
            <Button
              variant="secondary"
              disabled={protectBusy}
              onClick={() => setProtectTarget(null)}
            >
              取消
            </Button>
            <Button
              variant="primary"
              disabled={protectBusy}
              onClick={() => void saveProtect()}
            >
              {protectBusy ? "保存中…" : "保存"}
            </Button>
          </>
        }
      >
        {protectTarget && (
          <div className="space-y-3 pb-2">
            <div className="text-ds-text-2 text-[13px]">
              现价 {protectTarget.markPrice}。
              {protectTarget.side === "long"
                ? "多仓：止盈价高于现价，止损价低于现价。"
                : "空仓：止盈价低于现价，止损价高于现价。"}
              只填一个也可以；两个都留空 = 清除全部保护。保存时先撤掉该仓位全部手动触发单，再按输入重建。
            </div>
            <label className="block">
              <span className="text-ds-text-2 text-[13px]">止盈价</span>
              <input
                className="mt-1 w-full rounded border border-ds-line bg-ds-bg px-2 py-1.5 text-ds-text-1 num outline-none focus:border-ds-accent focus:ring-2 focus:ring-ds-focus-ring/50 disabled:bg-ds-bg-disabled disabled:text-ds-text-disabled"
                type="number"
                inputMode="decimal"
                placeholder="留空 = 不挂止盈"
                value={tpInput}
                disabled={protectBusy}
                onChange={(e) => setTpInput(e.target.value)}
              />
            </label>
            <label className="block">
              <span className="text-ds-text-2 text-[13px]">止损价</span>
              <input
                className="mt-1 w-full rounded border border-ds-line bg-ds-bg px-2 py-1.5 text-ds-text-1 num outline-none focus:border-ds-accent focus:ring-2 focus:ring-ds-focus-ring/50 disabled:bg-ds-bg-disabled disabled:text-ds-text-disabled"
                type="number"
                inputMode="decimal"
                placeholder="留空 = 不挂止损"
                value={slInput}
                disabled={protectBusy}
                onChange={(e) => setSlInput(e.target.value)}
              />
            </label>
            {protectError && (
              <div className="text-[13px] text-ds-down">{protectError}</div>
            )}
          </div>
        )}
      </Modal>
    </div>
  );
}
