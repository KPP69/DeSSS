import { useCallback, useEffect, useRef, useState } from "react";
import { useInvoke } from "./useInvoke";
import { useVenueSchemas } from "./useVenueSchemas";
import { api } from "../lib/tauri";
import type { OrderBook, OrderInfo, StrategyView } from "../lib/types";
import { useAccountStore } from "../store/useAccountStore";
import { useLiveStore } from "../store/useLiveStore";
import { toast } from "../store/useToastStore";
import { useVenueStore } from "../store/useVenueStore";

/**
 * Trading data (F4): everything the trading page's sections share —
 * live positions, the three lists (open orders / order records /
 * fills), the books the close price needs, the cancel mutation, and
 * the small pure helpers the sections render from. The hook lives at
 * page level, so switching segments never refetches and never loses
 * data; the lists refresh on a 10s interval regardless of segment.
 *
 * There is deliberately no manual order form here (phase 5A
 * follow-up, 2026-11-26): the `place_order` pipeline stays in the
 * backend for the Strategy module; this page only watches exchange
 * state, cancels, closes positions and hangs TP/SL protection.
 * PR-3.1.1's one exception: position protection (IOC reduceOnly
 * close, or reduceOnly positionTpsl trigger orders). Nothing here
 * opens a position.
 */

export type PositionRow = {
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

/** The close confirmation's frozen snapshot (price included). */
export type CloseTarget = {
  accountId: string;
  adapterId: string;
  symbol: string;
  side: string;
  qty: number;
  price: number;
};

export const STATUS_LABEL: Record<string, string> = {
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
export function originLabel(origin: string): string {
  if (origin === "manual") return "手动";
  if (origin.startsWith("strategy:")) return `策略 ${origin.slice(9, 17)}`;
  return origin;
}

export function statusTone(status: string): "up" | "down" | "warn" | "info" | "neutral" {
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
export function isTriggerOrder(o: OrderInfo): boolean {
  return o.triggerPx != null;
}

/** TP/SL label for one trigger order. The exchange reports it when
 * it can ("Take Profit Market" / "Stop Loss Market"); otherwise
 * derive it from the position — for a long, a sell trigger above
 * the mark is a take-profit and below it a stop-loss (short: the
 * reverse). A trigger exactly at the mark is degenerate and reads
 * as a stop-loss. */
export function tpslLabel(
  o: OrderInfo,
  position: { side: string; markPrice: number },
): "tp" | "sl" | null {
  if (o.tpsl === "tp" || o.tpsl === "sl") return o.tpsl;
  if (o.triggerPx == null) return null;
  const above = o.triggerPx > position.markPrice;
  if (position.side === "long") return above ? "tp" : "sl";
  return above ? "sl" : "tp";
}

/** The position's current protection, from the live open orders. */
export function protectionFor(
  accountId: string,
  symbol: string,
  open: OrderInfo[],
  position: PositionRow,
): { tp: number | null; sl: number | null; tpCount: number; slCount: number; orders: OrderInfo[] } {
  const orders = open.filter(
    (o) => o.accountId === accountId && o.symbol === symbol && isTriggerOrder(o),
  );
  let tp: number | null = null;
  let sl: number | null = null;
  let tpCount = 0;
  let slCount = 0;
  for (const o of orders) {
    const label = tpslLabel(o, position);
    if (label === "tp") {
      tp = o.triggerPx;
      tpCount++;
    } else if (label === "sl") {
      sl = o.triggerPx;
      slCount++;
    }
  }
  return { tp, sl, tpCount, slCount, orders };
}

/** Close price: sell a long just under the best bid, buy a short
 * just over the best ask (2 bps of give, so the IOC fills). */
export function closePrice(position: PositionRow, book: OrderBook | undefined): number | null {
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
export function bookFresh(book: OrderBook | undefined): boolean {
  return book != null && Date.now() - book.ts <= 5000;
}

/** Validate the protect inputs against the position's mark price,
 * per field. Both empty is legal: it clears all protection (cancel
 * only). Returns an error per field, or an empty object when the
 * inputs are fine. */
export function validateProtect(
  side: string,
  mark: number,
  tp: string,
  sl: string,
): { tp?: string; sl?: string } {
  const out: { tp?: string; sl?: string } = {};
  const check = (raw: string, field: "tp" | "sl"): number | null => {
    if (raw.trim() === "") return null;
    const n = Number(raw);
    if (!(n > 0)) {
      out[field] = `${field === "tp" ? "止盈" : "止损"}价必须是大于 0 的数字`;
      return null;
    }
    return n;
  };
  const tpNum = check(tp, "tp");
  const slNum = check(sl, "sl");
  if (tpNum != null && out.tp == null) {
    if (side === "long" && tpNum <= mark) out.tp = `多仓止盈价必须高于现价 ${mark}`;
    if (side !== "long" && tpNum >= mark) out.tp = `空仓止盈价必须低于现价 ${mark}`;
  }
  if (slNum != null && out.sl == null) {
    if (side === "long" && slNum >= mark) out.sl = `多仓止损价必须低于现价 ${mark}`;
    if (side !== "long" && slNum <= mark) out.sl = `空仓止损价必须高于现价 ${mark}`;
  }
  return out;
}

export function useTradingData() {
  const live = useLiveStore((s) => s.live);
  const accounts = useAccountStore((s) => s.accounts);
  const positions: PositionRow[] = Object.values(live).flatMap((a) =>
    a.positions.map((p) => ({ ...p, accountId: a.accountId, adapterId: a.adapterId, currency: a.currency })),
  );
  const labelOf = (accountId: string) =>
    accounts.find((a) => a.id === accountId)?.label ?? accountId;

  // Open orders come from every connected Hyperliquid / Entropy
  // account; the backend syncs each list with the exchange.
  const tradeAccounts = accounts.filter(
    (a) => a.exchangeId === "hyperliquid" || a.exchangeId === "entropy",
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
  // Full-page loading only before the first load; a failed refresh
  // keeps the old data on screen with the error set (useInvoke).
  const listsLoading = lists.loading && lists.data == null;
  const listsError = lists.error;

  // Manual refresh (F5.1a): the refresh button's loading reflects
  // ONLY a manual click — the 10s interval never sets it, and the
  // initial page load still goes through PageState (listsLoading),
  // not the button. A click sets the flag; the flag clears when the
  // fetch it started has actually run (loading true) and finished
  // (loading false). The seenLoading guard stops the flag clearing
  // in the same render as the click, before the fetch has started.
  const [manualRefreshing, setManualRefreshing] = useState(false);
  const manualSeenLoadingRef = useRef(false);
  const refreshManual = useCallback(() => {
    manualSeenLoadingRef.current = false;
    setManualRefreshing(true);
    lists.refresh();
  }, [lists.refresh]);
  useEffect(() => {
    if (!manualRefreshing) return;
    if (lists.loading) {
      manualSeenLoadingRef.current = true;
      return;
    }
    if (manualSeenLoadingRef.current) {
      manualSeenLoadingRef.current = false;
      setManualRefreshing(false);
    }
  }, [manualRefreshing, lists.loading]);

  // Strategy instances (F4.1): a position shows the strategy whose leg
  // it is (matched by account + native symbol), or 手动 when none.
  const strategies = useInvoke(() => api.listStrategies(), []);
  const strategyFor = (accountId: string, symbol: string): StrategyView | null =>
    (strategies.data ?? []).find(
      (v) =>
        (v.legA.accountId === accountId && v.legA.symbol === symbol) ||
        (v.legB.accountId === accountId && v.legB.symbol === symbol),
    ) ?? null;

  useEffect(() => {
    const t = setInterval(() => {
      lists.refresh();
      strategies.refresh();
    }, 10_000);
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

  const [cancelingId, setCancelingId] = useState<string | null>(null);
  const [cancelResult, setCancelResult] = useState<OrderInfo | null>(null);

  const cancel = async (order: OrderInfo) => {
    setCancelingId(order.orderId || order.clientOrderId);
    setCancelResult(null);
    try {
      const info = await api.cancelOrder(
        order.accountId,
        order.orderId || order.clientOrderId,
        order.symbol,
      );
      setCancelResult(info);
      // A failed cancel shows inline (cancelResult.error, unchanged)
      // AND toasts the backend text verbatim, once per cancel call —
      // the toast lives here, not in render, so a re-render or the
      // 10s refresh can never re-toast the same failure.
      if (info.error) toast.error(info.error);
    } catch (e) {
      toast.error(e instanceof Error ? e.message : String(e));
    } finally {
      setCancelingId(null);
      lists.refresh();
    }
  };

  /** Build the close target from a position, or null (with a toast)
   * when the book isn't fresh enough to price the close. */
  const askClose = (p: PositionRow): CloseTarget | null => {
    const price = closePrice(p, books[`${p.adapterId}:${p.symbol}`]);
    if (price == null) {
      toast.error("盘口未就绪，无法平仓");
      return null;
    }
    return {
      accountId: p.accountId,
      adapterId: p.adapterId,
      symbol: p.symbol,
      side: p.side,
      qty: Math.abs(p.qty),
      price,
    };
  };

  return {
    positions,
    openOrders,
    records,
    fills,
    books,
    tradable,
    degraded: Object.values(live).some((a) => a.degraded),
    labelOf,
    venueName,
    strategyFor,
    tradeAccounts,
    listsLoading,
    listsError,
    manualRefreshing,
    refreshManual,
    refresh: lists.refresh,
    cancel,
    cancelingId,
    cancelResult,
    askClose,
  };
}
