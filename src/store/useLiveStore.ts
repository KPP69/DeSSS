import { create } from "zustand";
import type { AccountLive, OrderBook } from "../lib/types";

/**
 * Live read-only exchange state (phase 3). Ephemeral only: nothing here is
 * persisted — reconnecting re-fetches everything from the backend.
 *
 * - `live`: latest AccountLive per account id (real equity, latency, status).
 * - `books`: latest OrderBook per `${adapterId}:${symbol}`.
 *
 * Book updates are throttled at the view layer: the backend still receives
 * every WS frame, but rendering merges them at most once per 150ms so a
 * fast Lighter feed does not thrash the DOM.
 */
const BOOK_FLUSH_MS = 150;

type S = {
  live: Record<string, AccountLive>;
  books: Record<string, OrderBook>;
  setLive: (a: AccountLive) => void;
  removeLive: (accountId: string) => void;
  setBook: (b: OrderBook) => void;
};

export const useLiveStore = create<S>((set, get) => {
  let pending = new Map<string, OrderBook>();
  let timer: ReturnType<typeof setTimeout> | null = null;
  const flush = () => {
    timer = null;
    if (pending.size === 0) return;
    const next = pending;
    pending = new Map();
    set((s) => ({ books: { ...s.books, ...Object.fromEntries(next) } }));
  };
  return {
    live: {},
    books: {},
    setLive: (a) => set((s) => ({ live: { ...s.live, [a.accountId]: a } })),
    removeLive: (accountId) =>
      set((s) => {
        const live = { ...s.live };
        delete live[accountId];
        return { live };
      }),
    setBook: (b) => {
      pending.set(`${b.adapterId}:${b.symbol}`, b);
      if (timer === null) timer = setTimeout(flush, BOOK_FLUSH_MS);
    },
  };
});

/** Latency light: green < 200ms, yellow < 1000ms, red otherwise/offline.
 * Status comes from REST health only (no WS on an account anymore). */
export function latencyTone(latencyMs: number | null, status: string): "online" | "warn" | "danger" | "offline" {
  if (status !== "connected") return status === "connecting" ? "warn" : "offline";
  if (latencyMs === null) return "warn";
  if (latencyMs < 200) return "online";
  if (latencyMs < 1000) return "warn";
  return "danger";
}
