import { create } from "zustand";
import type { AccountLive, OrderBook } from "../lib/types";

/**
 * Live read-only exchange state (phase 3). Ephemeral only: nothing here is
 * persisted — reconnecting re-fetches everything from the backend.
 *
 * - `live`: latest AccountLive per account id (real equity, latency, status).
 * - `books`: latest OrderBook per `${adapterId}:${symbol}`.
 */
type S = {
  live: Record<string, AccountLive>;
  books: Record<string, OrderBook>;
  setLive: (a: AccountLive) => void;
  removeLive: (accountId: string) => void;
  setBook: (b: OrderBook) => void;
};

export const useLiveStore = create<S>((set) => ({
  live: {},
  books: {},
  setLive: (a) => set((s) => ({ live: { ...s.live, [a.accountId]: a } })),
  removeLive: (accountId) =>
    set((s) => {
      const live = { ...s.live };
      delete live[accountId];
      return { live };
    }),
  setBook: (b) =>
    set((s) => ({ books: { ...s.books, [`${b.adapterId}:${b.symbol}`]: b } })),
}));

/** Latency light: green < 200ms, yellow < 1000ms, red otherwise/offline.
 * "rest-only" (WS down, REST ok) is yellow: the numbers are real, the
 * live book is not. */
export function latencyTone(latencyMs: number | null, status: string): "online" | "warn" | "danger" | "offline" {
  if (status === "rest-only") return "warn";
  if (status !== "connected") return status === "connecting" || status === "reconnecting" ? "warn" : "offline";
  if (latencyMs === null) return "warn";
  if (latencyMs < 200) return "online";
  if (latencyMs < 1000) return "warn";
  return "danger";
}
