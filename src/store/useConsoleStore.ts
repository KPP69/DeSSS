import { create } from "zustand";
import type { LogEntry } from "../lib/types";

/** Runtime log console: newest first, capped at 300 entries. */
const MAX_LOGS = 300;

type Filter = "all" | "error";

type S = {
  logs: LogEntry[];
  filter: Filter;
  /** Error logs that arrived while the drawer was closed. */
  unreadErrorCount: number;
  /** Whether the drawer is open; opening it marks errors read. */
  consoleOpen: boolean;
  addLog: (l: LogEntry) => void;
  /** Merge snapshot logs (newest first), deduping lines already present. */
  addLogs: (ls: LogEntry[]) => void;
  setFilter: (f: Filter) => void;
  setConsoleOpen: (open: boolean) => void;
  clear: () => void;
};

export const useConsoleStore = create<S>((set, get) => ({
  logs: [],
  filter: "all",
  unreadErrorCount: 0,
  consoleOpen: false,
  addLog: (l) =>
    set((s) => ({
      logs: [l, ...s.logs].slice(0, MAX_LOGS),
      unreadErrorCount:
        l.level === "error" && !s.consoleOpen
          ? s.unreadErrorCount + 1
          : s.unreadErrorCount,
    })),
  addLogs: (ls) =>
    set((s) => {
      const seen = new Set(s.logs.map((l) => `${l.ts}|${l.source}|${l.message}`));
      const fresh = ls.filter((l) => !seen.has(`${l.ts}|${l.source}|${l.message}`));
      return { logs: [...fresh, ...s.logs].slice(0, MAX_LOGS) };
    }),
  setFilter: (filter) => set({ filter }),
  setConsoleOpen: (open) =>
    set({ consoleOpen: open, unreadErrorCount: open ? 0 : get().unreadErrorCount }),
  clear: () => set({ logs: [], unreadErrorCount: 0 }),
}));

/** Count of error-level log lines (the drawer's error tab total). */
export function errorCount(logs: LogEntry[]): number {
  return logs.reduce((n, l) => (l.level === "error" ? n + 1 : n), 0);
}
