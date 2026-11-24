import { create } from "zustand";
import type { LogEntry } from "../lib/types";

/** Runtime log console: newest first, capped at 300 entries. */
const MAX_LOGS = 300;

type Filter = "all" | "error";

type S = {
  logs: LogEntry[];
  filter: Filter;
  addLog: (l: LogEntry) => void;
  setFilter: (f: Filter) => void;
  clear: () => void;
};

export const useConsoleStore = create<S>((set) => ({
  logs: [],
  filter: "all",
  addLog: (l) =>
    set((s) => ({ logs: [l, ...s.logs].slice(0, MAX_LOGS) })),
  setFilter: (filter) => set({ filter }),
  clear: () => set({ logs: [] }),
}));

/** Count of error-level log lines (the console button's badge). */
export function errorCount(logs: LogEntry[]): number {
  return logs.reduce((n, l) => (l.level === "error" ? n + 1 : n), 0);
}
