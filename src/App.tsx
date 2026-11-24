import React, { useEffect, useState } from "react";
import Sidebar from "./components/Sidebar";
import ConsoleDrawer from "./components/ConsoleDrawer";
import { useAppStore, type PageKey } from "./store/useAppStore";
import { useMarketStore } from "./store/useMarketStore";
import { api, onTick, onBalance, onBook, onAccountLive, onLog } from "./lib/tauri";
import { useLiveStore } from "./store/useLiveStore";
import { useConsoleStore, errorCount } from "./store/useConsoleStore";
import AccountPage from "./pages/AccountPage";
import StrategyPage from "./pages/StrategyPage";
import TradingPage from "./pages/TradingPage";
import DataPage from "./pages/DataPage";
import RiskPage from "./pages/RiskPage";
import SystemPage from "./pages/SystemPage";

const PAGE_TITLES: Record<PageKey, string> = {
  account: "账户",
  strategy: "策略",
  trading: "交易",
  data: "数据",
  risk: "风控",
  system: "系统",
};

const PAGE_KEYS: PageKey[] = ["account", "strategy", "trading", "data", "risk", "system"];

/** Fixed three-zone layout: title bar (top) / scrollable page (middle) /
 * console drawer (bottom, slides up). The current page is synced to the
 * URL hash so a refresh stays on the same page. */
export default function App() {
  const page = useAppStore((s) => s.page);
  const setPage = useAppStore((s) => s.setPage);
  const setTick = useMarketStore((s) => s.setTick);
  const setBalance = useMarketStore((s) => s.setBalance);
  const setConnected = useMarketStore((s) => s.setConnected);
  const consoleLogs = useConsoleStore((s) => s.logs);
  const [consoleOpen, setConsoleOpen] = useState(false);

  // Current page <-> URL hash (#/account, #/trading, ...).
  useEffect(() => {
    const fromHash = () => {
      const h = window.location.hash.replace(/^#\/?/, "");
      if ((PAGE_KEYS as string[]).includes(h)) setPage(h as PageKey);
    };
    fromHash();
    window.addEventListener("hashchange", fromHash);
    return () => window.removeEventListener("hashchange", fromHash);
  }, [setPage]);
  useEffect(() => {
    window.history.replaceState(null, "", `#/${page}`);
  }, [page]);

  useEffect(() => {
    let u1: (() => void) | undefined, u2: (() => void) | undefined;
    let u3: (() => void) | undefined, u4: (() => void) | undefined;
    let u5: (() => void) | undefined;
    onTick(setTick).then((u) => (u1 = u));
    onBalance(setBalance).then((u) => (u2 = u));
    onBook((b) => useLiveStore.getState().setBook(b)).then((u) => (u3 = u));
    onAccountLive((a) => useLiveStore.getState().setLive(a)).then((u) => (u4 = u));
    onLog((l) => useConsoleStore.getState().addLog(l)).then((u) => (u5 = u));
    api.startMock().then(() => setConnected(true)).catch(() => {});
    // Instant numbers after a refresh: latest books + account states the
    // backend already holds in memory, instead of waiting for the next
    // WS frame / REST poll.
    api
      .getRuntimeSnapshot()
      .then((snap) => {
        snap.live.forEach((a) => useLiveStore.getState().setLive(a));
        snap.books.forEach((b) => useLiveStore.getState().setBook(b));
      })
      .catch(() => {});
    return () => {
      u1?.();
      u2?.();
      u3?.();
      u4?.();
      u5?.();
    };
  }, []);

  const errors = errorCount(consoleLogs);

  return (
    <div className="flex h-screen bg-app overflow-hidden">
      <Sidebar />
      <div className="flex-1 min-w-0 flex flex-col">
        <header className="shrink-0 h-12 flex items-center justify-between px-4 border-b border-line bg-panel">
          <h1 className="text-base font-semibold text-t1">{PAGE_TITLES[page]}</h1>
          <button
            onClick={() => setConsoleOpen((o) => !o)}
            className="relative px-3 py-1.5 rounded text-sm text-t1 bg-overlay hover:bg-line transition-colors"
          >
            控制台
            {errors > 0 && (
              <span className="absolute -top-1.5 -right-1.5 min-w-5 h-5 px-1 rounded-full bg-loss text-white text-[11px] num flex items-center justify-center">
                {errors}
              </span>
            )}
          </button>
        </header>
        <main className="flex-1 min-w-0 overflow-y-auto p-6">
          {page === "account" && <AccountPage />}
          {page === "strategy" && <StrategyPage />}
          {page === "trading" && <TradingPage />}
          {page === "data" && <DataPage />}
          {page === "risk" && <RiskPage />}
          {page === "system" && <SystemPage />}
        </main>
      </div>
      <ConsoleDrawer open={consoleOpen} onClose={() => setConsoleOpen(false)} />
    </div>
  );
}
