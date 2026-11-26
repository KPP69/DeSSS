import React, { useEffect } from "react";
import AppShell from "./components/AppShell";
import { Button } from "./components/ui";
import { useLiveStream } from "./hooks/useLiveStream";
import { useVenueSchemas } from "./hooks/useVenueSchemas";
import { useAppStore, type PageKey } from "./store/useAppStore";
import { useAccountUiStore } from "./store/useAccountUiStore";
import { useDataUiStore } from "./store/useDataUiStore";
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

/** Shell + wiring. The current page is synced to the URL hash so a
 * refresh stays on the same page, and the runtime snapshot restores
 * books, account states and recent logs instantly (0 delay). */
export default function App() {
  const page = useAppStore((s) => s.page);
  const setPage = useAppStore((s) => s.setPage);
  const openAdd = useAccountUiStore((s) => s.openAdd);
  const dataView = useDataUiStore((s) => s.view);
  const setDataView = useDataUiStore((s) => s.setView);

  // The app's only event subscriptions (Phase 2, I11) + the one-time
  // venue-schema fetch, both at the root; pages only read stores.
  useLiveStream();
  useVenueSchemas();

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

  // Secondary toolbar (AppShell slot): the account page gets its add
  // button here, the data page its view toggle (left 自定义信号, right
  // 标的查询) — never inside the page content area.
  const toolbar =
    page === "account" ? (
      <Button onClick={openAdd}>添加账户</Button>
    ) : page === "data" ? (
      <div className="flex rounded-lg border border-ds-line bg-ds-panel p-0.5">
        <button
          onClick={() => setDataView("signals")}
          className={`px-3 py-1 rounded-md text-sm ${
            dataView === "signals"
              ? "bg-ds-accent text-ds-accent-contrast"
              : "text-ds-text-2 hover:text-ds-text-1"
          }`}
        >
          自定义信号
        </button>
        <button
          onClick={() => setDataView("search")}
          className={`px-3 py-1 rounded-md text-sm ${
            dataView === "search"
              ? "bg-ds-accent text-ds-accent-contrast"
              : "text-ds-text-2 hover:text-ds-text-1"
          }`}
        >
          标的查询
        </button>
      </div>
    ) : undefined;

  return (
    <AppShell title={PAGE_TITLES[page]} toolbar={toolbar}>
      {page === "account" && <AccountPage />}
      {page === "strategy" && <StrategyPage />}
      {page === "trading" && <TradingPage />}
      {page === "data" && <DataPage />}
      {page === "risk" && <RiskPage />}
      {page === "system" && <SystemPage />}
    </AppShell>
  );
}
