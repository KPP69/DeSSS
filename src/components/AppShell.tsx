import React, { useState } from "react";
import Sidebar from "./Sidebar";
import ConsoleDrawer from "./ConsoleDrawer";
import { useConsoleStore, errorCount } from "../store/useConsoleStore";

/**
 * The one fixed three-zone shell, shared by all 6 pages:
 * - title bar: fixed h-12, page title left, Console button right
 * - secondary toolbar: fixed h-11, page actions (strictly centered)
 * - main: scrollable content area
 * Console drawer slides up from the bottom. Pages only provide their
 * title, toolbar actions, and content — never their own chrome.
 */
export default function AppShell({
  title,
  toolbar,
  children,
}: {
  title: string;
  toolbar?: React.ReactNode;
  children: React.ReactNode;
}) {
  const logs = useConsoleStore((s) => s.logs);
  const errors = errorCount(logs);
  const [consoleOpen, setConsoleOpen] = useState(false);

  return (
    <div className="flex h-screen bg-app overflow-hidden">
      <Sidebar />
      <div className="flex-1 min-w-0 flex flex-col">
        <header className="shrink-0 h-12 flex items-center justify-between px-6 border-b border-line bg-panel">
          <h1 className="text-[15px] font-semibold text-t1">{title}</h1>
          <button
            onClick={() => setConsoleOpen((o) => !o)}
            className="relative px-3 py-1.5 rounded text-[13px] text-t1 bg-overlay hover:bg-line transition-colors"
          >
            控制台
            {errors > 0 && (
              <span className="absolute -top-1.5 -right-1.5 min-w-5 h-5 px-1 rounded-full bg-loss text-white text-xs num flex items-center justify-center">
                {errors}
              </span>
            )}
          </button>
        </header>
        <div className="shrink-0 h-11 flex items-center px-6 border-b border-line bg-app">
          {toolbar}
        </div>
        <main className="flex-1 min-w-0 overflow-y-auto px-6 py-4">{children}</main>
      </div>
      <ConsoleDrawer open={consoleOpen} onClose={() => setConsoleOpen(false)} />
    </div>
  );
}
