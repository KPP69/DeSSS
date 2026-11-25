import React, { useState } from "react";
import Sidebar from "./Sidebar";
import ConsoleDrawer from "./ConsoleDrawer";
import ThemeSwitcher from "./ThemeSwitcher";
import { useConsoleStore, errorCount } from "../store/useConsoleStore";

/**
 * The one fixed three-zone shell, shared by all 6 pages:
 * - title bar: fixed h-14 (56px), page title left (18px semibold),
 *   ThemeSwitcher + Console button right
 * - secondary toolbar: fixed h-12 (48px), page actions strictly centered
 * - main: scrollable content area, 20px padding
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
    <div className="flex h-screen bg-ds-bg overflow-hidden">
      <Sidebar />
      <div className="flex-1 min-w-0 flex flex-col">
        <header className="shrink-0 h-14 flex items-center justify-between px-5 border-b border-ds-line bg-ds-panel">
          <h1 className="text-lg font-semibold text-ds-text-1">{title}</h1>
          <div className="flex items-center gap-2">
            <ThemeSwitcher />
            <button
              onClick={() => setConsoleOpen((o) => !o)}
              className="relative h-8 px-3 rounded-lg text-[13px] text-ds-text-1 bg-ds-panel-2 hover:bg-ds-bg-2 transition-colors"
            >
              控制台
              {errors > 0 && (
                <span className="absolute -top-1.5 -right-1.5 min-w-5 h-5 px-1 rounded-full bg-ds-down text-ds-down-contrast text-[11px] num flex items-center justify-center">
                  {errors}
                </span>
              )}
            </button>
          </div>
        </header>
        <div className="shrink-0 h-12 flex items-center px-5 border-b border-ds-line bg-ds-bg">
          {toolbar}
        </div>
        <main className="flex-1 min-w-0 overflow-y-auto px-5 py-5">{children}</main>
      </div>
      <ConsoleDrawer open={consoleOpen} onClose={() => setConsoleOpen(false)} />
    </div>
  );
}
