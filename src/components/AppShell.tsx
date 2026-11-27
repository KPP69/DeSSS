import React from "react";
import Sidebar from "./Sidebar";
import ConsoleDrawer from "./ConsoleDrawer";
import Toast from "./Toast";
import { useAppStore } from "../store/useAppStore";
import { useConsoleStore } from "../store/useConsoleStore";
import { useShellActions } from "../store/useShellActions";

/**
 * The one fixed three-zone shell, shared by all 6 pages:
 * - title bar: fixed h-14 (56px), page title left, Console button
 *   right (unread-error badge kept)
 * - toolbar: fixed h-12 (48px), left primary actions, right key
 *   status (text-xs text-ds-text-2, ml-auto) — content comes from
 *   the current page via useShellActions, never from App
 * - main: independently scrolling content area, px-5 py-4
 * Console drawer slides up from the bottom; toasts sit top-right.
 * Pages only provide their title, toolbar content, and content —
 * never their own chrome.
 */
export default function AppShell({
  title,
  children,
}: {
  title: string;
  children: React.ReactNode;
}) {
  const page = useAppStore((s) => s.page);
  const shell = useShellActions((s) => s.byPage[page]);
  const unreadErrors = useConsoleStore((s) => s.unreadErrorCount);
  const consoleOpen = useConsoleStore((s) => s.consoleOpen);
  const setConsoleOpen = useConsoleStore((s) => s.setConsoleOpen);

  return (
    <div className="flex h-screen bg-ds-bg overflow-hidden">
      <Sidebar />
      <div className="flex-1 min-w-0 flex flex-col">
        <header className="shrink-0 h-14 flex items-center justify-between px-5 border-b border-ds-line bg-ds-panel">
          <h1 className="text-lg font-semibold text-ds-text-1">{title}</h1>
          <div className="flex items-center gap-2">
            <button
              onClick={() => setConsoleOpen(!consoleOpen)}
              className="relative h-8 px-3 rounded-lg text-[13px] text-ds-text-1 bg-ds-panel-2 hover:bg-ds-bg-2 transition-colors"
            >
              控制台
              {unreadErrors > 0 && (
                <span className="absolute -top-1.5 -right-1.5 min-w-5 h-5 px-1 rounded-full bg-ds-down text-ds-down-contrast text-[11px] num flex items-center justify-center">
                  {unreadErrors}
                </span>
              )}
            </button>
          </div>
        </header>
        <div className="shrink-0 h-12 flex items-center px-5 border-b border-ds-line bg-ds-bg">
          {shell?.actions != null && <div className="flex gap-2">{shell.actions}</div>}
          {shell?.status != null && (
            <div className="ml-auto text-xs text-ds-text-2">{shell.status}</div>
          )}
        </div>
        <main className="flex-1 min-w-0 overflow-y-auto px-5 py-4">{children}</main>
      </div>
      <ConsoleDrawer open={consoleOpen} onClose={() => setConsoleOpen(false)} />
      <Toast />
    </div>
  );
}
