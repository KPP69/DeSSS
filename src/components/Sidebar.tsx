import React from "react";
import ThemeSwitcher from "./ThemeSwitcher";
import { useAppStore, type PageKey } from "../store/useAppStore";

const items: { key: PageKey; label: string }[] = [
  { key: "account", label: "账户" },
  { key: "strategy", label: "策略" },
  { key: "trading", label: "交易" },
  { key: "data", label: "数据" },
  { key: "risk", label: "风控" },
  { key: "system", label: "系统" },
];

export default function Sidebar() {
  const { page, setPage } = useAppStore();
  return (
    <aside className="w-56 shrink-0 h-full bg-ds-panel border-r border-ds-line flex flex-col">
      <div className="shrink-0 h-14 flex items-center px-4 text-lg font-semibold text-ds-text-1">
        DeSSS
      </div>
      <nav className="flex flex-col gap-1.5 px-3">
        {items.map((i) => {
          const active = page === i.key;
          return (
            <button
              key={i.key}
              onClick={() => setPage(i.key)}
              className={
                "relative h-10 flex items-center px-3 text-sm rounded-lg transition-colors focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ds-focus-ring/50 " +
                (active
                  ? "bg-ds-accent-soft/10 text-ds-text-1 font-medium"
                  : "text-ds-text-2 hover:bg-ds-bg-2 hover:text-ds-text-1")
              }
            >
              {active && (
                <span className="absolute left-0 top-1.5 bottom-1.5 w-0.5 rounded-full bg-ds-accent" />
              )}
              {i.label}
            </button>
          );
        })}
      </nav>
      <div className="mt-auto px-3 pb-3">
        <ThemeSwitcher />
        <div className="mt-3 px-1 text-xs text-ds-text-3 space-y-0.5">
          <div>DeSSS v0.1.0</div>
          <div>Portable · ./data/desss.db</div>
        </div>
      </div>
    </aside>
  );
}
