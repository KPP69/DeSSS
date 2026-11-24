import React from "react";
import { useAppStore, type PageKey } from "../store/useAppStore";
import { useMarketStore } from "../store/useMarketStore";

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
  const connected = useMarketStore((s) => s.connected);
  return (
    <aside className="w-52 shrink-0 h-full bg-panel border-r border-line flex flex-col">
      <div className="px-4 pt-4 pb-3 text-[15px] font-semibold text-t1">DeSSS</div>
      <div className="px-4 pb-3 text-xs text-t2">
        <span className={connected ? "text-profit" : "text-t3"}>●</span>{" "}
        {connected ? "Mock 已连接" : "Mock 未启动"}
      </div>
      <nav className="flex flex-col gap-1 px-2">
        {items.map((i) => (
          <button
            key={i.key}
            onClick={() => setPage(i.key)}
            className={
              "text-left px-3 py-2 text-[13px] rounded transition-colors " +
              (page === i.key
                ? "bg-overlay text-t1"
                : "text-t2 hover:bg-overlay/60 hover:text-t1")
            }
          >
            {i.label}
          </button>
        ))}
      </nav>
      <div className="mt-auto p-3 text-xs text-t3">Portable · ./data/desss.db</div>
    </aside>
  );
}
