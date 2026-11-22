import React from "react";
import { Badge, DataTable, EmptyState, PageHeader } from "../components/ui";
import { useMarketStore } from "../store/useMarketStore";

export default function TradingPage() {
  const ticks = useMarketStore((s) => s.ticks);
  const list = Object.values(ticks);
  return (
    <div>
      <PageHeader title="交易">
        <Badge tone="mock">Mock 行情</Badge>
      </PageHeader>
      <div className="text-sm text-t2 mb-3">
        Mock 行情 · 每 500ms 推送 · 来自 EventBus → Tauri event <code className="num">market://tick</code>
      </div>
      {list.length === 0 ? (
        <EmptyState>等待 Mock 行情推送…</EmptyState>
      ) : (
        <DataTable
          headers={["交易对", "最新价", "买一 / 卖一"]}
          minWidth={560}
          rows={list.map((t) => [
            <span key="s" className="num">{t.symbol}</span>,
            <span key="l" className="num">{t.last.toFixed(2)}</span>,
            <span key="ba" className="num text-t2">
              {t.bid.toFixed(2)} / {t.ask.toFixed(2)}
            </span>,
          ])}
        />
      )}
    </div>
  );
}
