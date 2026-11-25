import React from "react";
import { DataTable, EmptyState } from "../components/ui";
import { useLiveStore } from "../store/useLiveStore";
import { getExchange } from "../lib/exchanges";

/**
 * Data page: real-time market data. The BBO table and top-5 depth books
 * moved here from the Trading page; they render from the live book store
 * (150ms render throttle in useLiveStore), with bids green / asks red and
 * tabular numerals throughout. No mock data anywhere.
 */
export default function DataPage() {
  const books = useLiveStore((s) => s.books);
  const bookList = Object.values(books);

  return (
    <div>
      <div className="font-semibold text-ds-text-1 mb-2 flex items-center gap-2">
        真实行情 · 交易所 WebSocket 实时订阅
      </div>
      {bookList.length === 0 ? (
        <EmptyState>暂无真实行情 — 请先在「账户」页面连接任意交易所账户。</EmptyState>
      ) : (
        <div className="mb-6">
          <DataTable
            headers={["交易所", "交易对", "买一", "卖一", "中间价", "盘口深度"]}
            minWidth={680}
            rows={bookList.map((b) => [
              <span key="e">{getExchange(b.adapterId).name}</span>,
              <span key="s" className="num">{b.symbol}</span>,
              <span key="bid" className="num text-ds-up">{b.bids[0] ? b.bids[0][0].toFixed(2) : "—"}</span>,
              <span key="ask" className="num text-ds-down">{b.asks[0] ? b.asks[0][0].toFixed(2) : "—"}</span>,
              <span key="mid" className="num">
                {b.bids[0] && b.asks[0] ? ((b.bids[0][0] + b.asks[0][0]) / 2).toFixed(2) : "—"}
              </span>,
              <span key="d" className="num text-ds-text-2">
                {b.bids.length} / {b.asks.length}
              </span>,
            ])}
          />
          <div className="mt-3 grid grid-cols-2 gap-4">
            {bookList.map((b) => (
              <div key={`${b.adapterId}:${b.symbol}`} className="rounded-[10px] border border-ds-line bg-ds-panel shadow-card p-3">
                <div className="text-xs text-ds-text-2 mb-2">
                  {getExchange(b.adapterId).name} · <span className="num">{b.symbol}</span> · 盘口前 5 档
                </div>
                <div className="grid grid-cols-2 gap-3 text-xs num">
                  <div>
                    <div className="text-ds-text-3 mb-1">买盘</div>
                    {b.bids.slice(0, 5).map(([p, q], i) => (
                      <div key={i} className="flex justify-between py-0.5">
                        <span className="text-ds-up">{p.toFixed(2)}</span>
                        <span className="text-ds-up">{q}</span>
                      </div>
                    ))}
                  </div>
                  <div>
                    <div className="text-ds-text-3 mb-1">卖盘</div>
                    {b.asks.slice(0, 5).map(([p, q], i) => (
                      <div key={i} className="flex justify-between py-0.5">
                        <span className="text-ds-down">{p.toFixed(2)}</span>
                        <span className="text-ds-down">{q}</span>
                      </div>
                    ))}
                  </div>
                </div>
              </div>
            ))}
          </div>
        </div>
      )}
      <div className="text-xs text-ds-text-3">历史数据存储与回放尚未接入。</div>
    </div>
  );
}
