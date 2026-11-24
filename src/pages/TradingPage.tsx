import React from "react";
import { Badge, DataTable, EmptyState } from "../components/ui";
import { useMarketStore } from "../store/useMarketStore";
import { useLiveStore } from "../store/useLiveStore";
import { useAccountStore } from "../store/useAccountStore";
import { getExchange } from "../lib/exchanges";

export default function TradingPage() {
  const ticks = useMarketStore((s) => s.ticks);
  const books = useLiveStore((s) => s.books);
  const live = useLiveStore((s) => s.live);
  const accounts = useAccountStore((s) => s.accounts);
  const mockList = Object.values(ticks).filter((t) => t.adapterId === "mock");
  const bookList = Object.values(books);
  const positions = Object.values(live).flatMap((a) =>
    a.positions.map((p) => ({ ...p, accountId: a.accountId, adapterId: a.adapterId, currency: a.currency }))
  );
  const labelOf = (accountId: string) =>
    accounts.find((a) => a.id === accountId)?.label ?? accountId;

  return (
    <div>
      {/* Real market data */}
      <div className="text-sm font-medium text-t1 mb-2 flex items-center gap-2">
        真实行情 · 交易所 WebSocket 实时订阅 <Badge tone="mock">Mock 行情</Badge>
      </div>
      {bookList.length === 0 ? (
        <EmptyState>暂无真实行情 — 请先在「账户」页面连接 Hyperliquid / Entropy / Lighter 账户。</EmptyState>
      ) : (
        <div className="mb-6">
          <DataTable
            headers={["交易所", "交易对", "买一", "卖一", "中间价", "盘口深度"]}
            minWidth={680}
            rows={bookList.map((b) => [
              <span key="e">{getExchange(b.adapterId).name}</span>,
              <span key="s" className="num">{b.symbol}</span>,
              <span key="bid" className="num text-profit">{b.bids[0] ? b.bids[0][0].toFixed(2) : "—"}</span>,
              <span key="ask" className="num text-loss">{b.asks[0] ? b.asks[0][0].toFixed(2) : "—"}</span>,
              <span key="mid" className="num">
                {b.bids[0] && b.asks[0] ? ((b.bids[0][0] + b.asks[0][0]) / 2).toFixed(2) : "—"}
              </span>,
              <span key="d" className="num text-t2">
                {b.bids.length} / {b.asks.length}
              </span>,
            ])}
          />
          <div className="mt-3 grid grid-cols-2 gap-4">
            {bookList.map((b) => (
              <div key={`${b.adapterId}:${b.symbol}`} className="rounded border border-line bg-panel p-3">
                <div className="text-xs text-t2 mb-2">
                  {getExchange(b.adapterId).name} · <span className="num">{b.symbol}</span> · 盘口前 5 档
                </div>
                <div className="grid grid-cols-2 gap-3 text-xs num">
                  <div>
                    <div className="text-t3 mb-1">买盘</div>
                    {b.bids.slice(0, 5).map(([p, q], i) => (
                      <div key={i} className="flex justify-between py-0.5">
                        <span className="text-profit">{p.toFixed(2)}</span>
                        <span className="text-t2">{q}</span>
                      </div>
                    ))}
                  </div>
                  <div>
                    <div className="text-t3 mb-1">卖盘</div>
                    {b.asks.slice(0, 5).map(([p, q], i) => (
                      <div key={i} className="flex justify-between py-0.5">
                        <span className="text-loss">{p.toFixed(2)}</span>
                        <span className="text-t2">{q}</span>
                      </div>
                    ))}
                  </div>
                </div>
              </div>
            ))}
          </div>
        </div>
      )}

      {/* Real positions */}
      <div className="text-sm font-medium text-t1 mb-2">真实持仓 · 来自已连接账户的只读同步</div>
      {positions.length === 0 ? (
        <EmptyState>暂无真实持仓 — 连接账户后，交易所同步到的当前持仓会显示在这里。</EmptyState>
      ) : (
        <div className="mb-6">
          <DataTable
            headers={["账户", "交易所", "交易对", "方向", "数量", "开仓价", "标记价", "未实现盈亏"]}
            minWidth={760}
            rows={positions.map((p, i) => [
              <span key="a">{labelOf(p.accountId)}</span>,
              <span key="e">{getExchange(p.adapterId).name}</span>,
              <span key="s" className="num">{p.symbol}</span>,
              <span key="d" className={p.side === "long" ? "text-profit" : "text-loss"}>
                {p.side === "long" ? "多" : "空"}
              </span>,
              <span key="q" className="num">{p.qty}</span>,
              <span key="ep" className="num">{p.entryPrice.toFixed(2)}</span>,
              <span key="mp" className="num">{p.markPrice.toFixed(2)}</span>,
              <span
                key="p"
                className={`num ${p.unrealizedPnl > 0 ? "text-profit" : p.unrealizedPnl < 0 ? "text-loss" : ""}`}
              >
                {p.unrealizedPnl > 0 ? "+" : ""}
                {p.unrealizedPnl.toFixed(2)} {p.currency}
              </span>,
            ])}
          />
        </div>
      )}

      {/* Mock data, kept separate and labelled */}
      <div className="text-sm font-medium text-t1 mb-2">模拟行情</div>
      <div className="text-sm text-t2 mb-3">
        Mock 行情 · 每 500ms 推送 · 来自 EventBus → Tauri event <code className="num">market://tick</code>
      </div>
      {mockList.length === 0 ? (
        <EmptyState>等待 Mock 行情推送…</EmptyState>
      ) : (
        <DataTable
          headers={["交易对", "最新价", "买一 / 卖一"]}
          minWidth={560}
          rows={mockList.map((t) => [
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
