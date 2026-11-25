import React, { useCallback, useEffect, useRef, useState } from "react";
import { Button, DataTable, EmptyState, tdClsCompact, trCls } from "../components/ui";
import { useLiveStore } from "../store/useLiveStore";
import { getExchange } from "../lib/exchanges";
import { api } from "../lib/tauri";
import type { MarketInfo } from "../lib/types";

/**
 * Data page: on-demand symbol search over the bulk market catalog (phase 4)
 * plus the live WS books for connected accounts. The page never dumps the
 * whole market — without a query it shows a search guide only.
 */

type DataView = "search" | "signals";

/** Decimals to show for a price, derived from the market's tick size. */
function priceDecimals(tick: number | null): number {
  if (!tick || tick <= 0) return 2;
  return Math.max(0, Math.min(8, -Math.floor(Math.log10(tick))));
}

function fmtPrice(px: number | null, tick: number | null): string {
  if (px == null) return "—";
  return px.toFixed(priceDecimals(tick));
}

/** Step / notional values print without scientific notation. */
function fmtStep(n: number | null): string {
  if (n == null) return "—";
  return parseFloat(n.toFixed(8)).toString();
}

function fmtTime(ms: number | null): string {
  if (ms == null) return "—";
  return new Date(ms).toLocaleString("zh-CN", { hour12: false });
}

function fmtClock(ms: number): string {
  return new Date(ms).toLocaleTimeString("zh-CN", { hour12: false });
}

/** Compact USD amount: $3.41B / $913.79M / $12.5K / $480. */
function fmtUsdCompact(v: number | null): string {
  if (v == null) return "—";
  const abs = Math.abs(v);
  if (abs >= 1e9) return `$${(v / 1e9).toFixed(2)}B`;
  if (abs >= 1e6) return `$${(v / 1e6).toFixed(2)}M`;
  if (abs >= 1e3) return `$${(v / 1e3).toFixed(1)}K`;
  return `$${v.toFixed(0)}`;
}

const HEADERS = [
  "交易所",
  "原始合约",
  "买一 / 卖一",
  "标记价",
  "最大杠杆",
  "1h 费率 / 年化 APR",
  "最小步长 / 名义",
  "结算周期 & 币种",
];
// Fixed column ratios, summing to 100 (960x640-safe, horizontal scroll).
const COL_WIDTHS = ["12%", "14%", "16%", "12%", "9%", "15%", "12%", "10%"];

function MarketRows({
  rows,
  expandedKey,
  onToggle,
}: {
  rows: MarketInfo[];
  expandedKey: string | null;
  onToggle: (key: string) => void;
}) {
  return (
    <>
      {rows.map((m) => {
        const key = `${m.exchangeId}:${m.symbol}`;
        const fundingCls =
          m.funding1h == null
            ? ""
            : m.funding1h > 0
              ? "text-ds-up"
              : m.funding1h < 0
                ? "text-ds-down"
                : "";
        const fundingText =
          m.funding1h == null
            ? "—"
            : `${m.funding1h >= 0 ? "+" : ""}${(m.funding1h * 100).toFixed(4)}% / 年化 ${
                m.fundingApr == null ? "—" : `${m.fundingApr.toFixed(2)}%`
              }`;
        return (
          <React.Fragment key={key}>
            <tr className={`${trCls} cursor-pointer`} onClick={() => onToggle(key)}>
              <td className={tdClsCompact}>{getExchange(m.exchangeId).name}</td>
              <td className={`${tdClsCompact} num`}>{m.symbol}</td>
              <td className={`${tdClsCompact} num`}>
                <span className="text-ds-up">{fmtPrice(m.bidPx, m.tickSize)}</span>
                {" / "}
                <span className="text-ds-down">{fmtPrice(m.askPx, m.tickSize)}</span>
              </td>
              <td className={`${tdClsCompact} num`}>{fmtPrice(m.markPx, m.tickSize)}</td>
              <td className={`${tdClsCompact} num`}>
                {m.maxLeverage == null ? "—" : `${m.maxLeverage}x`}
              </td>
              <td className={`${tdClsCompact} num ${fundingCls}`}>{fundingText}</td>
              <td className={`${tdClsCompact} num`}>
                {fmtStep(m.stepSize)} / {m.minNotional == null ? "—" : `$${fmtStep(m.minNotional)}`}
              </td>
              <td className={tdClsCompact}>
                {m.fundingIntervalHours}h · {m.settlementCurrency}
              </td>
            </tr>
            {expandedKey === key && (
              <tr className="border-t border-ds-line bg-ds-panel-2/50">
                <td colSpan={HEADERS.length} className="px-4 py-3 text-left">
                  <div className="flex flex-wrap gap-x-5 gap-y-1 text-xs text-ds-text-2 num">
                    <span>指数价：<span className="text-ds-text-1">{fmtPrice(m.indexPx, m.tickSize)}</span></span>
                    <span>中间价：<span className="text-ds-text-1">{fmtPrice(m.midPx, m.tickSize)}</span></span>
                    <span>价格步长：<span className="text-ds-text-1">{fmtStep(m.tickSize)}</span></span>
                    <span>数量步长：<span className="text-ds-text-1">{fmtStep(m.stepSize)}</span></span>
                    <span>最小名义：<span className="text-ds-text-1">{m.minNotional == null ? "—" : `$${fmtStep(m.minNotional)}`}</span></span>
                    <span>结算币种：<span className="text-ds-text-1">{m.settlementCurrency}</span></span>
                    <span>24h成交额：<span className="text-ds-text-1">{fmtUsdCompact(m.volume24hUsd)}</span></span>
                    <span>持仓量：<span className="text-ds-text-1">{fmtUsdCompact(m.openInterestUsd)}</span></span>
                    <span>下次结算：<span className="text-ds-text-1">{fmtTime(m.nextFundingTime)}</span></span>
                    <span>更新：<span className="text-ds-text-1">{fmtClock(m.updatedAt)}</span></span>
                  </div>
                </td>
              </tr>
            )}
          </React.Fragment>
        );
      })}
    </>
  );
}

export default function DataPage() {
  const books = useLiveStore((s) => s.books);
  const bookList = Object.values(books);
  const [view, setView] = useState<DataView>("search");
  const [query, setQuery] = useState("");
  const [results, setResults] = useState<MarketInfo[]>([]);
  const [searched, setSearched] = useState(false);
  const [searching, setSearching] = useState(false);
  const [refreshing, setRefreshing] = useState(false);
  const [cacheUpdatedAt, setCacheUpdatedAt] = useState<number | null>(null);
  const [expandedKey, setExpandedKey] = useState<string | null>(null);
  const reqId = useRef(0);

  const runSearch = useCallback(async (q: string) => {
    const trimmed = q.trim();
    if (!trimmed) {
      setResults([]);
      setSearched(false);
      return;
    }
    const id = ++reqId.current;
    setSearching(true);
    try {
      const rows = await api.searchMarketInfo(trimmed);
      if (reqId.current === id) {
        setResults(rows);
        setSearched(true);
        setExpandedKey(null);
        if (rows.length > 0) {
          setCacheUpdatedAt(Math.max(...rows.map((r) => r.updatedAt)));
        }
      }
    } catch {
      if (reqId.current === id) {
        setResults([]);
        setSearched(true);
      }
    } finally {
      if (reqId.current === id) setSearching(false);
    }
  }, []);

  // Debounced live search while typing; Enter searches immediately.
  useEffect(() => {
    if (!query.trim()) {
      setResults([]);
      setSearched(false);
      return;
    }
    const t = setTimeout(() => void runSearch(query), 300);
    return () => clearTimeout(t);
  }, [query, runSearch]);

  const onRefresh = async () => {
    setRefreshing(true);
    try {
      const r = await api.refreshMarketCatalog();
      if (r.updatedAt > 0) setCacheUpdatedAt(r.updatedAt);
      if (query.trim()) await runSearch(query);
    } finally {
      setRefreshing(false);
    }
  };

  return (
    <div>
      <div className="flex items-center gap-2 mb-3">
        <div className="flex rounded-lg border border-ds-line bg-ds-panel p-0.5">
          <button
            onClick={() => setView("search")}
            className={`px-3 py-1 rounded-md text-sm ${
              view === "search"
                ? "bg-ds-accent text-ds-accent-contrast"
                : "text-ds-text-2 hover:text-ds-text-1"
            }`}
          >
            标的查询
          </button>
          <button
            onClick={() => setView("signals")}
            className={`px-3 py-1 rounded-md text-sm ${
              view === "signals"
                ? "bg-ds-accent text-ds-accent-contrast"
                : "text-ds-text-2 hover:text-ds-text-1"
            }`}
          >
            自定义信号
          </button>
        </div>
      </div>

      {view === "signals" ? (
        <EmptyState>自定义信号尚未接入 — 下一阶段将支持信号规则创建。</EmptyState>
      ) : (
        <>
          <div className="flex items-center gap-2 mb-3">
            <input
              value={query}
              onChange={(e) => setQuery(e.target.value)}
              onKeyDown={(e) => {
                if (e.key === "Enter") void runSearch(query);
              }}
              placeholder="输入 BTC、ETH、SOL、NVDA 等标的名称，查询 6 所实时对比数据"
              className="flex-1 min-w-0 bg-ds-panel-2 border border-ds-line rounded-lg px-3 h-8 text-ds-text-1 outline-none focus:border-ds-accent num"
            />
            <Button onClick={onRefresh} disabled={refreshing}>
              {refreshing ? "刷新中…" : "刷新"}
            </Button>
            {cacheUpdatedAt != null && (
              <span className="text-xs text-ds-text-3 whitespace-nowrap">
                缓存更新于 {fmtClock(cacheUpdatedAt)}
              </span>
            )}
          </div>

          {!searched && !searching ? (
            <EmptyState>输入 BTC、ETH、SOL、NVDA 等标的名称，查询 6 所实时对比数据。</EmptyState>
          ) : searching && results.length === 0 ? (
            <EmptyState>搜索中…</EmptyState>
          ) : results.length === 0 ? (
            <EmptyState>未找到包含「{query.trim()}」的标的。</EmptyState>
          ) : (
            <div className="mb-6">
              <DataTable headers={HEADERS} minWidth={760} colWidths={COL_WIDTHS} compact>
                <MarketRows
                  rows={results}
                  expandedKey={expandedKey}
                  onToggle={(key) => setExpandedKey((cur) => (cur === key ? null : key))}
                />
              </DataTable>
              <div className="text-xs text-ds-text-3 mt-2">
                共 {results.length} 个交易所结果 · 点击行查看完整规则详情 · 费率正值（多头付费）为绿，负值为红
              </div>
            </div>
          )}

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
        </>
      )}
    </div>
  );
}
