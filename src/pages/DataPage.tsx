import React, { useEffect } from "react";
import { Button, DataTable, MultiSelectDropdown, tdClsCompact, trCls } from "../components/ui";
import PageState from "../components/PageState";
import { useMarketSearch } from "../hooks/useMarketSearch";
import { useVenueSchemas } from "../hooks/useVenueSchemas";
import { displaySymbol } from "../lib/symbol";
import type { MarketInfo, OrderBook } from "../lib/types";
import { useDataUiStore } from "../store/useDataUiStore";
import { useLiveStore } from "../store/useLiveStore";
import { useShellActions } from "../store/useShellActions";
import { useVenueStore } from "../store/useVenueStore";

/** A streamed book counts as live for 5s; older than that the row falls
 * back to the 2s REST values (same threshold as the backend). */
const LIVE_MS = 5000;

/**
 * Data page: on-demand symbol search over the bulk market catalog (phase 4).
 * The search lifecycle lives in `useMarketSearch` (Phase 2, I11); this
 * file only renders. Books come from the live store — the App root is
 * the only subscriber (the page's own `market://book` subscription is
 * gone, the store already throttles frames to 150ms). The view toggle
 * lives in this page's toolbar registration (useShellActions); the tool
 * row below is absolutely static — the cache time lives in the results footer,
 * never in the row, so nothing ever shifts the controls.
 */

/** Decimals to show for a price, derived from the market's tick size. */
function priceDecimals(tick: number | null): number {
  if (!tick || tick <= 0) return 2;
  return Math.max(0, Math.min(8, -Math.floor(Math.log10(tick))));
}

function fmtPrice(px: number | null, tick: number | null): string {
  if (px == null || px === 0) return "—";
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
  "标记价",
  "买一 / 卖一",
  "资金费率",
  "年化 APR",
  "最大杠杆",
];
// Fixed column ratios, summing to 100 (960x640-safe, horizontal scroll).
const COL_WIDTHS = ["14%", "16%", "14%", "16%", "14%", "13%", "13%"];

function MarketRows({
  rows,
  books,
  expandedKey,
  onToggle,
  venueName,
}: {
  rows: MarketInfo[];
  books: Record<string, OrderBook>;
  expandedKey: string | null;
  onToggle: (key: string) => void;
  venueName: (id: string) => string;
}) {
  return (
    <>
      {rows.map((m) => {
        const key = `${m.exchangeId}:${m.symbol}`;
        // A fresh streamed book overrides the 2s REST bid/ask; a stale
        // one falls back to the catalog values so a dead stream never
        // freezes the row on an old price.
        const book = books[key];
        const liveFresh = book != null && Date.now() - book.ts <= LIVE_MS;
        const bidPx = liveFresh ? (book.bids[0]?.[0] ?? null) : m.bidPx;
        const askPx = liveFresh ? (book.asks[0]?.[0] ?? null) : m.askPx;
        const rateCls =
          m.funding1h == null
            ? ""
            : m.funding1h > 0
              ? "text-ds-up"
              : m.funding1h < 0
                ? "text-ds-down"
                : "";
        const aprCls =
          m.fundingApr == null
            ? ""
            : m.fundingApr > 0
              ? "text-ds-up"
              : m.fundingApr < 0
                ? "text-ds-down"
                : "";
        const fundingText =
          m.funding1h == null
            ? "—"
            : `${m.funding1h >= 0 ? "+" : ""}${(m.funding1h * 100).toFixed(4)}%`;
        const aprText =
          m.fundingApr == null
            ? "—"
            : `${m.fundingApr >= 0 ? "+" : ""}${m.fundingApr.toFixed(2)}%`;
        return (
          <React.Fragment key={key}>
            <tr className={`${trCls} cursor-pointer`} onClick={() => onToggle(key)}>
              <td className={tdClsCompact}>{venueName(m.exchangeId)}</td>
              <td className={`${tdClsCompact} num whitespace-nowrap`}>{displaySymbol(m.symbol)}</td>
              <td className={`${tdClsCompact} num whitespace-nowrap`}>{fmtPrice(m.markPx, m.tickSize)}</td>
              <td className={`${tdClsCompact} num whitespace-nowrap`}>
                <span className="text-ds-up">{fmtPrice(bidPx, m.tickSize)}</span>
                {" / "}
                <span className="text-ds-down">{fmtPrice(askPx, m.tickSize)}</span>
              </td>
              <td className={`${tdClsCompact} num whitespace-nowrap ${rateCls}`}>{fundingText}</td>
              <td className={`${tdClsCompact} num whitespace-nowrap ${aprCls}`}>{aprText}</td>
              <td className={`${tdClsCompact} num whitespace-nowrap`}>
                {m.maxLeverage == null ? "—" : `${m.maxLeverage}x`}
              </td>
            </tr>
            {expandedKey === key && (
              <tr className="border-t border-ds-line bg-ds-panel-2/50">
                <td colSpan={HEADERS.length} className="px-4 py-3 text-left">
                  {/* 2x7 detail grid (Phase 3.1, commit 2): value above
                   * label, one cell per field. Mid is always
                   * (bid + ask) / 2 from the live book — never the
                   * catalog midPx, which no longer carries a value. */}
                  <div className="flex flex-wrap gap-y-3 text-xs num">
                    {(
                      [
                        ["指数价", fmtPrice(m.indexPx, m.tickSize)],
                        ["持仓量", fmtUsdCompact(m.openInterestUsd)],
                        [
                          "资金费率",
                          m.fundingRateRaw == null
                            ? "—"
                            : `${m.fundingRateRaw >= 0 ? "+" : ""}${(m.fundingRateRaw * 100).toFixed(4)}%`,
                        ],
                        ["价格步长", fmtStep(m.tickSize)],
                        ["最小名义", m.minNotional == null ? "—" : `$${fmtStep(m.minNotional)}`],
                        ["费率周期", m.fundingIntervalHours == null ? "—" : `${m.fundingIntervalHours}h`],
                        ["盘口更新", liveFresh ? fmtClock(book.ts) : "—"],
                        [
                          "中间价",
                          liveFresh && book.bids[0]?.[0] != null && book.asks[0]?.[0] != null
                            ? fmtPrice((book.bids[0][0] + book.asks[0][0]) / 2, m.tickSize)
                            : "—",
                        ],
                        ["成交额", fmtUsdCompact(m.volume24hUsd)],
                        ["下次结算", fmtTime(m.nextFundingTime)],
                        ["数量步长", fmtStep(m.stepSize)],
                        ["最大杠杆", m.maxLeverage == null ? "—" : `${m.maxLeverage}x`],
                        ["结算币种", m.settlementCurrency],
                        ["目录更新", fmtClock(m.updatedAt)],
                      ] as [string, string][]
                    ).map(([label, value]) => (
                      <div key={label} className="w-28 shrink-0">
                        <div className="text-ds-text-1">{value}</div>
                        <div className="text-ds-text-2">{label}</div>
                      </div>
                    ))}
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
  const view = useDataUiStore((s) => s.view);
  const setView = useDataUiStore((s) => s.setView);
  const search = useMarketSearch();
  const books = useLiveStore((s) => s.books);
  const { schemas } = useVenueSchemas();
  const names = useVenueStore((s) => s.names);
  const venueName = (id: string) => names[id] ?? id;

  const {
    query,
    setQuery,
    checked,
    setChecked,
    results,
    searched,
    searching,
    notReady,
    refreshing,
    cacheUpdatedAt,
    venueStatus,
    expandedKey,
    setExpandedKey,
    searchNow,
    onRefresh,
  } = search;

  // Toolbar (F1): view toggle + refresh on the left, catalog
  // update time on the right.
  const register = useShellActions((s) => s.register);
  const unregister = useShellActions((s) => s.unregister);
  useEffect(() => {
    register("data", {
      actions: (
        <>
          <div className="flex rounded-lg border border-ds-line bg-ds-panel p-0.5">
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
          </div>
          <Button onClick={onRefresh} disabled={refreshing}>
            {refreshing ? "刷新中…" : "刷新"}
          </Button>
        </>
      ),
      status: `目录更新于 ${cacheUpdatedAt != null ? fmtClock(cacheUpdatedAt) : "—"}`,
    });
    return () => unregister("data");
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [register, unregister, view, refreshing, cacheUpdatedAt, onRefresh, setView]);

  return (
    <div>
      {view === "signals" ? (
        <PageState empty>自定义信号尚未接入 — 下一阶段将支持信号规则创建。</PageState>
      ) : (
        <>
          {/* Absolutely static tool row: the search input stretches, the
              exchange dropdown and the refresh button are fixed-width —
              nothing is ever inserted here, so no click, loading state or
              cache timestamp can push a control sideways. The cache time
              lives in the results footer below the table instead. */}
          <div className="flex items-center gap-2 mb-3">
            <input
              value={query}
              onChange={(e) => setQuery(e.target.value)}
              onKeyDown={(e) => {
                if (e.key === "Enter") searchNow();
              }}
              placeholder="输入 BTC、ETH、SOL、NVDA 等标的名称，查询 6 所实时对比数据"
              className="flex-1 min-w-0 bg-ds-panel-2 border border-ds-line rounded-lg px-3 h-8 text-ds-text-1 outline-none focus:border-ds-accent focus:ring-2 focus:ring-ds-focus-ring/50 disabled:bg-ds-bg-disabled disabled:text-ds-text-disabled num"
            />
            <div className="w-44 shrink-0">
              <MultiSelectDropdown
                options={(schemas ?? []).map((e) => ({ value: e.id, label: e.name }))}
                values={checked}
                onChange={setChecked}
                label="交易所"
              />
            </div>
            <div className="shrink-0">
              <Button onClick={onRefresh} disabled={refreshing}>
                {refreshing ? "刷新中…" : "刷新"}
              </Button>
            </div>
          </div>

          {/* Per-venue catalog status (updated time / not ready / failed).
              Lives below the tool row so the row itself stays static. */}
          {venueStatus && (
            <div className="text-xs text-ds-text-3 mb-3 num">
              {venueStatus.venues.map((v) => {
                const st = v.error
                  ? "失败"
                  : v.updatedAt == null
                    ? "未就绪"
                    : fmtClock(v.updatedAt);
                return (
                  <span key={v.exchangeId} className="mr-3">
                    {venueName(v.exchangeId)} {st}
                  </span>
                );
              })}
            </div>
          )}

          {!searched && !searching ? (
            <PageState empty>输入 BTC、ETH、SOL、NVDA 等标的名称，查询 6 所实时对比数据。</PageState>
          ) : searching && results.length === 0 ? (
            <PageState loading loadingText="搜索中…" />
          ) : notReady ? (
            <PageState empty>目录未就绪，正在拉取…</PageState>
          ) : results.length === 0 ? (
            <PageState empty>未找到包含「{query.trim()}」的标的。</PageState>
          ) : (
            <div className="mb-6">
              <DataTable headers={HEADERS} minWidth={720} colWidths={COL_WIDTHS} compact>
                <MarketRows
                  rows={results}
                  books={books}
                  expandedKey={expandedKey}
                  onToggle={(key) => setExpandedKey((cur) => (cur === key ? null : key))}
                  venueName={venueName}
                />
              </DataTable>
              <div className="text-xs text-ds-text-3 mt-2">
                共 {results.length} 个交易所结果
                {searching && <> · 搜索中…</>}
                {cacheUpdatedAt != null && <> · 缓存更新于 {fmtClock(cacheUpdatedAt)}</>}
                {" · "}点击行查看完整规则详情 · 费率正值（多头付费）为绿，负值为红
              </div>
            </div>
          )}
        </>
      )}
    </div>
  );
}
