import React, { useLayoutEffect } from "react";
import { Button, DataTable, Input, MultiSelectDropdown, tdClsCompact, tdClsCompactRight, trCls } from "../components/ui";
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
  return trimZeros(px.toFixed(priceDecimals(tick)));
}

/** Trim trailing zeros after the decimal point (F4.1): 90000.00 →
 * 90000, 0.0100 → 0.01. Applied before any % suffix is added. */
function trimZeros(s: string): string {
  if (!s.includes(".")) return s;
  return s.replace(/0+$/, "").replace(/\.$/, "");
}

/** Short venue names for the exchange column (F4.1) — the full
 * names stay in the account page and the venue status line. */
const SHORT_VENUE_NAMES: Record<string, string> = {
  hyperliquid: "HL",
  entropy: "Entropy",
  lighter: "Lighter",
  lighter_robinhood: "Lighter RH",
  asterdex: "AsterDEX",
  arcus: "Arcus",
};

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
  "品种",
  "标记价",
  "买/卖",
  "资金费 / 年化",
  "杠杆",
];
// Fixed column ratios, summing to 100 (960x640-safe, horizontal scroll).
const COL_WIDTHS = ["16%", "18%", "16%", "18%", "20%", "12%"];
// Numbers right-aligned (header and cells alike); names centered.
const COL_ALIGNS = ["center", "center", "right", "right", "right", "right"] as const;

/** One fragment per market (row + optional detail row), returned as
 * an array so DataTable sees each market as one row group — that is
 * what lets the table virtualize past 200 results. Rows are
 * keyboard reachable: Tab to a row, Enter expands it. */
function marketRowNodes({
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
}): React.ReactNode[] {
  return rows.map((m) => {
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
        : `${m.funding1h >= 0 ? "+" : ""}${trimZeros((m.funding1h * 100).toFixed(4))}%`;
    const aprText =
      m.fundingApr == null
        ? "—"
        : `${m.fundingApr >= 0 ? "+" : ""}${trimZeros(m.fundingApr.toFixed(2))}%`;
    // 2x7 detail grid (F4.1 order): row 1 (primary): 指数价 / 中间价 /
    // 资金费率 / 价格步长 / 数量步长 / 结算币种 / 目录更新; row 2
    // (secondary): 持仓量 / 成交额 / 下次结算 / 最小名义 / 最大杠杆 /
    // 费率周期 / 盘口更新. Mid is always (bid + ask) / 2 from the live
    // book — never the catalog midPx, which no longer carries a value.
    // Leverage prints floored (13.33 → 13x); the other numbers trim
    // trailing zeros.
    const detail: [string, string][] = [
      ["指数价", fmtPrice(m.indexPx, m.tickSize)],
      [
        "中间价",
        liveFresh && book.bids[0]?.[0] != null && book.asks[0]?.[0] != null
          ? fmtPrice((book.bids[0][0] + book.asks[0][0]) / 2, m.tickSize)
          : "—",
      ],
      [
        "资金费率",
        m.fundingRateRaw == null
          ? "—"
          : `${m.fundingRateRaw >= 0 ? "+" : ""}${trimZeros((m.fundingRateRaw * 100).toFixed(4))}%`,
      ],
      ["价格步长", fmtStep(m.tickSize)],
      ["数量步长", fmtStep(m.stepSize)],
      ["结算币种", m.settlementCurrency],
      ["目录更新", fmtClock(m.updatedAt)],
      ["持仓量", fmtUsdCompact(m.openInterestUsd)],
      ["成交额", fmtUsdCompact(m.volume24hUsd)],
      ["下次结算", fmtTime(m.nextFundingTime)],
      ["最小名义", m.minNotional == null ? "—" : `$${fmtStep(m.minNotional)}`],
      ["最大杠杆", m.maxLeverage == null ? "—" : `${Math.floor(m.maxLeverage)}x`],
      ["费率周期", m.fundingIntervalHours == null ? "—" : `${m.fundingIntervalHours}h`],
      ["盘口更新", liveFresh ? fmtClock(book.ts) : "—"],
    ];
    return (
      <React.Fragment key={key}>
        <tr
          className={`${trCls} cursor-pointer`}
          tabIndex={0}
          onClick={() => onToggle(key)}
          onKeyDown={(e) => {
            if (e.key === "Enter") {
              e.preventDefault();
              onToggle(key);
            }
          }}
        >
          <td className={tdClsCompact}>{venueName(m.exchangeId)}</td>
          <td className={`${tdClsCompact} num whitespace-nowrap`}>{displaySymbol(m.symbol)}</td>
          <td className={`${tdClsCompactRight} num whitespace-nowrap`}>{fmtPrice(m.markPx, m.tickSize)}</td>
          <td className={`${tdClsCompactRight} num whitespace-nowrap`}>
            <span className="text-ds-up">{fmtPrice(bidPx, m.tickSize)}</span>
            {" / "}
            <span className="text-ds-down">{fmtPrice(askPx, m.tickSize)}</span>
          </td>
          <td className={`${tdClsCompactRight} num whitespace-nowrap`}>
            <span className={rateCls}>{fundingText}</span>
            {" / "}
            <span className={aprCls}>{aprText}</span>
          </td>
          <td className={`${tdClsCompactRight} num whitespace-nowrap`}>
            {m.maxLeverage == null ? "—" : `${Math.floor(m.maxLeverage)}x`}
          </td>
        </tr>
        {expandedKey === key && (
          <tr className="border-t border-ds-line bg-ds-panel-2/50">
            <td colSpan={HEADERS.length} className="px-4 py-3 text-left">
              <div className="grid grid-cols-7 gap-x-2 gap-y-3 text-xs num">
                {detail.map(([label, value], i) => (
                  <div key={label} className="min-w-0">
                    <div className={`break-words ${i < 7 ? "text-ds-text-1" : "text-ds-text-2"}`}>{value}</div>
                    <div className="text-ds-text-3">{label}</div>
                  </div>
                ))}
              </div>
            </td>
          </tr>
        )}
      </React.Fragment>
    );
  });
}

export default function DataPage() {
  const view = useDataUiStore((s) => s.view);
  const setView = useDataUiStore((s) => s.setView);
  const search = useMarketSearch();
  const books = useLiveStore((s) => s.books);
  const { schemas } = useVenueSchemas();
  const names = useVenueStore((s) => s.names);
  const venueName = (id: string) => names[id] ?? id;
  const shortVenueName = (id: string) => SHORT_VENUE_NAMES[id] ?? venueName(id);

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

  // Toolbar (F1): view toggle + exchange filter + refresh on the
  // left, catalog update time on the right (F4.1: refresh and the
  // exchange filter moved up here; the search row keeps only the
  // input). The dropdown menus render through a fixed portal, so
  // the toolbar never clips them.
  const register = useShellActions((s) => s.register);
  const unregister = useShellActions((s) => s.unregister);
  useLayoutEffect(() => {
    register("data", {
      actions: (
        <>
          <div className="flex rounded-lg border border-ds-line bg-ds-panel p-0.5">
            <button
              onClick={() => setView("signals")}
              className={`px-3 py-1 rounded-md text-sm focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ds-focus-ring/50 ${
                view === "signals"
                  ? "bg-ds-accent text-ds-accent-contrast"
                  : "text-ds-text-2 hover:text-ds-text-1"
              }`}
            >
              自定义信号
            </button>
            <button
              onClick={() => setView("search")}
              className={`px-3 py-1 rounded-md text-sm focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ds-focus-ring/50 ${
                view === "search"
                  ? "bg-ds-accent text-ds-accent-contrast"
                  : "text-ds-text-2 hover:text-ds-text-1"
              }`}
            >
              标的查询
            </button>
          </div>
          <div className="w-44 shrink-0">
            <MultiSelectDropdown
              options={(schemas ?? []).map((e) => ({ value: e.id, label: e.name }))}
              values={checked}
              onChange={setChecked}
              label="交易所"
            />
          </div>
          <Button onClick={onRefresh} loading={refreshing}>
            刷新
          </Button>
        </>
      ),
      status: `目录更新于 ${cacheUpdatedAt != null ? fmtClock(cacheUpdatedAt) : "—"}`,
    });
    return () => unregister("data");
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [register, unregister, view, refreshing, cacheUpdatedAt, onRefresh, setView, schemas, checked, setChecked]);

  return (
    <div>
      {view === "signals" ? (
        <PageState empty>自定义信号尚未接入 — 下一阶段将支持信号规则创建。</PageState>
      ) : (
        <>
          {/* Search row (F4.1): only the input lives here now —
              the exchange filter and refresh moved to the toolbar. */}
          <div className="flex items-center gap-2 mb-3">
            <div className="flex-1 min-w-0">
              <Input
                value={query}
                onChange={(e) => setQuery(e.target.value)}
                onKeyDown={(e) => {
                  if (e.key === "Enter") searchNow();
                }}
                placeholder="输入 BTC、ETH、SOL、NVDA 等标的名称，查询 6 所实时对比数据"
              />
            </div>
          </div>

          {/* Per-venue catalog status (rows / update time / not ready /
              failed). Lives below the tool row so the row itself stays
              static. Refreshing never blocks the table. */}
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
                    {venueName(v.exchangeId)} {v.rows}行 · {st}
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
              {/* Reserved line: searching must not push the table down
                  when it appears (F4.1 jitter fix). */}
              <div className="text-xs text-ds-text-3 mb-2 h-4">
                {searching ? "搜索中…" : ""}
              </div>
              <DataTable headers={HEADERS} minWidth={680} colWidths={COL_WIDTHS} aligns={[...COL_ALIGNS]} compact>
                {marketRowNodes({
                  rows: results,
                  books,
                  expandedKey,
                  onToggle: (key) => setExpandedKey((cur) => (cur === key ? null : key)),
                  venueName: shortVenueName,
                })}
              </DataTable>
              <div className="text-xs text-ds-text-3 mt-2">
                共 {results.length} 个交易所结果
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
