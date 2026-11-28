import React, { useEffect, useLayoutEffect, useRef } from "react";
import { Button, DataTable, Input, MultiSelectDropdown, RefreshButton, SegmentedControl, Tooltip, trCls } from "../components/ui";
import PageState from "../components/PageState";
import { useMarketSearch } from "../hooks/useMarketSearch";
import { useVenueSchemas } from "../hooks/useVenueSchemas";
import { displaySymbol } from "../lib/symbol";
import { fmtClock } from "../lib/format";
import type { MarketInfo, OrderBook } from "../lib/types";
import { useDataUiStore, type DataView } from "../store/useDataUiStore";
import { useLiveStore } from "../store/useLiveStore";
import { useShellActions } from "../store/useShellActions";
import { toast } from "../store/useToastStore";
import { useVenueStore } from "../store/useVenueStore";

/** A streamed book counts as live for 5s; older than that the row's
 * bid/ask read 无实时数据 (F4.3 — the old REST fallback is gone). */
const LIVE_MS = 5000;
/** A quote (mark/funding, from quote_at) counts as live for 15s
 * (PR-B1); older than that the row's mark/funding read 无实时数据. */
const QUOTE_STALE_MS = 15_000;

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

/** F4.3: 16px column spacing (px-2 each side) for the data table. */
const TD = "num text-left px-2 py-1.5 text-[13px] text-ds-text-1";
const TD_RIGHT = "num text-right px-2 py-1.5 text-[13px] text-ds-text-1";

/** Step / notional values print without scientific notation. */
function fmtStep(n: number | null): string {
  if (n == null) return "—";
  return parseFloat(n.toFixed(8)).toString();
}

function fmtTime(ms: number | null): string {
  if (ms == null) return "—";
  return new Date(ms).toLocaleString("zh-CN", { hour12: false });
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
const COL_WIDTHS = ["12%", "16%", "16%", "24%", "22%", "10%"];
// Numbers right-aligned (header and cells alike); identifiers left.
const COL_ALIGNS = ["left", "left", "right", "right", "right", "right"] as const;

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
  firstSeenAt,
}: {
  rows: MarketInfo[];
  books: Record<string, OrderBook>;
  expandedKey: string | null;
  onToggle: (key: string) => void;
  venueName: (id: string) => string;
  /** When each row first appeared on screen (F4.3): a row with no WS
   * frame yet reads 载入中 for its first 5s, then 无实时数据. */
  firstSeenAt: Map<string, number>;
}): React.ReactNode[] {
  return rows.map((m) => {
    const key = `${m.exchangeId}:${m.symbol}`;
    const book = books[key];
    const liveFresh = book != null && Date.now() - book.ts <= LIVE_MS;
    // F4.3 three states for the live columns (bid/ask + funding):
    // live = a WS frame within 5s; loading = no frame yet and the row
    // appeared under 5s ago; stale = a frame that went quiet, or no
    // frame at all for 5s+. The REST bid/ask snapshot is never shown.
    const seenAt = firstSeenAt.get(key);
    const liveState: "loading" | "live" | "stale" = liveFresh
      ? "live"
      : book != null || (seenAt != null && Date.now() - seenAt >= LIVE_MS)
        ? "stale"
        : "loading";
    const bidPx = liveFresh ? (book.bids[0]?.[0] ?? null) : null;
    const askPx = liveFresh ? (book.asks[0]?.[0] ?? null) : null;
    // PR-B1: mark/funding freshness from quote_at (15s), not the WS
    // book: a known-old quote_at reads 无实时数据 at once; a row that
    // never had a quote write reads 载入中 for its first 15s.
    const quoteFresh = m.quoteAt != null && Date.now() - m.quoteAt <= QUOTE_STALE_MS;
    const quoteState: "loading" | "live" | "stale" = quoteFresh
      ? "live"
      : m.quoteAt != null || (seenAt != null && Date.now() - seenAt >= QUOTE_STALE_MS)
        ? "stale"
        : "loading";
    const quoteStateText = (
      <span className="text-ds-text-3">{quoteState === "loading" ? "载入中" : "无实时数据"}</span>
    );
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
    // 2x7 detail fields (F4.1 order): block 1 — 指数价 / 中间价 /
    // 资金费率 / 价格步长 / 数量步长 / 结算币种 / 目录更新; block 2 —
    // 持仓量 / 成交额 / 下次结算 / 最小名义 / 最大杠杆 / 费率周期 /
    // 盘口更新. Mid is always (bid + ask) / 2 from the live book —
    // never the catalog midPx, which no longer carries a value.
    const detail: [string, React.ReactNode][] = [
      ["指数价", quoteState === "live" ? fmtPrice(m.indexPx, m.tickSize) : quoteStateText],
      [
        "中间价",
        liveFresh && book.bids[0]?.[0] != null && book.asks[0]?.[0] != null
          ? fmtPrice((book.bids[0][0] + book.asks[0][0]) / 2, m.tickSize)
          : "—",
      ],
      [
        "资金费率",
        quoteState === "live"
          ? m.fundingRateRaw == null
            ? "—"
            : `${m.fundingRateRaw >= 0 ? "+" : ""}${trimZeros((m.fundingRateRaw * 100).toFixed(4))}%`
          : quoteStateText,
      ],
      ["价格步长", fmtStep(m.tickSize)],
      ["数量步长", fmtStep(m.stepSize)],
      ["结算币种", m.settlementCurrency],
      ["目录更新", fmtClock(m.updatedAt)],
      ["持仓量", quoteState === "live" ? fmtUsdCompact(m.openInterestUsd) : quoteStateText],
      ["成交额", quoteState === "live" ? fmtUsdCompact(m.volume24hUsd) : quoteStateText],
      ["下次结算", quoteState === "live" ? fmtTime(m.nextFundingTime) : quoteStateText],
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
          <td className={TD}>{venueName(m.exchangeId)}</td>
          <td className={TD}>
            <Tooltip content={displaySymbol(m.symbol)}>
              <span className="inline-block max-w-[90px] truncate align-middle num">
                {displaySymbol(m.symbol)}
              </span>
            </Tooltip>
          </td>
          <td className={`${TD_RIGHT} whitespace-nowrap`}>
            {quoteState === "live" ? fmtPrice(m.markPx, m.tickSize) : quoteStateText}
          </td>
          <td className={`${TD_RIGHT} whitespace-nowrap`}>
            {liveState === "live" ? (
              <>
                <span className="text-ds-up">{fmtPrice(bidPx, m.tickSize)}</span>
                {" / "}
                <span className="text-ds-down">{fmtPrice(askPx, m.tickSize)}</span>
              </>
            ) : (
              <span className="text-ds-text-3">
                {liveState === "loading" ? "载入中" : "无实时数据"}
              </span>
            )}
          </td>
          <td className={`${TD_RIGHT} whitespace-nowrap`}>
            {quoteState === "live" ? (
              <>
                <span className={rateCls}>{fundingText}</span>
                {" / "}
                <span className={aprCls}>{aprText}</span>
              </>
            ) : (
              quoteStateText
            )}
          </td>
          <td className={`${TD_RIGHT} whitespace-nowrap`}>
            {m.maxLeverage == null ? "—" : `${Math.floor(m.maxLeverage)}x`}
          </td>
        </tr>
        {expandedKey === key && (
          <tr className="border-t border-ds-line bg-ds-panel-2/50">
            <td colSpan={HEADERS.length} className="px-4 py-3 text-left">
              {/* F4.3: label row + value row per block (two blocks of 7
                  columns); block 1 values primary, block 2 secondary. */}
              <div className="space-y-3 text-xs num">
                {[detail.slice(0, 7), detail.slice(7)].map((block, b) => (
                  <div key={b} className="grid grid-cols-7 gap-x-2 gap-y-1">
                    {block.map(([label]) => (
                      <div key={label} className="text-ds-text-3">{label}</div>
                    ))}
                    {block.map(([label, value]) => (
                      <div
                        key={label}
                        className={`min-w-0 break-words ${b === 0 ? "text-ds-text-1" : "text-ds-text-2"}`}
                      >
                        {value}
                      </div>
                    ))}
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

  const {
    query,
    setQuery,
    submittedQuery,
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

  // First-seen times for the three-state rows (F4.3): a row with no
  // WS frame reads 载入中 for its first 5s, then 无实时数据.
  const firstSeenRef = useRef(new Map<string, number>());
  useEffect(() => {
    const now = Date.now();
    const seen = firstSeenRef.current;
    for (const r of results) {
      const k = `${r.exchangeId}:${r.symbol}`;
      if (!seen.has(k)) seen.set(k, now);
    }
    for (const k of [...seen.keys()]) {
      if (!results.some((r) => `${r.exchangeId}:${r.symbol}` === k)) seen.delete(k);
    }
  }, [results]);

  // Refresh failures surface as a toast (F4.3): the refresh runs in
  // the background and only reports through the venue status, so the
  // error is toasted when it first appears (or changes) — a
  // persisting error is never re-toasted, and success stays silent.
  // The first observation is recorded without toasting.
  const prevVenueErrors = useRef<Map<string, string | null> | null>(null);
  useEffect(() => {
    if (!venueStatus) return;
    const cur = new Map(
      venueStatus.venues.map((v) => [v.exchangeId, v.error ?? null] as const),
    );
    const prev = prevVenueErrors.current;
    prevVenueErrors.current = cur;
    if (prev == null) return;
    for (const [id, err] of cur) {
      if (err != null && prev.get(id) !== err) {
        toast.error(`${venueName(id)} 拉取失败：${err}`);
      }
    }
  }, [venueStatus, venueName]);

  // Toolbar (F1): view toggle + exchange filter + refresh on the
  // left (F4.4: the catalog-update status on the right is gone). The
  // dropdown menus render through a fixed portal, so the toolbar
  // never clips them.
  const register = useShellActions((s) => s.register);
  const unregister = useShellActions((s) => s.unregister);
  useLayoutEffect(() => {
    register("data", {
      actions: (
        <>
          <SegmentedControl
            ariaLabel="视图切换"
            options={[
              { value: "signals", label: "自定义信号" },
              { value: "search", label: "标的查询" },
            ]}
            value={view}
            onChange={(v) => setView(v as DataView)}
          />
          <div className="w-44 shrink-0">
            <MultiSelectDropdown
              options={(schemas ?? []).map((e) => ({ value: e.id, label: e.name }))}
              values={checked}
              onChange={setChecked}
              label="交易所"
            />
          </div>
          <RefreshButton onClick={onRefresh} loading={refreshing} />
        </>
      ),
    });
    return () => unregister("data");
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [register, unregister, view, refreshing, onRefresh, setView, schemas, checked, setChecked]);

  return (
    <div>
      {view === "signals" ? (
        <PageState empty>自定义信号尚未接入 — 下一阶段将支持信号规则创建。</PageState>
      ) : (
        <>
          {/* Search row (F4.4): submit-style — typing only edits the
              input; Enter / the magnifier button submits. The button
              is the same height as the input (h-8) and disabled while
              the input is empty. */}
          <div className="flex items-center gap-2 mb-3">
            <Button
              onClick={searchNow}
              disabled={!query.trim()}
              aria-label="搜索"
              className="shrink-0 px-2.5"
            >
              <svg
                className="h-4 w-4"
                viewBox="0 0 16 16"
                fill="none"
                aria-hidden="true"
              >
                <circle cx="7" cy="7" r="4.5" stroke="currentColor" strokeWidth="1.5" />
                <path d="M10.5 10.5 14 14" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" />
              </svg>
            </Button>
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

          {!searched && !searching ? (
            <PageState empty>输入 BTC、ETH、SOL、NVDA 等标的名称，查询 6 所实时对比数据。</PageState>
          ) : searching && results.length === 0 ? (
            <PageState loading loadingText="搜索中…" />
          ) : notReady ? (
            <PageState empty>目录未就绪，正在拉取…</PageState>
          ) : results.length === 0 ? (
            <PageState empty>未找到包含「{submittedQuery.trim()}」的标的。</PageState>
          ) : (
            <div className="mb-6">
              {/* Reserved line: searching must not push the table down
                  when it appears (F4.1 jitter fix). */}
              <div className="text-xs text-ds-text-3 mb-2 h-4">
                {searching ? "搜索中…" : ""}
              </div>
              <DataTable headers={HEADERS} minWidth={680} colWidths={COL_WIDTHS} aligns={[...COL_ALIGNS]} compact dense>
                {marketRowNodes({
                  rows: results,
                  books,
                  expandedKey,
                  onToggle: (key) => setExpandedKey((cur) => (cur === key ? null : key)),
                  venueName,
                  firstSeenAt: firstSeenRef.current,
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
