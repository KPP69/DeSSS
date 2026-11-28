import { useCallback, useEffect, useRef, useState } from "react";
import { api, onCatalogUpdated } from "../lib/tauri";
import type { CatalogStatus, MarketInfo } from "../lib/types";
import { useDataUiStore } from "../store/useDataUiStore";
import { useInvoke } from "./useInvoke";

/** Minimum quote-refresh interval per venue (PR-B1): Lighter / RH
 * 15s, Hyperliquid / Entropy / AsterDEX / Arcus 5s. Unknown ids
 * default to 5s. */
const QUOTE_INTERVAL_MS: Record<string, number> = {
  hyperliquid: 5_000,
  entropy: 5_000,
  lighter: 15_000,
  lighter_robinhood: 15_000,
  asterdex: 5_000,
  arcus: 5_000,
};

/**
 * Data page search state (Phase 2, I11), moved out of DataPage
 * unchanged in behavior:
 *
 * - Search is zero-network (PR-0.3): it reads the SQLite-backed
 *   in-memory catalog only. Prices come from WS frames; the page
 *   re-runs the same local search every 2s while the user stays on
 *   标的查询 with a submitted query so the rows tick.
 * - Submit-style search (F4.4): typing only edits the input value —
 *   no request, no subscription change, no touch to the results on
 *   screen. Enter / the search button writes the submitted query to
 *   the store and searches at once; the 2s tick, the 5s quote
 *   refresh and the stream reconciliation all read the submitted
 *   query. An empty submitted query clears the results and
 *   unsubscribes everything.
 * - One search, two modes. "fresh" is a new query / selection /
 *   manual refresh: it resets the expanded row and, on error, shows
 *   the empty state. "tick" is the 2s live refresh: it only swaps in
 *   newer rows — it never collapses the expanded row and never blanks
 *   the screen on a transient failure. Both modes share the request
 *   id: a response whose id is no longer current is discarded, so an
 *   older request can never overwrite newer state or a cleared state.
 * - The catalog status comes from `useInvoke` (this hook's one
 *   `useInvoke` use); a `catalog://updated` event reloads it and
 *   re-runs the current search once in tick mode.
 * - `query` / `submittedQuery` / `checked` live in useDataUiStore,
 *   so switching pages never loses the search. The stream set is
 *   reconciled with the rows on screen; clearing the submitted query
 *   or leaving the page unsubscribes everything at once.
 */
export function useMarketSearch() {
  const view = useDataUiStore((s) => s.view);
  const query = useDataUiStore((s) => s.query);
  const setQuery = useDataUiStore((s) => s.setQuery);
  const submittedQuery = useDataUiStore((s) => s.submittedQuery);
  const setSubmittedQuery = useDataUiStore((s) => s.setSubmittedQuery);
  const checked = useDataUiStore((s) => s.checked);
  const setChecked = useDataUiStore((s) => s.setChecked);
  const [results, setResults] = useState<MarketInfo[]>([]);
  const [searched, setSearched] = useState(false);
  const [searching, setSearching] = useState(false);
  const [notReady, setNotReady] = useState(false);
  const [refreshing, setRefreshing] = useState(false);
  const [cacheUpdatedAt, setCacheUpdatedAt] = useState<number | null>(null);
  const [expandedKey, setExpandedKey] = useState<string | null>(null);
  const reqId = useRef(0);

  const status = useInvoke(() => api.getCatalogStatus(), []);
  const venueStatus: CatalogStatus | null = status.data;

  const runSearch = useCallback(
    async (q: string, exchangeIds: string[], mode: "fresh" | "tick" = "fresh") => {
      const trimmed = q.trim();
      if (!trimmed) {
        // Invalidate any in-flight search FIRST, then clear: its late
        // response must not resurrect the cleared state.
        reqId.current += 1;
        setResults([]);
        setSearched(false);
        setSearching(false);
        return;
      }
      const id = ++reqId.current;
      setSearching(true);
      try {
        const rows = await api.searchMarketInfo(trimmed, exchangeIds);
        if (reqId.current === id) {
          setResults(rows);
          setSearched(true);
          setNotReady(false);
          if (mode === "fresh") setExpandedKey(null);
          if (rows.length > 0) {
            setCacheUpdatedAt(Math.max(...rows.map((r) => r.updatedAt)));
          }
        }
      } catch (e) {
        if (reqId.current === id && mode === "fresh") {
          const msg = e instanceof Error ? e.message : String(e);
          setResults([]);
          setSearched(true);
          setNotReady(msg.includes("目录未就绪"));
        }
      } finally {
        if (reqId.current === id) setSearching(false);
      }
    },
    [],
  );

  // Submit-style search (F4.4): typing only edits the input — this
  // effect does not exist any more. A selection change re-runs the
  // submitted query at once (tick mode: never collapses the expanded
  // row, never blanks on failure); an empty submitted query clears
  // immediately and invalidates any search still in flight.
  useEffect(() => {
    if (!submittedQuery.trim()) {
      void runSearch(submittedQuery, checked);
      return;
    }
    void runSearch(submittedQuery, checked, "tick");
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [checked]);

  // Per-venue catalog status, reloaded on every background catalog
  // update. The same event re-runs the submitted search once (tick
  // mode: never collapses the expanded row, never blanks on failure).
  useEffect(() => {
    let unlisten: (() => void) | undefined;
    let cancelled = false;
    void onCatalogUpdated(() => {
      status.refresh();
      const s = useDataUiStore.getState();
      if (s.view === "search" && s.submittedQuery.trim()) {
        void runSearch(s.submittedQuery, s.checked, "tick");
      }
    }).then((u) => {
      if (cancelled) u();
      else unlisten = u;
    });
    return () => {
      cancelled = true;
      unlisten?.();
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [runSearch]);

  // Live watching, only while the user is on 标的查询 with a submitted
  // query: re-run the same lightweight search every 2s so the rows
  // tick. Clearing the submitted query or leaving the view clears the
  // timer at once — the background goes back to zero requests.
  useEffect(() => {
    if (view !== "search" || !submittedQuery.trim()) return;
    const t = setInterval(() => void runSearch(submittedQuery, checked, "tick"), 2000);
    return () => clearInterval(t);
  }, [view, submittedQuery, checked, runSearch]);

  // Batch quote refresh (PR-B1): one 5s tick for all venues; each
  // venue is requested only when its own minimum interval has passed
  // since its last request (Lighter / RH 15s, the rest 5s). The
  // last-request times live in a ref, so re-runs of this effect
  // (submitted-query / checked changes) never reset a venue's clock.
  // Same visibility condition as the 2s search tick, so leaving the
  // page stops the calls at once.
  const lastQuoteAt = useRef<Map<string, number>>(new Map());
  useEffect(() => {
    if (view !== "search" || !submittedQuery.trim()) return;
    const t = setInterval(() => {
      const now = Date.now();
      const due = checked.filter((id) => {
        const last = lastQuoteAt.current.get(id) ?? 0;
        return now - last >= (QUOTE_INTERVAL_MS[id] ?? 5_000);
      });
      if (due.length === 0) return;
      for (const id of due) lastQuoteAt.current.set(id, now);
      void api.refreshQuotes(due, false);
    }, 5000);
    return () => clearInterval(t);
  }, [view, submittedQuery, checked]);

  // Reconcile the realtime streams with the rows on screen: whenever
  // the hits change, the backend subscribes exactly those pairs —
  // every row of the submitted result, no viewport limit, no count
  // cap (F4.4). Clearing the submitted query or leaving the view
  // unsubscribes everything at once; the backend diffs the set, so
  // repeat calls with the same keys are no-ops.
  useEffect(() => {
    if (view !== "search" || !submittedQuery.trim()) {
      void api.setStreamSymbols([]);
      return;
    }
    void api.setStreamSymbols(
      results.map((r) => ({ exchangeId: r.exchangeId, symbol: r.symbol })),
    );
  }, [view, submittedQuery, results]);

  // Leaving the page stops every stream.
  useEffect(() => () => {
    void api.setStreamSymbols([]);
  }, []);

  /** Enter key / search button (F4.4): write the submitted query to
   * the store and search at once. An empty input clears the submitted
   * query, which clears the results and unsubscribes (see above). */
  const searchNow = useCallback(() => {
    const q = query.trim();
    setSubmittedQuery(q);
    void runSearch(q, checked);
  }, [query, checked, runSearch, setSubmittedQuery]);

  const onRefresh = useCallback(async () => {
    setRefreshing(true);
    try {
      // Force both the catalog (as before) and the quotes: the
      // button must show fresh numbers now, not at the next 5s tick.
      const r = await api.refreshMarketCatalog(checked);
      await api.refreshQuotes(checked, true);
      if (r.updatedAt > 0) setCacheUpdatedAt(r.updatedAt);
      status.refresh();
      if (submittedQuery.trim()) await runSearch(submittedQuery, checked);
    } finally {
      setRefreshing(false);
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [checked, submittedQuery, runSearch]);

  return {
    view,
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
  };
}
