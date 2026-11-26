import { useCallback, useEffect, useRef, useState } from "react";
import { api, onCatalogUpdated } from "../lib/tauri";
import type { CatalogStatus, MarketInfo } from "../lib/types";
import { useDataUiStore } from "../store/useDataUiStore";
import { useInvoke } from "./useInvoke";

/**
 * Data page search state (Phase 2, I11), moved out of DataPage
 * unchanged in behavior:
 *
 * - Search is zero-network (PR-0.3): it reads the SQLite-backed
 *   in-memory catalog only. Prices come from WS frames; the page
 *   re-runs the same local search every 2s while the user stays on
 *   标的查询 with a non-empty query so the rows tick.
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
 * - `query` / `checked` live in useDataUiStore, so switching pages
 *   never loses the search. The stream set is reconciled with the
 *   rows on screen; clearing the query or leaving the page
 *   unsubscribes everything at once.
 */
export function useMarketSearch() {
  const view = useDataUiStore((s) => s.view);
  const query = useDataUiStore((s) => s.query);
  const setQuery = useDataUiStore((s) => s.setQuery);
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
  // Pending debounce timer: Enter clears it first, then searches at once.
  const debounceRef = useRef<ReturnType<typeof setTimeout> | null>(null);

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

  // Debounced live search while typing; Enter searches immediately.
  // Re-runs when the checked exchanges change (same query, new selection).
  // An emptied query clears immediately (no debounce) and invalidates
  // any search still in flight.
  useEffect(() => {
    if (!query.trim()) {
      void runSearch(query, checked);
      return;
    }
    debounceRef.current = setTimeout(() => void runSearch(query, checked), 300);
    return () => {
      if (debounceRef.current) clearTimeout(debounceRef.current);
      debounceRef.current = null;
    };
  }, [query, checked, runSearch]);

  // Per-venue catalog status, reloaded on every background catalog
  // update. The same event re-runs the current search once (tick
  // mode: never collapses the expanded row, never blanks on failure).
  useEffect(() => {
    let unlisten: (() => void) | undefined;
    let cancelled = false;
    void onCatalogUpdated(() => {
      status.refresh();
      const s = useDataUiStore.getState();
      if (s.view === "search" && s.query.trim()) {
        void runSearch(s.query, s.checked, "tick");
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

  // Live watching, only while the user is on 标的查询 with a non-empty
  // query: re-run the same lightweight search every 2s so the rows tick.
  // Clearing the query or leaving the view clears the timer at once —
  // the background goes back to zero requests.
  useEffect(() => {
    if (view !== "search" || !query.trim()) return;
    const t = setInterval(() => void runSearch(query, checked, "tick"), 2000);
    return () => clearInterval(t);
  }, [view, query, checked, runSearch]);

  // Reconcile the realtime streams with the rows on screen: whenever the
  // hits change, the backend subscribes exactly those pairs. Clearing
  // the query or leaving the view unsubscribes everything at once; the
  // backend diffs the set, so repeat calls with the same keys are no-ops.
  useEffect(() => {
    if (view !== "search" || !query.trim()) {
      void api.setStreamSymbols([]);
      return;
    }
    void api.setStreamSymbols(
      results.map((r) => ({ exchangeId: r.exchangeId, symbol: r.symbol })),
    );
  }, [view, query, results]);

  // Leaving the page stops every stream.
  useEffect(() => () => {
    void api.setStreamSymbols([]);
  }, []);

  /** Enter key: cancel the pending debounce, search at once. */
  const searchNow = useCallback(() => {
    if (debounceRef.current) {
      clearTimeout(debounceRef.current);
      debounceRef.current = null;
    }
    void runSearch(query, checked);
  }, [query, checked, runSearch]);

  const onRefresh = useCallback(async () => {
    setRefreshing(true);
    try {
      const r = await api.refreshMarketCatalog(checked);
      if (r.updatedAt > 0) setCacheUpdatedAt(r.updatedAt);
      status.refresh();
      if (query.trim()) await runSearch(query, checked);
    } finally {
      setRefreshing(false);
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [checked, query, runSearch]);

  return {
    view,
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
  };
}
